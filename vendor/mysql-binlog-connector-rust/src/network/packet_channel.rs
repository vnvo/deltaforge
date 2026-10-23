#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;

use std::{
    io::{Cursor, Write},
    time::Duration,
};

use async_std::{future::timeout, net::TcpStream, prelude::*};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::{trace, warn};

use crate::binlog_error::BinlogError;

const MAX_PACKET_LENGTH: usize = 16777215;

/// DeltaForge patch: the size of one read from the socket.
const READ_CHUNK: usize = 64 * 1024;

pub struct PacketChannel {
    stream: TcpStream,
    timeout_secs: u64,
    /// DeltaForge patch: bytes read from the socket and not yet returned as
    /// a packet. They live here, not in a read's future, so a read that is
    /// cancelled (dropped before it completes) loses nothing: the next read
    /// continues the same packet. See [`Self::read`].
    pending: Vec<u8>,
    /// DeltaForge patch: where the next packet starts in `pending` (the
    /// bytes before it were returned; compacted now and then).
    head: usize,
    /// DeltaForge patch: the buffer one socket read fills.
    scratch: Vec<u8>,
}

pub struct KeepAliveConfig {
    pub keepidle_secs: u64,
    pub keepintvl_secs: u64,
}

impl PacketChannel {
    pub async fn new(
        ip: &str,
        port: &str,
        timeout_secs: u64,
        keepalive_config: &Option<KeepAliveConfig>,
    ) -> Result<Self, BinlogError> {
        let addr = format!("{}:{}", ip, port);
        let stream =
            match timeout(Duration::from_secs(timeout_secs), TcpStream::connect(&addr)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => return Err(BinlogError::from(e)),
                Err(_) => {
                    return Err(BinlogError::ConnectError(format!(
                        "Connection timeout after {} seconds while connecting to {}",
                        timeout_secs, addr
                    )))
                }
            };

        if let Some(config) = keepalive_config {
            Self::configure_keepalive(&stream, config)?;
        }

        Ok(Self {
            stream,
            timeout_secs,
            pending: Vec::new(),
            head: 0,
            scratch: vec![0u8; READ_CHUNK],
        })
    }

    /// Configure TCP keepalive settings for the stream
    /// This is safe because:
    /// 1. We only borrow the stream temporarily
    /// 2. set_tcp_keepalive is a fast syscall (setsockopt) that doesn't block
    /// 3. Keepalive is handled by the kernel, doesn't affect async operations
    fn configure_keepalive(
        stream: &TcpStream,
        config: &KeepAliveConfig,
    ) -> Result<(), BinlogError> {
        if config.keepidle_secs == 0 || config.keepintvl_secs == 0 {
            return Ok(());
        }

        #[cfg(unix)]
        {
            use socket2::{SockRef, TcpKeepalive};
            use std::os::unix::io::BorrowedFd;

            let raw_fd = stream.as_raw_fd();
            let borrowed_fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
            let socket_ref = SockRef::from(&borrowed_fd);

            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(config.keepidle_secs))
                .with_interval(Duration::from_secs(config.keepintvl_secs));

            socket_ref
                .set_tcp_keepalive(&keepalive)
                .map_err(BinlogError::IoError)?;
        }

        #[cfg(windows)]
        {
            use socket2::{SockRef, TcpKeepalive};
            use std::os::windows::io::BorrowedSocket;

            let raw_socket = stream.as_raw_socket();
            let borrowed_socket = unsafe { BorrowedSocket::borrow_raw(raw_socket) };
            let socket_ref = SockRef::from(&borrowed_socket);

            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(config.keepidle_secs))
                .with_interval(Duration::from_secs(config.keepintvl_secs));

            socket_ref
                .set_tcp_keepalive(&keepalive)
                .map_err(|e| BinlogError::IoError(e))?;
        }

        Ok(())
    }

    /// DeltaForge patch: arm an abortive close. With `SO_LINGER` set to zero,
    /// dropping the stream resets the connection (RST) instead of a graceful
    /// close, so the server's next write to it (a binlog dump's heartbeat)
    /// fails at once and its dump thread ends. Acts on this connection only.
    pub fn abort_on_drop(&self) -> Result<(), BinlogError> {
        #[cfg(unix)]
        {
            use socket2::SockRef;
            use std::os::unix::io::BorrowedFd;

            let raw_fd = self.stream.as_raw_fd();
            let borrowed_fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
            SockRef::from(&borrowed_fd)
                .set_linger(Some(Duration::ZERO))
                .map_err(BinlogError::IoError)?;
        }
        #[cfg(windows)]
        {
            use socket2::SockRef;
            use std::os::windows::io::BorrowedSocket;

            let raw_socket = self.stream.as_raw_socket();
            let borrowed_socket = unsafe { BorrowedSocket::borrow_raw(raw_socket) };
            SockRef::from(&borrowed_socket)
                .set_linger(Some(Duration::ZERO))
                .map_err(BinlogError::IoError)?;
        }
        Ok(())
    }

    /// DeltaForge patch: the socket's `SO_LINGER` (to verify
    /// [`Self::abort_on_drop`] is armed).
    pub fn linger(&self) -> Result<Option<Duration>, BinlogError> {
        #[cfg(unix)]
        {
            use socket2::SockRef;
            use std::os::unix::io::BorrowedFd;

            let raw_fd = self.stream.as_raw_fd();
            let borrowed_fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
            SockRef::from(&borrowed_fd)
                .linger()
                .map_err(BinlogError::IoError)
        }
        #[cfg(windows)]
        {
            use socket2::SockRef;
            use std::os::windows::io::BorrowedSocket;

            let raw_socket = self.stream.as_raw_socket();
            let borrowed_socket = unsafe { BorrowedSocket::borrow_raw(raw_socket) };
            SockRef::from(&borrowed_socket)
                .linger()
                .map_err(BinlogError::IoError)
        }
    }

    pub async fn close(&self) -> Result<(), BinlogError> {
        self.stream.shutdown(std::net::Shutdown::Both)?;
        Ok(())
    }

    pub async fn write(&mut self, buf: &[u8], sequence: u8) -> Result<(), BinlogError> {
        let mut wtr = Vec::new();
        wtr.write_u24::<LittleEndian>(buf.len() as u32)?;
        wtr.write_u8(sequence)?;
        Write::write(&mut wtr, buf)?;
        self.stream.write_all(&wtr).await?;
        Ok(())
    }

    /// DeltaForge patch: a complete packet at `head` in `pending`: its
    /// payload (chunks of a large packet joined), its first sequence number,
    /// and how many bytes it takes; `None` until all of it was read.
    fn complete_packet(&self) -> Result<Option<(Vec<u8>, u8, usize)>, BinlogError> {
        let mut at = self.head;
        let mut sequence = None;
        let mut parts = Vec::new();
        loop {
            if self.pending.len() < at + 4 {
                return Ok(None);
            }
            let mut rdr = Cursor::new(&self.pending[at..at + 4]);
            let length = rdr.read_u24::<LittleEndian>()? as usize;
            let seq = rdr.read_u8()?;
            sequence.get_or_insert(seq);
            if self.pending.len() < at + 4 + length {
                return Ok(None);
            }
            parts.push(at + 4..at + 4 + length);
            at += 4 + length;
            if length != MAX_PACKET_LENGTH {
                break;
            }
        }
        let mut buf = Vec::with_capacity(parts.iter().map(|r| r.len()).sum());
        for r in parts {
            buf.extend_from_slice(&self.pending[r]);
        }
        if buf.len() >= MAX_PACKET_LENGTH {
            trace!("Received big binlog data, full length: {}", buf.len());
        }
        Ok(Some((buf, sequence.unwrap_or(0), at - self.head)))
    }

    /// DeltaForge patch: cancel-safe. A packet is taken from `pending` only
    /// once it is complete; each socket read appends what it read before
    /// its future completes. Dropping this future at any await point
    /// therefore leaves every byte read so far in `pending`, and the next
    /// call returns the same packet. Each socket read is bounded by
    /// `timeout_secs`, as before.
    pub async fn read_with_sequece(&mut self) -> Result<(Vec<u8>, u8), BinlogError> {
        let wait_data_millis = 10;
        let max_zero_reads = self.timeout_secs * 1000 / wait_data_millis;
        let mut zero_reads = 0;
        loop {
            if let Some((buf, sequence, used)) = self.complete_packet()? {
                self.head += used;
                if self.head == self.pending.len() {
                    self.pending.clear();
                    self.head = 0;
                } else if self.head >= READ_CHUNK {
                    self.pending.drain(..self.head);
                    self.head = 0;
                }
                return Ok((buf, sequence));
            }
            match timeout(
                Duration::from_secs(self.timeout_secs),
                self.stream.read(&mut self.scratch),
            )
            .await
            {
                Ok(Ok(0)) => {
                    zero_reads += 1;
                    if zero_reads >= max_zero_reads {
                        return Err(BinlogError::UnexpectedData(format!(
                            "Too many zero-length reads. Read so far: {}",
                            self.pending.len() - self.head
                        )));
                    }
                    warn!(
                        "Stream reading binlog returns zero-length data, read so far: {}",
                        self.pending.len() - self.head
                    );
                    async_std::task::sleep(Duration::from_millis(wait_data_millis)).await;
                }
                Ok(Ok(n)) => {
                    zero_reads = 0;
                    self.pending.extend_from_slice(&self.scratch[..n]);
                    trace!(
                        "Stream reading binlog data, buffered: {}",
                        self.pending.len()
                    );
                }
                Ok(Err(e)) => return Err(BinlogError::from(e)),
                Err(_) => {
                    return Err(BinlogError::UnexpectedData(format!(
                        "Read binlog timeout after {}s, buffered: {}",
                        self.timeout_secs,
                        self.pending.len() - self.head
                    )));
                }
            }
        }
    }

    pub async fn read(&mut self) -> Result<Vec<u8>, BinlogError> {
        let (buf, _sequence) = Self::read_with_sequece(self).await?;
        Ok(buf)
    }
}

/// DeltaForge patch: reads are cancel-safe.
#[cfg(test)]
mod tests {
    use super::*;
    use async_std::net::TcpListener;

    fn packet(payload: &[u8], sequence: u8) -> Vec<u8> {
        let mut out = Vec::new();
        out.write_u24::<LittleEndian>(payload.len() as u32).unwrap();
        out.push(sequence);
        out.extend_from_slice(payload);
        out
    }

    /// After the peer's last part.
    #[derive(Clone, Copy)]
    enum End {
        Hold,
        Close,
    }

    /// A channel (read timeout `timeout_secs`) to a local peer that writes
    /// `parts`, pausing `pause` between them, then holds or closes.
    async fn peer_with(
        parts: Vec<Vec<u8>>,
        pause: Duration,
        end: End,
        timeout_secs: u64,
    ) -> PacketChannel {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        async_std::task::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            for p in parts {
                s.write_all(&p).await.unwrap();
                s.flush().await.unwrap();
                async_std::task::sleep(pause).await;
            }
            match end {
                End::Hold => async_std::task::sleep(Duration::from_secs(30)).await,
                End::Close => s.shutdown(std::net::Shutdown::Both).unwrap(),
            }
        });
        PacketChannel::new("127.0.0.1", &port.to_string(), timeout_secs, &None)
            .await
            .unwrap()
    }

    async fn peer(parts: Vec<Vec<u8>>) -> PacketChannel {
        peer_with(parts, Duration::from_millis(100), End::Hold, 5).await
    }

    /// Reads one packet, cancelling every attempt after `wait`; returns the
    /// packet and how many attempts were cancelled.
    async fn read_cancelling(ch: &mut PacketChannel, wait: Duration) -> ((Vec<u8>, u8), u32) {
        let mut cancelled = 0;
        loop {
            match timeout(wait, ch.read_with_sequece()).await {
                Ok(r) => return (r.unwrap(), cancelled),
                Err(_) => cancelled += 1,
            }
        }
    }

    /// A deterministic generator (no extra dependency).
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    /// A read cancelled after part of a packet's payload arrived loses
    /// nothing: the next read returns that packet whole, then the next one.
    #[test]
    fn a_cancelled_read_keeps_the_partial_packet() {
        async_std::task::block_on(async {
            let one = packet(b"the first packet", 1);
            let two = packet(b"second", 2);
            let (head, tail) = one.split_at(7);
            let mut ch = peer(vec![head.to_vec(), [tail, &two[..]].concat()]).await;
            // Cancelled while the packet is incomplete.
            let r = timeout(Duration::from_millis(50), ch.read()).await;
            assert!(r.is_err(), "the packet is not complete yet");
            assert_eq!(
                ch.read_with_sequece().await.unwrap(),
                (b"the first packet".to_vec(), 1)
            );
            assert_eq!(
                ch.read_with_sequece().await.unwrap(),
                (b"second".to_vec(), 2)
            );
        });
    }

    /// Cancelled with only part of the 4-byte header read.
    #[test]
    fn a_read_cancelled_inside_the_header_continues() {
        async_std::task::block_on(async {
            let one = packet(b"header split", 7);
            let mut ch = peer(vec![one[..2].to_vec(), one[2..].to_vec()]).await;
            let r = timeout(Duration::from_millis(50), ch.read()).await;
            assert!(r.is_err());
            assert_eq!(
                ch.read_with_sequece().await.unwrap(),
                (b"header split".to_vec(), 7)
            );
        });
    }

    /// Many cancellations across one packet, in its header and payload.
    #[test]
    fn repeated_cancellation_loses_nothing() {
        async_std::task::block_on(async {
            let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
            let wire = [packet(&payload, 9), packet(b"after", 10)].concat();
            // Cut inside the header (after 1 and 3 bytes) and the payload.
            let parts: Vec<Vec<u8>> = [0usize, 1, 3, 200, 400, 401, wire.len()]
                .windows(2)
                .map(|w| wire[w[0]..w[1]].to_vec())
                .collect();
            let mut ch = peer_with(parts, Duration::from_millis(60), End::Hold, 5).await;
            let ((buf, seq), cancelled) = read_cancelling(&mut ch, Duration::from_millis(15)).await;
            assert_eq!((buf, seq), (payload, 9));
            assert!(cancelled >= 4, "{cancelled} cancellations");
            assert_eq!(
                ch.read_with_sequece().await.unwrap(),
                (b"after".to_vec(), 10)
            );
        });
    }

    /// Packets arriving a byte at a time.
    #[test]
    fn fragmented_packets_are_read_whole() {
        async_std::task::block_on(async {
            let wire = [packet(b"one", 1), packet(b"", 2), packet(b"three", 3)].concat();
            let parts = wire.iter().map(|b| vec![*b]).collect();
            let mut ch = peer_with(parts, Duration::from_millis(2), End::Hold, 5).await;
            assert_eq!(ch.read_with_sequece().await.unwrap(), (b"one".to_vec(), 1));
            assert_eq!(ch.read_with_sequece().await.unwrap(), (Vec::new(), 2));
            assert_eq!(
                ch.read_with_sequece().await.unwrap(),
                (b"three".to_vec(), 3)
            );
        });
    }

    /// A peer that closes inside a packet: the read fails (never a short
    /// packet); complete packets before the close are still returned.
    #[test]
    fn eof_inside_a_packet_is_an_error() {
        async_std::task::block_on(async {
            let one = packet(b"complete", 1);
            let two = packet(b"cut short by the close", 2);
            let wire = [&one[..], &two[..10]].concat();
            let mut ch = peer_with(vec![wire], Duration::from_millis(10), End::Close, 1).await;
            assert_eq!(
                ch.read_with_sequece().await.unwrap(),
                (b"complete".to_vec(), 1)
            );
            assert!(ch.read().await.is_err());
        });
    }

    /// A long stream of packets of random sizes (some spanning several
    /// socket reads), sent in random fragments and read with random
    /// cancellations: every packet arrives once, exactly, in order.
    #[test]
    fn random_cancellation_neither_loses_nor_duplicates_bytes() {
        async_std::task::block_on(async {
            let mut rng = Lcg(42);
            let packets: Vec<Vec<u8>> = (0..300)
                .map(|i| {
                    let len = match rng.next(10) {
                        0 => 70_000 + rng.next(70_000),
                        _ => rng.next(300),
                    } as usize;
                    (0..len).map(|j| ((i * 31 + j) % 256) as u8).collect()
                })
                .collect();
            let wire: Vec<u8> = packets
                .iter()
                .enumerate()
                .flat_map(|(i, p)| packet(p, i as u8))
                .collect();
            let mut parts = Vec::new();
            let mut at = 0;
            while at < wire.len() {
                let n = (1 + rng.next(20_000) as usize).min(wire.len() - at);
                parts.push(wire[at..at + n].to_vec());
                at += n;
            }
            let mut ch = peer_with(parts, Duration::from_micros(300), End::Hold, 5).await;
            let mut cancelled = 0;
            for (i, p) in packets.iter().enumerate() {
                let wait = Duration::from_micros(50 + rng.next(2_000));
                let ((buf, seq), c) = read_cancelling(&mut ch, wait).await;
                cancelled += c;
                assert_eq!(seq, i as u8, "packet {i}");
                assert_eq!(&buf, p, "packet {i}");
            }
            assert!(cancelled > 0, "the run cancelled reads");
        });
    }

    /// A packet of the maximum length continues in the next chunk; they are
    /// joined, also when the chunks arrive in pieces.
    #[test]
    fn large_packets_are_joined_across_chunks() {
        async_std::task::block_on(async {
            let big: Vec<u8> = (0..MAX_PACKET_LENGTH).map(|i| i as u8).collect();
            let mut wire = packet(&big, 3);
            wire.extend(packet(b"tail", 4));
            wire.extend(packet(b"next", 5));
            let (a, b) = wire.split_at(MAX_PACKET_LENGTH / 2);
            let mut ch = peer(vec![a.to_vec(), b.to_vec()]).await;
            let r = timeout(Duration::from_millis(50), ch.read()).await;
            assert!(r.is_err());
            let (buf, seq) = ch.read_with_sequece().await.unwrap();
            assert_eq!(seq, 3);
            assert_eq!(buf.len(), MAX_PACKET_LENGTH + 4);
            assert_eq!(&buf[..MAX_PACKET_LENGTH], &big[..]);
            assert_eq!(&buf[MAX_PACKET_LENGTH..], b"tail");
            assert_eq!(ch.read().await.unwrap(), b"next".to_vec());
        });
    }
}
