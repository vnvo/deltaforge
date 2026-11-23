//! First registration of many tables ahead of a batch: the misses of
//! different subjects resolve concurrently, those of one subject in event
//! order, a process never has more than its limit of requests in flight to
//! one registry, and the encoded batch (bytes, errors) is what encoding it
//! in order alone gives.

// The support registry is not used here (this file has its own, with latency).
#[allow(dead_code)]
mod support;

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use deltaforge_core::NativeOpts;
use deltaforge_core::encoding::avro::{
    AvroEncoder, SourceSchemaProvider, SubjectStrategy, registry_requests,
};
use deltaforge_core::envelope::EnvelopeType;
use support::avro::{Provider, event};

/// What the mock registry saw: the subjects of its POSTs and their bodies
/// in arrival order, and the most requests it ever had in flight at once.
#[derive(Default)]
struct Seen {
    posts: Mutex<Vec<String>>,
    bodies: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

/// A schema registry that answers each POST after `latency` with an ID
/// derived from the subject (the same for any arrival order), failing with
/// HTTP 500 for a subject containing `fail`. One thread per connection.
fn registry(
    latency: Duration,
    fail: Option<&'static str>,
) -> (String, Arc<Seen>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Seen::default());
    let shared = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let _ = s.set_nodelay(true);
            let seen = shared.clone();
            std::thread::spawn(move || {
                loop {
                    let Some((head, body)) = read_request(&mut s) else {
                        return;
                    };
                    let path = head
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .to_string();
                    let subject = path
                        .strip_prefix("/subjects/")
                        .and_then(|p| p.strip_suffix("/versions"))
                        .unwrap_or("")
                        .to_string();
                    let now = seen.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    seen.peak.fetch_max(now, Ordering::SeqCst);
                    seen.posts.lock().unwrap().push(subject.clone());
                    seen.bodies.lock().unwrap().push(body);
                    std::thread::sleep(latency);
                    seen.in_flight.fetch_sub(1, Ordering::SeqCst);
                    let failing = fail.is_some_and(|f| subject.contains(f));
                    let (status, body) = if failing {
                        (
                            "500 Internal Server Error",
                            r#"{"error_code":50001,"message":"down"}"#
                                .to_string(),
                        )
                    } else {
                        let id = subject.bytes().fold(17u32, |h, b| {
                            h.wrapping_mul(31).wrapping_add(b as u32)
                        }) % 1_000_000
                            + 1;
                        ("200 OK", format!(r#"{{"id":{id}}}"#))
                    };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    if s.write_all(resp.as_bytes()).is_err() {
                        return;
                    }
                }
            });
        }
    });
    (url, seen)
}

/// One request's head and body; `None` at the connection's end.
fn read_request(s: &mut std::net::TcpStream) -> Option<(String, String)> {
    let mut buf = vec![0u8; 1 << 16];
    let mut read = 0;
    loop {
        let n = s.read(&mut buf[read..]).ok()?;
        if n == 0 {
            return None;
        }
        read += n;
        let text = String::from_utf8_lossy(&buf[..read]).to_string();
        if let Some(h) = text.find("\r\n\r\n") {
            let len = text[..h]
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if read >= h + 4 + len {
                return Some((
                    text[..h].to_string(),
                    text[h + 4..].to_string(),
                ));
            }
        }
    }
}

fn encoder(url: &str, strategy: SubjectStrategy) -> AvroEncoder {
    let provider: Arc<dyn SourceSchemaProvider> =
        Arc::new(Provider { extra: 3 });
    AvroEncoder::with_source_schemas(url, strategy, None, None, Some(provider))
        .unwrap()
}

/// Each event encoded in order, as a sink's batch loop does.
async fn encode_all(
    encoder: &AvroEncoder,
    topic: &str,
    events: &[deltaforge_core::Event],
) -> Vec<Result<Vec<u8>, String>> {
    let envelope = EnvelopeType::Native.build();
    let mut out = Vec::new();
    for e in events {
        let data = envelope.wrap(e, NativeOpts::default()).unwrap();
        out.push(
            encoder
                .encode_event(topic, e, &data)
                .await
                .map(|b| b.to_vec())
                .map_err(|e| e.to_string()),
        );
    }
    out
}

fn tables(n: usize, prefix: &str) -> Vec<deltaforge_core::Event> {
    (0..n)
        .map(|i| event(&format!("{prefix}{i:03}"), true))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tables_of_their_own_subjects_resolve_concurrently_to_the_same_bytes() {
    let events = tables(40, "c");
    let (url, seen) = registry(Duration::from_millis(30), None);
    let ahead = encoder(&url, SubjectStrategy::TopicRecordName);
    ahead.resolve_ahead(events.iter().map(|e| ("cdc", e))).await;
    let peak = seen.peak.load(Ordering::SeqCst);
    assert!(peak > 1, "registrations ran one at a time");
    assert!(peak <= registry_requests() as usize, "{peak} in flight");
    assert_eq!(seen.posts.lock().unwrap().len(), 40);
    // Every schema is cached now: encoding in order registers nothing more.
    let with = encode_all(&ahead, "cdc", &events).await;
    assert_eq!(seen.posts.lock().unwrap().len(), 40);
    // The bytes are those of encoding in order without resolving ahead.
    let (url, _) = registry(Duration::ZERO, None);
    let without = encode_all(
        &encoder(&url, SubjectStrategy::TopicRecordName),
        "cdc",
        &events,
    )
    .await;
    assert!(with.iter().all(Result::is_ok));
    assert_eq!(with, without);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tables_sharing_a_subject_resolve_in_event_order() {
    let events = tables(12, "o");
    let (url, seen) = registry(Duration::from_millis(5), None);
    let ahead = encoder(&url, SubjectStrategy::TopicName);
    ahead
        .resolve_ahead(events.iter().map(|e| ("orders", e)))
        .await;
    assert_eq!(
        seen.peak.load(Ordering::SeqCst),
        1,
        "one subject: one at a time"
    );
    // Each registration is the next table's schema: event order.
    let bodies = seen.bodies.lock().unwrap();
    assert_eq!(bodies.len(), 12);
    for (i, body) in bodies.iter().enumerate() {
        assert!(
            body.contains(&format!("o{i:03}")),
            "registration {i} out of order"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_resolve_fails_the_in_order_encode_as_before() {
    let events = tables(6, "f");
    let (url, _) = registry(Duration::ZERO, Some("f003"));
    let ahead = encoder(&url, SubjectStrategy::TopicRecordName);
    ahead.resolve_ahead(events.iter().map(|e| ("cdc", e))).await;
    let with = encode_all(&ahead, "cdc", &events).await;
    let (url, _) = registry(Duration::ZERO, Some("f003"));
    let without = encode_all(
        &encoder(&url, SubjectStrategy::TopicRecordName),
        "cdc",
        &events,
    )
    .await;
    assert_eq!(with, without);
    assert!(
        with[3].is_err() && with.iter().filter(|r| r.is_err()).count() == 1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encoders_of_one_registry_share_the_process_limit() {
    let (url, seen) = registry(Duration::from_millis(30), None);
    let (a, b) = (
        encoder(&url, SubjectStrategy::TopicRecordName),
        encoder(&url, SubjectStrategy::TopicRecordName),
    );
    let (ea, eb) = (tables(40, "a"), tables(40, "b"));
    tokio::join!(
        a.resolve_ahead(ea.iter().map(|e| ("one", e))),
        b.resolve_ahead(eb.iter().map(|e| ("two", e))),
    );
    assert_eq!(seen.posts.lock().unwrap().len(), 80);
    let peak = seen.peak.load(Ordering::SeqCst);
    assert!(
        peak <= registry_requests() as usize,
        "{peak} in flight from two encoders, limit {}",
        registry_requests()
    );
}

#[test]
fn the_limit_is_bounded_and_set_once() {
    use deltaforge_core::encoding::avro::configure_registry_requests;
    assert!(configure_registry_requests(0).is_err());
    assert!(configure_registry_requests(65).is_err());
    let set = registry_requests();
    assert!(configure_registry_requests(set).is_ok());
    let other = if set == 1 { 2 } else { 1 };
    assert!(configure_registry_requests(other).is_err());
}
