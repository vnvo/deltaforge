//! A window's bounded prefix (`docs/design/mysql-mass-invalidation.md`,
//! "Bounded memory from capture to commit"): the window's tables are
//! accumulated in first-touch order, each with its complete per-table cost,
//! and the prefix closes at the first table that would take the plan over
//! any cap. Every later step works on that prefix only; the rest is decided
//! by later windows. A table no window could ever take alone is invalid and
//! fails closed when it is reached, never cut again and again.

/// `W`: a plan's encoded payload, everything accumulated for its tables
/// from capture through commit.
pub(crate) const PLAN_BYTES: u64 = 64 * 1024 * 1024;
/// `T`: one table's aggregate; a valid table always fits a plan alone.
pub(crate) const TABLE_BYTES: u64 = PLAN_BYTES;
/// Read items of a plan: plan reads of both phases, precondition checks and
/// capture-id checks.
pub(crate) const PLAN_READ_ITEMS: u64 = 16_384;
/// The parameter payload of those reads.
pub(crate) const PLAN_READ_KEY_BYTES: u64 = 4 * 1024 * 1024;
/// Write ops of a plan (a table writes at most 7).
pub(crate) const PLAN_WRITE_OPS: u64 = 16_384;

/// One table's complete contribution to a plan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Cost {
    pub bytes: u64,
    pub read_items: u64,
    pub read_key_bytes: u64,
    pub write_ops: u64,
}

impl Cost {
    fn plus(self, o: Cost) -> Cost {
        Cost {
            bytes: self.bytes + o.bytes,
            read_items: self.read_items + o.read_items,
            read_key_bytes: self.read_key_bytes + o.read_key_bytes,
            write_ops: self.write_ops + o.write_ops,
        }
    }
}

/// The plan caps and the per-table limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Caps {
    pub plan: Cost,
    pub table_bytes: u64,
}

impl Caps {
    /// The fixed caps, or the ones a test set
    /// ([`crate::stream_probe::set_window_caps`]).
    pub(crate) fn current() -> Self {
        crate::stream_probe::window_caps().map_or(
            Self {
                plan: Cost {
                    bytes: PLAN_BYTES,
                    read_items: PLAN_READ_ITEMS,
                    read_key_bytes: PLAN_READ_KEY_BYTES,
                    write_ops: PLAN_WRITE_OPS,
                },
                table_bytes: TABLE_BYTES,
            },
            |(plan_bytes, read_items, table_bytes)| Self {
                plan: Cost {
                    bytes: plan_bytes,
                    read_items,
                    read_key_bytes: PLAN_READ_KEY_BYTES,
                    write_ops: PLAN_WRITE_OPS,
                },
                table_bytes,
            },
        )
    }
}

/// A table no plan can take: it halts the source (operator action).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Invalid {
    /// Its aggregate exceeds the per-table limit.
    StateTooLarge { bytes: u64, limit: u64 },
    /// Its read items (or their keys) exceed a plan's.
    RegistryTooLarge { read_items: u64, limit: u64 },
}

/// The prefix being accumulated.
#[derive(Debug)]
pub(crate) struct Prefix {
    caps: Caps,
    total: Cost,
    tables: usize,
    closed: bool,
}

impl Prefix {
    pub(crate) fn new(caps: Caps) -> Self {
        Self {
            caps,
            total: Cost::default(),
            tables: 0,
            closed: false,
        }
    }

    /// Offer the next table (first-touch order). `Ok(true)`: it is in the
    /// prefix. `Ok(false)`: the prefix closed before it (it and every later
    /// table are deferred). `Err`: the table is invalid.
    pub(crate) fn offer(&mut self, cost: Cost) -> Result<bool, Invalid> {
        if self.closed {
            return Ok(false);
        }
        if cost.bytes > self.caps.table_bytes {
            return Err(Invalid::StateTooLarge {
                bytes: cost.bytes,
                limit: self.caps.table_bytes,
            });
        }
        let p = self.caps.plan;
        if cost.read_items > p.read_items
            || cost.read_key_bytes > p.read_key_bytes
        {
            return Err(Invalid::RegistryTooLarge {
                read_items: cost.read_items,
                limit: p.read_items,
            });
        }
        let next = self.total.plus(cost);
        let fits = next.bytes <= p.bytes
            && next.read_items <= p.read_items
            && next.read_key_bytes <= p.read_key_bytes
            && next.write_ops <= p.write_ops;
        if !fits {
            self.closed = true;
            return Ok(false);
        }
        self.total = next;
        self.tables += 1;
        Ok(true)
    }

    /// The tables in the prefix.
    #[cfg(test)]
    pub(crate) fn tables(&self) -> usize {
        self.tables
    }

    /// What the prefix holds.
    #[cfg(test)]
    pub(crate) fn total(&self) -> Cost {
        self.total
    }
}

/// What a table writes besides its registration's entry: its index and
/// version entries, its activation record and its share of the commit
/// outcome (a conservative fixed allowance).
const TABLE_WRITE_OVERHEAD: u64 = 4096;
/// A table's write ops: a registration's (at most 6) and its activation
/// record.
const TABLE_WRITE_OPS: u64 = 7;

/// One table's complete cost, from its encoded captured schema, its
/// timeline and the source's other lineages (`key_bytes`: its registry key).
/// The schema is held three times (as captured, as its registration's
/// request and as its encoded entry); every record and barrier of its
/// timeline counts with its encoding.
pub(crate) fn table_cost(
    schema_bytes: u64,
    timeline: &super::mysql_selection::Timeline,
    other_lineages: u64,
    key_bytes: u64,
) -> anyhow::Result<Cost> {
    let mut timeline_bytes = 0u64;
    for s in timeline.records.iter().chain(&timeline.barriers) {
        timeline_bytes += (super::mysql_activation::encode_record(&s.record)?
            .len()
            + s.capture_id.len()) as u64;
    }
    // Plan reads: 7 in the first phase, at most 8 (two per existing entry)
    // and one per other lineage in the second; preconditions: 8, one per
    // other lineage and one per barrier stream; 2 capture-id checks.
    let barrier_heads = timeline.barrier_heads.len() as u64;
    let read_items =
        7 + 8 + other_lineages + 8 + other_lineages + barrier_heads + 2;
    Ok(Cost {
        bytes: 3 * schema_bytes + timeline_bytes + TABLE_WRITE_OVERHEAD,
        read_items,
        read_key_bytes: read_items * (key_bytes + 64),
        write_ops: TABLE_WRITE_OPS,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps() -> Caps {
        Caps {
            plan: Cost {
                bytes: 100,
                read_items: 50,
                read_key_bytes: 1000,
                write_ops: 20,
            },
            table_bytes: 100,
        }
    }

    fn cost(bytes: u64, read_items: u64, write_ops: u64) -> Cost {
        Cost {
            bytes,
            read_items,
            read_key_bytes: read_items * 10,
            write_ops,
        }
    }

    /// The prefix is the longest run of tables, in the order offered, whose
    /// totals fit every cap; the first table that does not fit closes it,
    /// and no later table enters, even one that would fit.
    #[test]
    fn the_prefix_closes_at_the_first_table_over_any_cap() {
        for (costs, admitted) in [
            // Bytes.
            (vec![cost(40, 1, 1), cost(40, 1, 1), cost(40, 1, 1)], 2),
            // Read items.
            (vec![cost(1, 20, 1), cost(1, 20, 1), cost(1, 20, 1)], 2),
            // Write ops.
            (vec![cost(1, 1, 7), cost(1, 1, 7), cost(1, 1, 7)], 2),
            // A small table after the close stays out.
            (vec![cost(90, 1, 1), cost(20, 1, 1), cost(1, 1, 1)], 1),
            (vec![cost(1, 1, 1); 5], 5),
        ] {
            let mut p = Prefix::new(caps());
            let took: Vec<bool> =
                costs.iter().map(|c| p.offer(*c).unwrap()).collect();
            assert_eq!(p.tables(), admitted, "{costs:?}");
            assert!(took[..admitted].iter().all(|t| *t));
            assert!(took[admitted..].iter().all(|t| !*t));
        }
    }

    /// A valid table always fits a plan alone: the first table offered is
    /// always taken, so every window makes progress.
    #[test]
    fn a_valid_first_table_is_always_taken() {
        let mut p = Prefix::new(caps());
        assert_eq!(p.offer(cost(100, 50, 7)), Ok(true));
        assert_eq!(p.total(), cost(100, 50, 7));
    }

    /// A table over the per-table limit or the read-item cap is invalid
    /// when reached, at any rank; one past the close is never examined.
    #[test]
    fn an_invalid_table_fails_when_reached() {
        let mut p = Prefix::new(caps());
        assert_eq!(
            p.offer(cost(101, 1, 1)),
            Err(Invalid::StateTooLarge {
                bytes: 101,
                limit: 100
            })
        );
        let mut p = Prefix::new(caps());
        assert_eq!(p.offer(cost(1, 1, 1)), Ok(true));
        assert_eq!(
            p.offer(cost(1, 51, 1)),
            Err(Invalid::RegistryTooLarge {
                read_items: 51,
                limit: 50
            })
        );
        let mut p = Prefix::new(caps());
        assert_eq!(p.offer(cost(90, 1, 1)), Ok(true));
        assert_eq!(p.offer(cost(20, 1, 1)), Ok(false));
        assert_eq!(p.offer(cost(1000, 1, 1)), Ok(false));
    }
}
