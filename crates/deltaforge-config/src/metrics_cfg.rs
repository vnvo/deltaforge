use serde::{Deserialize, Serialize};

/// Pipeline metrics configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsCfg {
    /// Per-table metric detail (off by default).
    #[serde(default)]
    pub per_table: PerTableMetricsCfg,
}

/// Opt-in per-table metric labels.
///
/// Off (the default), no metric carries a `table` label, so the series
/// count does not depend on the number of tables. On, the first
/// `max_tables` distinct tables the pipeline reports keep their own series
/// (`table_scope="exact"`) for the life of the pipeline process; every
/// other table shares one overflow series (`table="__other__"`,
/// `table_scope="overflow"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerTableMetricsCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Tables admitted to exact series.
    #[serde(default = "default_max_tables")]
    pub max_tables: u32,
    /// A table's per-table lag series is removed after this many seconds
    /// without an event for it.
    #[serde(default = "default_lag_idle_secs")]
    pub lag_idle_secs: u32,
}

pub const PER_TABLE_MAX_TABLES: std::ops::RangeInclusive<u32> = 1..=10_000;
pub const PER_TABLE_LAG_IDLE_SECS: std::ops::RangeInclusive<u32> = 10..=86_400;

fn default_max_tables() -> u32 {
    100
}

fn default_lag_idle_secs() -> u32 {
    300
}

impl Default for PerTableMetricsCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            max_tables: default_max_tables(),
            lag_idle_secs: default_lag_idle_secs(),
        }
    }
}

impl PerTableMetricsCfg {
    /// Reject out-of-range bounds (checked even when disabled, so enabling
    /// later never surfaces a stale invalid value).
    pub fn validate(&self) -> Result<(), String> {
        if !PER_TABLE_MAX_TABLES.contains(&self.max_tables) {
            return Err(format!(
                "metrics.per_table.max_tables must be in {}..={}, got {}",
                PER_TABLE_MAX_TABLES.start(),
                PER_TABLE_MAX_TABLES.end(),
                self.max_tables
            ));
        }
        if !PER_TABLE_LAG_IDLE_SECS.contains(&self.lag_idle_secs) {
            return Err(format!(
                "metrics.per_table.lag_idle_secs must be in {}..={}, got {}",
                PER_TABLE_LAG_IDLE_SECS.start(),
                PER_TABLE_LAG_IDLE_SECS.end(),
                self.lag_idle_secs
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_off_and_valid() {
        let cfg: MetricsCfg = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg, MetricsCfg::default());
        assert!(!cfg.per_table.enabled);
        assert_eq!(
            (cfg.per_table.max_tables, cfg.per_table.lag_idle_secs),
            (100, 300)
        );
        cfg.per_table.validate().unwrap();
    }

    #[test]
    fn bounds_are_validated() {
        let at = |max_tables, lag_idle_secs| PerTableMetricsCfg {
            enabled: true,
            max_tables,
            lag_idle_secs,
        };
        at(1, 10).validate().unwrap();
        at(10_000, 86_400).validate().unwrap();
        assert!(at(0, 300).validate().unwrap_err().contains("max_tables"));
        assert!(at(10_001, 300).validate().is_err());
        assert!(at(100, 9).validate().unwrap_err().contains("lag_idle_secs"));
        assert!(at(100, 86_401).validate().is_err());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(
            serde_yaml::from_str::<MetricsCfg>("per_table: {enable: true}")
                .is_err()
        );
    }
}
