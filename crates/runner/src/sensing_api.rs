//! Sensing API - Schema sensing and drift detection endpoints.

use std::collections::HashMap;
use std::sync::Arc;

use rest_api::{
    CacheStats, ColumnDrift, DriftInfo, InferredField, InferredSchemaDetail,
    InferredSchemaInfo, PipelineAPIError, SensingController, TableCacheStats,
};
use schema_sensing::InferredSchemaType;
use serde_json::Value;

use crate::drift_detector::DriftEvent;
use crate::pipeline_manager::PipelineManager;

/// Newtype wrapper providing SensingController implementation.
#[derive(Clone)]
pub struct SensingApi(pub Arc<PipelineManager>);

impl SensingApi {
    pub fn new(manager: Arc<PipelineManager>) -> Self {
        Self(manager)
    }
}

#[async_trait::async_trait]
impl SensingController for SensingApi {
    async fn list_inferred(
        &self,
        pipeline: &str,
    ) -> Result<Vec<InferredSchemaInfo>, PipelineAPIError> {
        let sensor_state = self.0.get_sensor(pipeline)?;
        let guard = sensor_state.sensor().lock();

        // Use all_snapshots() which has all the info we need
        Ok(guard
            .all_snapshots()
            .into_iter()
            .map(|s| InferredSchemaInfo {
                table: s.table,
                fingerprint: s.fingerprint,
                sequence: s.sequence,
                event_count: s.event_count,
                stabilized: s.stabilized,
                first_seen: s.first_seen,
                last_seen: s.last_seen,
            })
            .collect())
    }

    async fn get_inferred(
        &self,
        pipeline: &str,
        table: &str,
    ) -> Result<InferredSchemaDetail, PipelineAPIError> {
        let sensor_state = self.0.get_sensor(pipeline)?;
        let guard = sensor_state.sensor().lock();

        let snapshot = guard.get_snapshot(table).ok_or_else(|| {
            PipelineAPIError::NotFound(format!("schema for table {}", table))
        })?;

        // Extract field info from the inferred schema
        let fields = extract_fields_from_schema(&snapshot.schema);

        Ok(InferredSchemaDetail {
            table: snapshot.table,
            fingerprint: snapshot.fingerprint,
            sequence: snapshot.sequence,
            event_count: snapshot.event_count,
            stabilized: snapshot.stabilized,
            fields,
            first_seen: snapshot.first_seen,
            last_seen: snapshot.last_seen,
        })
    }

    async fn export_json_schema(
        &self,
        pipeline: &str,
        table: &str,
    ) -> Result<Value, PipelineAPIError> {
        let sensor_state = self.0.get_sensor(pipeline)?;
        let guard = sensor_state.sensor().lock();

        let json_schema = guard.to_json_schema(table).ok_or_else(|| {
            PipelineAPIError::NotFound(format!("schema for table {}", table))
        })?;

        serde_json::to_value(json_schema)
            .map_err(|e| PipelineAPIError::Failed(e.into()))
    }

    async fn get_drift(
        &self,
        pipeline: &str,
    ) -> Result<Vec<DriftInfo>, PipelineAPIError> {
        let sensor_state = self.0.get_sensor(pipeline)?;
        let guard = sensor_state.drift_detector().lock();

        Ok(guard
            .all_summaries()
            .into_iter()
            .map(|summary| {
                // Get detailed drift events for this table
                let columns = guard
                    .tracker(&summary.table)
                    .map(|tracker| build_column_drift(tracker.drift_events()))
                    .unwrap_or_default();

                DriftInfo {
                    table: summary.table,
                    has_drift: summary.total_drift_events > 0,
                    columns,
                    events_analyzed: 0, // TODO: add total_events to DriftSummary
                    events_with_drift: summary.total_drift_events as u64,
                }
            })
            .collect())
    }

    async fn get_table_drift(
        &self,
        pipeline: &str,
        table: &str,
    ) -> Result<DriftInfo, PipelineAPIError> {
        let sensor_state = self.0.get_sensor(pipeline)?;
        let guard = sensor_state.drift_detector().lock();

        let tracker = guard.tracker(table).ok_or_else(|| {
            PipelineAPIError::NotFound(format!(
                "drift info for table {}",
                table
            ))
        })?;

        let summary = tracker.summary();
        let columns = build_column_drift(tracker.drift_events());

        Ok(DriftInfo {
            table: summary.table,
            has_drift: summary.total_drift_events > 0,
            columns,
            events_analyzed: 0,
            events_with_drift: summary.total_drift_events as u64,
        })
    }

    async fn get_cache_stats(
        &self,
        pipeline: &str,
    ) -> Result<CacheStats, PipelineAPIError> {
        let sensor_state = self.0.get_sensor(pipeline)?;
        let guard = sensor_state.sensor().lock();

        // Use all_cache_stats() which returns full statistics
        let stats = guard.all_cache_stats();

        let total_hits: u64 = stats.iter().map(|s| s.cache_hits).sum();
        let total_misses: u64 = stats.iter().map(|s| s.cache_misses).sum();
        let total = total_hits + total_misses;
        let hit_rate = if total > 0 {
            total_hits as f64 / total as f64
        } else {
            0.0
        };

        let tables = stats
            .into_iter()
            .map(|s| TableCacheStats {
                table: s.table,
                cached_structures: s.cached_structures,
                max_cache_size: s.max_cache_size,
                cache_hits: s.cache_hits,
                cache_misses: s.cache_misses,
            })
            .collect();

        Ok(CacheStats {
            tables,
            total_cache_hits: total_hits,
            total_cache_misses: total_misses,
            hit_rate,
        })
    }
}

// ============================================================================
// Schema Field Extraction
// ============================================================================

/// Extract field information from schema_analysis::Schema.
fn extract_fields_from_schema(
    schema: &InferredSchemaType,
) -> Vec<InferredField> {
    match schema {
        InferredSchemaType::Struct { fields, .. } => fields
            .iter()
            .map(|(name, field)| {
                let types = field
                    .schema
                    .as_ref()
                    .map(|s| vec![schema_type_name(s)])
                    .unwrap_or_default();

                let (array_element_types, nested_field_count) = field
                    .schema
                    .as_ref()
                    .map(extract_nested_info)
                    .unwrap_or((None, None));

                InferredField {
                    name: name.clone(),
                    types,
                    nullable: field.status.may_be_null,
                    optional: field.status.may_be_missing,
                    array_element_types,
                    nested_field_count,
                }
            })
            .collect(),
        _ => vec![],
    }
}

/// Get human-readable type name for a schema.
fn schema_type_name(schema: &InferredSchemaType) -> String {
    match schema {
        InferredSchemaType::Null(_) => "null".to_string(),
        InferredSchemaType::Boolean(_) => "boolean".to_string(),
        InferredSchemaType::Integer(_) => "integer".to_string(),
        InferredSchemaType::Float(_) => "number".to_string(),
        InferredSchemaType::String(_) => "string".to_string(),
        InferredSchemaType::Bytes(_) => "bytes".to_string(),
        InferredSchemaType::Sequence { .. } => "array".to_string(),
        InferredSchemaType::Struct { .. } => "object".to_string(),
        InferredSchemaType::Union { variants } => {
            let types: Vec<_> = variants.iter().map(schema_type_name).collect();
            types.join(" | ")
        }
    }
}

/// Extract array element types and nested field count.
fn extract_nested_info(
    schema: &InferredSchemaType,
) -> (Option<Vec<String>>, Option<usize>) {
    match schema {
        InferredSchemaType::Sequence { field, .. } => {
            let element_types =
                field.schema.as_ref().map(|s| vec![schema_type_name(s)]);
            (element_types, None)
        }
        InferredSchemaType::Struct { fields, .. } => (None, Some(fields.len())),
        _ => (None, None),
    }
}

// ============================================================================
// Drift Column Aggregation
// ============================================================================

/// Build ColumnDrift entries from DriftEvents, grouped by column.
fn build_column_drift(events: &[DriftEvent]) -> Vec<ColumnDrift> {
    let mut by_column: HashMap<String, Vec<&DriftEvent>> = HashMap::new();

    for event in events {
        by_column
            .entry(event.column.clone())
            .or_default()
            .push(event);
    }

    by_column
        .into_iter()
        .map(|(column, events)| {
            let expected_type = events
                .first()
                .map(|e| e.expected.clone())
                .unwrap_or_default();

            let observed_types: Vec<String> = events
                .iter()
                .map(|e| e.observed.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            let mismatch_count: u64 = events.iter().map(|e| e.count).sum();

            let examples: Vec<String> = events
                .iter()
                .flat_map(|e| e.samples.iter().cloned())
                .take(5)
                .collect();

            ColumnDrift {
                column,
                expected_type,
                observed_types,
                mismatch_count,
                examples,
            }
        })
        .collect()
}
