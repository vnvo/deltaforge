//! JSON Schema export functionality.
//!
//! Converts inferred schemas to JSON Schema format for documentation
//! and validation purposes.

use schema_analysis::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Simplified JSON Schema representation.
/// This is a subset of JSON Schema that captures what we can infer.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JsonSchema {
    #[serde(rename = "$schema", skip_serializing_if = "Option::is_none")]
    pub schema_uri: Option<String>,

    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<JsonSchemaType>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Box<JsonSchema>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<JsonSchema>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<Vec<String>>,

    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    #[serde(
        rename = "additionalProperties",
        skip_serializing_if = "Option::is_none"
    )]
    pub additional_properties: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// For union types (oneOf)
    #[serde(rename = "oneOf", skip_serializing_if = "Option::is_none")]
    pub one_of: Option<Vec<JsonSchema>>,
}

/// JSON Schema type values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JsonSchemaType {
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array,
    Object,
}

impl JsonSchema {
    /// Create a new JSON Schema with the standard schema URI.
    pub fn new() -> Self {
        Self {
            schema_uri: Some(
                "https://json-schema.org/draft/2020-12/schema".to_string(),
            ),
            ..Default::default()
        }
    }

    /// Create a type-only schema.
    pub fn typed(t: JsonSchemaType) -> Self {
        Self {
            schema_type: Some(t),
            ..Default::default()
        }
    }
}

/// Convert an inferred schema to JSON Schema.
pub fn to_json_schema(schema: &Schema) -> JsonSchema {
    let mut js = JsonSchema::new();
    convert_schema(schema, &mut js, 0, 10);
    js
}

/// Convert schema with depth limiting.
fn convert_schema(
    schema: &Schema,
    target: &mut JsonSchema,
    depth: usize,
    max_depth: usize,
) {
    if depth > max_depth {
        target.description = Some("(depth limit reached)".to_string());
        return;
    }

    match schema {
        Schema::Null(_) => {
            target.schema_type = Some(JsonSchemaType::Null);
        }

        Schema::Boolean(_) => {
            target.schema_type = Some(JsonSchemaType::Boolean);
        }

        Schema::Integer(_) => {
            target.schema_type = Some(JsonSchemaType::Integer);
        }

        Schema::Float(_) => {
            target.schema_type = Some(JsonSchemaType::Number);
        }

        Schema::String(ctx) => {
            target.schema_type = Some(JsonSchemaType::String);
            // Check for fixed-length strings using MinMax
            if let (Some(min), Some(max)) =
                (ctx.min_max_length.min, ctx.min_max_length.max)
                && min == max
                && min > 0
            {
                target.description = Some(format!("Fixed length: {}", min));
            }
        }

        Schema::Bytes(_) => {
            target.schema_type = Some(JsonSchemaType::String);
            target.format = Some("byte".to_string());
            target.description = Some("Base64-encoded binary data".to_string());
        }

        Schema::Sequence { field, .. } => {
            target.schema_type = Some(JsonSchemaType::Array);
            // Field contains status and optional schema
            if let Some(ref item_schema) = field.schema {
                let mut item_js = JsonSchema::default();
                convert_schema(item_schema, &mut item_js, depth + 1, max_depth);
                target.items = Some(Box::new(item_js));
            }
        }

        Schema::Struct { fields, .. } => {
            target.schema_type = Some(JsonSchemaType::Object);

            let mut props = HashMap::new();
            let mut required_fields = Vec::new();

            for (field_name, field) in fields {
                if let Some(ref field_schema) = field.schema {
                    let mut field_js = JsonSchema::default();
                    convert_schema(
                        field_schema,
                        &mut field_js,
                        depth + 1,
                        max_depth,
                    );
                    props.insert(field_name.clone(), Box::new(field_js));

                    // If field is never null or missing, it's required
                    if !field.status.may_be_null && !field.status.may_be_missing
                    {
                        required_fields.push(field_name.clone());
                    }
                }
            }

            if !props.is_empty() {
                target.properties = Some(props);
            }
            if !required_fields.is_empty() {
                target.required = Some(required_fields);
            }
        }

        Schema::Union { variants } => {
            let converted: Vec<JsonSchema> = variants
                .iter()
                .map(|v| {
                    let mut variant_js = JsonSchema::default();
                    convert_schema(v, &mut variant_js, depth + 1, max_depth);
                    variant_js
                })
                .collect();

            if converted.len() == 1 {
                if let Some(v) = converted.into_iter().next() {
                    *target = v;
                }
            } else if !converted.is_empty() {
                target.one_of = Some(converted);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema_analysis::InferredSchema;

    #[test]
    fn test_simple_object_schema() {
        let json = br#"{"id": 1, "name": "Alice", "active": true}"#;
        let inferred: InferredSchema = serde_json::from_slice(json).unwrap();

        let js = to_json_schema(&inferred.schema);

        assert_eq!(js.schema_type, Some(JsonSchemaType::Object));
        assert!(js.properties.is_some());

        let props = js.properties.unwrap();
        assert!(props.contains_key("id"));
        assert!(props.contains_key("name"));
        assert!(props.contains_key("active"));
    }

    #[test]
    fn test_nested_schema() {
        let json = br#"{"user": {"id": 1, "profile": {"bio": "Hello"}}}"#;
        let inferred: InferredSchema = serde_json::from_slice(json).unwrap();

        let js = to_json_schema(&inferred.schema);

        assert_eq!(js.schema_type, Some(JsonSchemaType::Object));
        let props = js.properties.unwrap();
        let user = props.get("user").unwrap();
        assert_eq!(user.schema_type, Some(JsonSchemaType::Object));
    }

    #[test]
    fn test_array_schema() {
        let json = br#"{"items": [1, 2, 3]}"#;
        let inferred: InferredSchema = serde_json::from_slice(json).unwrap();

        let js = to_json_schema(&inferred.schema);
        let props = js.properties.unwrap();
        let items = props.get("items").unwrap();

        assert_eq!(items.schema_type, Some(JsonSchemaType::Array));
    }

    #[test]
    fn test_to_json_string() {
        let json = br#"{"id": 1}"#;
        let inferred: InferredSchema = serde_json::from_slice(json).unwrap();

        let js = to_json_schema(&inferred.schema);
        let json_str = serde_json::to_string_pretty(&js).unwrap();

        assert!(json_str.contains("\"type\": \"object\""));
        assert!(json_str.contains("\"$schema\""));
    }

    #[test]
    fn test_default_json_schema() {
        let js = JsonSchema::default();
        assert!(js.schema_uri.is_none());
        assert!(js.schema_type.is_none());
    }

    #[test]
    fn test_typed_json_schema() {
        let js = JsonSchema::typed(JsonSchemaType::String);
        assert_eq!(js.schema_type, Some(JsonSchemaType::String));
    }
}
