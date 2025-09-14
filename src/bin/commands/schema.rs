use crate::{CliError, CliResult, SchemaErrorKind};
use clap::ArgMatches;
use pilgrimage::schema::compatibility::Compatibility;
use pilgrimage::schema::registry::{Schema, SchemaRegistry};
use pilgrimage::schema::version::SchemaVersion;
use serde_json::{Value, json};
use std::error::Error;
use std::fs;

fn parse_compatibility(value: Option<&str>) -> Compatibility {
    value.map_or(Compatibility::Backward, |c| match c {
        "BACKWARD" => Compatibility::Backward,
        "FORWARD" => Compatibility::Forward,
        "FULL" => Compatibility::Full,
        "NONE" => Compatibility::None,
        _ => Compatibility::Backward,
    })
}

fn get_compatibility_description(comp: &Compatibility) -> &'static str {
    match comp {
        Compatibility::Backward => "BACKWARD",
        Compatibility::Forward => "FORWARD",
        Compatibility::Full => "FULL",
        Compatibility::None => "NONE",
    }
}

fn validate_schema_compatibility(
    registry: &SchemaRegistry,
    topic: &str,
    schema_content: &str,
    compatibility: &Compatibility,
) -> CliResult<()> {
    if let Some(schemas) = registry.get_all_schemas(topic) {
        if let Some(latest_schema) = schemas.last() {
            let new_schema = Schema {
                id: latest_schema.id + 1,
                version: SchemaVersion::new(latest_schema.version.major + 1),
                definition: schema_content.to_string(),
            };

            if !compatibility.check(&new_schema, latest_schema) {
                return Err(CliError::SchemaError {
                    kind: SchemaErrorKind::IncompatibleChange,
                    message: format!(
                        "Schema compatibility validation error:\nTopic.: {}\nRequired Compatibility: {}\nIncompatible with current schema",
                        topic,
                        get_compatibility_description(compatibility)
                    ),
                });
            }
        }
    }
    Ok(())
}

pub async fn handle_schema_register_command(matches: &ArgMatches) -> CliResult<()> {
    // Argument Validation
    let topic = matches
        .value_of("topic")
        .ok_or_else(|| CliError::ParseError {
            field: "topic".to_string(),
            message: "Topic not specified".to_string(),
        })?;

    let schema_file = matches
        .value_of("schema")
        .ok_or_else(|| CliError::ParseError {
            field: "schema".to_string(),
            message: "Schema file not specified".to_string(),
        })?;

    let schema_content = fs::read_to_string(schema_file).map_err(|e| CliError::IoError(e.to_string()))?;

    let compatibility = parse_compatibility(matches.value_of("compatibility"));

    let mut registry = SchemaRegistry::new();
    registry.set_compatibility(compatibility);

    // Schema compatibility check
    validate_schema_compatibility(&registry, topic, &schema_content, &compatibility)?;

    // Register schema
    match registry.register_schema(topic, &schema_content) {
        Ok(schema) => {
            println!(
                "Schema registered:\nTopic: {}\nSchema ID: {}\nVersion: {}",
                topic, schema.id, schema.version
            );
            Ok(())
        }
        Err(e) => Err(CliError::SchemaError {
            kind: SchemaErrorKind::RegistryError,
            message: format!("Failed to register schema: {}", e),
        }),
    }
}

pub async fn handle_schema_list_command(matches: &ArgMatches) -> CliResult<()> {
    let topic = matches
        .value_of("topic")
        .ok_or_else(|| CliError::ParseError {
            field: "topic".to_string(),
            message: "Topic is not specified".to_string(),
        })?;

    let registry = SchemaRegistry::new();
    match registry.get_all_schemas(topic) {
        Some(schemas) => {
            if schemas.is_empty() {
                println!("No schemas registered for topic {}", topic);
            } else {
                println!("Schema list for topic {}:", topic);
                for schema in schemas {
                    println!(
                        "ID: {}, Version: {}\nDefinition:\n{}",
                        schema.id, schema.version, schema.definition
                    );
                }
            }
            Ok(())
        }
        None => Err(CliError::SchemaError {
            kind: SchemaErrorKind::NotFound,
            message: format!("Topic {} schema not found", topic),
        }),
    }
}

/// Handle schema get command
pub async fn handle_schema_get_command(matches: &ArgMatches) -> CliResult<()> {
    let topic = matches
        .get_one::<String>("topic")
        .ok_or_else(|| CliError::ParseError {
            field: "topic".to_string(),
            message: "Topic not specified".to_string(),
        })?;

    let schema_id = matches.get_one::<String>("schema-id");
    let version = matches.get_one::<String>("version");
    let default_format = "json".to_string();
    let format = matches
        .get_one::<String>("format")
        .unwrap_or(&default_format);

    println!("🔍 Retrieving schema for topic '{}'...", topic);

    // Simulate schema retrieval
    let schema_data = simulate_get_schema(topic, schema_id, version).await?;

    match format.as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&schema_data).unwrap());
        }
        "raw" => {
            println!(
                "{}",
                schema_data["schema"].as_str().unwrap_or("Schema not found")
            );
        }
        "table" | _ => {
            display_schema_details(&schema_data)?;
        }
    }

    Ok(())
}

/// Handle schema validate command
pub async fn handle_schema_validate_command(matches: &ArgMatches) -> CliResult<()> {
    let schema_file = matches
        .get_one::<String>("schema")
        .ok_or_else(|| CliError::ParseError {
            field: "schema".to_string(),
            message: "Schema file not specified".to_string(),
        })?;

    let message_file = matches.get_one::<String>("message");
    let topic = matches.get_one::<String>("topic");

    println!("✅ Validating schema from '{}'...", schema_file);

    let schema_content = fs::read_to_string(schema_file).map_err(|e| CliError::IoError(e.to_string()))?;

    // Validate schema syntax
    let validation_result = validate_schema_syntax(&schema_content).await?;
    display_validation_result(&validation_result)?;

    // If message file provided, validate message against schema
    if let Some(msg_file) = message_file {
        println!("\n🔍 Validating message against schema...");
    let message_content = fs::read_to_string(msg_file).map_err(|e| CliError::IoError(e.to_string()))?;
        let message_validation =
            validate_message_against_schema(&schema_content, &message_content).await?;
        display_message_validation(&message_validation)?;
    }

    // If topic provided, check compatibility
    if let Some(topic_name) = topic {
        println!("\n🔄 Checking compatibility with existing schemas...");
        let compatibility_result = check_schema_compatibility(topic_name, &schema_content).await?;
        display_compatibility_result(&compatibility_result)?;
    }

    Ok(())
}

/// Handle schema evolution command
pub async fn handle_schema_evolution_command(matches: &ArgMatches) -> CliResult<()> {
    let topic = matches
        .get_one::<String>("topic")
        .ok_or_else(|| CliError::ParseError {
            field: "topic".to_string(),
            message: "Topic not specified".to_string(),
        })?;

    let detailed = matches.get_flag("detailed");

    println!("📈 Analyzing schema evolution for topic '{}'...", topic);

    let evolution_data = simulate_get_schema_evolution(topic).await?;

    if detailed {
        display_detailed_evolution(&evolution_data)?;
    } else {
        display_simple_evolution(&evolution_data)?;
    }

    Ok(())
}

/// Handle schema compatibility check command
pub async fn handle_schema_compatibility_command(matches: &ArgMatches) -> CliResult<()> {
    let topic = matches
        .get_one::<String>("topic")
        .ok_or_else(|| CliError::ParseError {
            field: "topic".to_string(),
            message: "Topic not specified".to_string(),
        })?;

    let schema_file = matches
        .get_one::<String>("schema")
        .ok_or_else(|| CliError::ParseError {
            field: "schema".to_string(),
            message: "Schema file not specified".to_string(),
        })?;

    let default_level = "BACKWARD".to_string();
    let compatibility_level = matches.get_one::<String>("level").unwrap_or(&default_level);

    println!("🔄 Checking schema compatibility...");
    println!("   Topic: {}", topic);
    println!("   Schema: {}", schema_file);
    println!("   Level: {}", compatibility_level);

    let schema_content = fs::read_to_string(schema_file).map_err(|e| CliError::IoError(e.to_string()))?;

    let compatibility_result =
        check_detailed_compatibility(topic, &schema_content, compatibility_level).await?;
    display_detailed_compatibility(&compatibility_result)?;

    Ok(())
}

// Helper functions
async fn simulate_get_schema(
    topic: &str,
    schema_id: Option<&String>,
    version: Option<&String>,
) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(json!({
        "topic": topic,
        "schema_id": schema_id.unwrap_or(&"schema_001".to_string()),
        "version": version.unwrap_or(&"1.0.0".to_string()),
        "schema": r#"{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.pilgrimage.events",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "properties", "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}"#,
        "created_at": "2024-01-15T10:30:00Z",
        "updated_at": "2024-01-15T10:30:00Z",
        "compatibility": "BACKWARD",
        "references": [],
        "metadata": {
            "description": "User event schema for analytics",
            "owner": "analytics-team",
            "tags": ["user", "events", "analytics"]
        }
    }))
}

async fn validate_schema_syntax(schema_content: &str) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Simulate schema validation
    let is_valid_json = serde_json::from_str::<Value>(schema_content).is_ok();

    Ok(json!({
        "valid": is_valid_json,
        "syntax_errors": if is_valid_json {
            json!([])
        } else {
            json!([
                {"line": 5, "column": 12, "message": "Unexpected token"},
                {"line": 8, "column": 1, "message": "Missing closing brace"}
            ])
        },
        "warnings": json!([
            {"message": "Field 'deprecated_field' is marked as deprecated"},
            {"message": "Consider using union types for optional fields"}
        ]),
        "schema_type": "AVRO",
        "validation_time": "45ms"
    }))
}

async fn validate_message_against_schema(
    _schema: &str,
    message: &str,
) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let message_valid = serde_json::from_str::<Value>(message).is_ok();

    Ok(json!({
        "valid": message_valid,
        "errors": if message_valid {
            json!([])
        } else {
            json!([
                {"field": "user_id", "message": "Required field missing"},
                {"field": "timestamp", "message": "Invalid type: expected long, got string"}
            ])
        },
        "warnings": json!([
            {"field": "properties", "message": "Optional field not provided"}
        ]),
        "validation_time": "12ms"
    }))
}

async fn check_schema_compatibility(_topic: &str, _schema: &str) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(json!({
        "compatible": true,
        "compatibility_level": "BACKWARD",
        "existing_versions": ["1.0.0", "1.1.0", "2.0.0"],
        "breaking_changes": [],
        "non_breaking_changes": [
            {"type": "field_addition", "field": "new_optional_field", "description": "Added optional field with default value"}
        ],
        "recommendations": [
            "Consider bumping major version for field type changes",
            "Document migration path for consumers"
        ]
    }))
}

async fn check_detailed_compatibility(
    topic: &str,
    _schema: &str,
    level: &str,
) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

    let compatible = match level {
        "FORWARD" => true,
        "BACKWARD" => true,
        "FULL" => false,
        "NONE" => true,
        _ => true,
    };

    Ok(json!({
        "topic": topic,
        "compatibility_level": level,
        "compatible": compatible,
        "checked_against": json!([
            {"version": "1.0.0", "compatible": true},
            {"version": "1.1.0", "compatible": true},
            {"version": "2.0.0", "compatible": !compatible}
        ]),
        "issues": if compatible {
            json!([])
        } else {
            json!([
                {
                    "severity": "ERROR",
                    "type": "field_removal",
                    "field": "old_field",
                    "message": "Field removed without proper deprecation"
                },
                {
                    "severity": "WARNING",
                    "type": "type_change",
                    "field": "timestamp",
                    "message": "Field type changed from string to long"
                }
            ])
        },
        "analysis_time": "125ms",
        "migration_suggestions": json!([
            "Add migration script for field type changes",
            "Deprecate fields before removal",
            "Use union types for gradual transitions"
        ])
    }))
}

async fn simulate_get_schema_evolution(topic: &str) -> Result<Value, Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    Ok(json!({
        "topic": topic,
        "evolution_timeline": [
            {
                "version": "1.0.0",
                "created_at": "2024-01-01T00:00:00Z",
                "author": "alice@company.com",
                "changes": ["Initial schema"],
                "fields_count": 3,
                "breaking": false
            },
            {
                "version": "1.1.0",
                "created_at": "2024-01-15T10:30:00Z",
                "author": "bob@company.com",
                "changes": ["Added optional properties field"],
                "fields_count": 4,
                "breaking": false
            },
            {
                "version": "2.0.0",
                "created_at": "2024-02-01T14:45:00Z",
                "author": "charlie@company.com",
                "changes": ["Changed timestamp type from string to long", "Removed deprecated field"],
                "fields_count": 4,
                "breaking": true
            }
        ],
        "statistics": {
            "total_versions": 3,
            "breaking_changes": 1,
            "field_additions": 2,
            "field_removals": 1,
            "type_changes": 1,
            "avg_fields_per_version": 3.67
        },
        "compatibility_matrix": {
            "1.0.0": {"1.1.0": true, "2.0.0": false},
            "1.1.0": {"2.0.0": false},
            "2.0.0": {}
        }
    }))
}

// Display functions
fn display_schema_details(schema: &Value) -> Result<(), Box<dyn Error>> {
    println!();
    println!("📋 Schema Details");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Topic: {}", schema["topic"].as_str().unwrap_or("Unknown"));
    println!(
        "Schema ID: {}",
        schema["schema_id"].as_str().unwrap_or("Unknown")
    );
    println!(
        "Version: {}",
        schema["version"].as_str().unwrap_or("Unknown")
    );
    println!(
        "Compatibility: {}",
        schema["compatibility"].as_str().unwrap_or("Unknown")
    );
    println!(
        "Created: {}",
        schema["created_at"].as_str().unwrap_or("Unknown")
    );
    println!(
        "Updated: {}",
        schema["updated_at"].as_str().unwrap_or("Unknown")
    );

    if let Some(metadata) = schema["metadata"].as_object() {
        println!();
        println!("📝 Metadata:");
        if let Some(desc) = metadata["description"].as_str() {
            println!("   Description: {}", desc);
        }
        if let Some(owner) = metadata["owner"].as_str() {
            println!("   Owner: {}", owner);
        }
        if let Some(tags) = metadata["tags"].as_array() {
            println!("   Tags: {:?}", tags);
        }
    }

    println!();
    println!("📄 Schema Definition:");
    println!(
        "{}",
        schema["schema"]
            .as_str()
            .unwrap_or("Schema content not available")
    );
    println!();

    Ok(())
}

fn display_validation_result(result: &Value) -> Result<(), Box<dyn Error>> {
    let valid = result["valid"].as_bool().unwrap_or(false);
    let status_icon = if valid { "✅" } else { "❌" };

    println!(
        "{} Schema Validation: {}",
        status_icon,
        if valid { "PASSED" } else { "FAILED" }
    );
    println!(
        "   Schema Type: {}",
        result["schema_type"].as_str().unwrap_or("Unknown")
    );
    println!(
        "   Validation Time: {}",
        result["validation_time"].as_str().unwrap_or("Unknown")
    );

    if let Some(errors) = result["syntax_errors"].as_array() {
        if !errors.is_empty() {
            println!();
            println!("❌ Syntax Errors:");
            for error in errors {
                println!(
                    "   Line {}, Column {}: {}",
                    error["line"].as_u64().unwrap_or(0),
                    error["column"].as_u64().unwrap_or(0),
                    error["message"].as_str().unwrap_or("Unknown error")
                );
            }
        }
    }

    if let Some(warnings) = result["warnings"].as_array() {
        if !warnings.is_empty() {
            println!();
            println!("⚠️  Warnings:");
            for warning in warnings {
                println!(
                    "   {}",
                    warning["message"].as_str().unwrap_or("Unknown warning")
                );
            }
        }
    }

    Ok(())
}

fn display_message_validation(result: &Value) -> Result<(), Box<dyn Error>> {
    let valid = result["valid"].as_bool().unwrap_or(false);
    let status_icon = if valid { "✅" } else { "❌" };

    println!(
        "{} Message Validation: {}",
        status_icon,
        if valid { "PASSED" } else { "FAILED" }
    );
    println!(
        "   Validation Time: {}",
        result["validation_time"].as_str().unwrap_or("Unknown")
    );

    if let Some(errors) = result["errors"].as_array() {
        if !errors.is_empty() {
            println!();
            println!("❌ Validation Errors:");
            for error in errors {
                println!(
                    "   Field '{}': {}",
                    error["field"].as_str().unwrap_or("unknown"),
                    error["message"].as_str().unwrap_or("Unknown error")
                );
            }
        }
    }

    if let Some(warnings) = result["warnings"].as_array() {
        if !warnings.is_empty() {
            println!();
            println!("⚠️  Warnings:");
            for warning in warnings {
                println!(
                    "   Field '{}': {}",
                    warning["field"].as_str().unwrap_or("unknown"),
                    warning["message"].as_str().unwrap_or("Unknown warning")
                );
            }
        }
    }

    Ok(())
}

fn display_compatibility_result(result: &Value) -> Result<(), Box<dyn Error>> {
    let compatible = result["compatible"].as_bool().unwrap_or(false);
    let status_icon = if compatible { "✅" } else { "❌" };

    println!(
        "{} Compatibility Check: {}",
        status_icon,
        if compatible {
            "COMPATIBLE"
        } else {
            "INCOMPATIBLE"
        }
    );
    println!(
        "   Compatibility Level: {}",
        result["compatibility_level"].as_str().unwrap_or("Unknown")
    );

    if let Some(versions) = result["existing_versions"].as_array() {
        println!("   Checked Against Versions: {:?}", versions);
    }

    if let Some(breaking) = result["breaking_changes"].as_array() {
        if !breaking.is_empty() {
            println!();
            println!("💥 Breaking Changes:");
            for change in breaking {
                println!("   {}", change.as_str().unwrap_or("Unknown change"));
            }
        }
    }

    if let Some(non_breaking) = result["non_breaking_changes"].as_array() {
        if !non_breaking.is_empty() {
            println!();
            println!("✨ Non-Breaking Changes:");
            for change in non_breaking {
                println!(
                    "   {}: {}",
                    change["type"].as_str().unwrap_or("unknown"),
                    change["description"].as_str().unwrap_or("No description")
                );
            }
        }
    }

    if let Some(recommendations) = result["recommendations"].as_array() {
        if !recommendations.is_empty() {
            println!();
            println!("💡 Recommendations:");
            for rec in recommendations {
                println!("   • {}", rec.as_str().unwrap_or("No recommendation"));
            }
        }
    }

    Ok(())
}

fn display_detailed_compatibility(result: &Value) -> Result<(), Box<dyn Error>> {
    let compatible = result["compatible"].as_bool().unwrap_or(false);
    let status_icon = if compatible { "✅" } else { "❌" };

    println!();
    println!("🔄 Detailed Compatibility Analysis");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Topic: {}", result["topic"].as_str().unwrap_or("Unknown"));
    println!(
        "Compatibility Level: {}",
        result["compatibility_level"].as_str().unwrap_or("Unknown")
    );
    println!(
        "Overall Result: {}{}",
        status_icon,
        if compatible {
            "COMPATIBLE"
        } else {
            "INCOMPATIBLE"
        }
    );

    if let Some(checked) = result["checked_against"].as_array() {
        println!();
        println!("📊 Version Compatibility:");
        for check in checked {
            let version = check["version"].as_str().unwrap_or("Unknown");
            let version_compatible = check["compatible"].as_bool().unwrap_or(false);
            let version_icon = if version_compatible { "✅" } else { "❌" };
            println!("   {} Version {}", version_icon, version);
        }
    }

    if let Some(issues) = result["issues"].as_array() {
        if !issues.is_empty() {
            println!();
            println!("⚠️  Compatibility Issues:");
            for issue in issues {
                let severity = issue["severity"].as_str().unwrap_or("UNKNOWN");
                let severity_icon = match severity {
                    "ERROR" => "❌",
                    "WARNING" => "⚠️",
                    _ => "ℹ️",
                };
                println!(
                    "   {} [{}] {}: {}",
                    severity_icon,
                    severity,
                    issue["type"].as_str().unwrap_or("unknown"),
                    issue["message"].as_str().unwrap_or("No message")
                );
            }
        }
    }

    if let Some(suggestions) = result["migration_suggestions"].as_array() {
        if !suggestions.is_empty() {
            println!();
            println!("💡 Migration Suggestions:");
            for suggestion in suggestions {
                println!("   • {}", suggestion.as_str().unwrap_or("No suggestion"));
            }
        }
    }

    println!();
    Ok(())
}

fn display_simple_evolution(evolution: &Value) -> Result<(), Box<dyn Error>> {
    println!();
    println!("📈 Schema Evolution Summary");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    if let Some(stats) = evolution["statistics"].as_object() {
        println!(
            "Total Versions: {}",
            stats["total_versions"].as_u64().unwrap_or(0)
        );
        println!(
            "Breaking Changes: {}",
            stats["breaking_changes"].as_u64().unwrap_or(0)
        );
        println!(
            "Field Additions: {}",
            stats["field_additions"].as_u64().unwrap_or(0)
        );
        println!(
            "Field Removals: {}",
            stats["field_removals"].as_u64().unwrap_or(0)
        );
        println!(
            "Type Changes: {}",
            stats["type_changes"].as_u64().unwrap_or(0)
        );
    }

    if let Some(timeline) = evolution["evolution_timeline"].as_array() {
        println!();
        println!("📋 Version History:");
        for version in timeline {
            let version_num = version["version"].as_str().unwrap_or("Unknown");
            let breaking = version["breaking"].as_bool().unwrap_or(false);
            let breaking_icon = if breaking { "💥" } else { "✨" };
            let author = version["author"].as_str().unwrap_or("Unknown");

            println!(
                "   {} {} by {} - {}",
                breaking_icon,
                version_num,
                author,
                version["created_at"].as_str().unwrap_or("Unknown")
            );
        }
    }

    println!();
    Ok(())
}

fn display_detailed_evolution(evolution: &Value) -> Result<(), Box<dyn Error>> {
    println!();
    println!("📈 Detailed Schema Evolution Analysis");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    if let Some(timeline) = evolution["evolution_timeline"].as_array() {
        for (i, version) in timeline.iter().enumerate() {
            if i > 0 {
                println!();
            }

            let version_num = version["version"].as_str().unwrap_or("Unknown");
            let breaking = version["breaking"].as_bool().unwrap_or(false);
            let breaking_icon = if breaking { "💥" } else { "✨" };

            println!("📦 Version {} {}", version_num, breaking_icon);
            println!(
                "   Created: {}",
                version["created_at"].as_str().unwrap_or("Unknown")
            );
            println!(
                "   Author: {}",
                version["author"].as_str().unwrap_or("Unknown")
            );
            println!(
                "   Fields: {}",
                version["fields_count"].as_u64().unwrap_or(0)
            );
            println!("   Breaking: {}", if breaking { "Yes" } else { "No" });

            if let Some(changes) = version["changes"].as_array() {
                println!("   Changes:");
                for change in changes {
                    println!("     • {}", change.as_str().unwrap_or("Unknown change"));
                }
            }
        }
    }

    if let Some(stats) = evolution["statistics"].as_object() {
        println!();
        println!("📊 Evolution Statistics:");
        println!(
            "   Total Versions: {}",
            stats["total_versions"].as_u64().unwrap_or(0)
        );
        println!(
            "   Breaking Changes: {}",
            stats["breaking_changes"].as_u64().unwrap_or(0)
        );
        println!(
            "   Field Additions: {}",
            stats["field_additions"].as_u64().unwrap_or(0)
        );
        println!(
            "   Field Removals: {}",
            stats["field_removals"].as_u64().unwrap_or(0)
        );
        println!(
            "   Type Changes: {}",
            stats["type_changes"].as_u64().unwrap_or(0)
        );
        println!(
            "   Avg Fields/Version: {:.2}",
            stats["avg_fields_per_version"].as_f64().unwrap_or(0.0)
        );
    }

    println!();
    Ok(())
}
