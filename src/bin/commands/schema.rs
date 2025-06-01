use crate::error::{CliError, CliResult, SchemaErrorKind};
use clap::ArgMatches;
use pilgrimage::schema::compatibility::Compatibility;
use pilgrimage::schema::registry::{Schema, SchemaRegistry};
use pilgrimage::schema::version::SchemaVersion;
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

    let schema_content = fs::read_to_string(schema_file).map_err(|e| CliError::IoError(e))?;

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
