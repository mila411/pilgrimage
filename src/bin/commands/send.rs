use crate::error::{BrokerErrorKind, CliError, CliResult, SchemaErrorKind};
use clap::ArgMatches;
use pilgrimage::broker::{Broker, MessageSchema};
use pilgrimage::schema::registry::SchemaRegistry;

fn validate_args(matches: &ArgMatches) -> CliResult<(String, String)> {
    let topic = matches
        .value_of("topic")
        .ok_or_else(|| CliError::ParseError {
            field: "topic".to_string(),
            message: "Topic not specified. Please use the --topic option.".to_string(),
        })?;

    if topic.trim().is_empty() {
        return Err(CliError::ParseError {
            field: "topic".to_string(),
            message: "Topic name cannot be empty.".to_string(),
        });
    }

    let message = matches
        .value_of("message")
        .ok_or_else(|| CliError::ParseError {
            field: "message".to_string(),
            message: "No message specified. Please specify the --message option.".to_string(),
        })?;

    if message.trim().is_empty() {
        return Err(CliError::ParseError {
            field: "message".to_string(),
            message: "The message content cannot be empty.".to_string(),
        });
    }

    Ok((topic.to_string(), message.to_string()))
}

fn handle_schema(
    registry: &SchemaRegistry,
    topic: &str,
    schema_file: Option<&str>,
) -> CliResult<MessageSchema> {
    if let Some(schema_path) = schema_file {
        let schema_content =
            std::fs::read_to_string(schema_path).map_err(|e| CliError::IoError(e))?;

        let schema = registry
            .register_schema(topic, &schema_content)
            .map_err(|e| CliError::SchemaError {
                kind: SchemaErrorKind::RegistryError,
                message: format!("Schema registration error: {}", e),
            })?;

        Ok(MessageSchema::new_with_schema(schema))
    } else {
        Ok(MessageSchema::new())
    }
}

pub async fn handle_send_command(matches: &ArgMatches) -> CliResult<()> {
    // Validate arguments
    let (topic, message) = validate_args(matches)?;
    let schema_file = matches.value_of("schema");

    // Process schema
    let registry = SchemaRegistry::new();
    let topic_schema = handle_schema(&registry, &topic, schema_file)?;

    // Validate message
    if let Err(e) = topic_schema.validate(&message) {
        return Err(CliError::SchemaError {
            kind: SchemaErrorKind::ValidationFailed,
            message: format!(
                "The message does not comply with the schema definition.\nTopic: {}\nError: {}",
                topic, e
            ),
        });
    }

    // Execute broker operations
    send_message_to_broker(message, topic, schema_file.is_some())?;

    Ok(())
}

fn send_message_to_broker(
    message: String,
    topic: String,
    schema_registered: bool,
) -> CliResult<()> {
    // Create message schema
    let schema = MessageSchema::new()
        .with_content(message.clone())
        .with_topic(topic.clone())
        .with_partition(0);

    // Broker initialization and connection
    let mut broker = Broker::new(
        "cli-broker",
        1, // Number of partitions
        1, // Replication factor
        "storage",
    )
    .map_err(|e| CliError::BrokerError {
        kind: BrokerErrorKind::ConnectionFailed,
        message: format!("Failed to initialize broker: {}", e),
    })?;

    // Send message and process results
    broker.send_message(schema).map_err(|e| {
        let error_str = e.to_string().to_lowercase();
        match error_str {
            e if e.contains("connection") => CliError::BrokerError {
                kind: BrokerErrorKind::ConnectionFailed,
                message: format!(
                    "Failed to connect to broker.\nPlease check:\n- The broker is running\n- The connection port is correct\nError details: {}",
                    e
                ),
            },
            e if e.contains("timeout") => CliError::BrokerError {
                kind: BrokerErrorKind::Timeout,
                message: format!(
                    "Message sending timed out.\nTopic: {}\nMessage length: {} bytes\nError details: {}",
                    topic,
                    message.len(),
                    e
                ),
            },
            e if e.contains("topic") => CliError::BrokerError {
                kind: BrokerErrorKind::TopicNotFound,
                message: format!(
                    "The specified topic '{}' was not found.\n- Check if the topic name is correct\n- The topic will be created automatically if it doesn't exist\nError details: {}",
                    topic,
                    e
                ),
            },
            e if e.contains("partition") => CliError::BrokerError {
                kind: BrokerErrorKind::PartitionError,
                message: format!(
                    "A partition-related error occurred.\nTopic: {}\nError details: {}",
                    topic,
                    e
                ),
            },
            e if e.contains("operation") => CliError::BrokerError {
                kind: BrokerErrorKind::OperationFailed,
                message: format!(
                    "Message sending operation failed.\nTopic: {}\nMessage length: {} bytes\nError details: {}",
                    topic,
                    message.len(),
                    e
                ),
            },
            _ => CliError::BrokerError {
                kind: BrokerErrorKind::Unknown,
                message: format!(
                    "An unexpected error occurred while sending the message.\nTopic: {}\nError details: {}",
                    topic,
                    e
                ),
            },
        }
    })?;

    println!("Message sent successfully.");
    if schema_registered {
        println!("Schema validation is enabled.");
    }

    Ok(())
}
