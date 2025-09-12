use clap::{Arg, Command};
use std::error::Error;

mod commands;
mod error;

use commands::*;
use error::CliError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = Command::new("Pilgrimage")
        .version("1.0.0")
        .author("Kenny (Miller) Song")
        .about("Production-Ready Distributed Message Broker CLI")
        .arg_required_else_help(true)
        // Global options
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Configuration file path")
                .global(true),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose output")
                .action(clap::ArgAction::Count)
                .global(true),
        )
        .arg(
            Arg::new("host")
                .short('H')
                .long("host")
                .value_name("HOST")
                .help("Broker host address")
                .default_value("localhost")
                .global(true),
        )
        .arg(
            Arg::new("port")
                .short('P')
                .long("port")
                .value_name("PORT")
                .help("Broker port")
                .default_value("8080")
                .global(true),
        )
        // Start command - enhanced
        .subcommand(
            Command::new("start")
                .about("Start the message broker")
                .arg(
                    Arg::new("id")
                        .short('i')
                        .long("id")
                        .value_name("ID")
                        .help("Broker instance ID")
                        .required(true),
                )
                .arg(
                    Arg::new("partitions")
                        .short('p')
                        .long("partitions")
                        .value_name("NUM")
                        .help("Default number of partitions for new topics")
                        .default_value("3"),
                )
                .arg(
                    Arg::new("replication")
                        .short('r')
                        .long("replication")
                        .value_name("NUM")
                        .help("Default replication factor")
                        .default_value("2"),
                )
                .arg(
                    Arg::new("storage")
                        .short('s')
                        .long("storage")
                        .value_name("PATH")
                        .help("Storage directory path")
                        .default_value("./storage"),
                )
                .arg(
                    Arg::new("cluster-mode")
                        .long("cluster")
                        .help("Enable cluster mode")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("cluster-nodes")
                        .long("nodes")
                        .value_name("NODES")
                        .help("Comma-separated list of cluster nodes (host:port)")
                        .requires("cluster-mode"),
                )
                .arg(
                    Arg::new("test-mode")
                        .long("test-mode")
                        .help("Run in test mode")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("security")
                        .long("security")
                        .help("Enable security features")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("tls")
                        .long("tls")
                        .help("Enable TLS encryption")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("metrics")
                        .long("metrics")
                        .help("Enable metrics collection")
                        .action(clap::ArgAction::SetTrue),
                ),
        )
        // Stop command - enhanced
        .subcommand(
            Command::new("stop")
                .about("Stop the message broker")
                .arg(
                    Arg::new("id")
                        .short('i')
                        .long("id")
                        .value_name("ID")
                        .help("Broker instance ID to stop")
                        .required(true),
                )
                .arg(
                    Arg::new("force")
                        .short('f')
                        .long("force")
                        .help("Force stop without graceful shutdown")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("timeout")
                        .short('t')
                        .long("timeout")
                        .value_name("SECONDS")
                        .help("Graceful shutdown timeout")
                        .default_value("30"),
                ),
        )
        // Send command - enhanced
        .subcommand(
            Command::new("send")
                .about("Send message to a topic")
                .arg(
                    Arg::new("topic")
                        .short('t')
                        .long("topic")
                        .value_name("TOPIC")
                        .help("Target topic name")
                        .required(true),
                )
                .arg(
                    Arg::new("message")
                        .short('m')
                        .long("message")
                        .value_name("MESSAGE")
                        .help("Message content")
                        .required(true),
                )
                .arg(
                    Arg::new("key")
                        .short('k')
                        .long("key")
                        .value_name("KEY")
                        .help("Message key for partitioning"),
                )
                .arg(
                    Arg::new("partition")
                        .short('p')
                        .long("partition")
                        .value_name("NUM")
                        .help("Target partition number"),
                )
                .arg(
                    Arg::new("headers")
                        .long("headers")
                        .value_name("HEADERS")
                        .help("Message headers in key=value format (comma-separated)"),
                )
                .arg(
                    Arg::new("schema")
                        .short('s')
                        .long("schema")
                        .value_name("SCHEMA")
                        .help("Schema file path for validation"),
                )
                .arg(
                    Arg::new("async")
                        .long("async")
                        .help("Send asynchronously without waiting for ack")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("batch")
                        .short('b')
                        .long("batch")
                        .value_name("FILE")
                        .help("Send multiple messages from file"),
                ),
        )
        // Consume command - enhanced
        .subcommand(
            Command::new("consume")
                .about("Consume messages from topics")
                .arg(
                    Arg::new("topic")
                        .short('t')
                        .long("topic")
                        .value_name("TOPIC")
                        .help("Topic name to consume from")
                        .required(true),
                )
                .arg(
                    Arg::new("group")
                        .short('g')
                        .long("group")
                        .value_name("GROUP")
                        .help("Consumer group ID")
                        .required(true),
                )
                .arg(
                    Arg::new("partition")
                        .short('p')
                        .long("partition")
                        .value_name("NUM")
                        .help("Specific partition to consume from"),
                )
                .arg(
                    Arg::new("offset")
                        .short('o')
                        .long("offset")
                        .value_name("OFFSET")
                        .help("Starting offset (earliest, latest, or specific number)")
                        .default_value("latest"),
                )
                .arg(
                    Arg::new("max-messages")
                        .short('n')
                        .long("max-messages")
                        .value_name("NUM")
                        .help("Maximum number of messages to consume"),
                )
                .arg(
                    Arg::new("timeout")
                        .long("timeout")
                        .value_name("SECONDS")
                        .help("Consumer timeout in seconds")
                        .default_value("10"),
                )
                .arg(
                    Arg::new("format")
                        .short('f')
                        .long("format")
                        .value_name("FORMAT")
                        .help("Output format (json, plain, table)")
                        .default_value("plain"),
                )
                .arg(
                    Arg::new("save")
                        .long("save")
                        .value_name("FILE")
                        .help("Save consumed messages to file"),
                ),
        )
        // Status command - enhanced
        .subcommand(
            Command::new("status")
                .about("Check broker and cluster status")
                .arg(
                    Arg::new("id")
                        .short('i')
                        .long("id")
                        .value_name("ID")
                        .help("Specific broker ID to check"),
                )
                .arg(
                    Arg::new("detailed")
                        .short('d')
                        .long("detailed")
                        .help("Show detailed status information")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("json")
                        .short('j')
                        .long("json")
                        .help("Output in JSON format")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("watch")
                        .short('w')
                        .long("watch")
                        .help("Watch mode - refresh every N seconds")
                        .value_name("SECONDS"),
                ),
        )
        // Topic management
        .subcommand(
            Command::new("topic")
                .about("Manage topics")
                .subcommand(
                    Command::new("create")
                        .about("Create a new topic")
                        .arg(
                            Arg::new("name")
                                .short('n')
                                .long("name")
                                .value_name("NAME")
                                .help("Topic name")
                                .required(true),
                        )
                        .arg(
                            Arg::new("partitions")
                                .short('p')
                                .long("partitions")
                                .value_name("NUM")
                                .help("Number of partitions")
                                .default_value("3"),
                        )
                        .arg(
                            Arg::new("replication")
                                .short('r')
                                .long("replication")
                                .value_name("NUM")
                                .help("Replication factor")
                                .default_value("2"),
                        )
                        .arg(
                            Arg::new("retention")
                                .long("retention")
                                .value_name("HOURS")
                                .help("Retention time in hours")
                                .default_value("168"),
                        ),
                )
                .subcommand(
                    Command::new("list").about("List all topics").arg(
                        Arg::new("detailed")
                            .short('d')
                            .long("detailed")
                            .help("Show detailed topic information")
                            .action(clap::ArgAction::SetTrue),
                    ),
                )
                .subcommand(
                    Command::new("delete")
                        .about("Delete a topic")
                        .arg(
                            Arg::new("name")
                                .short('n')
                                .long("name")
                                .value_name("NAME")
                                .help("Topic name to delete")
                                .required(true),
                        )
                        .arg(
                            Arg::new("force")
                                .short('f')
                                .long("force")
                                .help("Force deletion without confirmation")
                                .action(clap::ArgAction::SetTrue),
                        ),
                )
                .subcommand(
                    Command::new("describe")
                        .about("Describe topic details")
                        .arg(
                            Arg::new("name")
                                .short('n')
                                .long("name")
                                .value_name("NAME")
                                .help("Topic name")
                                .required(true),
                        ),
                ),
        )
        // Schema management - enhanced
        .subcommand(
            Command::new("schema")
                .about("Manage message schemas")
                .subcommand(
                    Command::new("register")
                        .about("Register a new schema")
                        .arg(
                            Arg::new("topic")
                                .short('t')
                                .long("topic")
                                .value_name("TOPIC")
                                .help("Topic name")
                                .required(true),
                        )
                        .arg(
                            Arg::new("schema")
                                .short('s')
                                .long("schema")
                                .value_name("FILE")
                                .help("Schema file path")
                                .required(true),
                        )
                        .arg(
                            Arg::new("compatibility")
                                .short('c')
                                .long("compatibility")
                                .value_name("LEVEL")
                                .help("Compatibility level (BACKWARD, FORWARD, FULL, NONE)")
                                .default_value("BACKWARD"),
                        )
                        .arg(
                            Arg::new("version")
                                .short('v')
                                .long("version")
                                .value_name("VERSION")
                                .help("Schema version"),
                        ),
                )
                .subcommand(
                    Command::new("list").about("List schemas for a topic").arg(
                        Arg::new("topic")
                            .short('t')
                            .long("topic")
                            .value_name("TOPIC")
                            .help("Topic name")
                            .required(true),
                    ),
                )
                .subcommand(
                    Command::new("get")
                        .about("Get specific schema version")
                        .arg(
                            Arg::new("topic")
                                .short('t')
                                .long("topic")
                                .value_name("TOPIC")
                                .help("Topic name")
                                .required(true),
                        )
                        .arg(
                            Arg::new("version")
                                .short('v')
                                .long("version")
                                .value_name("VERSION")
                                .help("Schema version")
                                .required(true),
                        ),
                )
                .subcommand(
                    Command::new("validate")
                        .about("Validate message against schema")
                        .arg(
                            Arg::new("topic")
                                .short('t')
                                .long("topic")
                                .value_name("TOPIC")
                                .help("Topic name")
                                .required(true),
                        )
                        .arg(
                            Arg::new("message")
                                .short('m')
                                .long("message")
                                .value_name("MESSAGE")
                                .help("Message to validate")
                                .required(true),
                        )
                        .arg(
                            Arg::new("version")
                                .short('v')
                                .long("version")
                                .value_name("VERSION")
                                .help("Schema version to validate against"),
                        ),
                ),
        )
        // Consumer group management
        .subcommand(
            Command::new("group")
                .about("Manage consumer groups")
                .subcommand(
                    Command::new("list").about("List all consumer groups").arg(
                        Arg::new("detailed")
                            .short('d')
                            .long("detailed")
                            .help("Show detailed group information")
                            .action(clap::ArgAction::SetTrue),
                    ),
                )
                .subcommand(
                    Command::new("describe")
                        .about("Describe consumer group")
                        .arg(
                            Arg::new("group")
                                .short('g')
                                .long("group")
                                .value_name("GROUP")
                                .help("Consumer group ID")
                                .required(true),
                        ),
                )
                .subcommand(
                    Command::new("reset")
                        .about("Reset consumer group offsets")
                        .arg(
                            Arg::new("group")
                                .short('g')
                                .long("group")
                                .value_name("GROUP")
                                .help("Consumer group ID")
                                .required(true),
                        )
                        .arg(
                            Arg::new("topic")
                                .short('t')
                                .long("topic")
                                .value_name("TOPIC")
                                .help("Topic name")
                                .required(true),
                        )
                        .arg(
                            Arg::new("to-offset")
                                .long("to-offset")
                                .value_name("OFFSET")
                                .help("Reset to specific offset"),
                        )
                        .arg(
                            Arg::new("to-earliest")
                                .long("to-earliest")
                                .help("Reset to earliest offset")
                                .action(clap::ArgAction::SetTrue),
                        )
                        .arg(
                            Arg::new("to-latest")
                                .long("to-latest")
                                .help("Reset to latest offset")
                                .action(clap::ArgAction::SetTrue),
                        ),
                ),
        )
        // Security management
        .subcommand(
            Command::new("security")
                .about("Manage security settings")
                .subcommand(
                    Command::new("user")
                        .about("User management")
                        .subcommand(
                            Command::new("create")
                                .about("Create a new user")
                                .arg(
                                    Arg::new("username")
                                        .short('u')
                                        .long("username")
                                        .value_name("USERNAME")
                                        .help("Username")
                                        .required(true),
                                )
                                .arg(
                                    Arg::new("password")
                                        .short('p')
                                        .long("password")
                                        .value_name("PASSWORD")
                                        .help("Password")
                                        .required(true),
                                )
                                .arg(
                                    Arg::new("roles")
                                        .short('r')
                                        .long("roles")
                                        .value_name("ROLES")
                                        .help("Comma-separated list of roles")
                                        .default_value("user"),
                                ),
                        )
                        .subcommand(Command::new("list").about("List all users"))
                        .subcommand(
                            Command::new("delete").about("Delete a user").arg(
                                Arg::new("username")
                                    .short('u')
                                    .long("username")
                                    .value_name("USERNAME")
                                    .help("Username to delete")
                                    .required(true),
                            ),
                        ),
                )
                .subcommand(
                    Command::new("role").about("Role management").subcommand(
                        Command::new("assign")
                            .about("Assign role to user")
                            .arg(
                                Arg::new("username")
                                    .short('u')
                                    .long("username")
                                    .value_name("USERNAME")
                                    .help("Username")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("role")
                                    .short('r')
                                    .long("role")
                                    .value_name("ROLE")
                                    .help("Role to assign")
                                    .required(true),
                            ),
                    ),
                )
                .subcommand(
                    Command::new("audit")
                        .about("View audit logs")
                        .arg(
                            Arg::new("user")
                                .short('u')
                                .long("user")
                                .value_name("USER")
                                .help("Filter by user"),
                        )
                        .arg(
                            Arg::new("action")
                                .short('a')
                                .long("action")
                                .value_name("ACTION")
                                .help("Filter by action"),
                        )
                        .arg(
                            Arg::new("since")
                                .long("since")
                                .value_name("TIME")
                                .help("Show logs since this time (e.g., '1h', '30m', '2d')"),
                        ),
                ),
        )
        // Metrics and monitoring
        .subcommand(
            Command::new("metrics")
                .about("View system metrics")
                .arg(
                    Arg::new("type")
                        .short('t')
                        .long("type")
                        .value_name("TYPE")
                        .help("Metric type (broker, topic, consumer, system)")
                        .default_value("broker"),
                )
                .arg(
                    Arg::new("format")
                        .short('f')
                        .long("format")
                        .value_name("FORMAT")
                        .help("Output format (json, table, prometheus)")
                        .default_value("table"),
                )
                .arg(
                    Arg::new("watch")
                        .short('w')
                        .long("watch")
                        .help("Watch mode - refresh every N seconds")
                        .value_name("SECONDS"),
                ),
        )
        // Load testing
        .subcommand(
            Command::new("load-test")
                .about("Run load tests")
                .arg(
                    Arg::new("topic")
                        .short('t')
                        .long("topic")
                        .value_name("TOPIC")
                        .help("Target topic")
                        .required(true),
                )
                .arg(
                    Arg::new("producers")
                        .short('p')
                        .long("producers")
                        .value_name("NUM")
                        .help("Number of producer threads")
                        .default_value("5"),
                )
                .arg(
                    Arg::new("consumers")
                        .short('c')
                        .long("consumers")
                        .value_name("NUM")
                        .help("Number of consumer threads")
                        .default_value("5"),
                )
                .arg(
                    Arg::new("messages")
                        .short('m')
                        .long("messages")
                        .value_name("NUM")
                        .help("Total number of messages to send")
                        .default_value("10000"),
                )
                .arg(
                    Arg::new("message-size")
                        .short('s')
                        .long("message-size")
                        .value_name("BYTES")
                        .help("Message size in bytes")
                        .default_value("1024"),
                )
                .arg(
                    Arg::new("duration")
                        .short('d')
                        .long("duration")
                        .value_name("SECONDS")
                        .help("Test duration in seconds")
                        .default_value("60"),
                )
                .arg(
                    Arg::new("report")
                        .short('r')
                        .long("report")
                        .value_name("FILE")
                        .help("Save test report to file"),
                ),
        )
        .get_matches();

    let result = match matches.subcommand() {
        Some(("start", sub_matches)) => handle_start_command(sub_matches).await,
        Some(("stop", sub_matches)) => handle_stop_command(sub_matches).await,
        Some(("send", sub_matches)) => handle_send_command(sub_matches).await,
        Some(("consume", sub_matches)) => handle_consume_command(sub_matches).await,
        Some(("status", sub_matches)) => handle_status_command(sub_matches).await,

        Some(("topic", sub_matches)) => match sub_matches.subcommand() {
            Some(("create", create_matches)) => handle_topic_create_command(create_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            Some(("list", list_matches)) => {
                handle_topic_list_command(list_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    })
            }
            Some(("delete", delete_matches)) => handle_topic_delete_command(delete_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            Some(("describe", describe_matches)) => handle_topic_describe_command(describe_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            _ => Err(CliError::InvalidCommand("topic".to_string())),
        },

        Some(("schema", sub_matches)) => match sub_matches.subcommand() {
            Some(("register", register_matches)) => {
                handle_schema_register_command(register_matches).await
            }
            Some(("list", list_matches)) => handle_schema_list_command(list_matches).await,
            Some(("get", get_matches)) => handle_schema_get_command(get_matches).await,
            Some(("validate", validate_matches)) => {
                handle_schema_validate_command(validate_matches).await
            }
            Some(("evolution", evolution_matches)) => {
                handle_schema_evolution_command(evolution_matches).await
            }
            Some(("compatibility", compat_matches)) => {
                handle_schema_compatibility_command(compat_matches).await
            }
            _ => Err(CliError::InvalidCommand("schema".to_string())),
        },

        Some(("group", sub_matches)) => match sub_matches.subcommand() {
            Some(("list", list_matches)) => {
                handle_group_list_command(list_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    })
            }
            Some(("describe", describe_matches)) => handle_group_describe_command(describe_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            Some(("reset", reset_matches)) => handle_group_reset_command(reset_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            _ => Err(CliError::InvalidCommand("group".to_string())),
        },

        Some(("security", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("auth", auth_matches)) => handle_auth_setup_command(auth_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    }),
                Some(("acl", acl_matches)) => {
                    handle_acl_command(acl_matches)
                        .await
                        .map_err(|e| CliError::BrokerError {
                            kind: crate::error::BrokerErrorKind::Unknown,
                            message: e.to_string(),
                        })
                }
                Some(("token", token_matches)) => handle_token_command(token_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    }),
                Some(("cert", cert_matches)) => {
                    handle_cert_command(cert_matches)
                        .await
                        .map_err(|e| CliError::BrokerError {
                            kind: crate::error::BrokerErrorKind::Unknown,
                            message: e.to_string(),
                        })
                }
                _ => Err(CliError::InvalidCommand("security".to_string())),
            }
        }

        Some(("metrics", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("show", show_matches)) => handle_metrics_show_command(show_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    }),
                Some(("export", export_matches)) => handle_metrics_export_command(export_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    }),
                Some(("health", health_matches)) => handle_health_command(health_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    }),
                Some(("performance", perf_matches)) => handle_performance_command(perf_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    }),
                Some(("alerts", alert_matches)) => handle_alerts_command(alert_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    }),
                _ => Err(CliError::InvalidCommand("metrics".to_string())),
            }
        }
        Some(("load-test", sub_matches)) => match sub_matches.subcommand() {
            Some(("producer", producer_matches)) => handle_producer_test_command(producer_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            Some(("consumer", consumer_matches)) => handle_consumer_test_command(consumer_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            Some(("e2e", e2e_matches)) => {
                handle_e2e_test_command(e2e_matches)
                    .await
                    .map_err(|e| CliError::BrokerError {
                        kind: crate::error::BrokerErrorKind::Unknown,
                        message: e.to_string(),
                    })
            }
            Some(("stress", stress_matches)) => handle_stress_test_command(stress_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            Some(("monitor", monitor_matches)) => handle_monitor_command(monitor_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            Some(("results", results_matches)) => handle_results_command(results_matches)
                .await
                .map_err(|e| CliError::BrokerError {
                    kind: crate::error::BrokerErrorKind::Unknown,
                    message: e.to_string(),
                }),
            _ => Err(CliError::InvalidCommand("load-test".to_string())),
        },

        Some((cmd, _)) => Err(CliError::UnknownCommand(cmd.to_string())),
        None => Err(CliError::NoCommand),
    };

    if let Err(err) = result {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    }

    Ok(())
}

// Fixed CLI Tests for Pilgrimage

#[cfg(test)]
mod tests {
    use clap::{Arg, Command};
    use std::fs;
    use std::path::Path;

    // Helper function to cleanup test directories
    fn cleanup_test_dir(dir: &str) {
        if Path::new(dir).exists() {
            let _ = fs::remove_dir_all(dir);
        }
    }

    // Helper function to cleanup PID files
    #[allow(dead_code)]
    fn cleanup_pid_file(broker_id: &str) {
        let pid_file = format!("{}.pid", broker_id);
        if Path::new(&pid_file).exists() {
            let _ = fs::remove_file(&pid_file);
        }
    }

    // === Command Parsing Tests ===

    #[test]
    fn test_start_command_basic() {
        let test_dir = "test_storage_basic";
        cleanup_test_dir(test_dir);

        let matches = create_test_app().get_matches_from(vec![
            "test",
            "start",
            "--id",
            "broker_basic",
            "--storage",
            test_dir,
        ]);

        if let Some(("start", start_matches)) = matches.subcommand() {
            assert_eq!(
                start_matches.get_one::<String>("id"),
                Some(&"broker_basic".to_string())
            );
            assert_eq!(
                start_matches.get_one::<String>("storage"),
                Some(&test_dir.to_string())
            );
        }

        cleanup_test_dir(test_dir);
    }

    #[test]
    fn test_start_command_with_cluster_mode() {
        let test_dir = "test_storage_cluster";
        cleanup_test_dir(test_dir);

        let matches = create_test_app().get_matches_from(vec![
            "test",
            "start",
            "--id",
            "broker_cluster",
            "--storage",
            test_dir,
            "--cluster",
            "--nodes",
            "node1:8081,node2:8082",
        ]);

        if let Some(("start", start_matches)) = matches.subcommand() {
            assert_eq!(
                start_matches.get_one::<String>("id"),
                Some(&"broker_cluster".to_string())
            );
            assert!(start_matches.get_flag("cluster-mode"));
            assert_eq!(
                start_matches.get_one::<String>("cluster-nodes"),
                Some(&"node1:8081,node2:8082".to_string())
            );
        }

        cleanup_test_dir(test_dir);
    }

    #[test]
    fn test_send_command_basic() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "send",
            "--topic",
            "test_topic",
            "--message",
            "test_message",
        ]);

        if let Some(("send", send_matches)) = matches.subcommand() {
            assert_eq!(
                send_matches.get_one::<String>("topic"),
                Some(&"test_topic".to_string())
            );
            assert_eq!(
                send_matches.get_one::<String>("message"),
                Some(&"test_message".to_string())
            );
        }
    }

    #[test]
    fn test_send_command_with_options() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "send",
            "--topic",
            "test_topic",
            "--message",
            "test_message",
            "--partition",
            "2",
            "--key",
            "test_key",
        ]);

        if let Some(("send", send_matches)) = matches.subcommand() {
            assert_eq!(
                send_matches.get_one::<String>("topic"),
                Some(&"test_topic".to_string())
            );
            assert_eq!(
                send_matches.get_one::<String>("message"),
                Some(&"test_message".to_string())
            );
            assert_eq!(
                send_matches.get_one::<String>("partition"),
                Some(&"2".to_string())
            );
            assert_eq!(
                send_matches.get_one::<String>("key"),
                Some(&"test_key".to_string())
            );
        }
    }

    #[test]
    fn test_consume_command_basic() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "consume",
            "--topic",
            "test_topic",
            "--group",
            "test_group",
        ]);

        if let Some(("consume", consume_matches)) = matches.subcommand() {
            assert_eq!(
                consume_matches.get_one::<String>("topic"),
                Some(&"test_topic".to_string())
            );
            assert_eq!(
                consume_matches.get_one::<String>("group"),
                Some(&"test_group".to_string())
            );
        }
    }

    #[test]
    fn test_topic_create_command() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "topic",
            "create",
            "--name",
            "new_topic",
            "--partitions",
            "6",
            "--replication",
            "3",
        ]);

        if let Some(("topic", topic_matches)) = matches.subcommand() {
            if let Some(("create", create_matches)) = topic_matches.subcommand() {
                assert_eq!(
                    create_matches.get_one::<String>("name"),
                    Some(&"new_topic".to_string())
                );
                assert_eq!(
                    create_matches.get_one::<String>("partitions"),
                    Some(&"6".to_string())
                );
                assert_eq!(
                    create_matches.get_one::<String>("replication"),
                    Some(&"3".to_string())
                );
            }
        }
    }

    #[test]
    fn test_topic_list_command() {
        let matches = create_test_app().get_matches_from(vec!["test", "topic", "list"]);

        if let Some(("topic", topic_matches)) = matches.subcommand() {
            assert!(topic_matches.subcommand_matches("list").is_some());
        }
    }

    #[test]
    fn test_topic_delete_command() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "topic",
            "delete",
            "--name",
            "old_topic",
        ]);

        if let Some(("topic", topic_matches)) = matches.subcommand() {
            if let Some(("delete", delete_matches)) = topic_matches.subcommand() {
                assert_eq!(
                    delete_matches.get_one::<String>("name"),
                    Some(&"old_topic".to_string())
                );
            }
        }
    }

    #[test]
    fn test_stop_command() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "stop",
            "--id",
            "broker_test",
            "--force",
            "--timeout",
            "30",
        ]);

        if let Some(("stop", stop_matches)) = matches.subcommand() {
            assert_eq!(
                stop_matches.get_one::<String>("id"),
                Some(&"broker_test".to_string())
            );
            assert!(stop_matches.get_flag("force"));
            assert_eq!(
                stop_matches.get_one::<String>("timeout"),
                Some(&"30".to_string())
            );
        }
    }

    #[test]
    fn test_status_command() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "status",
            "--id",
            "broker1",
            "--detailed",
        ]);

        if let Some(("status", status_matches)) = matches.subcommand() {
            assert_eq!(
                status_matches.get_one::<String>("id"),
                Some(&"broker1".to_string())
            );
            assert!(status_matches.get_flag("detailed"));
        }
    }

    #[test]
    fn test_global_options() {
        let matches = create_test_app().get_matches_from(vec![
            "test",
            "--config",
            "test.toml",
            "--verbose",
            "--verbose",
            "--host",
            "127.0.0.1",
            "--port",
            "9090",
            "start",
            "--id",
            "broker_global",
        ]);

        assert_eq!(
            matches.get_one::<String>("config"),
            Some(&"test.toml".to_string())
        );
        assert_eq!(matches.get_count("verbose"), 2);
        assert_eq!(
            matches.get_one::<String>("host"),
            Some(&"127.0.0.1".to_string())
        );
        assert_eq!(matches.get_one::<String>("port"), Some(&"9090".to_string()));
    }

    #[test]
    fn test_config_file_parsing() {
        let config_content = r#"
[broker]
id = "test_broker"
partitions = 5
replication = 3

[network]
host = "localhost"
port = 8080
"#;

        let config_file = "test_config.toml";
        fs::write(config_file, config_content).unwrap();

        let matches = create_test_app().get_matches_from(vec![
            "test",
            "--config",
            config_file,
            "start",
            "--id",
            "broker1",
        ]);

        assert_eq!(
            matches.get_one::<String>("config"),
            Some(&config_file.to_string())
        );

        // Clean up
        let _ = fs::remove_file(config_file);
    }

    #[test]
    fn test_storage_directory_creation() {
        let test_dir = "test_storage_creation";
        cleanup_test_dir(test_dir);

        // Verify directory doesn't exist initially
        assert!(!Path::new(test_dir).exists());

        let matches = create_test_app().get_matches_from(vec![
            "test",
            "start",
            "--id",
            "broker_fs",
            "--storage",
            test_dir,
        ]);

        if let Some(("start", start_matches)) = matches.subcommand() {
            assert_eq!(
                start_matches.get_one::<String>("storage"),
                Some(&test_dir.to_string())
            );
        }

        cleanup_test_dir(test_dir);
    }

    // Helper function to create test app with same structure as main app
    fn create_test_app() -> Command<'static> {
        Command::new("test")
            .version("1.0.0")
            .about("Test CLI")
            // Global options
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .value_name("FILE")
                    .help("Configuration file path")
                    .global(true),
            )
            .arg(
                Arg::new("verbose")
                    .short('v')
                    .long("verbose")
                    .help("Enable verbose output")
                    .action(clap::ArgAction::Count)
                    .global(true),
            )
            .arg(
                Arg::new("host")
                    .short('H')
                    .long("host")
                    .value_name("HOST")
                    .help("Broker host address")
                    .default_value("localhost")
                    .global(true),
            )
            .arg(
                Arg::new("port")
                    .short('P')
                    .long("port")
                    .value_name("PORT")
                    .help("Broker port")
                    .default_value("8080")
                    .global(true),
            )
            // Start command
            .subcommand(
                Command::new("start")
                    .about("Start the message broker")
                    .arg(
                        Arg::new("id")
                            .short('i')
                            .long("id")
                            .value_name("ID")
                            .help("Broker instance ID")
                            .required(true),
                    )
                    .arg(
                        Arg::new("partitions")
                            .short('p')
                            .long("partitions")
                            .value_name("NUM")
                            .help("Default number of partitions for new topics")
                            .default_value("3"),
                    )
                    .arg(
                        Arg::new("replication")
                            .short('r')
                            .long("replication")
                            .value_name("NUM")
                            .help("Default replication factor")
                            .default_value("2"),
                    )
                    .arg(
                        Arg::new("storage")
                            .short('s')
                            .long("storage")
                            .value_name("PATH")
                            .help("Storage directory path")
                            .default_value("./storage"),
                    )
                    .arg(
                        Arg::new("cluster-mode")
                            .long("cluster")
                            .help("Enable cluster mode")
                            .action(clap::ArgAction::SetTrue),
                    )
                    .arg(
                        Arg::new("cluster-nodes")
                            .long("nodes")
                            .value_name("NODES")
                            .help("Comma-separated list of cluster nodes (host:port)")
                            .requires("cluster-mode"),
                    ),
            )
            // Send command
            .subcommand(
                Command::new("send")
                    .about("Send message to a topic")
                    .arg(
                        Arg::new("topic")
                            .short('t')
                            .long("topic")
                            .value_name("TOPIC")
                            .help("Target topic name")
                            .required(true),
                    )
                    .arg(
                        Arg::new("message")
                            .short('m')
                            .long("message")
                            .value_name("MESSAGE")
                            .help("Message content")
                            .required(true),
                    )
                    .arg(
                        Arg::new("key")
                            .short('k')
                            .long("key")
                            .value_name("KEY")
                            .help("Message key for partitioning"),
                    )
                    .arg(
                        Arg::new("partition")
                            .short('p')
                            .long("partition")
                            .value_name("NUM")
                            .help("Target partition number"),
                    ),
            )
            // Consume command
            .subcommand(
                Command::new("consume")
                    .about("Consume messages from a topic")
                    .arg(
                        Arg::new("topic")
                            .short('t')
                            .long("topic")
                            .value_name("TOPIC")
                            .help("Topic name to consume from")
                            .required(true),
                    )
                    .arg(
                        Arg::new("group")
                            .short('g')
                            .long("group")
                            .value_name("GROUP")
                            .help("Consumer group ID")
                            .required(true),
                    )
                    .arg(
                        Arg::new("partition")
                            .short('p')
                            .long("partition")
                            .value_name("NUM")
                            .help("Specific partition to consume from"),
                    )
                    .arg(
                        Arg::new("offset")
                            .short('o')
                            .long("offset")
                            .value_name("OFFSET")
                            .help("Starting offset"),
                    ),
            )
            // Topic management
            .subcommand(
                Command::new("topic")
                    .about("Topic management operations")
                    .subcommand(
                        Command::new("create")
                            .about("Create a new topic")
                            .arg(
                                Arg::new("name")
                                    .short('n')
                                    .long("name")
                                    .value_name("NAME")
                                    .help("Topic name")
                                    .required(true),
                            )
                            .arg(
                                Arg::new("partitions")
                                    .short('p')
                                    .long("partitions")
                                    .value_name("NUM")
                                    .help("Number of partitions")
                                    .default_value("3"),
                            )
                            .arg(
                                Arg::new("replication")
                                    .short('r')
                                    .long("replication")
                                    .value_name("NUM")
                                    .help("Replication factor")
                                    .default_value("2"),
                            ),
                    )
                    .subcommand(Command::new("list").about("List all topics"))
                    .subcommand(
                        Command::new("delete").about("Delete a topic").arg(
                            Arg::new("name")
                                .short('n')
                                .long("name")
                                .value_name("NAME")
                                .help("Topic name")
                                .required(true),
                        ),
                    ),
            )
            // Stop command
            .subcommand(
                Command::new("stop")
                    .about("Stop the message broker")
                    .arg(
                        Arg::new("id")
                            .short('i')
                            .long("id")
                            .value_name("ID")
                            .help("Broker instance ID")
                            .required(true),
                    )
                    .arg(
                        Arg::new("force")
                            .short('f')
                            .long("force")
                            .help("Force stop without graceful shutdown")
                            .action(clap::ArgAction::SetTrue),
                    )
                    .arg(
                        Arg::new("timeout")
                            .short('t')
                            .long("timeout")
                            .value_name("SECONDS")
                            .help("Graceful shutdown timeout")
                            .default_value("10"),
                    ),
            )
            // Status command
            .subcommand(
                Command::new("status")
                    .about("Check broker status")
                    .arg(
                        Arg::new("id")
                            .short('i')
                            .long("id")
                            .value_name("ID")
                            .help("Broker instance ID"),
                    )
                    .arg(
                        Arg::new("detailed")
                            .short('d')
                            .long("detailed")
                            .help("Show detailed status information")
                            .action(clap::ArgAction::SetTrue),
                    ),
            )
    }
}
