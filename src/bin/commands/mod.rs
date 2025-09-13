pub mod consume;
pub mod schema;
pub mod send;
pub mod start;
pub mod status;
pub mod stop;
pub mod web;

// New command modules
pub mod topic;
pub mod group;
pub mod security;
pub mod metrics;
pub mod load_test;

// Authentication command modules
pub mod login;
pub mod logout;
pub mod whoami;

// Export existing command handlers
pub use consume::handle_consume_command;
pub use schema::{handle_schema_list_command, handle_schema_register_command, handle_schema_get_command, handle_schema_validate_command, handle_schema_evolution_command, handle_schema_compatibility_command};
pub use send::handle_send_command;
pub use start::handle_start_command;
pub use status::handle_status_command;
pub use stop::handle_stop_command;
pub use web::handle_web_command;

// Export new command handlers
pub use topic::{handle_topic_create_command, handle_topic_list_command, handle_topic_delete_command, handle_topic_describe_command};
pub use group::{handle_group_list_command, handle_group_describe_command, handle_group_reset_command};
pub use security::{handle_auth_setup_command, handle_acl_command, handle_token_command, handle_cert_command};
pub use metrics::{handle_metrics_show_command, handle_metrics_export_command, handle_health_command, handle_performance_command, handle_alerts_command};
pub use load_test::{handle_producer_test_command, handle_consumer_test_command, handle_e2e_test_command, handle_stress_test_command, handle_monitor_command, handle_results_command};

// Export authentication command handlers
pub use login::handle_login_command;
pub use logout::handle_logout_command;
pub use whoami::handle_whoami_command;
