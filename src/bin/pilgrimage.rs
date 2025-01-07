use clap::{App, Arg, SubCommand};
use pilgrimage::broker::{Broker, Node};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    let matches = App::new("Pilgrimage")
        .version("1.0")
        .author("Your Name")
        .about("Message Broker CLI")
        .subcommand(
            SubCommand::with_name("start")
                .about("Start the broker")
                .arg(
                    Arg::new("id")
                        .short('i')
                        .long("id")
                        .value_name("ID")
                        .help("Sets the broker ID")
                        .required(true),
                )
                .arg(
                    Arg::new("partitions")
                        .short('p')
                        .long("partitions")
                        .value_name("PARTITIONS")
                        .help("Sets the number of partitions")
                        .required(true),
                )
                .arg(
                    Arg::new("replication")
                        .short('r')
                        .long("replication")
                        .value_name("REPLICATION")
                        .help("Sets the replication factor")
                        .required(true),
                )
                .arg(
                    Arg::new("storage")
                        .short('s')
                        .long("storage")
                        .value_name("STORAGE")
                        .help("Sets the storage path")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("stop").about("Stop the broker").arg(
                Arg::new("id")
                    .short('i')
                    .long("id")
                    .value_name("ID")
                    .help("Sets the broker ID")
                    .required(true),
            ),
        )
        .subcommand(
            SubCommand::with_name("send")
                .about("Send a message")
                .arg(Arg::with_name("id").required(true))
                .arg(Arg::with_name("message").required(true)),
        )
        .subcommand(
            SubCommand::with_name("consume")
                .about("Consume message")
                .arg(Arg::with_name("id").required(true)),
        )
        .subcommand(
            SubCommand::with_name("status")
                .about("Check broker status")
                .arg(
                    Arg::new("id")
                        .short('i')
                        .long("id")
                        .value_name("ID")
                        .help("Sets the broker ID")
                        .required(true),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("start", start_matches)) => {
            let id = start_matches.value_of("id").unwrap();
            let partitions: usize = start_matches
                .value_of("partitions")
                .unwrap()
                .parse()
                .unwrap();
            let replication: usize = start_matches
                .value_of("replication")
                .unwrap()
                .parse()
                .unwrap();
            let storage_path = start_matches.value_of("storage").unwrap();

            println!("Starting broker {} with {} partitions...", id, partitions);
            let broker = Broker::new(id, partitions, replication, storage_path);

            let node = Node {
                id: "node1".to_string(),
                address: "127.0.0.1:8080".to_string(),
                is_active: true,
                data: Arc::new(Mutex::new(Vec::new())),
            };
            broker.add_node("node1".to_string(), node);

            loop {
                if let Some(message) = broker.receive_message() {
                    println!("Received: {}", message);
                } else {
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
        Some(("stop", stop_matches)) => {
            let id = stop_matches.value_of("id").unwrap();
            println!("Stopping broker {}...", id);
        }
        Some(("send", send_matches)) => {
            let id = send_matches.value_of("id").unwrap();
            let message = send_matches.value_of("message").unwrap();
            println!("Sending '{}' to broker {}...", message, id);
        }
        Some(("consume", consume_matches)) => {
            let id = consume_matches.value_of("id").unwrap();
            println!("Consuming messages from broker {}...", id);
        }
        Some(("status", status_matches)) => {
            let id = status_matches.value_of("id").unwrap();
            println!("Checking status of broker {}...", id);
        }
        _ => {
            println!("Invalid command. Use --help for usage information.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{App, Arg, SubCommand};

    #[test]
    fn test_start_command() {
        let matches = App::new("test")
            .subcommand(
                SubCommand::with_name("start")
                    .arg(Arg::with_name("id").required(true))
                    .arg(Arg::with_name("partitions").required(true))
                    .arg(Arg::with_name("replication").required(true))
                    .arg(Arg::with_name("storage").required(true)),
            )
            .get_matches_from(vec![
                "test",
                "start",
                "broker1",
                "3",
                "2",
                "/tmp/storage_broker1.db",
            ]);

        match matches.subcommand() {
            Some(("start", start_matches)) => {
                let id = start_matches.value_of("id").unwrap();
                let partitions: usize = start_matches
                    .value_of("partitions")
                    .unwrap()
                    .parse()
                    .unwrap();
                let replication: usize = start_matches
                    .value_of("replication")
                    .unwrap()
                    .parse()
                    .unwrap();
                let storage = start_matches.value_of("storage").unwrap();

                println!(
                    "Starting broker {} with {} partitions, replication factor of {}, and storage path {}...",
                    id, partitions, replication, storage
                );

                let broker = Broker::new(id, partitions, replication, storage);
                let broker = Arc::new(Mutex::new(broker));

                let broker_clone = Arc::clone(&broker);
                thread::spawn(move || {
                    let _broker = broker_clone.lock().unwrap();
                    println!("Broker is running...");
                });

                thread::sleep(Duration::from_secs(1)); // Simulate some work
                println!("Broker {} started.", id);
            }
            _ => {}
        }
    }

    #[test]
    fn test_stop_command() {
        let matches = App::new("test")
            .subcommand(SubCommand::with_name("stop").arg(Arg::with_name("id").required(true)))
            .get_matches_from(vec!["test", "stop", "broker1"]);

        match matches.subcommand() {
            Some(("stop", stop_matches)) => {
                let id = stop_matches.value_of("id").unwrap();
                println!("Stopping broker {}...", id);
            }
            _ => {}
        }
    }
}
