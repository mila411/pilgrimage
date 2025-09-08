//! Module for the heartbeat mechanism of the broker leader.
//!
//! The `Heartbeat` struct manages the heartbeat mechanism for a broker.
//! This struct provides methods to create a new heartbeat instance,
//! start the heartbeat mechanism, send heartbeats to peers, and check for timeouts.
//!
//! The heartbeat is sent every 500 milliseconds and monitored every 100 milliseconds.

use super::election::LeaderElection;
use super::state::BrokerState;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The `Heartbeat` struct manages the heartbeat mechanism for a broker.
///
/// This struct provides methods to create a new heartbeat instance,
/// start the heartbeat mechanism, send heartbeats to peers, and check for timeouts.
pub struct Heartbeat {
    pub last_beat: Arc<Mutex<Instant>>,
    pub timeout: Duration,
}

impl Heartbeat {
    /// Creates a new heartbeat instance.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The duration after which the heartbeat times out.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::leader::heartbeat::Heartbeat;
    /// use std::time::Duration;
    ///
    /// let heartbeat = Heartbeat::new(Duration::from_secs(1));
    /// assert!(heartbeat.last_beat.lock().unwrap().elapsed() < Duration::from_secs(1));
    /// ```
    pub fn new(timeout: Duration) -> Self {
        Heartbeat {
            last_beat: Arc::new(Mutex::new(Instant::now())),
            timeout,
        }
    }

    /// Starts the heartbeat mechanism.
    ///
    /// This method starts two threads:
    /// 1. A thread to send heartbeats to peers.
    /// 2. A thread to monitor the heartbeat and start an election if the heartbeat times out.
    ///
    /// The heartbeat is sent every 500 milliseconds and monitored every 100 milliseconds.
    ///
    /// # Arguments
    ///
    /// * `election` - The leader election instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::leader::heartbeat::Heartbeat;
    /// use pilgrimage::broker::leader::election::LeaderElection;
    /// use std::collections::HashMap;
    /// use std::time::Duration;
    /// use std::sync::{Arc, Mutex};
    ///
    /// let peers = HashMap::new();
    /// let election = Arc::new(Mutex::new(LeaderElection::new("broker1", peers)));
    /// Heartbeat::start(election);
    /// ```
    pub fn start(election: Arc<Mutex<LeaderElection>>) {
        let heartbeat = Arc::new(Self::new(Duration::from_secs(1)));

        // Heartbeat Transmission Thread
        let send_election = election.clone();
        let send_heartbeat = heartbeat.clone();
        std::thread::spawn(move || {
            loop {
                {
                    let election_guard = send_election.lock().unwrap();
                    if *election_guard.state.lock().unwrap() != BrokerState::Leader {
                        break;
                    }
                    // For now, we pass None for transport until we integrate it
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(Self::send_heartbeat(&election_guard, None, 1));
                }
                *send_heartbeat.last_beat.lock().unwrap() = Instant::now();
                std::thread::sleep(Duration::from_millis(500));
            }
        });

        // Heartbeat monitoring thread
        let monitor_election = election;
        let monitor_heartbeat = heartbeat;
        std::thread::spawn(move || {
            loop {
                {
                    let mut election_guard = monitor_election.lock().unwrap();
                    if *election_guard.state.lock().unwrap() == BrokerState::Leader {
                        break;
                    }
                    if Self::check_timeout(&monitor_heartbeat) {
                        election_guard.start_election();
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        });
    }

    /// Sends a heartbeat to the peers.
    ///
    /// This method sends a heartbeat to all the peers in the leader election.
    /// Now implements actual network communication through the distributed messaging system.
    ///
    /// # Arguments
    ///
    /// * `election` - The leader election instance.
    /// * `transport` - The network transport for sending messages.
    /// * `term` - The current term number.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::leader::heartbeat::Heartbeat;
    /// use pilgrimage::broker::leader::election::LeaderElection;
    /// use std::collections::HashMap;
    ///
    /// let peers = HashMap::new();
    /// let election = LeaderElection::new("broker1", peers);
    /// // Heartbeat::send_heartbeat(&election, &transport, 1).await;
    /// ```
    pub async fn send_heartbeat(
        election: &LeaderElection,
        transport: Option<&crate::network::NetworkTransport>,
        term: u64,
    ) {
        if let Ok(votes) = election.votes.lock() {
            for (peer_id, _) in votes.iter() {
                if let Some(transport) = transport {
                    use crate::network::protocol::{DistributedMessage, MessageType, MessagePayload};

                    let heartbeat_msg = DistributedMessage::new(
                        election.node_id.clone(),
                        Some(peer_id.clone()),
                        MessageType::Heartbeat,
                        MessagePayload::Heartbeat {
                            term,
                            leader_id: election.node_id.clone(),
                            prev_log_index: 0, // Would be actual log index
                            prev_log_term: 0,  // Would be actual log term
                            entries: vec![],   // Would be actual log entries
                            leader_commit: 0,  // Would be actual commit index
                        },
                    );

                    if let Err(e) = transport.send_message(heartbeat_msg).await {
                        log::error!("Failed to send heartbeat to {}: {}", peer_id, e);
                    }
                } else {
                    log::debug!("No transport available for heartbeat to {}", peer_id);
                }
            }
        }
    }

    /// Checks if the heartbeat has timed out.
    ///
    /// This method checks if the heartbeat has timed out based on the timeout duration.
    ///
    /// # Returns
    /// * `bool` - Returns `true` if the heartbeat has timed out, otherwise `false`.
    fn check_timeout(&self) -> bool {
        let last = *self.last_beat.lock().unwrap();
        last.elapsed() > self.timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the timeout of the heartbeat.
    ///
    /// # Purpose
    /// The purpose of this test is to verify that the heartbeat times out
    /// after the specified duration.
    ///
    /// # Steps
    /// 1. Create a new heartbeat instance with a timeout of 100 milliseconds.
    /// 2. Sleep for 150 milliseconds.
    /// 3. Check if the heartbeat has timed out.
    #[test]
    fn test_heartbeat_timeout() {
        let heartbeat = Heartbeat::new(Duration::from_millis(100));
        std::thread::sleep(Duration::from_millis(150));
        assert!(heartbeat.check_timeout());
    }

    /// Tests the heartbeat within the timeout.
    ///
    /// # Purpose
    /// The purpose of this test is to verify that the heartbeat does not time out
    /// within the specified duration.
    ///
    /// # Steps
    /// 1. Create a new heartbeat instance with a timeout of 100 milliseconds.
    /// 2. Check if the heartbeat has timed out.
    #[test]
    fn test_heartbeat_within_timeout() {
        let heartbeat = Heartbeat::new(Duration::from_millis(100));
        assert!(!heartbeat.check_timeout());
    }
}
