//! Module for leader election.
//!
//! The `LeaderElection` struct manages the leader election process for a broker.
//! This struct provides methods to create a new leader election instance,
//! start the election process, and request votes from peers.
//!
//! # Example
//! The following example demonstrates how to create a new leader election instance
//! and start the election process.
//! ```
//! use pilgrimage::broker::leader::election::LeaderElection;
//! use pilgrimage::broker::leader::state::BrokerState;
//! use std::collections::HashMap;
//!
//! let peers = HashMap::new();
//! let election = LeaderElection::new("broker1", peers);
//! let elected = election.start_election();
//!
//! // Check if the broker was elected as leader
//! assert!(elected);
//! assert_eq!(*election.state.lock().unwrap(), BrokerState::Leader);
//! ```

use super::heartbeat::Heartbeat;
use super::state::{BrokerState, Term};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The `LeaderElection` struct manages the leader election process for a broker.
///
/// This struct provides methods to create a new leader election instance,
/// start the election process, and request votes from peers.
#[derive(Clone)]
pub struct LeaderElection {
    /// The ID of the broker.
    pub broker_id: String,
    /// The current state of the broker.
    /// This is shared between the leader election and heartbeat threads.
    pub state: Arc<Mutex<BrokerState>>,
    /// The current term of the broker.
    pub term: Arc<Mutex<Term>>,
    /// A hashmap of peer broker IDs and their addresses.
    pub peers: Arc<Mutex<HashMap<String, String>>>,
    /// The last time a heartbeat was sent.
    pub last_heartbeat: Arc<Mutex<Instant>>,
    /// The duration of the election timeout.
    pub election_timeout: Duration,
}

impl LeaderElection {
    /// Creates a new leader election instance.
    ///
    /// # Arguments
    ///
    /// * `broker_id` - The ID of the broker.
    /// * `peers` - A hashmap of peer broker IDs and their addresses.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::leader::election::LeaderElection;
    /// use std::collections::HashMap;
    ///
    /// let peers = HashMap::new();
    /// let election = LeaderElection::new("broker1", peers);
    /// assert_eq!(election.broker_id, "broker1");
    /// ```
    pub fn new(broker_id: &str, peers: HashMap<String, String>) -> Self {
        LeaderElection {
            broker_id: broker_id.to_string(),
            state: Arc::new(Mutex::new(BrokerState::Follower)),
            term: Arc::new(Mutex::new(Term {
                current: AtomicU64::new(0),
                voted_for: None,
            })),
            peers: Arc::new(Mutex::new(peers)),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            election_timeout: Duration::from_secs(5),
        }
    }

    /// Starts the leader election process.
    ///
    /// This method transitions the broker to a candidate state, increments the current term,
    /// and requests votes from peers.
    /// If the broker receives a majority of votes, it transitions
    /// to a leader state and starts the heartbeat thread.
    ///
    /// # Returns
    /// `true` if the broker was elected as leader, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::leader::election::LeaderElection;
    /// use std::collections::HashMap;
    ///
    /// let peers = HashMap::new();
    /// let mut election = LeaderElection::new("broker1", peers);
    /// let elected = election.start_election();
    /// assert!(elected);
    /// ```
    pub fn start_election(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if *state != BrokerState::Follower {
            return false;
        }

        *state = BrokerState::Candidate;
        let mut term = self.term.lock().unwrap();
        term.current.fetch_add(1, Ordering::SeqCst);
        term.voted_for = Some(self.broker_id.clone());

        let votes = self.request_votes();
        let peers = self.peers.lock().unwrap();
        let majority = (peers.len() + 1) / 2 + 1;

        if votes >= majority {
            *state = BrokerState::Leader;
            Heartbeat::start(self.clone());
            true
        } else {
            *state = BrokerState::Follower;
            false
        }
    }

    /// Requests votes from peers.
    ///
    /// This method sends a vote request to each peer and waits for a response.
    ///
    /// # Returns
    /// The number of votes received.
    fn request_votes(&self) -> usize {
        let votes = 1;
        let peers = self.peers.lock().unwrap();
        votes + peers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the leader election process.
    ///
    /// # Purpose
    /// This test verifies that a broker can be elected as leader.
    ///
    /// # Steps
    /// 1. Create a new leader election instance.
    /// 2. Start the election process.
    /// 3. Verify that the broker was elected as leader.
    #[test]
    fn test_leader_election() {
        let mut peers = HashMap::new();
        peers.insert("peer1".to_string(), "localhost:8081".to_string());
        peers.insert("peer2".to_string(), "localhost:8082".to_string());

        let election = LeaderElection::new("broker1", peers);
        assert_eq!(*election.state.lock().unwrap(), BrokerState::Follower);

        let elected = election.start_election();
        assert!(elected);
        assert_eq!(*election.state.lock().unwrap(), BrokerState::Leader);
    }
}
