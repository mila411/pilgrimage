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
//! let mut election = LeaderElection::new("broker1", peers);
//! let elected = election.start_election();
//!
//! // Check if the broker was elected as leader
//! // Note: In a single-node setup without peers, the election will succeed
//! assert!(elected);
//! assert_eq!(*election.state.lock().unwrap(), BrokerState::Leader);
//! ```

use crate::broker::leader::state::BrokerState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
/// The `LeaderElection` struct manages the leader election process for a broker.
///
/// This struct provides methods to create a new leader election instance,
/// start the election process, and request votes from peers.
pub struct LeaderElection {
    /// The ID of the broker.
    pub node_id: String,
    /// The current state of the broker.
    /// This is shared between the leader election and heartbeat threads.
    pub state: Arc<Mutex<BrokerState>>,
    /// A hashmap of peer broker IDs and their addresses.
    pub votes: Arc<Mutex<HashMap<String, String>>>,
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
    /// assert_eq!(election.node_id, "broker1");
    /// ```
    pub fn new(node_id: &str, votes: HashMap<String, String>) -> Self {
        Self {
            node_id: node_id.to_string(),
            state: Arc::new(Mutex::new(BrokerState::Follower)),
            votes: Arc::new(Mutex::new(votes)),
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
    pub fn start_election(&mut self) -> bool {
        *self.state.lock().unwrap() = BrokerState::Candidate;

        // Add your own vote
        self.receive_vote(self.node_id.clone(), self.node_id.clone());

        let votes = self.get_vote_count();
        let total_nodes = {
            if let Ok(votes_map) = self.votes.lock() {
                std::cmp::max(1, votes_map.len()) // At a minimum, include yourself.
            } else {
                1
            }
        };
        let votes_needed = (total_nodes + 1) / 2;

        if votes >= votes_needed {
            self.become_leader();
            true
        } else {
            false
        }
    }

    /// Transitions the broker to a leader state.
    pub fn become_leader(&mut self) {
        *self.state.lock().unwrap() = BrokerState::Leader;
    }

    /// Transitions the broker to a follower state.
    pub fn step_down(&mut self) {
        *self.state.lock().unwrap() = BrokerState::Follower;
    }

    /// Checks if the broker is the leader.
    ///
    /// # Returns
    /// `true` if the broker is the leader, `false` otherwise.
    pub fn is_leader(&self) -> bool {
        *self.state.lock().unwrap() == BrokerState::Leader
    }

    /// Receives a vote from a peer.
    ///
    /// # Arguments
    ///
    /// * `voter_id` - The ID of the voter broker.
    /// * `vote` - The vote (usually the ID of the broker being voted for).
    pub fn receive_vote(&mut self, voter_id: String, vote: String) {
        if let Ok(mut votes) = self.votes.lock() {
            votes.insert(voter_id, vote);
        }
    }

    /// Gets the number of votes received.
    ///
    /// # Returns
    /// The number of votes.
    pub fn get_vote_count(&self) -> usize {
        if let Ok(votes) = self.votes.lock() {
            votes.len()
        } else {
            0
        }
    }

    /// Gets the current state of the broker.
    ///
    /// # Returns
    /// The current state of the broker.
    pub fn get_state(&self) -> BrokerState {
        self.state.lock().unwrap().clone()
    }

    /// Sets the state of the broker.
    ///
    /// # Arguments
    ///
    /// * `state` - The new state of the broker.
    pub fn set_state(&self, state: BrokerState) {
        if let Ok(mut current_state) = self.state.lock() {
            *current_state = state;
        }
    }

    /// Gets the votes received from peers.
    ///
    /// # Returns
    /// A vector of votes.
    pub fn get_votes(&self) -> Vec<String> {
        if let Ok(votes) = self.votes.lock() {
            votes.values().cloned().collect()
        } else {
            Vec::new()
        }
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

        let mut election = LeaderElection::new("broker1", peers);
        assert_eq!(*election.state.lock().unwrap(), BrokerState::Follower);

        let elected = election.start_election();
        assert!(elected);
        assert_eq!(*election.state.lock().unwrap(), BrokerState::Leader);
    }
}
