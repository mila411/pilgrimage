//! Module for the broker leader state.
//!
//! The `BrokerState` enum represents the different states a broker can be in.
//! A broker can be a follower, a candidate in an election, or a leader.
//!
//! The `Term` struct represents the term information for a broker.
//! It contains the current term number and the ID of the broker that received the vote.

use std::sync::atomic::AtomicU64;

/// The `BrokerState` enum represents the different states a broker can be in.
///
/// A broker can be a follower, a candidate in an election, or a leader.
#[derive(Debug, Clone, PartialEq)]
pub enum BrokerState {
    /// The broker is a follower, not leading an election or managing state.
    Follower,
    /// The broker is a candidate in an election, trying to become a leader.
    Candidate,
    /// The broker is the leader, managing the state and directing the followers.
    Leader,
}

/// The `Term` struct represents the term information for a broker.
///
/// It contains the current term number and the ID of the broker that received the vote.
pub struct Term {
    /// The current term number.
    pub current: AtomicU64,
    /// The ID of the broker that received the vote.
    pub voted_for: Option<String>,
}
