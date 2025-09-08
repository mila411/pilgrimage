//! Module containing the schema version struct.
//!
//! The schema version is a three-part version number that follows the format `major.minor.patch`.
//!
//! Each part of the version number is an unsigned 32-bit integer.
//!
//! # Example
//! The following example demonstrates how to create a new schema version.
//! ```
//! use pilgrimage::schema::version::SchemaVersion;
//!
//! let version = SchemaVersion::new(1);
//! assert_eq!(version.major, 1);
//! assert_eq!(version.minor, 0);
//! assert_eq!(version.patch, 0);
//! ```

use core::fmt;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Struct representing a schema version. It follows the [Semantic Versioning][SV] format.
///
/// A schema version is a three-part version number that follows the format `major.minor.patch`.
///
/// Each part of the version number is an unsigned 32-bit integer.
///
/// [SV]: https://en.wikipedia.org/wiki/Software_versioning#Semantic_versioning
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SchemaVersion {
    /// The major version number.
    pub major: u32,
    /// The minor version number.
    pub minor: u32,
    /// The patch version number.
    pub patch: u32,
}

impl SchemaVersion {
    /// Creates a new `SchemaVersion` with the specified major version.
    ///
    /// # Arguments
    ///
    /// * `version` - The major version number.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let version = SchemaVersion::new(1);
    /// assert_eq!(version.major, 1);
    /// assert_eq!(version.minor, 0);
    /// assert_eq!(version.patch, 0);
    /// ```
    pub fn new(version: u32) -> Self {
        SchemaVersion {
            major: version,
            minor: 0,
            patch: 0,
        }
    }

    /// Creates a new `SchemaVersion` with the specified major, minor, and patch versions.
    ///
    /// # Arguments
    ///
    /// * `major` - The major version number.
    /// * `minor` - The minor version number.
    /// * `patch` - The patch version number.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let version = SchemaVersion::new_with_version(1, 2, 3);
    /// assert_eq!(version.major, 1);
    /// assert_eq!(version.minor, 2);
    /// assert_eq!(version.patch, 3);
    /// ```
    pub fn new_with_version(major: u32, minor: u32, patch: u32) -> Self {
        SchemaVersion {
            major,
            minor,
            patch,
        }
    }

    /// Increments the major version, resetting minor and patch versions to 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let mut version = SchemaVersion::new(1);
    /// version.increment_major();
    /// assert_eq!(version.major, 2);
    /// assert_eq!(version.minor, 0);
    /// assert_eq!(version.patch, 0);
    /// ```
    pub fn increment_major(&mut self) {
        self.major += 1;
        self.minor = 0;
        self.patch = 0;
    }

    /// Increments the minor version, resetting the patch version to 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let mut version = SchemaVersion::new(1);
    /// version.increment_minor();
    /// assert_eq!(version.minor, 1);
    /// assert_eq!(version.patch, 0);
    /// ```
    pub fn increment_minor(&mut self) {
        self.minor += 1;
        self.patch = 0;
    }

    /// Increments the patch version.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let mut version = SchemaVersion::new(1);
    /// version.increment_patch();
    /// assert_eq!(version.patch, 1);
    /// ```
    pub fn increment_patch(&mut self) {
        self.patch += 1;
    }
}

impl fmt::Display for SchemaVersion {
    /// Formats the schema version as a string.
    ///
    /// The format is `major.minor.patch`.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let version = SchemaVersion::new_with_version(1, 2, 3);
    /// assert_eq!(version.to_string(), "1.2.3");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl PartialOrd for SchemaVersion {
    /// Compares two schema versions.
    ///
    /// # Arguments
    /// * `other` - The other schema version to compare.
    ///
    /// # Returns
    /// * `Some(Ordering)` if the comparison is possible.
    ///   The ordering is based on the major, minor, and patch versions.
    /// * `None` if the comparison is not possible.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SchemaVersion {
    /// Compares two schema versions.
    ///
    /// The ordering is based on the major, minor, and patch versions.
    ///
    /// # Arguments
    /// * `other` - The other schema version to compare.
    ///
    /// # Returns
    /// * `Ordering` based on the major, minor, and patch versions.
    ///
    /// # Examples
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    /// use std::cmp::Ordering;
    ///
    /// let v1 = SchemaVersion::new_with_version(1, 0, 0);
    /// let v2 = SchemaVersion::new_with_version(1, 1, 0);
    /// let v3 = SchemaVersion::new_with_version(2, 0, 0);
    ///
    /// assert!(v1 < v2);
    /// assert!(v2 < v3);
    /// assert!(v1 < v3);
    /// ```
    fn cmp(&self, other: &Self) -> Ordering {
        match self.major.cmp(&other.major) {
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Equal => self.patch.cmp(&other.patch),
                ord => ord,
            },
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the creation of a new schema version.
    ///
    /// # Purpose
    /// This test ensures that a new schema version can be created.
    ///
    /// # Steps
    /// 1. Create a new schema version with a major version.
    /// 2. Verify the major, minor, and patch versions.
    #[test]
    fn test_version_creation() {
        let version = SchemaVersion::new(1);
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 0);
        assert_eq!(version.patch, 0);
    }

    /// Tests the incrementing of a schema version.
    ///
    /// # Purpose
    /// This test ensures that a schema version can be incremented.
    ///
    /// # Steps
    /// 1. Create a new schema version.
    /// 2. Increment the major version.
    /// 3. Increment the minor version.
    /// 4. Increment the patch version.
    /// 5. Verify the major, minor, and patch versions.
    #[test]
    fn test_version_increment() {
        let mut version = SchemaVersion::new(1);
        version.increment_minor();
        assert_eq!(version.to_string(), "1.1.0");
        version.increment_patch();
        assert_eq!(version.to_string(), "1.1.1");
        version.increment_major();
        assert_eq!(version.to_string(), "2.0.0");
    }

    /// Tests the comparison of schema versions.
    ///
    /// # Purpose
    /// This test ensures that schema versions can be compared.
    ///
    /// # Steps
    /// 1. Create three schema versions.
    /// 2. Compare the schema versions.
    /// 3. Verify the ordering of the schema versions.
    #[test]
    fn test_version_comparison() {
        let v1 = SchemaVersion::new_with_version(1, 0, 0);
        let v2 = SchemaVersion::new_with_version(1, 1, 0);
        let v3 = SchemaVersion::new_with_version(2, 0, 0);

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1 < v3);
    }
}
