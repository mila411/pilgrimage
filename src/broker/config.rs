#[derive(Clone, Debug, Default)]
pub struct TopicConfig {
    pub num_partitions: usize,
    pub replication_factor: usize,
}
