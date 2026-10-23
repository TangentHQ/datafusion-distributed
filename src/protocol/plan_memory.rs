//! Query-scoped owner for serialized physical-plan envelopes. Custom codecs
//! may transfer their output charge here before returning bytes to the generic
//! DataFusion codec API, which has no owned-byte return slot.
use datafusion::common::Result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct PlanMemory(MemoryReservation);
impl PlanMemory {
    pub(crate) fn new(pool: &Arc<dyn MemoryPool>) -> Self {
        Self(MemoryConsumer::new("DistributedPlanEnvelope").register(pool))
    }
    /// Keep an additional encoded allocation admitted through this task's
    /// coordinator stream or worker TaskContext lifetime.
    pub(crate) fn retain(&self, bytes: usize) -> Result<()> {
        self.0.try_grow(bytes)
    }
}
