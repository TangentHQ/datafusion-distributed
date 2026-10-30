//! Governed Flight decoding over Arrow's native IPC readers. Inspect IPC
//! lengths before decompression/alignment and keep dictionary owners in the
//! arrays they back, independently of queue credits and decoder lifetime.
use crate::common::memory::{allocation_bytes, hold_batch, schema_bytes};
use arrow_flight::FlightData;
use datafusion::{
    arrow::{
        array::{Array, ArrayData, ArrayRef, RecordBatch},
        buffer::Buffer,
        datatypes::{Field, Schema, SchemaRef},
    },
    common::{DataFusionError, Result, exec_datafusion_err},
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
};
use std::sync::Arc;

// Arrow IPC accepts its concrete standard-library dictionary table.
#[allow(clippy::disallowed_types)]
type Dictionaries = std::collections::HashMap<i64, ArrayRef>;

pub(super) struct Decoder {
    pool: Arc<dyn MemoryPool>,
    schema: Option<SchemaRef>,
    dictionaries: Dictionaries,
    metadata: Arc<MemoryReservation>,
}
impl Decoder {
    pub(super) fn new(pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            metadata: Arc::new(MemoryConsumer::new("FlightSchema").register(&pool)),
            pool,
            schema: None,
            dictionaries: Dictionaries::new(),
        }
    }
    pub(super) fn decode(
        &mut self,
        data: FlightData,
        covered: impl Fn(usize, usize) -> bool,
    ) -> Result<Option<RecordBatch>> {
        let message = arrow_ipc::root_as_message(&data.data_header)
            .map_err(|error| exec_datafusion_err!("Invalid Flight IPC header: {error}"))?;
        if message.header_type() == arrow_ipc::MessageHeader::Schema {
            let memory = Arc::new(MemoryConsumer::new("FlightSchema").register(&self.pool));
            memory.try_grow(
                data.data_header
                    .len()
                    .checked_mul(8)
                    .and_then(|bytes| bytes.checked_add(8192))
                    .ok_or_else(size)?,
            )?;
            let schema = Arc::new(Schema::try_from(&data)?);
            if schema_bytes(&schema) > memory.size() {
                return Err(exec_datafusion_err!(
                    "Flight schema exceeds its decode allowance"
                ));
            }
            memory.try_resize(schema_bytes(&schema))?;
            self.metadata = memory;
            self.schema = Some(schema);
            self.dictionaries.clear();
            return Ok(None);
        }
        if message.header_type() == arrow_ipc::MessageHeader::NONE {
            return Ok(None);
        }
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| exec_datafusion_err!("Flight data arrived before its schema"))?;
        let dictionary = message.header_as_dictionary_batch();
        let batch = message
            .header_as_record_batch()
            .or_else(|| dictionary.and_then(|dictionary| dictionary.data()))
            .ok_or_else(|| exec_datafusion_err!("Unsupported Flight IPC message"))?;
        let nodes = batch.nodes().map_or(0, |nodes| nodes.len());
        let overhead = nodes
            .checked_mul(512)
            .and_then(|bytes| bytes.checked_add(self.dictionaries.len().saturating_mul(256)))
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(size)?;
        let mut allowance = decoded_bytes(batch, &data.data_body)?
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(overhead))
            .ok_or_else(size)?;
        if dictionary.is_some_and(|dictionary| dictionary.isDelta()) {
            // Native delta dictionary concatenation can copy the old values.
            for values in self.dictionaries.values() {
                allowance = allowance
                    .checked_add(array_bytes(values)?.checked_mul(2).ok_or_else(size)?)
                    .ok_or_else(size)?;
            }
        }
        let memory = MemoryConsumer::new("FlightDecode").register(&self.pool);
        memory.try_grow(allowance)?;
        let buffer = Buffer::from(data.data_body.clone());
        if let Some(dictionary) = dictionary {
            let prior = self.dictionaries.clone();
            arrow_ipc::reader::read_dictionary(
                &buffer,
                dictionary,
                schema,
                &mut self.dictionaries,
                &message.version(),
            )?;
            let values = self
                .dictionaries
                .get(&dictionary.id())
                .cloned()
                .ok_or_else(|| exec_datafusion_err!("Flight dictionary was not decoded"))?;
            let table = RecordBatch::try_from_iter([("dictionary", values)])?;
            let bytes = allocation_bytes(&table, |start, length| {
                covered(start, length)
                    || prior
                        .values()
                        .any(|array| contains(&array.to_data(), start, length))
            });
            retain(&memory, bytes, overhead)?;
            let table = hold_batch(table, memory)?;
            self.dictionaries
                .insert(dictionary.id(), Arc::clone(table.column(0)));
            return Ok(None);
        }
        let table = arrow_ipc::reader::read_record_batch(
            &buffer,
            batch,
            Arc::clone(schema),
            &self.dictionaries,
            None,
            &message.version(),
        )?;
        let bytes = allocation_bytes(&table, |start, length| {
            covered(start, length)
                || self
                    .dictionaries
                    .values()
                    .any(|array| contains(&array.to_data(), start, length))
        });
        retain(&memory, bytes, overhead)?;
        Ok(Some(hold_batch(
            table,
            (memory, Arc::clone(&self.metadata)),
        )?))
    }
}
fn retain(memory: &MemoryReservation, bytes: usize, overhead: usize) -> Result<()> {
    let bytes = bytes.checked_add(overhead).ok_or_else(size)?;
    if bytes > memory.size() {
        return Err(exec_datafusion_err!(
            "Flight allocation exceeded its pre-decode allowance"
        ));
    }
    memory.try_resize(bytes)
}
pub(super) fn contains(data: &ArrayData, start: usize, length: usize) -> bool {
    data.buffers()
        .iter()
        .chain(data.nulls().map(|nulls| nulls.buffer()))
        .any(|buffer| {
            let base = buffer.as_ptr() as usize;
            start >= base
                && start.checked_add(length).is_some_and(|end| {
                    end <= base.saturating_add(buffer.len().max(buffer.capacity()))
                })
        })
        || data
            .child_data()
            .iter()
            .any(|child| contains(child, start, length))
}
fn array_bytes(array: &ArrayRef) -> Result<usize> {
    let table = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "dictionary",
            array.data_type().clone(),
            true,
        )])),
        vec![Arc::clone(array)],
    )?;
    Ok(allocation_bytes(&table, |_, _| false))
}
fn decoded_bytes(batch: arrow_ipc::RecordBatch<'_>, body: &[u8]) -> Result<usize> {
    let compressed = batch.compression().is_some();
    batch
        .buffers()
        .into_iter()
        .flatten()
        .try_fold(0usize, |bytes, buffer| {
            let offset = usize::try_from(buffer.offset()).map_err(|_| size())?;
            let length = usize::try_from(buffer.length()).map_err(|_| size())?;
            let end = offset.checked_add(length).ok_or_else(size)?;
            let buffer = body
                .get(offset..end)
                .ok_or_else(|| exec_datafusion_err!("Flight IPC buffer exceeds its body"))?;
            let length = if !compressed || length == 0 {
                length
            } else {
                let prefix: [u8; 8] = buffer
                    .get(..8)
                    .ok_or_else(|| exec_datafusion_err!("Compressed Flight buffer has no size"))?
                    .try_into()
                    .map_err(|_| size())?;
                let length = i64::from_le_bytes(prefix);
                if length == -1 {
                    buffer.len() - 8
                } else {
                    usize::try_from(length).map_err(|_| size())?
                }
            };
            bytes
                .checked_add(length.checked_add(63).ok_or_else(size)? & !63)
                .ok_or_else(size)
        })
}
fn size() -> DataFusionError {
    exec_datafusion_err!("Flight decoded size exceeds its address space")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::{DictionaryArray, StringArray},
            datatypes::Int32Type,
        },
        execution::memory_pool::GreedyMemoryPool,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use futures::StreamExt;

    async fn transfer(
        batches: Vec<RecordBatch>,
        compression: Option<arrow_ipc::CompressionType>,
        target: usize,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        let writer: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
        let schema = batches[0].schema();
        let source = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        let mut encoded =
            super::super::encode::encode(source, Arc::clone(&writer), compression, target)?;
        let mut decoder = Decoder::new(Arc::clone(&pool));
        let mut output = vec![];
        let mut largest = 0;
        while let Some(data) = encoded.next().await {
            let data = data
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                .into_data();
            largest = largest.max(data.data_body.len());
            let start = data.data_body.as_ptr() as usize;
            let length = data.data_body.len();
            if let Some(batch) = decoder.decode(data, |pointer, size| {
                pointer >= start
                    && pointer
                        .checked_add(size)
                        .is_some_and(|end| end <= start + length)
            })? {
                output.push(batch);
            }
        }
        drop(decoder);
        drop(encoded);
        Ok((output, largest))
    }

    #[tokio::test]
    async fn compressed_dictionaries_and_projected_children_keep_owned_allocations() {
        for compression in [
            None,
            Some(arrow_ipc::CompressionType::ZSTD),
            Some(arrow_ipc::CompressionType::LZ4_FRAME),
        ] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
            let values = [Some("alpha"), None, Some("omega"), Some("alpha")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>();
            let batch =
                RecordBatch::try_from_iter([("value", Arc::new(values) as ArrayRef)]).unwrap();
            let (mut actual, _) = transfer(
                vec![batch.clone(), batch.clone()],
                compression,
                1024,
                Arc::clone(&pool),
            )
            .await
            .unwrap();
            let expected = datafusion::arrow::compute::concat_batches(
                &batch.schema(),
                &[batch.clone(), batch.clone()],
            )
            .unwrap();
            let combined =
                datafusion::arrow::compute::concat_batches(&batch.schema(), &actual).unwrap();
            assert_eq!(combined, expected);
            // The fragment's original array keeps its owner independently of
            // the decoder, other fragments and queue permits.
            let child = actual.remove(0).column(0).slice(0, 1);
            drop(combined);
            drop(actual);
            assert!(pool.reserved() > 0);
            let values = child
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            assert_eq!(values.len(), 1);
            drop(child);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn wide_single_values_exceed_fragment_target_without_truncation() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let text = "x".repeat(1024 * 1024 + 1);
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef,
        )])
        .unwrap();
        let (actual, largest) = transfer(vec![batch.clone()], None, 1024, Arc::clone(&pool))
            .await
            .unwrap();
        assert!(largest > 1024 * 1024);
        assert_eq!(actual, vec![batch]);
        drop(actual);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn repeated_view_values_survive_fragmentation_without_gc_expansion() {
        use datafusion::arrow::array::{StringViewArray, UInt32Array};
        let value = StringViewArray::from(vec!["x".repeat(64 * 1024)]);
        let views =
            datafusion::arrow::compute::take(&value, &UInt32Array::from(vec![0; 128]), None)
                .unwrap();
        let batch = RecordBatch::try_from_iter([("value", views)]).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let (actual, _) = transfer(vec![batch.clone()], None, 1024, Arc::clone(&pool))
            .await
            .unwrap();
        let combined =
            datafusion::arrow::compute::concat_batches(&batch.schema(), &actual).unwrap();
        assert_eq!(combined, batch);
        drop(actual);
        drop(combined);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn compressed_body_is_rejected_before_expanding_past_the_pool() {
        let writer: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)])) as ArrayRef,
        )])
        .unwrap();
        let source = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter([Ok(batch)]),
        ));
        let mut encoded = super::super::encode::encode(
            source,
            writer,
            Some(arrow_ipc::CompressionType::ZSTD),
            1024 * 1024,
        )
        .unwrap();
        let mut decoder = Decoder::new(Arc::clone(&pool));
        let mut refused = false;
        while let Some(data) = encoded.next().await {
            match decoder.decode(data.unwrap().into_data(), |_, _| false) {
                Ok(None) => {}
                Err(DataFusionError::ResourcesExhausted(_)) => {
                    refused = true;
                    break;
                }
                result => panic!("compressed expansion escaped admission: {result:?}"),
            }
        }
        assert!(refused);
        drop(decoder);
        drop(encoded);
        assert_eq!(pool.reserved(), 0);
    }
}
