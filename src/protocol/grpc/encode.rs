//! Pre-admitted Flight encoding. One source batch owns a bounded encoding
//! allowance shared by all its fragments; queue slots never own that charge.
use crate::common::memory::{allocation_bytes, hold_batch, hold_bytes, schema_bytes};
use arrow_flight::{
    FlightData,
    encode::{DictionaryHandling, FlightDataEncoderBuilder},
    error::FlightError,
};
use arrow_ipc::{CompressionType, writer::IpcWriteOptions};
use datafusion::{
    common::{Result, exec_datafusion_err},
    execution::{
        SendableRecordBatchStream,
        memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    },
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use std::sync::{Arc, Mutex};

pub(super) struct Encoded {
    pub(super) data: FlightData,
    memory: Arc<MemoryReservation>,
}
impl Encoded {
    pub(super) fn metadata(mut self, bytes: Vec<u8>) -> Self {
        self.data.app_metadata = bytes.into();
        self
    }
    pub(super) fn into_data(mut self) -> FlightData {
        self.data.data_header = hold_bytes(self.data.data_header, Arc::clone(&self.memory));
        self.data.data_body = hold_bytes(self.data.data_body, Arc::clone(&self.memory));
        self.data.app_metadata = hold_bytes(self.data.app_metadata, self.memory);
        self.data
    }
    #[cfg(test)]
    pub(super) fn admitted(
        data: FlightData,
        pool: &Arc<dyn MemoryPool>,
    ) -> std::result::Result<Self, FlightError> {
        let memory = Arc::new(MemoryConsumer::new("FlightFixture").register(pool));
        memory
            .try_grow(data.data_header.len() + data.data_body.len() + data.app_metadata.len())
            .map_err(|error| FlightError::ExternalError(Box::new(error)))?;
        Ok(Self { data, memory })
    }
}

pub(super) fn encode(
    stream: SendableRecordBatchStream,
    pool: Arc<dyn MemoryPool>,
    compression: Option<CompressionType>,
    size: usize,
) -> Result<BoxStream<'static, std::result::Result<Encoded, FlightError>>> {
    let schema = stream.schema();
    let schema_memory = Arc::new(MemoryConsumer::new("FlightEncodeSchema").register(&pool));
    schema_memory.try_grow(
        schema_bytes(&schema)
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| exec_datafusion_err!("Flight schema allowance overflow"))?,
    )?;
    let current = Arc::new(Mutex::new(None::<Arc<MemoryReservation>>));
    let source = Arc::clone(&current);
    let completed = Arc::clone(&current);
    let mut stream = stream;
    let input = futures::stream::poll_fn(move |cx| {
        let next = std::task::ready!(stream.as_mut().poll_next(cx));
        if next.is_none() {
            if let Ok(mut memory) = completed.lock() {
                memory.take();
            }
        }
        std::task::Poll::Ready(next)
    });
    let batches = input
        .and_then(move |batch| {
            let result = (|| {
                // Includes native compaction, IPC padding, compressed output and
                // vector growth. No view or dictionary GC may expand beyond it.
                // Native Flight requests the next source batch only after all
                // fragments of its previous batch have left its internal queue.
                source
                    .lock()
                    .map_err(|_| exec_datafusion_err!("Flight encoding owner is poisoned"))?
                    .take();
                let memory = Arc::new(MemoryConsumer::new("FlightEncodeBatch").register(&pool));
                memory.try_grow(
                    schema_bytes(&batch.schema())
                        .saturating_mul(8)
                        .saturating_add(65536),
                )?;
                let payload = batch.columns().iter().try_fold(0usize, |bytes, column| {
                    bytes
                        .checked_add(column.get_buffer_memory_size())
                        .ok_or_else(|| exec_datafusion_err!("Flight encoding size overflow"))
                })?;
                // Match the native fragmenter's capacity as well as the actual
                // number of fragments: a single wide value cannot be split.
                let chunks = payload.div_ceil(size.max(1)).max(1);
                let fragments = batch
                    .num_rows()
                    .div_ceil((batch.num_rows() / chunks).max(1));
                let shared = batch.columns().iter().try_fold(0usize, |bytes, column| {
                    bytes
                        .checked_add(variadic(&column.to_data())?)
                        .ok_or_else(|| exec_datafusion_err!("Flight view size overflow"))
                })?;
                let bytes = shared
                    .checked_mul(fragments.saturating_sub(1))
                    .and_then(|bytes| bytes.checked_add(payload))
                    .and_then(|bytes| bytes.checked_mul(8))
                    .and_then(|bytes| {
                        bytes.checked_add(
                            schema_bytes(&batch.schema())
                                .checked_mul(8usize.checked_add(fragments.checked_mul(2)?)?)?,
                        )
                    })
                    .and_then(|bytes| bytes.checked_add(chunks.checked_mul(128)?))
                    .and_then(|bytes| bytes.checked_add(65536))
                    .ok_or_else(|| exec_datafusion_err!("Flight encoding allowance overflow"))?;
                memory.try_resize(bytes)?;
                let prior = batch.clone();
                let batch = super::worker_service::garbage_collect_arrays(batch)?;
                let bytes = allocation_bytes(&batch, |start, length| {
                    prior
                        .columns()
                        .iter()
                        .any(|column| super::decode::contains(&column.to_data(), start, length))
                });
                // Arrow's dictionary tracker may keep compacted values after
                // encoding. Their charge follows the actual child buffers.
                if bytes > memory.size() {
                    return Err(exec_datafusion_err!(
                        "Flight compaction exceeds its admitted allowance"
                    ));
                }
                let retained = memory.split(bytes);
                let batch = hold_batch(batch, retained)?;
                *source
                    .lock()
                    .map_err(|_| exec_datafusion_err!("Flight encoding owner is poisoned"))? =
                    Some(memory);
                Ok(batch)
            })();
            std::future::ready(result)
        })
        .map_err(|error| FlightError::ExternalError(Box::new(error)));
    let options = IpcWriteOptions::default().try_with_compression(compression)?;
    let encoded = FlightDataEncoderBuilder::new()
        .with_options(options)
        .with_schema(schema)
        .with_dictionary_handling(DictionaryHandling::Resend)
        .with_max_flight_data_size(size.max(1))
        .build(batches);
    Ok(encoded
        .map(move |data| {
            let data = data?;
            let memory = current
                .lock()
                .map_err(|_| {
                    FlightError::ExternalError(Box::new(exec_datafusion_err!(
                        "Flight encoding owner is poisoned"
                    )))
                })?
                .clone()
                .unwrap_or_else(|| Arc::clone(&schema_memory));
            let bytes = data
                .data_header
                .len()
                .saturating_add(data.data_body.len())
                .saturating_add(data.app_metadata.len())
                .saturating_add(128);
            if bytes > memory.size() {
                return Err(FlightError::ExternalError(Box::new(exec_datafusion_err!(
                    "Flight fragment exceeds its encoding allowance"
                ))));
            }
            // Transfer only retained wire bytes to the returned fragment. The
            // remaining conversion scratch ends when the encoder advances.
            let memory = Arc::new(memory.split(bytes));
            Ok(Encoded { data, memory })
        })
        .boxed())
}

// Native Flight slices retain a view array's variadic buffers. Each fragment
// can encode that backing allocation again, even if all its rows share one
// long value. Account for this before the native encoder queues fragments.
fn variadic(data: &datafusion::arrow::array::ArrayData) -> Result<usize> {
    let mut bytes = if matches!(
        data.data_type(),
        datafusion::arrow::datatypes::DataType::Utf8View
            | datafusion::arrow::datatypes::DataType::BinaryView
    ) {
        data.buffers()
            .iter()
            .skip(1)
            .try_fold(0usize, |bytes, buffer| {
                bytes
                    .checked_add(buffer.len().max(buffer.capacity()))
                    .ok_or_else(|| exec_datafusion_err!("Flight view size overflow"))
            })?
    } else {
        0
    };
    for child in data.child_data() {
        bytes = bytes
            .checked_add(variadic(child)?)
            .ok_or_else(|| exec_datafusion_err!("Flight view size overflow"))?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::array::{ArrayRef, RecordBatch, StringArray},
        execution::memory_pool::GreedyMemoryPool,
        physical_plan::stream::RecordBatchStreamAdapter,
    };

    #[tokio::test]
    async fn eof_releases_encoding_scratch_while_returned_slices_keep_only_wire_charges() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 * 1024 * 1024));
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec!["x".repeat(4 * 1024 * 1024)])) as ArrayRef,
        )])
        .unwrap();
        let input = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter([Ok(batch)]),
        ));
        let mut stream = encode(input, Arc::clone(&pool), None, 1024 * 1024).unwrap();
        let mut frames = vec![];
        while let Some(frame) = stream.next().await {
            frames.push(frame.unwrap().into_data());
        }
        let wire = frames
            .iter()
            .map(|frame| {
                frame.data_header.len() + frame.data_body.len() + frame.app_metadata.len() + 128
            })
            .sum::<usize>();
        assert!(
            pool.reserved() < wire + 65536,
            "completed encoder retained transient batch scratch"
        );
        let frame = frames.pop().unwrap();
        let charge =
            frame.data_header.len() + frame.data_body.len() + frame.app_metadata.len() + 128;
        let child = frame.data_body.slice(1..2);
        drop(frame);
        drop(frames);
        drop(stream);
        assert_eq!(pool.reserved(), charge);
        drop(child);
        assert_eq!(pool.reserved(), 0);
    }
}
