//! Memory charges follow retained wire bytes and Arrow child arrays.
use bytes::Bytes;
use datafusion::{
    arrow::{
        array::{ArrayData, RecordBatch, RecordBatchOptions, make_array},
        buffer::{BooleanBuffer, Buffer, NullBuffer},
    },
    common::{DataFusionError, Result},
};
use std::{
    collections::BTreeMap,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

pub(crate) fn hold_bytes<T: Send + Sync + 'static>(bytes: Bytes, memory: Arc<T>) -> Bytes {
    struct Owned<T> {
        bytes: Bytes,
        _memory: Arc<T>,
    }
    impl<T> AsRef<[u8]> for Owned<T> {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    Bytes::from_owner(Owned {
        bytes,
        _memory: memory,
    })
}

pub(crate) fn allocation_bytes(
    batch: &RecordBatch,
    covered: impl Fn(usize, usize) -> bool,
) -> usize {
    fn visit(data: &ArrayData, buffers: &mut BTreeMap<usize, usize>) {
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|nulls| nulls.buffer()))
        {
            buffers
                .entry(buffer.data_ptr().as_ptr() as usize)
                .and_modify(|size| *size = (*size).max(buffer.capacity()))
                .or_insert(buffer.capacity());
        }
        for child in data.child_data() {
            visit(child, buffers);
        }
    }
    let mut buffers = BTreeMap::new();
    for column in batch.columns() {
        visit(&column.to_data(), &mut buffers);
    }
    buffers
        .into_iter()
        .filter(|(start, bytes)| !covered(*start, *bytes))
        .map(|(_, bytes)| bytes)
        .sum()
}

pub(crate) fn hold_batch(batch: RecordBatch, memory: impl Send + 'static) -> Result<RecordBatch> {
    let memory = Arc::new(Mutex::new(Box::new(memory) as Box<dyn Send>));
    let columns = batch
        .columns()
        .iter()
        .map(|column| retain(column.to_data(), &memory).map(make_array))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

type Owner = Arc<Mutex<Box<dyn Send>>>;
struct Allocation {
    _buffer: Buffer,
    _memory: Owner,
}
fn buffer(buffer: &Buffer, memory: &Owner) -> Result<Buffer> {
    let pointer = NonNull::new(buffer.as_ptr().cast_mut())
        .ok_or_else(|| DataFusionError::Execution("Arrow buffer has no allocation".into()))?;
    let owner = Arc::new(Allocation {
        _buffer: buffer.clone(),
        _memory: Arc::clone(memory),
    });
    // The owner retains the original immutable buffer without changing its
    // address, length or contents, together with this allocation's pool charge.
    Ok(unsafe { Buffer::from_custom_allocation(pointer, buffer.len(), owner) })
}
fn retain(data: ArrayData, memory: &Owner) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(|value| buffer(value, memory))
        .collect::<Result<Vec<_>>>()?;
    let children = data
        .child_data()
        .iter()
        .map(|child| retain(child.clone(), memory))
        .collect::<Result<Vec<_>>>()?;
    let nulls = data
        .nulls()
        .map(|nulls| {
            buffer(nulls.buffer(), memory).map(|buffer| {
                NullBuffer::new(BooleanBuffer::new(buffer, nulls.offset(), nulls.len()))
            })
        })
        .transpose()?;
    Ok(data
        .into_builder()
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::array::StringArray,
        execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool},
    };

    #[test]
    fn projected_slices_retain_their_full_allocation() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let values: datafusion::arrow::array::ArrayRef =
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("omega")]));
        let batch = RecordBatch::try_from_iter([("a", values.clone()), ("b", values)]).unwrap();
        let bytes = allocation_bytes(&batch, |_, _| false);
        let memory = MemoryConsumer::new("test").register(&pool);
        memory.try_grow(bytes).unwrap();
        let batch = hold_batch(batch, memory).unwrap();
        let child = batch.column(0).slice(1, 2);
        drop(batch);
        assert_eq!(pool.reserved(), bytes);
        assert_eq!(
            child
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "omega"
        );
        drop(child);
        assert_eq!(pool.reserved(), 0);
    }
}
