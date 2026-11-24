// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A native Hudi table scan, reading file slices through hudi-rs.
//!
//! The Spark driver plans the read (file listing, partition pruning, slice
//! grouping as of the query instant) and ships each task its file slices plus
//! the table base URI. hudi-rs opens the table from `hoodie.properties` under
//! that URI, so merge mode, ordering field, and table version never travel in
//! the plan; this operator only reads the named slices and projects the
//! requested columns. The tasks of one process share the opened table and its
//! readers for as long as the driver's timeline is unchanged.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use arrow::array::{Array as _, RecordBatch, RecordBatchOptions};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use hudi::file_group::reader::FileGroupReader;
use hudi::table::builder::TableBuilder;
use hudi::table::{ReadOptions, Table};
use object_store::ObjectStore;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

/// Opened tables kept by this process. A task that finds its table here skips
/// reading `hoodie.properties`, the global config file and the timeline.
const TABLE_CACHE_CAPACITY: usize = 32;
/// Readers kept by this process. A reader holds the table schema it resolved
/// from the latest commit, which a task finding one here does not read again.
const READER_CACHE_CAPACITY: usize = 64;
/// File slices a partition reads at once: the one being consumed, and the next
/// one, which opens and reads ahead meanwhile if it has no log files.
const SLICES_IN_FLIGHT: usize = 2;
/// Batches a slice read may produce ahead of its consumer.
const SLICE_READ_AHEAD: usize = 2;

/// What a task's table is opened from. The timeline snapshot changes with every
/// instant added to, completed on or removed from the active timeline the
/// driver planned against, and with every table config change, so an entry
/// never serves a plan newer than the timeline and config it read.
#[derive(Clone, PartialEq, Eq, Hash)]
struct TableKey {
    base_uri: String,
    timeline_snapshot: String,
    options: Vec<(String, String)>,
    object_store_options: Vec<(String, String)>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ReaderKey {
    table: TableKey,
    projection: Vec<String>,
    batch_size: usize,
}

/// A bounded map of opened values; the least recently used entry goes first.
/// Only successful opens are kept, so a task that misses opens on its own and a
/// failing open never makes other tasks wait on it.
struct Lru<K, V> {
    capacity: usize,
    clock: u64,
    entries: HashMap<K, (u64, Arc<V>)>,
}

impl<K: Clone + Eq + Hash, V> Lru<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            clock: 0,
            entries: HashMap::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<Arc<V>> {
        self.clock += 1;
        let clock = self.clock;
        self.entries.get_mut(key).map(|(used, value)| {
            *used = clock;
            Arc::clone(value)
        })
    }

    /// Keeps the value already present if another task stored one first.
    fn insert(&mut self, key: K, value: Arc<V>) -> Arc<V> {
        if let Some(existing) = self.get(&key) {
            return existing;
        }
        if self.entries.len() >= self.capacity {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, (used, _))| *used)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(key, (self.clock, Arc::clone(&value)));
        value
    }
}

type Cache<K, V> = Mutex<Lru<K, V>>;

static TABLES: LazyLock<Cache<TableKey, Table>> =
    LazyLock::new(|| Mutex::new(Lru::new(TABLE_CACHE_CAPACITY)));
static READERS: LazyLock<Cache<ReaderKey, FileGroupReader>> =
    LazyLock::new(|| Mutex::new(Lru::new(READER_CACHE_CAPACITY)));

fn cached<K: Clone + Eq + Hash, V>(cache: &Cache<K, V>, key: &K) -> Option<Arc<V>> {
    cache.lock().unwrap_or_else(PoisonError::into_inner).get(key)
}

fn store<K: Clone + Eq + Hash, V>(cache: &Cache<K, V>, key: K, value: Arc<V>) -> Arc<V> {
    cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key, value)
}

fn sorted_entries(map: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut entries: Vec<_> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    entries.sort();
    entries
}

async fn open_table(key: &TableKey, object_store: Option<Arc<dyn ObjectStore>>) -> DFResult<Table> {
    // hudi-rs takes the read options (the query instant) per read, not on the
    // table.
    let table_opts: Vec<(&str, &str)> = key
        .options
        .iter()
        .filter(|(k, _)| !k.starts_with("hoodie.read."))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut builder = TableBuilder::from_base_uri(&key.base_uri).with_options(table_opts);
    if let Some(store) = object_store {
        builder = builder.with_object_store(store);
    }
    builder.build().await.map_err(|e| {
        DataFusionError::Execution(format!("failed to open Hudi table {}: {e}", key.base_uri))
    })
}

/// The reader comes from an opened table rather than from the base URI alone
/// so that it knows the table's current schema: a base file written before a
/// column was added lacks that column, and only a reader holding the table
/// schema null-fills it (and casts a widened column) as the JVM reader does.
async fn file_group_reader(
    key: &ReaderKey,
    object_store: Option<Arc<dyn ObjectStore>>,
    read_options: &ReadOptions,
) -> DFResult<Arc<FileGroupReader>> {
    // Without a snapshot of the timeline the driver planned against, nothing
    // tells a cached table from a stale one.
    let use_cache = !key.table.timeline_snapshot.is_empty();
    if use_cache {
        if let Some(reader) = cached(&READERS, key) {
            return Ok(reader);
        }
    }
    let table = match use_cache.then(|| cached(&TABLES, &key.table)).flatten() {
        Some(table) => table,
        None => {
            let table = Arc::new(open_table(&key.table, object_store).await?);
            if use_cache {
                store(&TABLES, key.table.clone(), table)
            } else {
                table
            }
        }
    };
    let reader = table
        .create_file_group_reader_with_options(
            Some(read_options),
            std::iter::empty::<(&str, String)>(),
        )
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to create a reader for Hudi table {}: {e}",
                key.table.base_uri
            ))
        })?;
    let reader = Arc::new(reader);
    Ok(if use_cache {
        store(&READERS, key.clone(), reader)
    } else {
        reader
    })
}

/// Aborts a slice read whose consumer went away.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Reads one file slice. A slice with no log files, on a Tokio runtime, reads
/// on its own task, so it opens and fetches its first batches while the
/// consumer is still on the previous slice. A slice with log files reads only
/// when consumed, so at most one merge buffer is live per partition and a
/// LIMIT never pays for a merge it does not read.
fn read_slice(
    reader: Arc<FileGroupReader>,
    read_options: ReadOptions,
    slice: HudiFileSliceSpec,
) -> BoxStream<'static, DFResult<RecordBatch>> {
    let read_ahead = slice.log_file_paths.is_empty();
    let open = async move {
        let stream = reader
            .read_file_slice_from_paths_stream(
                &slice.base_file_path,
                slice.log_file_paths.iter().map(|s| s.as_str()),
                &read_options,
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "failed to open Hudi file slice {}: {e}",
                    slice.base_file_path
                ))
            })?;
        let base_file_path = slice.base_file_path;
        Ok::<_, DataFusionError>(stream.map_err(move |e| {
            DataFusionError::Execution(format!(
                "failed to read Hudi file slice {base_file_path}: {e}"
            ))
        }))
    };
    if !read_ahead || tokio::runtime::Handle::try_current().is_err() {
        return futures::stream::once(open).try_flatten().boxed();
    }
    let (tx, rx) = mpsc::channel(SLICE_READ_AHEAD);
    let task = tokio::spawn(async move {
        let mut batches = match open.await {
            Ok(batches) => batches,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        };
        while let Some(batch) = batches.next().await {
            let failed = batch.is_err();
            if tx.send(batch).await.is_err() || failed {
                return;
            }
        }
    });
    let abort = AbortOnDrop(task.abort_handle());
    // The channel closes when the task ends; a task that panicked or was
    // cancelled before sending every batch surfaces as an error.
    futures::stream::unfold(
        (rx, Some(task), abort),
        |(mut rx, task, abort)| async move {
            if let Some(batch) = rx.recv().await {
                return Some((batch, (rx, task, abort)));
            }
            match task?.await {
                Ok(()) => None,
                Err(e) => Some((
                    Err(DataFusionError::Execution(format!(
                        "Hudi file slice read ended early: {e}"
                    ))),
                    (rx, None, abort),
                )),
            }
        },
    )
    .boxed()
}

/// One file slice assigned to this partition: a base file and the log files
/// whose records amend it.
#[derive(Debug, Clone)]
pub struct HudiFileSliceSpec {
    pub base_file_path: String,
    pub log_file_paths: Vec<String>,
}

#[derive(Debug)]
pub struct HudiScanExec {
    table_base_uri: String,
    timeline_snapshot: String,
    options: HashMap<String, String>,
    object_store: Option<Arc<dyn ObjectStore>>,
    /// The settings `object_store` was built from; part of the cache key.
    object_store_options: HashMap<String, String>,
    output_schema: SchemaRef,
    file_slices: Vec<HudiFileSliceSpec>,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl HudiScanExec {
    pub fn new(
        table_base_uri: String,
        timeline_snapshot: String,
        options: HashMap<String, String>,
        object_store: Option<Arc<dyn ObjectStore>>,
        object_store_options: HashMap<String, String>,
        output_schema: SchemaRef,
        file_slices: Vec<HudiFileSliceSpec>,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table_base_uri,
            timeline_snapshot,
            options,
            object_store,
            object_store_options,
            output_schema,
            file_slices,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

/// Reorders and casts a batch's columns to the scan's output schema. hudi-rs
/// projects the requested columns but keeps the table schema's column order
/// and physical types; Spark expects its own order and logical types.
fn align_to_schema(batch: RecordBatch, schema: &SchemaRef) -> DFResult<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let index = batch.schema().index_of(field.name()).map_err(|_| {
            DataFusionError::Execution(format!(
                "Hudi scan did not produce required column {}",
                field.name()
            ))
        })?;
        let column = batch.column(index);
        let column = if column.data_type() == field.data_type() {
            Arc::clone(column)
        } else {
            cast(column, field.data_type()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Hudi scan cannot cast column {} from {} to {}: {e}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ))
            })?
        };
        columns.push(column);
    }
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .map_err(|e| DataFusionError::Execution(format!("Hudi scan batch assembly failed: {e}")))
}

impl DisplayAs for HudiScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HudiScanExec: table={}, file_slices={}",
            self.table_base_uri,
            self.file_slices.len()
        )
    }
}

impl ExecutionPlan for HudiScanExec {
    fn name(&self) -> &str {
        "HudiScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let table_key = TableKey {
            base_uri: self.table_base_uri.clone(),
            timeline_snapshot: self.timeline_snapshot.clone(),
            options: sorted_entries(&self.options),
            object_store_options: sorted_entries(&self.object_store_options),
        };
        let object_store = self.object_store.clone();
        let schema = Arc::clone(&self.output_schema);
        let slices = self.file_slices.clone();
        let projection: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let batch_size = context.session_config().batch_size();
        let baseline = BaselineMetrics::new(&self.metrics, 0);

        let batches = futures::stream::once(async move {
            // The projection is pushed into the base file read, and each slice
            // streams in batches of the session's batch size instead of being
            // read whole.
            let read_opts = table_key
                .options
                .iter()
                .filter(|(k, _)| k.starts_with("hoodie.read."))
                .map(|(k, v)| (k.as_str(), v.as_str()));
            let read_options = ReadOptions::new()
                .with_hudi_options(read_opts)
                .with_projection(projection.clone())
                .with_batch_size(batch_size)
                .map_err(|e| DataFusionError::Execution(format!("Hudi scan batch size: {e}")))?;
            let reader_key = ReaderKey {
                table: table_key,
                projection,
                batch_size,
            };
            let reader = file_group_reader(&reader_key, object_store, &read_options).await?;
            // Slices come out in order; buffering yields the next slice's
            // stream, already reading on its own task if it has no log files,
            // while this one drains.
            let slice_streams = futures::stream::iter(slices)
                .map(move |slice| {
                    futures::future::ready(read_slice(
                        Arc::clone(&reader),
                        read_options.clone(),
                        slice,
                    ))
                })
                .buffered(SLICES_IN_FLIGHT)
                .flatten();
            Ok::<_, DataFusionError>(slice_streams)
        })
        .try_flatten()
        .map(move |result| {
            let aligned = align_to_schema(result?, &schema)?;
            baseline.record_output(aligned.num_rows());
            Ok(aligned)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            batches.boxed(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
