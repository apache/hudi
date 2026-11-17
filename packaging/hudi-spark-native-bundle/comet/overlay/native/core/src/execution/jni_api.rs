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

//! Define JNI APIs which can be called from Java/Scala.
//!
//! An entry point is a thin JNI wrapper: it converts its JNI arguments into plain Rust values,
//! calls a core function whose signature has no JNI types (e.g. `decode_shuffle_block`), and
//! converts the result back. Errors and panics are raised as JVM exceptions by
//! `try_unwrap_or_throw`, through the boundary error protocol in `errors::NativeError`.
//!
//! `createPlan`, `setShufflePartitionPusher`, `executePlan` and `releasePlan` also depend on
//! upcalls into the JVM (input iterators, the task memory manager, metrics, UDFs and scalar
//! subqueries). Their core functions (`create_execution_context`,
//! `register_shuffle_partition_pusher`, `execute_plan` and `release_plan`) take the JVM objects as
//! JNI global references held by the `ExecutionContext` (`PlanJvmRefs`), and the steps that need
//! an `Env` as callbacks: the memory pool factory, the pusher factory, and `PublishMetrics`.
//! `setOutputStream` registers a native write's output stream through the same
//! `register_before_execution` check as the pusher.

use super::{serde, utils::SparkArrowConvert};
use crate::{
    errors::{try_unwrap_or_throw, CometError, CometResult},
    execution::{
        metrics::utils::update_comet_metric,
        planner::PhysicalPlanner,
        row_partition_sort::sort_row_partitions,
        serde::to_arrow_datatype,
        shuffle::spark_unsafe::row::{process_sorted_partitions, process_sorted_row_partition},
    },
    jvm_bridge::{
        JVMClasses, JavaOutputStreamSink, JavaShufflePartitionPusher, OutputStreamSink,
        ShufflePartitionPusher,
    },
};
use std::collections::HashSet;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::config::SpillCompression;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::{
    physical_plan::{display::DisplayableExecutionPlan, SendableRecordBatchStream},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_comet_common::decode_string_arrays;
use datafusion_comet_proto::spark_expression::agg_expr::ExprStruct as AggExprStruct;
use datafusion_comet_proto::spark_operator::{AggregateMode, Operator, ShuffleScan};
use datafusion_comet_spark_expr::url_funcs::{CometParseUrl, CometTryParseUrl};
use datafusion_spark::function::array::array_contains::SparkArrayContains;
use datafusion_spark::function::array::repeat::SparkArrayRepeat;
use datafusion_spark::function::bitwise::bit_count::SparkBitCount;
use datafusion_spark::function::bitwise::bit_get::SparkBitGet;
use datafusion_spark::function::bitwise::bit_shift::SparkBitShift;
use datafusion_spark::function::bitwise::bitwise_not::SparkBitwiseNot;
use datafusion_spark::function::datetime::date_add::SparkDateAdd;
use datafusion_spark::function::datetime::date_sub::SparkDateSub;
use datafusion_spark::function::datetime::from_utc_timestamp::SparkFromUtcTimestamp;
use datafusion_spark::function::datetime::last_day::SparkLastDay;
use datafusion_spark::function::datetime::to_utc_timestamp::SparkToUtcTimestamp;
use datafusion_spark::function::hash::crc32::SparkCrc32;
use datafusion_spark::function::hash::sha1::SparkSha1;
use datafusion_spark::function::hash::sha2::SparkSha2;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;
use datafusion_spark::function::map::str_to_map::SparkStrToMap;
use datafusion_spark::function::math::expm1::SparkExpm1;
use datafusion_spark::function::math::factorial::SparkFactorial;
use datafusion_spark::function::math::hex::SparkHex;
use datafusion_spark::function::math::rint::SparkRint;
use datafusion_spark::function::math::trigonometry::SparkCsc;
use datafusion_spark::function::math::trigonometry::SparkSec;
use datafusion_spark::function::math::width_bucket::SparkWidthBucket;
use datafusion_spark::function::string::char::CharFunc;
use datafusion_spark::function::string::concat::SparkConcat;
use datafusion_spark::function::string::length::SparkLengthFunc;
use datafusion_spark::function::string::luhn_check::SparkLuhnCheck;
use datafusion_spark::function::string::space::SparkSpace;
use datafusion_spark::function::string::substring::SparkSubstring;
use datafusion_spark::function::url::try_url_decode::TryUrlDecode as SparkTryUrlDecode;
use datafusion_spark::function::url::url_decode::UrlDecode as SparkUrlDecode;
use datafusion_spark::function::url::url_encode::UrlEncode as SparkUrlEncode;
use futures::stream::{Stream, StreamExt};
use futures::FutureExt;
use jni::objects::JByteBuffer;
use jni::sys::{jlongArray, JNI_FALSE};
use jni::{
    errors::Result as JNIResult,
    objects::{
        Global, JByteArray, JClass, JIntArray, JLongArray, JObject, JObjectArray, JString,
        ReleaseMode,
    },
    sys::{jboolean, jdouble, jint, jlong},
    Env, EnvUnowned,
};
use parking_lot::Mutex;
use prost::Message;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::{
    future::poll_fn,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::execution::memory_pools::{
    create_memory_pool, overcommit, parse_memory_pool_config, MemoryPoolConfig, PlanMemoryPool,
};
use crate::execution::operators::{ScanExec, ShuffleScanExec};
use crate::execution::shuffle::{
    decode_remote_shuffle_batch, read_ipc_compressed, CompressionCodec, ShuffleWriterExec,
};
use crate::execution::spark_plan::SparkPlan;
use crate::execution::spill_dirs::plan_disk_manager;

use crate::execution::tracing::{
    get_thread_id, log_memory_usage, trace_begin, trace_end, with_trace, POOL_TOTAL_METRIC,
};

use crate::execution::memory_pools::logging_pool::LoggingMemoryPool;
use crate::execution::spark_config::{
    SparkConfig, COMET_DEBUG_ENABLED, COMET_DEBUG_MEMORY,
    COMET_EXEC_AGGREGATE_SKIP_PARTIAL_ENABLED, COMET_EXEC_SPILL_COMPRESSION_CODEC,
    COMET_EXPLAIN_NATIVE_ENABLED, COMET_MAX_TEMP_DIRECTORY_SIZE,
    COMET_PARQUET_PREFETCH_NEXT_FILE_METADATA_ENABLED, COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED,
    COMET_STRICT_TIMESTAMP_LABELS, COMET_TRACING_ENABLED, SPARK_EXECUTOR_CORES,
};
use crate::parquet::encryption_support::{CometEncryptionFactory, ENCRYPTION_FACTORY_ID};
use crate::parquet::parquet_exec::CometParquetScanOptions;
use crate::parquet::parquet_support::CometObjectStoreRegistry;
use datafusion_comet_proto::spark_operator::operator::OpStruct;
use log::{info, warn};
use std::sync::OnceLock;
#[cfg(feature = "jemalloc")]
use tikv_jemalloc_ctl::{epoch, stats};

static TOKIO_RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);

#[cfg(feature = "jemalloc")]
fn log_jemalloc_usage() {
    let e = epoch::mib().unwrap();
    let allocated = stats::allocated::mib().unwrap();
    e.advance().unwrap();
    log_memory_usage("jemalloc_allocated", allocated.read().unwrap() as u64);
}

/// Reports the bytes currently handed out by the Rust global allocator, process-wide.
///
/// Logged alongside the per-thread pool reservations so the two can be compared directly: a large
/// and growing excess is native memory the pool is not accounting for.
fn log_native_allocated() {
    log_memory_usage(
        "native_allocated",
        crate::alloc_accounting::current_balance() as u64,
    );
}

/// Registry of active memory pools per Rust thread ID.
/// Used to sum memory reservations across all contexts for the memory usage log and tracing.
///
/// Never read a pool's reservation while holding this registry's lock; copy the pools out with
/// [`snapshot_registry`] and read them after it is released. `CometFairMemoryPool` holds its own
/// lock across the JNI call that acquires memory from Spark, and Spark can park that call until
/// another task frees memory. A finishing task frees its reservations only after `releasePlan` has
/// taken this lock to unregister, so a reservation read under this lock can wait on a pool that is
/// itself waiting on the lock.
type ThreadPoolMap = HashMap<u64, HashMap<i64, Arc<dyn MemoryPool>>>;

static THREAD_MEMORY_POOLS: OnceLock<Mutex<ThreadPoolMap>> = OnceLock::new();

fn get_thread_memory_pools() -> &'static Mutex<ThreadPoolMap> {
    THREAD_MEMORY_POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_memory_pool(thread_id: u64, context_id: i64, pool: Arc<dyn MemoryPool>) {
    get_thread_memory_pools()
        .lock()
        .entry(thread_id)
        .or_default()
        .insert(context_id, pool);
}

/// Removes a context's pool from the registry, without reading any reservation.
fn unregister_memory_pool(thread_id: u64, context_id: i64) {
    let removed = {
        let mut map = get_thread_memory_pools().lock();
        let Some(pools) = map.get_mut(&thread_id) else {
            return;
        };
        let removed = pools.remove(&context_id);
        if pools.is_empty() {
            map.remove(&thread_id);
        }
        removed
    };
    // Dropped after the lock is released, in case it was the last reference to the pool.
    drop(removed);
}

struct ThreadMemoryPoolRegistration {
    thread_id: u64,
    context_id: i64,
}

impl ThreadMemoryPoolRegistration {
    fn new(thread_id: u64, context_id: i64, pool: Arc<dyn MemoryPool>) -> Self {
        register_memory_pool(thread_id, context_id, pool);
        Self {
            thread_id,
            context_id,
        }
    }
}

impl Drop for ThreadMemoryPoolRegistration {
    fn drop(&mut self) {
        unregister_memory_pool(self.thread_id, self.context_id);
    }
}

/// Pools copied out of the registry in one acquisition of its lock, so that their reservations can
/// be read after it is released; see [`ThreadPoolMap`].
///
/// Execution contexts routinely share one pool (every context in a task under the task-shared pool
/// types, every context in the process under the global ones) and each of them registers it, so
/// both lists are deduplicated by pool identity or a sum over them would report one reservation
/// several times.
struct RegistrySnapshot {
    /// Distinct pools registered on the requested thread. Empty when no thread was requested.
    thread_pools: Vec<Arc<dyn MemoryPool>>,
    /// Distinct pools across every thread. Deduplicated across the whole registry, not within each
    /// thread: a task-shared or global pool spans threads.
    all_pools: Vec<Arc<dyn MemoryPool>>,
    /// Registered contexts, which is one per native plan created and not yet released.
    plans: usize,
}

fn snapshot_registry(thread_id: Option<u64>) -> RegistrySnapshot {
    fn distinct<'a>(
        pools: impl IntoIterator<Item = &'a Arc<dyn MemoryPool>>,
        seen: &mut HashSet<*const ()>,
    ) -> Vec<Arc<dyn MemoryPool>> {
        pools
            .into_iter()
            .filter(|pool| seen.insert(Arc::as_ptr(pool) as *const ()))
            .cloned()
            .collect()
    }

    let map = get_thread_memory_pools().lock();
    let thread_pools = thread_id
        .and_then(|id| map.get(&id))
        .map(|pools| distinct(pools.values(), &mut HashSet::new()))
        .unwrap_or_default();
    let all_pools = distinct(map.values().flat_map(HashMap::values), &mut HashSet::new());
    let plans = map.values().map(HashMap::len).sum();
    RegistrySnapshot {
        thread_pools,
        all_pools,
        plans,
    }
}

fn sum_reserved(pools: &[Arc<dyn MemoryPool>]) -> usize {
    pools.iter().map(|pool| pool.reserved()).sum()
}

/// Bytes reserved across `pools`, less the part that Spark has not granted them; see
/// [`MemoryUsage::pools_reserved`].
fn sum_reserved_less_overcommit(pools: &[Arc<dyn MemoryPool>]) -> usize {
    pools
        .iter()
        // The two figures are read at different moments, so a `grow` in between can leave the
        // overcommit larger than the reservation read before it.
        .map(|pool| pool.reserved().saturating_sub(overcommit(pool)))
        .sum()
}

fn total_reserved_for_thread(thread_id: u64) -> usize {
    sum_reserved(&snapshot_registry(Some(thread_id)).thread_pools)
}

/// Reservation totals read from one snapshot of the pool registry.
struct ReservedTotals {
    /// Bytes reserved by the pools registered on the requested thread, deduplicated within it.
    /// Zero when no thread was requested.
    for_thread: usize,
    /// Bytes reserved across every live Comet memory pool in the process.
    ///
    /// This is the figure to compare against a process-wide allocation counter, so it has to
    /// account for every live pool rather than only the ones a traced plan created. Allocation is
    /// process-wide and a plan running with tracing off still holds memory, which is why
    /// `createPlan` registers its pool unconditionally: with two concurrent plans configured
    /// differently, counting only the traced one would report the untraced plan's reservation as
    /// untracked allocation. It covers every pool type, because each plan registers whichever pool
    /// `create_memory_pool` gave it, and it counts a pool once however many contexts or threads
    /// hold it.
    ///
    /// The per-thread `thread_NNN_comet_memory_reserved` counters must not be summed to obtain it:
    /// a shared pool reports its full reservation on every thread that references it, so adding
    /// them across threads multiplies that pool by its thread count.
    across_threads: usize,
}

/// Reads both totals from one snapshot of the registry.
///
/// They are emitted as a pair, and a pair taken from two snapshots can describe two different
/// sets of pools. Taking the lock once also halves the tracing traffic through a mutex the
/// executor needs in order to register and release pools.
///
/// `thread_id` of `None` skips the per-thread figure; the caller is only after the process total.
fn total_reserved(thread_id: Option<u64>) -> ReservedTotals {
    let snapshot = snapshot_registry(thread_id);
    ReservedTotals {
        for_thread: sum_reserved(&snapshot.thread_pools),
        across_threads: sum_reserved(&snapshot.all_pools),
    }
}

/// Executor-wide memory figures for one line of the periodic memory usage log.
#[derive(Debug, PartialEq)]
struct MemoryUsage {
    /// Bytes handed out by the Rust global allocator, process-wide.
    native_allocated: usize,
    /// Bytes reserved across every live Comet memory pool, counting each pool once however many
    /// plans share it, less any the pools recorded beyond what Spark granted them; see
    /// [`overcommit`]. Spark's off-heap pool does not account for those bytes, so the log counts
    /// them with the native memory that no pool tracks.
    pools_reserved: usize,
    /// Live memory pools. With the task-shared pool types, which include both defaults, that is one
    /// per task running native plans.
    pools: usize,
    /// Native plans that have been created and not yet released.
    plans: usize,
}

/// Reads the executor's memory usage for the periodic memory usage log.
///
/// This runs on a timer thread, concurrently with every plan in the executor, so it reads only the
/// allocation counter and the pool registry, never an execution context.
fn memory_usage() -> MemoryUsage {
    let snapshot = snapshot_registry(None);
    MemoryUsage {
        native_allocated: crate::alloc_accounting::current_balance(),
        pools_reserved: sum_reserved_less_overcommit(&snapshot.all_pools),
        pools: snapshot.all_pools.len(),
        plans: snapshot.plans,
    }
}

fn parse_usize_env_var(name: &str) -> Option<usize> {
    std::env::var_os(name).and_then(|n| n.to_str().and_then(|s| s.parse::<usize>().ok()))
}

fn build_runtime(default_worker_threads: Option<usize>) -> Runtime {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = parse_usize_env_var("COMET_WORKER_THREADS") {
        info!("Comet tokio runtime: using COMET_WORKER_THREADS={n}");
        builder.worker_threads(n);
    } else if let Some(n) = default_worker_threads {
        info!("Comet tokio runtime: using spark.executor.cores={n} worker threads");
        builder.worker_threads(n);
    } else {
        info!("Comet tokio runtime: using default thread count");
    }
    if let Some(n) = parse_usize_env_var("COMET_MAX_BLOCKING_THREADS") {
        builder.max_blocking_threads(n);
    }
    builder
        .enable_all()
        .on_thread_start(attach_thread_as_daemon)
        .on_thread_stop(detach_thread)
        .build()
        .expect("Failed to create Tokio runtime")
}

/// Attaches a runtime thread to the JVM as a daemon thread.
///
/// jni-rs attaches threads lazily with `AttachCurrentThread`, which makes them non-daemon JVM
/// threads. `DestroyJavaVM` waits for all non-daemon threads to exit before it runs shutdown
/// hooks, but runtime threads only exit once the shutdown hook has called `SparkContext.stop()`
/// and [`release_runtime`]. An application that returns from `main` without calling
/// `SparkContext.stop()` would therefore never exit. Daemon threads are not waited for, and
/// jni-rs reuses an existing attachment rather than attaching again.
fn attach_thread_as_daemon() {
    let Some(vm) = crate::JAVA_VM.get() else {
        return;
    };
    let vm = vm.get_raw();
    let mut env: *mut std::ffi::c_void = std::ptr::null_mut();
    // SAFETY: `vm` is the JavaVM stored by `NativeBase.init` and outlives the runtime. Null
    // attach args select the default JNI version, thread name and thread group.
    let rc =
        unsafe { ((**vm).v1_4.AttachCurrentThreadAsDaemon)(vm, &mut env, std::ptr::null_mut()) };
    if rc != jni::sys::JNI_OK {
        warn!("Failed to attach tokio runtime thread to the JVM as a daemon thread: {rc}");
    }
}

/// Detaches a thread attached by [`attach_thread_as_daemon`] before it exits. jni-rs only
/// detaches threads it attached itself.
fn detach_thread() {
    let Some(vm) = crate::JAVA_VM.get() else {
        return;
    };
    let vm = vm.get_raw();
    // SAFETY: see `attach_thread_as_daemon`. Detaching an unattached thread is a JNI error,
    // not undefined behavior.
    unsafe {
        ((**vm).v1_1.DetachCurrentThread)(vm);
    }
}

/// Initialize the global Tokio runtime with the given default worker thread count.
/// If the runtime is already initialized, this is a no-op.
pub fn init_runtime(default_worker_threads: usize) {
    let mut guard = TOKIO_RUNTIME.lock();
    if guard.is_none() {
        *guard = Some(build_runtime(Some(default_worker_threads)));
    }
}

/// Returns a handle to the global Tokio runtime, lazily initializing it if needed.
///
/// A [`Handle`] is returned (rather than a `&'static Runtime`) so that the runtime
/// can be torn down via [`release_runtime`]. The handle is cheap to clone and can be
/// used with `spawn` / `block_on` just like a `Runtime`.
pub fn get_runtime() -> Handle {
    let mut guard = TOKIO_RUNTIME.lock();
    guard
        .get_or_insert_with(|| build_runtime(None))
        .handle()
        .clone()
}

/// Tears down the global Tokio runtime, if it has been initialized.
///
/// The runtime is moved out of the global slot, so the next [`init_runtime`] or
/// [`get_runtime`] call builds a new one. Shutting it down blocks the calling (JNI) thread
/// until the runtime's threads have stopped, for at most 3 seconds. Tasks still running on it
/// are dropped at their next yield, and any handles previously returned by [`get_runtime`]
/// will start failing their spawns, so this must only be called when no native execution is
/// in flight.
///
/// Must not be called from within the runtime's own worker threads, otherwise the shutdown
/// would deadlock/panic.
pub fn release_runtime() {
    crate::execution::operators::clear_file_io_cache();
    let runtime = TOKIO_RUNTIME.lock().take();
    if let Some(runtime) = runtime {
        runtime.shutdown_timeout(Duration::from_secs(3));
    }
}

/// Returns a short name for an OpStruct variant. Used for tracing event names;
/// no contrib-specific logic. The `OpStruct::ContribScan` arm is the generic
/// extension point for out-of-tree contrib scans (Delta, Lance, ...); it stays
/// unconditional even in non-contrib builds because the proto enum is generated
/// regardless of cargo feature flags and Rust requires an exhaustive match.
fn op_name(op: &OpStruct) -> &'static str {
    match op {
        OpStruct::Scan(_) => "Scan",
        OpStruct::Projection(_) => "Projection",
        OpStruct::Filter(_) => "Filter",
        OpStruct::Sort(_) => "Sort",
        OpStruct::HashAgg(_) => "HashAgg",
        OpStruct::Limit(_) => "Limit",
        OpStruct::ShuffleWriter(_) => "ShuffleWriter",
        OpStruct::Expand(_) => "Expand",
        OpStruct::SortMergeJoin(_) => "SortMergeJoin",
        OpStruct::HashJoin(_) => "HashJoin",
        OpStruct::Window(_) => "Window",
        OpStruct::NativeScan(_) => "NativeScan",
        OpStruct::IcebergScan(_) => "IcebergScan",
        OpStruct::IcebergWrite(_) => "IcebergWrite",
        OpStruct::HudiScan(_) => "HudiScan",
        OpStruct::ParquetWriter(_) => "ParquetWriter",
        OpStruct::Explode(_) => "Explode",
        OpStruct::CsvScan(_) => "CsvScan",
        OpStruct::ShuffleScan(_) => "ShuffleScan",
        OpStruct::BroadcastNestedLoopJoin(_) => "BroadcastNestedLoopJoin",
        OpStruct::Sample(_) => "Sample",
        OpStruct::MergeRows(_) => "MergeRows",
        OpStruct::RangeScan(_) => "RangeScan",
        OpStruct::ContribScan(_) => "ContribScan",
        OpStruct::WindowGroupLimit(_) => "WindowGroupLimit",
        OpStruct::ArrowPythonUdf(_) => "ArrowPythonUdf",
    }
}

/// Collect distinct operator names from a plan tree and build a tracing event name.
fn build_tracing_event_name(plan: &Operator) -> String {
    let mut names = std::collections::BTreeSet::new();
    collect_op_names(plan, &mut names);
    if names.is_empty() {
        "executePlan".to_string()
    } else {
        format!(
            "executePlan({})",
            names.into_iter().collect::<Vec<_>>().join(",")
        )
    }
}

fn collect_op_names<'a>(op: &'a Operator, names: &mut std::collections::BTreeSet<&'a str>) {
    if let Some(ref op_struct) = op.op_struct {
        names.insert(op_name(op_struct));
    }
    for child in &op.children {
        collect_op_names(child, names);
    }
}

/// Comet native execution context. Kept alive across JNI calls.
struct ExecutionContext {
    /// The id of the execution context.
    pub id: i64,
    /// The deserialized Spark plan
    pub spark_plan: Operator,
    /// The number of partitions
    pub partition_count: usize,
    /// The DataFusion root operator converted from the `spark_plan`
    pub root_op: Option<Arc<SparkPlan>>,
    /// The input sources for the DataFusion plan
    pub scans: Vec<ScanExec>,
    /// The shuffle scan input sources for the DataFusion plan
    pub shuffle_scans: Vec<ShuffleScanExec>,
    /// The global reference of input sources for the DataFusion plan
    pub input_sources: Vec<Arc<Global<JObject<'static>>>>,
    /// The record batch stream to pull results from
    pub stream: Option<SendableRecordBatchStream>,
    /// Runs the plan when it has no JVM input (async I/O path)
    batch_producer: Option<BatchProducer>,
    /// The pool every reservation the plan makes goes through
    plan_memory: Arc<PlanMemoryPool>,
    /// The JVM `CometMetricNode` the plan's metrics are published to. `None` only without a
    /// JVM (unit tests).
    pub metrics: Option<Arc<Global<JObject<'static>>>>,
    // The interval in milliseconds to update metrics
    pub metrics_update_interval: Option<Duration>,
    // The last update time of metrics
    pub metrics_last_update_time: Instant,
    /// The time it took to create the native plan and configure the context
    pub plan_creation_time: Duration,
    /// DataFusion SessionContext
    pub session_ctx: Arc<SessionContext>,
    /// Whether to enable additional debugging checks & messages
    pub debug_native: bool,
    /// Whether boundaries fail on a relabelled timestamp instead of casting it (test-only)
    pub strict_timestamp_labels: bool,
    /// Whether to write native plans with metrics to stdout
    pub explain_native: bool,
    /// Whether to log memory usage on each call to execute_plan
    pub tracing_enabled: bool,
    /// Rust thread ID, used for aggregating tracing metrics per thread
    pub rust_thread_id: u64,
    /// Pre-computed metric name for tracing memory usage
    pub tracing_memory_metric_name: String,
    /// Pre-computed tracing event name for executePlan calls
    pub tracing_event_name: String,
    /// Spark `TaskContext` captured on the driving Spark task thread at `createPlan` time.
    /// Threaded into every JVM scalar UDF the planner builds so the JNI bridge can install it
    /// as the thread-local `TaskContext` for the Tokio worker running the UDF. `None` when no
    /// driving Spark task is present (unit tests, direct native driver runs). The `Arc` is
    /// cheap to clone; the underlying `Global<JObject>` releases its JNI global ref on drop
    /// via `jni`'s `Drop` impl.
    pub task_context: Option<Arc<Global<JObject<'static>>>>,
    /// Context `ClassLoader` of the driving Spark task thread, captured at `createPlan` time and
    /// threaded into every JVM scalar UDF the planner builds; see `CometUdfBridge.evaluate` for why
    /// it has to travel with the plan. `None` when no driving Spark task is present (unit tests,
    /// direct native driver runs). Lifetime is as for `task_context` above.
    pub class_loader: Option<Arc<Global<JObject<'static>>>>,
    /// Task-owned remote shuffle callback, registered before native planning starts.
    /// The callback owns a JNI global reference and can safely run on Tokio workers.
    pub shuffle_partition_pusher: Option<Arc<dyn ShufflePartitionPusher>>,
    /// The output stream of the file this plan's Parquet writer writes, registered before native
    /// planning starts when the JVM created that file through Hadoop's FileSystem API. Like the
    /// shuffle callback, it owns a JNI global reference and can run on any native thread.
    pub output_stream: Option<Arc<dyn OutputStreamSink>>,
    /// Removes this context's tracing memory-pool entry on every exit path.
    memory_pool_registration: Option<ThreadMemoryPoolRegistration>,
}

/// Accept serialized query plan and return the address of the native query plan.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_createPlan(
    e: EnvUnowned,
    _class: JClass,
    id: jlong,
    iterators: JObjectArray,
    serialized_query: JByteArray,
    serialized_spark_configs: JByteArray,
    partition_count: jint,
    metrics_node: JObject,
    metrics_update_interval: jlong,
    comet_task_memory_manager_obj: JObject,
    local_dirs: JObjectArray,
    batch_size: jint,
    off_heap_mode: jboolean,
    memory_pool_type: JString,
    memory_limit: jlong,
    task_attempt_id: jlong,
    task_cpus: jlong,
    key_unwrapper_obj: JObject,
    task_context_obj: JObject,
    class_loader_obj: JObject,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        let bytes = env.convert_byte_array(serialized_spark_configs)?;
        let settings = prepare_plan_settings(&bytes)?;

        with_trace("createPlan", settings.tracing_enabled, || {
            // Init JVM classes
            JVMClasses::init(env);

            let start = Instant::now();

            let serialized_plan = env.convert_byte_array(serialized_query)?;

            let metrics = Arc::new(jni_new_global_ref!(env, metrics_node)?);

            // Get the global references of input sources
            let mut input_sources = vec![];
            let num_inputs = iterators.len(env)?;
            for i in 0..num_inputs {
                let input_source = iterators.get_element(env, i)?;
                let input_source = Arc::new(jni_new_global_ref!(env, input_source)?);
                input_sources.push(input_source);
            }

            let task_memory_manager =
                Arc::new(jni_new_global_ref!(env, comet_task_memory_manager_obj)?);

            let memory_pool_type = memory_pool_type.try_to_string(env)?;

            // Get local directories for storing spill files
            let num_local_dirs = local_dirs.len(env)?;
            let mut local_dirs_vec = vec![];
            for i in 0..num_local_dirs {
                let local_dir = local_dirs.get_element(env, i)?;
                let local_dir = unsafe { JString::from_raw(&*env, local_dir.into_raw()) };
                let local_dir = local_dir.try_to_string(env)?;
                local_dirs_vec.push(local_dir);
            }

            // Key unwrapper for encrypted files
            let key_unwrapper = if !key_unwrapper_obj.is_null() {
                Some(Arc::new(jni_new_global_ref!(env, key_unwrapper_obj)?))
            } else {
                None
            };

            // Capture the driving Spark task's TaskContext and context ClassLoader as JNI global
            // references when non-null. The `Arc<Global<JObject>>` releases its global ref on
            // drop, so cleanup is automatic when the ExecutionContext drops.
            let task_context = if !task_context_obj.is_null() {
                Some(Arc::new(jni_new_global_ref!(env, task_context_obj)?))
            } else {
                None
            };

            let class_loader = if !class_loader_obj.is_null() {
                Some(Arc::new(jni_new_global_ref!(env, class_loader_obj)?))
            } else {
                None
            };

            let inputs = PlanInputs {
                id,
                settings,
                serialized_plan,
                partition_count: partition_count as usize,
                batch_size: batch_size as usize,
                off_heap_mode: off_heap_mode != JNI_FALSE,
                memory_pool_type,
                memory_limit,
                task_attempt_id,
                task_cpus: task_cpus as usize,
                local_dirs: local_dirs_vec,
                metrics_update_interval,
                start,
                jvm: PlanJvmRefs {
                    metrics: Some(metrics),
                    input_sources,
                    key_unwrapper,
                    task_context,
                    class_loader,
                },
            };
            // The pool acquires memory from Spark's task memory manager.
            let exec_context = create_execution_context(inputs, |config| {
                create_memory_pool(config, task_memory_manager, task_attempt_id)
            })?;

            Ok(Box::into_raw(exec_context) as i64)
        })
    })
}

/// What `createPlan` takes from the Spark configs.
struct PlanSettings {
    spark_config: HashMap<String, String>,
    debug_native: bool,
    strict_timestamp_labels: bool,
    explain_native: bool,
    tracing_enabled: bool,
    max_temp_directory_size: u64,
    logging_memory_pool: bool,
}

/// Deserializes the Spark configs of a plan and initializes the Tokio runtime from them. The
/// first step of `Native.createPlan`, before its trace span opens.
fn prepare_plan_settings(serialized_spark_configs: &[u8]) -> CometResult<PlanSettings> {
    let spark_configs = serde::deserialize_config(serialized_spark_configs)?;
    let spark_config: HashMap<String, String> = spark_configs.entries.into_iter().collect();

    // Initialize the tokio runtime with spark.executor.cores as the default
    // worker thread count, falling back to 1 if not set.
    let executor_cores = spark_config.get_usize(SPARK_EXECUTOR_CORES, 1);
    init_runtime(executor_cores);

    // Access Comet configs
    Ok(PlanSettings {
        debug_native: spark_config.get_bool(COMET_DEBUG_ENABLED),
        strict_timestamp_labels: spark_config.get_bool(COMET_STRICT_TIMESTAMP_LABELS),
        explain_native: spark_config.get_bool(COMET_EXPLAIN_NATIVE_ENABLED),
        tracing_enabled: spark_config.get_bool(COMET_TRACING_ENABLED),
        max_temp_directory_size: spark_config
            .get_u64(COMET_MAX_TEMP_DIRECTORY_SIZE, 100 * 1024 * 1024 * 1024),
        logging_memory_pool: spark_config.get_bool(COMET_DEBUG_MEMORY),
        spark_config,
    })
}

/// The arguments of `Native.createPlan`, converted from JNI.
struct PlanInputs {
    id: i64,
    settings: PlanSettings,
    serialized_plan: Vec<u8>,
    partition_count: usize,
    batch_size: usize,
    off_heap_mode: bool,
    memory_pool_type: String,
    memory_limit: i64,
    task_attempt_id: i64,
    task_cpus: usize,
    local_dirs: Vec<String>,
    /// In milliseconds; zero or less publishes metrics only when the plan is released.
    metrics_update_interval: i64,
    /// When plan creation started, the start of `ExecutionContext::plan_creation_time`.
    start: Instant,
    jvm: PlanJvmRefs,
}

/// The JVM objects a plan calls back into, as JNI global references. Each is `None` or empty
/// only without a JVM (unit tests) or when Spark passes none.
struct PlanJvmRefs {
    metrics: Option<Arc<Global<JObject<'static>>>>,
    input_sources: Vec<Arc<Global<JObject<'static>>>>,
    key_unwrapper: Option<Arc<Global<JObject<'static>>>>,
    task_context: Option<Arc<Global<JObject<'static>>>>,
    class_loader: Option<Arc<Global<JObject<'static>>>>,
}

/// Creates the execution context of a native plan: deserializes the plan, sets up its memory
/// pool and DataFusion session, and registers the pool for tracing. The plan's operators are
/// created by the first `execute_plan`. Core of `Native.createPlan`.
///
/// `memory_pool_for` builds the pool every reservation of the plan goes through from its
/// config; `Native.createPlan` builds one that acquires memory from Spark.
fn create_execution_context(
    inputs: PlanInputs,
    memory_pool_for: impl FnOnce(&MemoryPoolConfig) -> Arc<dyn MemoryPool>,
) -> CometResult<Box<ExecutionContext>> {
    let PlanInputs {
        id,
        settings,
        serialized_plan,
        partition_count,
        batch_size,
        off_heap_mode,
        memory_pool_type,
        memory_limit,
        task_attempt_id,
        task_cpus,
        local_dirs,
        metrics_update_interval,
        start,
        jvm,
    } = inputs;

    // Deserialize query plan
    let spark_plan = serde::deserialize_op(serialized_plan.as_slice())?;

    // Create DataFusion memory pool
    let memory_pool_config =
        parse_memory_pool_config(off_heap_mode, memory_pool_type, memory_limit)?;
    let memory_pool = memory_pool_for(&memory_pool_config);

    // Register the shared base pool before wrapping it for per-plan debug logging. The
    // guard removes the entry if any later plan setup step fails.
    //
    // Registration is not conditional on this plan's tracing setting. `tracing.enabled` is
    // a session config, so an executor can run a traced and an untraced plan at once,
    // while the allocation counter a trace compares against is process-wide. Registering
    // only traced plans would leave the untraced plan's reservation out of the total and
    // report it as allocation held outside any pool.
    let rust_thread_id = get_thread_id();
    let memory_pool_registration = Some(ThreadMemoryPoolRegistration::new(
        rust_thread_id,
        id,
        Arc::clone(&memory_pool),
    ));

    let memory_pool = if settings.logging_memory_pool {
        Arc::new(LoggingMemoryPool::new(task_attempt_id as u64, memory_pool))
    } else {
        memory_pool
    };
    let plan_memory = Arc::new(PlanMemoryPool::new(memory_pool));

    // We need to keep the session context alive. Some session state like temporary
    // dictionaries are stored in session context. If it is dropped, the temporary
    // dictionaries will be dropped as well.
    let session = prepare_datafusion_session_context(
        batch_size,
        Arc::clone(&plan_memory) as Arc<dyn MemoryPool>,
        local_dirs,
        settings.max_temp_directory_size,
        task_cpus,
        &settings.spark_config,
        &spark_plan,
    )?;

    let plan_creation_time = start.elapsed();

    let metrics_update_interval = if metrics_update_interval > 0 {
        Some(Duration::from_millis(metrics_update_interval as u64))
    } else {
        None
    };

    // Handle key unwrapper for encrypted files
    if let Some(key_unwrapper) = jvm.key_unwrapper {
        let encryption_factory = CometEncryptionFactory { key_unwrapper };
        session.runtime_env().register_parquet_encryption_factory(
            ENCRYPTION_FACTORY_ID,
            Arc::new(encryption_factory),
        );
    }

    let session = Arc::new(session);

    let tracing_event_name = if settings.tracing_enabled {
        build_tracing_event_name(&spark_plan)
    } else {
        String::new()
    };

    Ok(Box::new(ExecutionContext {
        id,
        spark_plan,
        partition_count,
        root_op: None,
        scans: vec![],
        shuffle_scans: vec![],
        input_sources: jvm.input_sources,
        stream: None,
        batch_producer: None,
        plan_memory,
        metrics: jvm.metrics,
        metrics_update_interval,
        metrics_last_update_time: Instant::now(),
        plan_creation_time,
        session_ctx: session,
        debug_native: settings.debug_native,
        strict_timestamp_labels: settings.strict_timestamp_labels,
        explain_native: settings.explain_native,
        tracing_enabled: settings.tracing_enabled,
        rust_thread_id,
        tracing_memory_metric_name: format!("thread_{rust_thread_id}_comet_memory_reserved"),
        tracing_event_name,
        task_context: jvm.task_context,
        class_loader: jvm.class_loader,
        shuffle_partition_pusher: None,
        output_stream: None,
        memory_pool_registration,
    }))
}

/// Binds one task-owned shuffle callback before native execution is initialized.
///
/// Keeping callback registration separate preserves the existing `createPlan` JNI ABI for
/// all local shuffle and non-shuffle callers.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_Native_setShufflePartitionPusher(
    e: EnvUnowned,
    _class: JClass,
    exec_context: jlong,
    callback: JObject,
) {
    try_unwrap_or_throw(&e, |env| {
        if exec_context == 0 {
            return Err(CometError::NullPointer(
                "Remote shuffle callback requires a valid native execution plan".to_string(),
            ));
        }

        let exec_context = get_execution_context(exec_context);
        register_shuffle_partition_pusher(exec_context, || {
            Ok(Arc::new(JavaShufflePartitionPusher::try_new(
                env, &callback,
            )?))
        })
    })
}

/// Registers the plan's remote shuffle callback, built by `make_pusher` only once the plan
/// is known to accept one: before execution starts, and once. Core of
/// `Native.setShufflePartitionPusher`, whose pusher calls back into the JVM.
fn register_shuffle_partition_pusher(
    exec_context: &mut ExecutionContext,
    make_pusher: impl FnOnce() -> CometResult<Arc<dyn ShufflePartitionPusher>>,
) -> CometResult<()> {
    register_before_execution(
        exec_context.root_op.is_some(),
        &mut exec_context.shuffle_partition_pusher,
        "Remote shuffle callback",
        make_pusher,
    )
}

/// Registers one of a plan's task-owned JVM callbacks in `slot`, built by `make` only once the
/// plan is known to accept it: before execution has `started`, and once. `what` names the callback
/// in the errors.
fn register_before_execution<T: ?Sized>(
    started: bool,
    slot: &mut Option<Arc<T>>,
    what: &str,
    make: impl FnOnce() -> CometResult<Arc<T>>,
) -> CometResult<()> {
    if started {
        return Err(CometError::Internal(format!(
            "{what} cannot be registered after native execution starts"
        )));
    }

    if slot.is_some() {
        return Err(CometError::Internal(format!(
            "{what} has already been registered for this task"
        )));
    }

    *slot = Some(make()?);
    Ok(())
}

/// Binds the output stream of the file the plan's Parquet writer writes, for a file the JVM
/// created itself, before native execution is initialized.
///
/// Separate from `createPlan` for the same reason as `setShufflePartitionPusher`: only native
/// writes the JVM creates the file for need it, so every other caller keeps the existing ABI.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_Native_setOutputStream(
    e: EnvUnowned,
    _class: JClass,
    exec_context: jlong,
    output: JObject,
) {
    try_unwrap_or_throw(&e, |env| {
        if exec_context == 0 {
            return Err(CometError::NullPointer(
                "A native write's output stream requires a valid native execution plan".to_string(),
            ));
        }

        let exec_context = get_execution_context(exec_context);
        register_before_execution(
            exec_context.root_op.is_some(),
            &mut exec_context.output_stream,
            "A native write's output stream",
            || {
                let sink: Arc<dyn OutputStreamSink> =
                    Arc::new(JavaOutputStreamSink::try_new(env, &output)?);
                Ok(sink)
            },
        )
    })
}

/// Skipping is opt-in (`spark.comet.exec.aggregate.skipPartial.enabled`): once DataFusion's probe
/// decides to skip, it never aggregates again, so a task whose keys repeat after a mostly distinct
/// start shuffles every later row (#6466).
///
/// When enabled, only admit the validated native-shuffle path. A session belongs to one fused
/// Spark plan, so an unsafe partial aggregate disables skipping for the whole plan, including its
/// children. This deliberately gives up some opportunities rather than changing execution
/// contexts per op.
fn configure_skip_partial_aggregation(config: &mut SessionConfig, plan: &Operator, enabled: bool) {
    fn supported(plan: &Operator) -> bool {
        let supported_aggregate = match &plan.op_struct {
            Some(OpStruct::HashAgg(agg)) => match AggregateMode::try_from(agg.mode) {
                // Final never skips. Still inspect its children below.
                Ok(AggregateMode::Final) => true,
                Ok(AggregateMode::Partial) => {
                    agg.expr_modes
                        .iter()
                        .all(|mode| *mode == AggregateMode::Partial as i32)
                        && agg.agg_exprs.iter().all(|expr| {
                            matches!(&expr.expr_struct, Some(AggExprStruct::Count(count))
                                if count.children.len() == 1)
                        })
                }
                // PartialMerge consumes states, not rows.
                _ => false,
            },
            _ => true,
        };
        supported_aggregate && plan.children.iter().all(supported)
    }

    let eligible =
        enabled && matches!(&plan.op_struct, Some(OpStruct::ShuffleWriter(_))) && supported(plan);
    if !eligible {
        // Enforce this after config pass-through: a testing override can neither turn skipping
        // on nor make unsupported accumulators convertible. DF 55 removed
        // supports_convert_to_state().
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_ratio_threshold = 1.1;
    }
}

/// Configure DataFusion session context.
fn prepare_datafusion_session_context(
    batch_size: usize,
    memory_pool: Arc<dyn MemoryPool>,
    local_dirs: Vec<String>,
    max_temp_directory_size: u64,
    task_cpus: usize,
    spark_config: &HashMap<String, String>,
    spark_plan: &Operator,
) -> CometResult<SessionContext> {
    let paths = local_dirs.into_iter().map(PathBuf::from).collect();
    let mut rt_config = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(plan_disk_manager(paths, max_temp_directory_size))
        .with_object_store_registry(Arc::new(CometObjectStoreRegistry::default()));
    rt_config = rt_config.with_memory_pool(memory_pool);

    let mut session_config = SessionConfig::new()
        .with_target_partitions(task_cpus)
        // This DataFusion context is within the scope of an executing Spark Task. We want to set
        // its internal parallelism to the number of CPUs allocated to Spark Tasks. This can be
        // modified by changing spark.task.cpus in the Spark config.
        .with_batch_size(batch_size);

    // Translate the Comet-namespaced row-level pushdown flag into the equivalent
    // DataFusion session options. `pushdown_filters` enables the parquet reader's
    // RowFilter evaluation during decode (late materialization); `reorder_filters`
    // is only meaningful when pushdown_filters is on, so they move together. Set
    // before the `spark.comet.datafusion.*` testing escape hatch pass-through below,
    // so an explicit override of either key wins instead of being silently forced
    // back to `true`.
    if spark_config.get_bool(COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED) {
        session_config =
            session_config.set_str("datafusion.execution.parquet.pushdown_filters", "true");
        session_config =
            session_config.set_str("datafusion.execution.parquet.reorder_filters", "true");
    }

    // Native Parquet scan options that DataFusion has no setting for travel as a session config
    // extension, which every scan in the plan reads.
    session_config = session_config.with_extension(Arc::new(CometParquetScanOptions {
        prefetch_next_file_metadata: spark_config
            .get_bool(COMET_PARQUET_PREFETCH_NEXT_FILE_METADATA_ENABLED),
    }));

    // Translate Comet's spill codec into DataFusion's spill compression, which DataFusion
    // otherwise leaves uncompressed. Set before the pass-through below for the same reason,
    // so an explicit `spark.comet.datafusion.execution.spill_compression` still wins.
    let spill_compression = match spark_config
        .get(COMET_EXEC_SPILL_COMPRESSION_CODEC)
        .map(String::as_str)
    {
        None | Some("lz4") => SpillCompression::Lz4Frame,
        Some("zstd") => SpillCompression::Zstd,
        Some("none") => SpillCompression::Uncompressed,
        Some(codec) => {
            return Err(CometError::Config(format!(
                "Unsupported spill compression codec: {codec}"
            )))
        }
    };
    session_config = session_config.with_spill_compression(spill_compression);

    // Pass through DataFusion configs from Spark.
    // e.g: spark-shell --conf spark.comet.datafusion.sql_parser.parse_float_as_decimal=true
    // becomes datafusion.sql_parser.parse_float_as_decimal=true
    const SPARK_COMET_DF_PREFIX: &str = "spark.comet.datafusion.";
    for (key, value) in spark_config {
        if let Some(df_key) = key.strip_prefix(SPARK_COMET_DF_PREFIX) {
            let df_key = format!("datafusion.{df_key}");
            session_config = session_config.set_str(&df_key, value);
        }
    }

    // The planner reads this back to decide whether a plan may load a native UDF library.
    session_config = session_config.with_extension(Arc::new(
        crate::execution::c_udf::policy::NativeUdfPolicy::from_spark_config(spark_config),
    ));

    configure_skip_partial_aggregation(
        &mut session_config,
        spark_plan,
        spark_config.get_bool(COMET_EXEC_AGGREGATE_SKIP_PARTIAL_ENABLED),
    );

    let runtime = rt_config.build()?;

    let mut session_ctx = SessionContext::new_with_config_rt(session_config, Arc::new(runtime));

    datafusion::functions_nested::register_all(&mut session_ctx)?;
    register_datafusion_spark_function(&session_ctx);
    // Must be the last one to override existing functions with the same name
    datafusion_comet_spark_expr::register_all_comet_functions(&mut session_ctx)?;

    Ok(session_ctx)
}

// register UDFs from datafusion-spark crate
fn register_datafusion_spark_function(session_ctx: &SessionContext) {
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkExpm1::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkSha2::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(CharFunc::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkBitGet::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkDateAdd::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkDateSub::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkFromUtcTimestamp::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkLastDay::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkToUtcTimestamp::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkSha1::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkConcat::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkBitwiseNot::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkHex::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkWidthBucket::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(MapFromEntries::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkCrc32::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkLuhnCheck::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkSpace::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkBitCount::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkArrayContains::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkArrayRepeat::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkBin::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkStrToMap::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkUrlDecode::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkUrlEncode::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkTryUrlDecode::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkCsc::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(CometParseUrl::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(CometTryParseUrl::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkFactorial::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkSec::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkRint::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkBitShift::right_unsigned()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkSoundex::default()));
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkSubstring::default()));
    // Serves length, char_length, character_length and len. SparkLengthFunc does not unwrap
    // dictionary arrays and its uniform signature gets no planner coercion, so it relies on
    // every input reaching expressions as plain Utf8 or Binary: ScanExec and the shuffle scan
    // unpack dictionaries and the Parquet adapter casts to the required Spark type.
    session_ctx.register_udf(ScalarUDF::new_from_impl(SparkLengthFunc::default()));
}

/// Prepares arrow arrays for output.
fn prepare_output(
    env: &mut Env,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    output_batch: RecordBatch,
    validate: bool,
) -> CometResult<jlong> {
    let array_addrs = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
    let schema_addrs = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
    unsafe { export_batch(&array_addrs, &schema_addrs, output_batch, validate) }
}

/// Moves the columns of `output_batch` into the Arrow C Data Interface structs at `array_addrs`
/// and `schema_addrs`, one pair per column, and returns the row count. With no addresses, which
/// Spark passes when the results of a query are not used, only the row count is returned.
///
/// # Safety
/// Each address must point to a writable `FFI_ArrowArray` / `FFI_ArrowSchema` the caller owns.
/// Whatever the structs hold is overwritten without being released.
unsafe fn export_batch(
    array_addrs: &[i64],
    schema_addrs: &[i64],
    output_batch: RecordBatch,
    validate: bool,
) -> CometResult<i64> {
    let num_cols = array_addrs.len();

    let output_schema = output_batch.schema();
    let results = output_batch.columns();
    let num_rows = output_batch.num_rows();

    // there are edge cases where num_cols can be zero due to Spark optimizations
    // when the results of a query are not used
    if num_cols > 0 {
        if results.len() != num_cols {
            return Err(CometError::Internal(format!(
                "Output column count mismatch: expected {num_cols}, got {}",
                results.len()
            )));
        }

        if validate {
            // Validate the output arrays.
            for array in results.iter() {
                let array_data = array.to_data();
                array_data
                    .validate_full()
                    .expect("Invalid output array data");
            }
        }

        let mut i = 0;
        while i < results.len() {
            let array_ref = results.get(i).ok_or(CometError::IndexOutOfBounds(i))?;
            let field = output_schema.field(i);

            array_ref
                .to_data()
                .move_to_spark(field, array_addrs[i], schema_addrs[i])?;
            i += 1;
        }
    }

    Ok(num_rows as i64)
}

/// Pull the next input from JVM. Note that we cannot pull input batches in
/// `ScanStream.poll_next` when the execution stream is polled for output.
/// Because the input source could be another native execution stream, which
/// will be executed in another tokio blocking thread. It causes JNI throw
/// Java exception. So we pull input batches here and insert them into scan
/// operators before polling the stream,
#[inline]
fn pull_input_batches(exec_context: &mut ExecutionContext) -> Result<(), CometError> {
    exec_context.scans.iter_mut().try_for_each(|scan| {
        scan.get_next_batch()?;
        Ok::<(), CometError>(())
    })?;
    exec_context.shuffle_scans.iter_mut().try_for_each(|scan| {
        scan.get_next_batch()?;
        Ok::<(), CometError>(())
    })
}

/// Forwards a wake-up to the `block_on` task and records that it happened, so `next_batch` can
/// tell whether the stream was woken even if something else took the wake-up from the thread's
/// parker.
struct WakeFlag {
    woken: AtomicBool,
    parent: Waker,
}

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.parent.wake_by_ref();
    }
}

/// Drives `stream` to its next item. JVM-fed scans return `Pending` until `on_pending` refills
/// them, so every pending poll runs it, and the refill wakes the stream.
///
/// Each poll gives the stream a `WakeFlag` waker. If the flag is still clear when `on_pending`
/// returns, the stream is waiting on native I/O and `block_on` parks until it completes.
/// Otherwise `next_batch` wakes `block_on` itself so that it polls again at once. It can't rely on
/// the wake-up that set the flag, because `on_pending` can run another Comet plan on this thread,
/// as when a native writer's input is itself native. That plan's `block_on` shares this thread's
/// parker, which holds a single wake-up, and its park can take this one.
///
/// It polls again by yielding to `block_on` rather than looping here, so that each poll starts
/// with a fresh coop budget. A stream that has spent its budget wakes itself and returns
/// `Pending`, and a loop here would only get past that because `block_in_place` happens to leave
/// this thread's budget unconstrained.
async fn next_batch<S>(
    stream: &mut S,
    mut on_pending: impl FnMut() -> Result<(), CometError>,
) -> Result<Option<RecordBatch>, CometError>
where
    S: Stream<Item = DataFusionResult<RecordBatch>> + Unpin,
{
    poll_fn(|cx| {
        let flag = Arc::new(WakeFlag {
            woken: AtomicBool::new(false),
            parent: cx.waker().clone(),
        });
        let waker = Waker::from(Arc::clone(&flag));
        if let Poll::Ready(item) = stream.poll_next_unpin(&mut Context::from_waker(&waker)) {
            return Poll::Ready(Ok(item.transpose()?));
        }
        // `on_pending` calls into the JVM, which can run another Comet plan on this thread.
        // `block_in_place` exits the runtime context so that plan's `block_on` doesn't panic.
        tokio::task::block_in_place(&mut on_pending)?;
        if flag.woken.load(Ordering::Acquire) {
            // Poll again at once: a nested `block_on` may have taken the wake-up.
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    })
    .await
}

/// Runs a plan that has no JVM input on a Tokio task, which sends the plan's batches to the Spark
/// task thread.
struct BatchProducer {
    batches: mpsc::Receiver<DataFusionResult<RecordBatch>>,
    /// Set by the task once it has sent the stream's last batch. The channel closes whenever the
    /// task ends, and a runtime that shuts down ends every task it has by cancelling it, so only
    /// this tells the consumer that it has had every batch.
    stream_ended: Arc<AtomicBool>,
    /// The plan's stream, and with it every reservation the stream holds. The task locks it only
    /// while polling it, so `stop` can take it whenever the task is waiting.
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    task: JoinHandle<()>,
}

impl BatchProducer {
    fn spawn(runtime: &Handle, stream: SendableRecordBatchStream) -> Self {
        // Channel capacity of 2 allows the producer to work one batch
        // ahead while the consumer processes the current one via JNI,
        // without buffering excessive memory. Increasing this would
        // trade memory for latency hiding if JNI/FFI overhead dominates;
        // decreasing to 1 would serialize production and consumption.
        let (tx, batches) = mpsc::channel(2);
        let stream_ended = Arc::new(AtomicBool::new(false));
        let ended = Arc::clone(&stream_ended);
        let stream = Arc::new(Mutex::new(Some(stream)));
        let polled = Arc::clone(&stream);
        let task = runtime.spawn(async move {
            let result = std::panic::AssertUnwindSafe(async {
                // Ends once `stop` has taken the stream.
                let next = || {
                    poll_fn(|cx| match polled.lock().as_mut() {
                        Some(stream) => stream.poll_next_unpin(cx),
                        None => Poll::Ready(None),
                    })
                };
                while let Some(batch) = next().await {
                    if tx.send(batch).await.is_err() {
                        return;
                    }
                }
                ended.store(true, Ordering::Release);
            })
            .catch_unwind()
            .await;

            if let Err(panic) = result {
                let msg = match panic.downcast_ref::<&str>() {
                    Some(s) => s.to_string(),
                    None => match panic.downcast_ref::<String>() {
                        Some(s) => s.clone(),
                        None => "unknown panic".to_string(),
                    },
                };
                let _ = tx
                    .send(Err(DataFusionError::Execution(format!(
                        "native panic: {msg}"
                    ))))
                    .await;
            }
        });
        Self {
            batches,
            stream_ended,
            stream,
            task,
        }
    }

    /// Waits for the plan's next batch, and returns `None` once the stream has ended.
    ///
    /// A channel that closes before the stream has ended means the task was cancelled, and the
    /// plan's output is incomplete. That is an error, or the Spark task would end successfully
    /// with only the batches it has read so far.
    fn next_batch(&mut self) -> CometResult<Option<RecordBatch>> {
        match self.batches.blocking_recv() {
            Some(batch) => Ok(Some(batch?)),
            None if self.stream_ended.load(Ordering::Acquire) => Ok(None),
            None => Err(CometError::Internal(
                "The Tokio task running the native plan was cancelled before the plan produced \
                 all of its output, for instance because Comet's Tokio runtime was shut down"
                    .to_string(),
            )),
        }
    }

    /// Stops the task and drops the plan's stream on the calling thread.
    ///
    /// The stream is dropped here rather than by cancelling the task, because a cancellation only
    /// runs once a Tokio worker is free, and every worker can be tied up, for instance waiting in
    /// Spark's `acquireMemory` for the memory this stream holds. Taking the stream waits only for
    /// a poll the task is already in, so at most for the work the stream does between two await
    /// points.
    fn stop(self) -> CometResult<()> {
        let Self {
            batches,
            stream,
            task,
            ..
        } = self;
        drop(batches);
        task.abort();
        let stream = stream.lock().take();
        // A panic here is reported as it is when `releasePlan` drops a JVM-fed plan's stream.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(stream)))
            .map_err(CometError::from)
    }
}

/// Accept serialized query plan and the addresses of Arrow Arrays from Spark,
/// then execute the query. Return addresses of arrow vector.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_executePlan(
    e: EnvUnowned,
    _class: JClass,
    stage_id: jint,
    partition: jint,
    exec_context: jlong,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        // Retrieve the query
        let exec_context = get_execution_context(exec_context);

        let tracing_enabled = exec_context.tracing_enabled;
        // Clone the label only when tracing is enabled. The clone is needed
        // because the closure below mutably borrows exec_context.
        let owned_label;
        let tracing_label = if tracing_enabled {
            owned_label = exec_context.tracing_event_name.clone();
            owned_label.as_str()
        } else {
            ""
        };

        let result = with_trace(tracing_label, tracing_enabled, || {
            let mut publish_metrics = |ctx: &ExecutionContext| update_metrics(env, ctx);
            match execute_plan(exec_context, stage_id, partition, &mut publish_metrics)? {
                Some(batch) => prepare_output(
                    env,
                    array_addrs,
                    schema_addrs,
                    batch,
                    exec_context.debug_native,
                ),
                None => Ok(-1),
            }
        });

        if exec_context.tracing_enabled {
            #[cfg(feature = "jemalloc")]
            log_jemalloc_usage();
            log_native_allocated();
            // Both totals come from one read of the registry, so the pair describes a single
            // instant, and both are emitted next to the allocation counter above so a trace can
            // compare them. The per-thread counter cannot be summed across threads to obtain the
            // process-wide one: it reports a shared pool's full reservation once per referencing
            // thread.
            let totals = total_reserved(Some(exec_context.rust_thread_id));
            log_memory_usage(
                &exec_context.tracing_memory_metric_name,
                totals.for_thread as u64,
            );
            log_memory_usage(POOL_TOTAL_METRIC, totals.across_threads as u64);
        }

        result
    })
}

/// Publishes a plan's metrics to the JVM. The plan entry points pass one that calls into the JVM
/// through their `Env`, so the core functions that drive a plan take no JNI types.
type PublishMetrics<'a> = dyn FnMut(&ExecutionContext) -> CometResult<()> + 'a;

/// Returns the plan's next output batch, or `None` once the plan has produced all of its output.
/// The first call creates the plan's operators and starts its stream; a plan with no JVM input
/// then runs on a Tokio task. Metrics are published through `publish_metrics` on the configured
/// interval. Core of `Native.executePlan`; the JVM-fed scans call back into the JVM for their
/// input.
fn execute_plan(
    exec_context: &mut ExecutionContext,
    stage_id: i32,
    partition: i32,
    publish_metrics: &mut PublishMetrics,
) -> CometResult<Option<RecordBatch>> {
    let exec_context_id = exec_context.id;

    // Initialize the execution stream.
    // Because we don't know if input arrays are dictionary-encoded when we create
    // query plan, we need to defer stream initialization to first time execution.
    if exec_context.root_op.is_none() {
        let start = Instant::now();
        let planner = PhysicalPlanner::new(Arc::clone(&exec_context.session_ctx), partition)
            .with_exec_id(exec_context_id)
            .with_strict_timestamp_labels(exec_context.strict_timestamp_labels)
            .with_sql_text_pool(&exec_context.spark_plan)
            .with_task_context(exec_context.task_context.clone())
            .with_class_loader(exec_context.class_loader.clone())
            .with_shuffle_partition_pusher(exec_context.shuffle_partition_pusher.clone())
            .with_output_stream(exec_context.output_stream.clone());
        let (scans, shuffle_scans, root_op) = planner.create_plan(
            &exec_context.spark_plan,
            &mut exec_context.input_sources.clone(),
            exec_context.partition_count,
        )?;
        let physical_plan_time = start.elapsed();

        exec_context.plan_creation_time += physical_plan_time;
        exec_context.scans = scans;
        exec_context.shuffle_scans = shuffle_scans;

        if exec_context.explain_native {
            let formatted_plan_str =
                DisplayableExecutionPlan::new(root_op.native_plan.as_ref()).indent(true);
            info!("Comet native query plan:\n{formatted_plan_str:}");
        }

        let task_ctx = exec_context.session_ctx.task_ctx();
        // Each Comet native execution corresponds to a single Spark partition,
        // so we should always execute partition 0.
        let stream = root_op.native_plan.execute(0, task_ctx)?;

        if exec_context.scans.is_empty() && exec_context.shuffle_scans.is_empty() {
            // No JVM data sources — spawn onto tokio so the executor
            // thread parks in blocking_recv instead of busy-polling.
            exec_context.batch_producer = Some(BatchProducer::spawn(&get_runtime(), stream));
        } else {
            exec_context.stream = Some(stream);
        }
        exec_context.root_op = Some(root_op);
    } else {
        // Pull input batches
        pull_input_batches(exec_context)?;
    }

    if let Some(producer) = &mut exec_context.batch_producer {
        return match producer.next_batch()? {
            Some(batch) => {
                // Publish on the configured interval, as the ScanExec path below does,
                // since each publish walks the whole metric tree and calls into the JVM.
                // `releasePlan` publishes the final values.
                update_metrics_on_interval(exec_context, publish_metrics)?;
                Ok(Some(batch))
            }
            None => {
                log_plan_metrics(exec_context, stage_id, partition);
                Ok(None)
            }
        };
    }

    // ScanExec path: JVM-fed scans return `Pending` until `pull_input_batches` refills
    // them and wakes the stream. A poll that is still pending, with nothing having woken
    // the stream by the end of the pull, waits on native I/O, and `next_batch` parks
    // until it completes.
    let mut stream = exec_context.stream.take().unwrap();
    let next = get_runtime().block_on(next_batch(&mut stream, || {
        pull_input_batches(exec_context)?;
        update_metrics_on_interval(exec_context, publish_metrics)
    }));
    exec_context.stream = Some(stream);
    let next = next?;
    update_metrics_on_interval(exec_context, publish_metrics)?;
    if next.is_none() {
        log_plan_metrics(exec_context, stage_id, partition);
    }
    Ok(next)
}

#[no_mangle]
/// Drop the native query plan object and context object.
pub extern "system" fn Java_org_apache_comet_Native_releasePlan(
    e: EnvUnowned,
    _class: JClass,
    exec_context: jlong,
) {
    try_unwrap_or_throw(&e, |env| unsafe {
        // A null pointer from the JVM would be undefined behaviour in `Box::from_raw`; panic
        // instead, which `try_unwrap_or_throw` converts into a Java exception (this is the check
        // `get_execution_context` performs for the other JNI entry points).
        assert_ne!(
            exec_context, 0,
            "Comet execution context shouldn't be null!"
        );

        // Reclaim ownership of the context up front so that it is always freed, even if a step
        // below fails. Dropping it releases the memory pool and every JNI global ref the context
        // holds.
        let execution_context: Box<ExecutionContext> =
            Box::from_raw(exec_context as *mut ExecutionContext);

        release_plan(execution_context, &mut |ctx| update_metrics(env, ctx))
    })
}

/// Releases a plan: stops it, publishes its final metrics through `publish_metrics`, drops it,
/// and waits briefly for its memory reservations to be returned. The context is freed even if a
/// step fails. Core of `Native.releasePlan`.
fn release_plan(
    mut execution_context: Box<ExecutionContext>,
    publish_metrics: &mut PublishMetrics,
) -> CometResult<()> {
    // A plan with no JVM input runs on a Tokio task. Stop it and drop its stream first, so the
    // stream's memory is returned before the Spark task can end, and the metrics flushed below
    // include what the stream records as it is dropped.
    let producer_stopped = execution_context
        .batch_producer
        .take()
        .map_or(Ok(()), BatchProducer::stop);

    // Unregister this context's pool and, when tracing, emit the remaining total for the
    // thread. Every context registers, but only a traced one writes counters, so the
    // reservations are read only then.
    drop(execution_context.memory_pool_registration.take());
    if execution_context.tracing_enabled {
        log_memory_usage(
            &execution_context.tracing_memory_metric_name,
            total_reserved_for_thread(execution_context.rust_thread_id) as u64,
        );
    }

    let metrics_flushed = publish_metrics(&execution_context);

    // Once this returns the Spark task can end, and Spark hands whatever the task still holds
    // to other tasks. Dropping the plan aborts the tasks its operators spawned, and they give
    // back what they hold the next time they yield, so wait for them.
    let id = execution_context.id;
    let plan_memory = Arc::clone(&execution_context.plan_memory);
    drop(execution_context);
    let held = plan_memory.wait_until_released(Instant::now() + PLAN_MEMORY_RELEASE_TIMEOUT);
    if held > 0 {
        warn!(
            "Native plan {id} still holds {held} bytes {PLAN_MEMORY_RELEASE_TIMEOUT:?} after \
             it was released. They go back to Spark only when whatever holds them drops them, \
             which can be after the Spark task has ended and its memory has been given to \
             another task."
        );
    }

    producer_stopped.and(metrics_flushed)
}

/// How long `releasePlan` waits for a released plan's reservations to be returned. The tasks
/// holding them have been aborted and stop the next time they yield, so this is only reached if
/// something keeps a reservation it never gives back, where waiting longer would only stall the
/// Spark task.
const PLAN_MEMORY_RELEASE_TIMEOUT: Duration = Duration::from_secs(1);

/// Runs `publish_metrics` once the configured interval has passed and, with tracing on, samples
/// this thread's pool reservation at the same cadence.
fn update_metrics_on_interval(
    exec_context: &mut ExecutionContext,
    publish_metrics: &mut PublishMetrics,
) -> CometResult<()> {
    let Some(interval) = exec_context.metrics_update_interval else {
        return Ok(());
    };
    let now = Instant::now();
    if now - exec_context.metrics_last_update_time < interval {
        return Ok(());
    }
    publish_metrics(exec_context)?;
    exec_context.metrics_last_update_time = now;
    if exec_context.tracing_enabled {
        log_memory_usage(
            &exec_context.tracing_memory_metric_name,
            total_reserved_for_thread(exec_context.rust_thread_id) as u64,
        );
    }
    Ok(())
}

/// Publishes the plan's metrics to its JVM `CometMetricNode`, once the plan has been created.
fn update_metrics(env: &mut Env, exec_context: &ExecutionContext) -> CometResult<()> {
    match (&exec_context.root_op, &exec_context.metrics) {
        (Some(native_query), Some(metrics)) => {
            update_comet_metric(env, metrics.as_obj(), native_query)
        }
        _ => Ok(()),
    }
}

fn log_plan_metrics(exec_context: &ExecutionContext, stage_id: jint, partition: jint) {
    if exec_context.explain_native {
        if let Some(plan) = &exec_context.root_op {
            let formatted_plan_str =
                DisplayableExecutionPlan::with_metrics(plan.native_plan.as_ref()).indent(true);
            info!(
                "Comet native query plan with metrics (Plan #{} Stage {} Partition {}):\
                \n plan creation took {:?}:\
                \n{formatted_plan_str:}",
                plan.plan_id, stage_id, partition, exec_context.plan_creation_time
            );
        }
    }
}

fn convert_datatype_arrays(
    env: &mut Env,
    serialized_datatypes: JObjectArray,
) -> JNIResult<Vec<ArrowDataType>> {
    let array_len = serialized_datatypes.len(env)?;
    let mut res: Vec<ArrowDataType> = Vec::new();

    for i in 0..array_len {
        let inner_array = serialized_datatypes.get_element(env, i)?;
        let inner_array = unsafe { JByteArray::from_raw(&*env, inner_array.into_raw()) };
        let bytes = env.convert_byte_array(inner_array)?;
        let data_type = serde::deserialize_data_type(bytes.as_slice()).unwrap();
        let arrow_dt = to_arrow_datatype(&data_type);
        res.push(arrow_dt);
    }

    Ok(res)
}

fn get_execution_context<'a>(id: i64) -> &'a mut ExecutionContext {
    unsafe {
        (id as *mut ExecutionContext)
            .as_mut()
            .expect("Comet execution context shouldn't be null!")
    }
}

/// Returns the partition offsets published by a finished native shuffle write.
///
/// The returned array holds `num_output_partitions + 1` offsets, the last being the total data
/// file length.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_Native_getShufflePartitionOffsets(
    e: EnvUnowned,
    _class: JClass,
    exec_context: jlong,
) -> jlongArray {
    try_unwrap_or_throw(&e, |env| {
        let context = get_execution_context(exec_context);
        let root_plan = context.root_op.as_ref().map(|op| op.native_plan.as_ref());
        let offsets = shuffle_partition_offsets(root_plan)?;

        let long_array = env.new_long_array(offsets.len())?;
        long_array.set_region(env, 0, offsets)?;
        Ok(long_array.into_raw())
    })
}

/// The partition offsets published by `root_plan`, which must be a native shuffle write plan that
/// has been drained to completion. `root_plan` is `None` before the plan has been executed. Core
/// of `Native.getShufflePartitionOffsets`.
fn shuffle_partition_offsets(
    root_plan: Option<&dyn datafusion::physical_plan::ExecutionPlan>,
) -> CometResult<&[i64]> {
    let root_plan = root_plan.ok_or_else(|| {
        CometError::Internal(
            "Cannot read shuffle partition offsets before the plan has been executed".to_string(),
        )
    })?;

    // `ExecutionPlan` has `Any` as a supertrait but no `as_any` method of its own, so upcast
    // the trait object before downcasting to the writer.
    let writer = (root_plan as &dyn std::any::Any)
        .downcast_ref::<ShuffleWriterExec>()
        .ok_or_else(|| {
            CometError::Internal(
                "Shuffle partition offsets are only available on a native shuffle write plan"
                    .to_string(),
            )
        })?;

    let offsets = writer
        .partition_offsets()
        .ok_or_else(|| {
            CometError::Internal(
                "Shuffle partition offsets are not published by a remote shuffle destination"
                    .to_string(),
            )
        })?
        .get()
        .ok_or_else(|| {
            CometError::Internal(
                "Shuffle writer has not published its partition offsets; the plan was not \
                 drained to completion"
                    .to_string(),
            )
        })?;
    Ok(offsets)
}

/// Used by the bypass merge sort shuffle writer to write one partition's records to disk.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_writeSortedFileNative(
    e: EnvUnowned,
    _class: JClass,
    row_addresses: JLongArray,
    row_sizes: JIntArray,
    serialized_datatypes: JObjectArray,
    file_path: JString,
    prefer_dictionary_ratio: jdouble,
    batch_size: jlong,
    checksum_enabled: jboolean,
    checksum_algo: jint,
    current_checksum: jlong,
    compression_codec: JString,
    compression_level: jint,
    offsets_as_lengths: jboolean,
    tracing_enabled: jboolean,
) -> jlongArray {
    try_unwrap_or_throw(&e, |env| unsafe {
        with_trace(
            "writeSortedFileNative",
            tracing_enabled != JNI_FALSE,
            || {
                let data_types = convert_datatype_arrays(env, serialized_datatypes)?;
                let row_addresses = row_addresses.get_elements(env, ReleaseMode::NoCopyBack)?;
                let row_sizes = row_sizes.get_elements(env, ReleaseMode::NoCopyBack)?;
                let output_path: String = file_path.try_to_string(env).unwrap();
                let compression_codec: String = compression_codec.try_to_string(env).unwrap();

                let results = write_sorted_file(
                    &row_addresses,
                    &row_sizes,
                    &data_types,
                    output_path,
                    prefer_dictionary_ratio,
                    batch_size as usize,
                    checksum_enabled,
                    checksum_algo,
                    current_checksum,
                    &compression_codec,
                    compression_level,
                    offsets_as_lengths != JNI_FALSE,
                )?;

                let long_array = env.new_long_array(results.len())?;
                long_array.set_region(env, 0, &results)?;

                Ok(long_array.into_raw())
            },
        )
    })
}

/// Converts the Spark `UnsafeRow`s at `row_addresses`, with the sizes in `row_sizes`, into Arrow
/// batches of `data_types` and writes them to `output_path`. Returns `[bytes written, checksum,
/// encode nanos]`. Checksums cross the boundary as `i64`, with `i64::MIN` meaning none: Spark's
/// CRC32 and Adler32 checksums are both `u32`. Core of `Native.writeSortedFileNative`.
///
/// # Safety
/// Each address must point to a readable row of the corresponding size.
#[allow(clippy::too_many_arguments)]
unsafe fn write_sorted_file(
    row_addresses: &[i64],
    row_sizes: &[i32],
    data_types: &[ArrowDataType],
    output_path: String,
    prefer_dictionary_ratio: f64,
    batch_size: usize,
    checksum_enabled: bool,
    checksum_algo: i32,
    current_checksum: i64,
    compression_codec: &str,
    compression_level: i32,
    offsets_as_lengths: bool,
) -> CometResult<[i64; 3]> {
    if row_sizes.len() < row_addresses.len() {
        return Err(CometError::Internal(format!(
            "{} row sizes for {} row addresses",
            row_sizes.len(),
            row_addresses.len()
        )));
    }

    let current_checksum = if current_checksum == i64::MIN {
        // Initial checksum is not available.
        None
    } else {
        Some(current_checksum as u32)
    };

    let compression_codec = jvm_shuffle_codec(compression_codec, compression_level);

    // `process_sorted_row_partition` only reads through the pointers.
    let (written_bytes, checksum, encode_nanos) = process_sorted_row_partition(
        row_addresses.len(),
        batch_size,
        row_addresses.as_ptr() as *mut i64,
        row_sizes.as_ptr() as *mut i32,
        data_types,
        output_path,
        prefer_dictionary_ratio,
        checksum_enabled,
        checksum_algo,
        current_checksum,
        &compression_codec,
        offsets_as_lengths,
    )?;

    let checksum = checksum.map_or(i64::MIN, |checksum| checksum as i64);
    Ok([written_bytes, checksum, encode_nanos])
}

/// The codec `spark.comet.shuffle.compression.codec` names. Unknown names fall back to lz4.
fn jvm_shuffle_codec(name: &str, level: i32) -> CompressionCodec {
    match name {
        "zstd" => CompressionCodec::Zstd(level),
        "lz4" => CompressionCodec::Lz4Frame,
        "snappy" => CompressionCodec::Snappy,
        _ => CompressionCodec::Lz4Frame,
    }
}

/// Used by Comet shuffle external sorter to write every partition of one sorted spill to disk.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_writeSortedPartitionsNative(
    e: EnvUnowned,
    _class: JClass,
    record_pointers_addr: jlong,
    num_records: jint,
    page_addresses: JLongArray,
    uao_size: jint,
    serialized_datatypes: JObjectArray,
    file_path: JString,
    prefer_dictionary_ratio: jdouble,
    batch_size: jint,
    checksum_algo: jint,
    partition_checksums: JLongArray,
    partition_lengths: JLongArray,
    compression_codec: JString,
    compression_level: jint,
    offsets_as_lengths: jboolean,
    tracing_enabled: jboolean,
) -> jlong {
    try_unwrap_or_throw(&e, |env| unsafe {
        with_trace(
            "writeSortedPartitionsNative",
            tracing_enabled != JNI_FALSE,
            || {
                let num_records = usize::try_from(num_records).map_err(|_| {
                    CometError::Internal(format!("Negative record count {num_records}"))
                })?;
                let record_pointers: &[i64] = if num_records == 0 {
                    &[]
                } else {
                    // SAFETY: JVM unsafe memory allocation is aligned with long.
                    debug_assert!(
                        record_pointers_addr != 0,
                        "writeSortedPartitionsNative: null record pointers"
                    );
                    debug_assert_eq!(
                        (record_pointers_addr as usize) % std::mem::align_of::<i64>(),
                        0,
                        "writeSortedPartitionsNative: record pointers not aligned to i64"
                    );
                    std::slice::from_raw_parts(record_pointers_addr as *const i64, num_records)
                };
                let data_types = convert_datatype_arrays(env, serialized_datatypes)?;
                let mut pages = vec![0_i64; page_addresses.len(env)?];
                page_addresses.get_region(env, 0, &mut pages)?;
                let mut checksums = vec![0_i64; partition_checksums.len(env)?];
                partition_checksums.get_region(env, 0, &mut checksums)?;
                let mut lengths = vec![0_i64; partition_lengths.len(env)?];
                let output_path: String = file_path.try_to_string(env)?;
                let compression_codec: String = compression_codec.try_to_string(env)?;

                let encode_nanos = write_sorted_partitions(
                    record_pointers,
                    &pages,
                    uao_size,
                    &data_types,
                    &output_path,
                    prefer_dictionary_ratio,
                    batch_size,
                    checksum_algo,
                    &mut checksums,
                    &mut lengths,
                    &compression_codec,
                    compression_level,
                    offsets_as_lengths != JNI_FALSE,
                )?;

                partition_lengths.set_region(env, 0, &lengths)?;
                partition_checksums.set_region(env, 0, &checksums)?;
                Ok(encode_nanos)
            },
        )
    })
}

/// Converts the rows of one sorted spill, every partition of it, into Arrow batches of
/// `data_types` and appends them to `output_path` in partition order. `record_pointers` are the
/// sorter's sorted Spark `PackedRecordPointer`s, whose page numbers index `page_addresses`.
/// Fills `partition_lengths` and, when `checksum_algo` is not -1, continues each written
/// partition's checksum in `partition_checksums`, where `i64::MIN` is a checksum not yet started.
/// Returns the encode nanos. Core of `Native.writeSortedPartitionsNative`.
///
/// # Safety
/// Each pointer must resolve, through `page_addresses`, to a readable record.
#[allow(clippy::too_many_arguments)]
unsafe fn write_sorted_partitions(
    record_pointers: &[i64],
    page_addresses: &[i64],
    uao_size: i32,
    data_types: &[ArrowDataType],
    output_path: &str,
    prefer_dictionary_ratio: f64,
    batch_size: i32,
    checksum_algo: i32,
    partition_checksums: &mut [i64],
    partition_lengths: &mut [i64],
    compression_codec: &str,
    compression_level: i32,
    offsets_as_lengths: bool,
) -> CometResult<i64> {
    let uao_size = usize::try_from(uao_size)
        .map_err(|_| CometError::Internal(format!("Unsupported record length size {uao_size}")))?;
    let batch_size = usize::try_from(batch_size)
        .map_err(|_| CometError::Internal(format!("Unsupported batch size {batch_size}")))?;
    let checksum_algo = (checksum_algo != -1).then_some(checksum_algo);
    let compression_codec = jvm_shuffle_codec(compression_codec, compression_level);

    process_sorted_partitions(
        record_pointers,
        page_addresses,
        uao_size,
        data_types,
        output_path,
        prefer_dictionary_ratio,
        batch_size,
        checksum_algo,
        partition_checksums,
        partition_lengths,
        &compression_codec,
        offsets_as_lengths,
    )
}

#[no_mangle]
/// Used by Comet shuffle external sorter to sort in-memory row partition ids.
pub extern "system" fn Java_org_apache_comet_Native_sortRowPartitionsNative(
    e: EnvUnowned,
    _class: JClass,
    address: jlong,
    size: jlong,
    capacity: jlong,
    tracing_enabled: jboolean,
) {
    try_unwrap_or_throw(&e, |_| {
        with_trace(
            "sortRowPartitionsNative",
            tracing_enabled != JNI_FALSE,
            || {
                let size = usize::try_from(size).map_err(|_| {
                    CometError::Internal(format!("sortRowPartitionsNative: negative size {size}"))
                })?;
                // The sort uses the `size` longs after the records as scratch space.
                let capacity = usize::try_from(capacity)
                    .ok()
                    .filter(|capacity| capacity / 2 >= size)
                    .ok_or_else(|| {
                        CometError::Internal(format!(
                            "sortRowPartitionsNative: an array of {capacity} longs has no room \
                             for {size} longs of scratch space after {size} records"
                        ))
                    })?;
                // SAFETY: JVM unsafe memory allocation is aligned with long.
                debug_assert!(address != 0, "sortRowPartitionsNative: null address");
                debug_assert_eq!(
                    (address as usize) % std::mem::align_of::<i64>(),
                    0,
                    "sortRowPartitionsNative: address not aligned to i64"
                );
                let array =
                    unsafe { std::slice::from_raw_parts_mut(address as *mut i64, capacity) };
                sort_row_partitions(array, size);
                Ok(())
            },
        )
    })
}

#[no_mangle]
/// Used by Comet native shuffle reader
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_decodeShuffleBlock(
    e: EnvUnowned,
    _class: JClass,
    byte_buffer: JByteBuffer,
    length: jint,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    tracing_enabled: jboolean,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        with_trace("decodeShuffleBlock", tracing_enabled != JNI_FALSE, || {
            decode_shuffle_block_jni(env, byte_buffer, length, array_addrs, schema_addrs, None)
        })
    })
}

#[no_mangle]
/// Parse the expected schema once for a remote shuffle iterator.
///
/// The iterator owns the returned decoder and releases it when the input is closed.
pub extern "system" fn Java_org_apache_comet_Native_createRemoteShuffleDecoder(
    e: EnvUnowned,
    _class: JClass,
    expected_schema: JByteArray,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        let bytes = env.convert_byte_array(expected_schema)?;
        let decoder = RemoteShuffleDecoder::try_new(&bytes)?;
        Ok(Box::into_raw(Box::new(decoder)) as jlong)
    })
}

/// Immutable decoding state owned by one JVM remote shuffle iterator, not shared across tasks.
struct RemoteShuffleDecoder {
    expected_types: Vec<ArrowDataType>,
}

impl RemoteShuffleDecoder {
    /// Parses the serialized `ShuffleScan` holding the expected schema. Core of
    /// `Native.createRemoteShuffleDecoder`.
    fn try_new(expected_schema: &[u8]) -> CometResult<Self> {
        let schema = ShuffleScan::decode(expected_schema).map_err(|error| {
            CometError::Internal(format!("Invalid expected remote shuffle schema: {error}"))
        })?;
        Ok(RemoteShuffleDecoder {
            expected_types: schema.fields.iter().map(to_arrow_datatype).collect(),
        })
    }
}

#[no_mangle]
/// Release a remote shuffle iterator's decoder.
///
/// # Safety
/// A nonzero handle must have been returned by `createRemoteShuffleDecoder`, must not have
/// been released, and must not be in use by a concurrent decode call.
pub unsafe extern "system" fn Java_org_apache_comet_Native_releaseRemoteShuffleDecoder(
    e: EnvUnowned,
    _class: JClass,
    decoder_handle: jlong,
) {
    try_unwrap_or_throw(&e, |_| {
        if decoder_handle != 0 {
            drop(unsafe { Box::from_raw(decoder_handle as *mut RemoteShuffleDecoder) });
        }
        Ok(())
    })
}

#[no_mangle]
/// Decode a remote native shuffle block with Arrow array and logical type validation enabled.
/// # Safety
/// Buffer and output pointers must be valid. The decoder handle must have been returned by
/// `createRemoteShuffleDecoder` and must remain alive for the duration of this call.
pub unsafe extern "system" fn Java_org_apache_comet_Native_decodeShuffleBlockWithValidation(
    e: EnvUnowned,
    _class: JClass,
    byte_buffer: JByteBuffer,
    length: jint,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    tracing_enabled: jboolean,
    decoder_handle: jlong,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        with_trace("decodeShuffleBlock", tracing_enabled != JNI_FALSE, || {
            let decoder = unsafe { (decoder_handle as *const RemoteShuffleDecoder).as_ref() }
                .ok_or_else(|| {
                    CometError::Internal("Remote shuffle decoder is not initialized".to_owned())
                })?;
            decode_shuffle_block_jni(
                env,
                byte_buffer,
                length,
                array_addrs,
                schema_addrs,
                Some(&decoder.expected_types),
            )
        })
    })
}

/// Converts the JNI arguments of the `decodeShuffleBlock` entry points for
/// [`decode_shuffle_block`].
fn decode_shuffle_block_jni(
    env: &mut Env,
    byte_buffer: JByteBuffer,
    length: jint,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    expected_types: Option<&[ArrowDataType]>,
) -> CometResult<jlong> {
    let raw_pointer = env.get_direct_buffer_address(&byte_buffer)?;
    let length = length as usize;
    let block: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };
    let array_addrs = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
    let schema_addrs = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
    unsafe { decode_shuffle_block(block, &array_addrs, &schema_addrs, expected_types) }
}

/// Decodes one native shuffle block (codec header plus Arrow IPC stream) and exports its columns
/// as in [`export_batch`], returning the row count. `expected_types` is set for a remote shuffle
/// block, which is validated against it. Core of `Native.decodeShuffleBlock` and
/// `Native.decodeShuffleBlockWithValidation`.
///
/// # Safety
/// As for [`export_batch`].
unsafe fn decode_shuffle_block(
    block: &[u8],
    array_addrs: &[i64],
    schema_addrs: &[i64],
    expected_types: Option<&[ArrowDataType]>,
) -> CometResult<i64> {
    let batch = if let Some(expected_types) = expected_types {
        // Reject incompatible logical types, then decode dictionaries before JVM import. The
        // JVM importer supports fewer dictionary key/value layouts than the shuffle writer.
        decode_remote_shuffle_batch(block, expected_types)?
    } else {
        read_ipc_compressed(block)?
    };
    export_batch(array_addrs, schema_addrs, batch, false)
}

#[no_mangle]
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_traceBegin(
    e: EnvUnowned,
    _class: JClass,
    event: JString,
) {
    try_unwrap_or_throw(&e, |env| {
        let name: String = event.try_to_string(env).unwrap();
        trace_begin(&name);
        Ok(())
    })
}

#[no_mangle]
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_traceEnd(
    e: EnvUnowned,
    _class: JClass,
    event: JString,
) {
    try_unwrap_or_throw(&e, |env| {
        let name: String = event.try_to_string(env).unwrap();
        trace_end(&name);
        Ok(())
    })
}

#[no_mangle]
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_logMemoryUsage(
    e: EnvUnowned,
    _class: JClass,
    name: JString,
    value: jlong,
) {
    try_unwrap_or_throw(&e, |env| {
        let name: String = name.try_to_string(env).unwrap();
        log_memory_usage(&name, value as u64);
        Ok(())
    })
}

#[no_mangle]
/// Returns the Rust thread ID for the current thread.
/// This allows Java code to use Rust thread IDs in tracing metric names.
pub extern "system" fn Java_org_apache_comet_Native_getRustThreadId(
    _e: EnvUnowned,
    _class: JClass,
) -> jlong {
    get_thread_id() as jlong
}

#[no_mangle]
/// Returns the executor's memory usage for the periodic memory usage log, as
/// `[native_allocated, pools_reserved, pools, plans]`; see [`MemoryUsage`]. Safe to call from any
/// thread; see [`memory_usage`].
pub extern "system" fn Java_org_apache_comet_Native_getMemoryUsage(
    e: EnvUnowned,
    _class: JClass,
) -> jlongArray {
    try_unwrap_or_throw(&e, |env| {
        let usage = memory_usage();
        let values = [
            usage.native_allocated as jlong,
            usage.pools_reserved as jlong,
            usage.pools as jlong,
            usage.plans as jlong,
        ];
        let long_array = env.new_long_array(values.len())?;
        long_array.set_region(env, 0, &values)?;
        Ok(long_array.into_raw())
    })
}

// ============================================================================
// Native Columnar to Row Conversion
// ============================================================================

use crate::execution::columnar_to_row::ColumnarToRowContext;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion_spark::function::math::bin::SparkBin;
use datafusion_spark::function::string::soundex::SparkSoundex;

/// Initialize a native columnar to row converter.
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_columnarToRowInit(
    e: EnvUnowned,
    _class: JClass,
    serialized_schema: JObjectArray,
    batch_size: jint,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        // Deserialize the schema
        let schema = convert_datatype_arrays(env, serialized_schema)?;

        // Create the context
        let ctx = Box::new(ColumnarToRowContext::new(schema, batch_size as usize));

        Ok(Box::into_raw(ctx) as jlong)
    })
}

/// Convert Arrow columnar data to Spark UnsafeRow format.
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_columnarToRowConvert(
    e: EnvUnowned,
    _class: JClass,
    c2r_handle: jlong,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    num_rows: jint,
) -> jni::sys::jobject {
    try_unwrap_or_throw(&e, |env| {
        // Get the context
        debug_assert!(c2r_handle != 0, "columnarToRowConvert: c2r_handle is null");
        let ctx = (c2r_handle as *mut ColumnarToRowContext)
            .as_mut()
            .ok_or_else(|| CometError::Internal("Null columnar to row context".to_string()))?;

        // Get array and schema addresses
        let array_addrs = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
        let schema_addrs = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };

        let (buffer_ptr, offsets, lengths) =
            unsafe { columnar_to_row_convert(ctx, &array_addrs, &schema_addrs, num_rows)? };

        // Create Java int arrays for offsets and lengths
        let offsets_array = env.new_int_array(offsets.len())?;
        offsets_array.set_region(env, 0, offsets)?;

        let lengths_array = env.new_int_array(lengths.len())?;
        lengths_array.set_region(env, 0, lengths)?;

        // Create the NativeColumnarToRowInfo object
        let info_class =
            env.find_class(jni::jni_str!("org/apache/comet/NativeColumnarToRowInfo"))?;
        let info_obj = env.new_object(
            info_class,
            jni::jni_sig!("(J[I[I)V"),
            &[
                jni::objects::JValue::Long(buffer_ptr as jlong),
                jni::objects::JValue::Object(&offsets_array),
                jni::objects::JValue::Object(&lengths_array),
            ],
        )?;

        Ok(info_obj.into_raw())
    })
}

/// Imports the columns at `array_addrs` and `schema_addrs` and converts their first `num_rows`
/// rows to Spark `UnsafeRow`s in `ctx`'s buffer. Returns the buffer address and the offset and
/// length of each row in it, valid until the next conversion. Core of
/// `Native.columnarToRowConvert`.
///
/// # Safety
/// Each address must point to an exported `FFI_ArrowArray` / `FFI_ArrowSchema`, whose ownership
/// moves to this call.
unsafe fn columnar_to_row_convert<'a>(
    ctx: &'a mut ColumnarToRowContext,
    array_addrs: &[i64],
    schema_addrs: &[i64],
    num_rows: i32,
) -> CometResult<(*const u8, &'a [i32], &'a [i32])> {
    if schema_addrs.len() != array_addrs.len() {
        return Err(CometError::Internal(format!(
            "{} schema addresses for {} array addresses",
            schema_addrs.len(),
            array_addrs.len()
        )));
    }
    let num_cols = array_addrs.len();

    // Import Arrow arrays from FFI
    let mut arrays = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;

        debug_assert!(
            !array_ptr.is_null(),
            "columnarToRowConvert: null array pointer at index {}",
            i
        );
        debug_assert!(
            !schema_ptr.is_null(),
            "columnarToRowConvert: null schema pointer at index {}",
            i
        );

        // Take ownership of the FFI structures
        let ffi_array = unsafe { std::ptr::read(array_ptr) };
        let ffi_schema = unsafe { std::ptr::read(schema_ptr) };

        // Convert to Arrow ArrayData
        let array_data = from_ffi(ffi_array, &ffi_schema)
            .map_err(|e| CometError::Internal(format!("Failed to import array: {}", e)))?;

        let imported = arrow::array::make_array(array_data);
        arrays.push(decode_string_arrays(&imported)?);
    }

    // Convert columnar to row
    debug_assert!(
        num_rows >= 0,
        "columnarToRowConvert: num_rows is negative: {}",
        num_rows
    );
    ctx.convert(&arrays, num_rows as usize)
}

/// Close and release the native columnar to row converter.
///
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_columnarToRowClose(
    e: EnvUnowned,
    _class: JClass,
    c2r_handle: jlong,
) {
    try_unwrap_or_throw(&e, |_env| {
        debug_assert!(c2r_handle != 0, "columnarToRowClose: c2r_handle is null");
        if c2r_handle != 0 {
            let _ctx: Box<ColumnarToRowContext> =
                Box::from_raw(c2r_handle as *mut ColumnarToRowContext);
            // ctx is dropped here, freeing the buffer
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::operators::InputBatch;
    use crate::execution::planner::TEST_EXEC_CONTEXT_ID;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::{
        MemoryConsumer, MemoryReservation, UnboundedMemoryPool,
    };
    use datafusion::execution::FunctionRegistry;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::ReturnFieldArgs;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_comet_proto::spark_expression;
    use datafusion_comet_proto::spark_expression::{AggExpr, Count, Expr, Sum};
    use datafusion_comet_proto::spark_operator::{HashAggregate, ShuffleWriter};
    use std::cell::Cell;
    use std::future::Future;

    #[test]
    fn skip_partial_eligibility_is_fail_closed() {
        let count = AggExpr {
            expr_struct: Some(AggExprStruct::Count(Count {
                children: vec![Expr::default()],
            })),
            ..Default::default()
        };
        let sum = AggExpr {
            expr_struct: Some(AggExprStruct::Sum(Sum::default())),
            ..Default::default()
        };
        let partial = HashAggregate {
            grouping_exprs: vec![Expr::default()],
            agg_exprs: vec![count.clone()],
            mode: AggregateMode::Partial as i32,
            ..Default::default()
        };
        let writer = |agg: HashAggregate| Operator {
            op_struct: Some(OpStruct::ShuffleWriter(ShuffleWriter::default())),
            children: vec![Operator {
                op_struct: Some(OpStruct::HashAgg(agg)),
                ..Default::default()
            }],
            ..Default::default()
        };
        let ratio = |plan: &Operator, requested: f64, enabled: bool| {
            let mut config = SessionConfig::new();
            config
                .options_mut()
                .execution
                .skip_partial_aggregation_probe_rows_threshold = 37;
            config
                .options_mut()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold = requested;
            configure_skip_partial_aggregation(&mut config, plan, enabled);
            assert_eq!(
                config
                    .options()
                    .execution
                    .skip_partial_aggregation_probe_rows_threshold,
                37
            );
            config
                .options()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold
        };

        for agg in [
            partial.clone(),
            HashAggregate {
                agg_exprs: vec![],
                ..partial.clone()
            },
            HashAggregate {
                agg_exprs: vec![count.clone(), count],
                ..partial.clone()
            },
        ] {
            let plan = writer(agg);
            assert_eq!(ratio(&plan, 0.8, true), 0.8);
            assert_eq!(ratio(&plan, 0.5, true), 0.5);
            assert_eq!(ratio(&plan, 1.1, true), 1.1);
            // Skipping is opt-in, and a DataFusion override cannot turn it on.
            assert_eq!(ratio(&plan, 0.8, false), 1.1);
            // Non-native shuffle / standalone native blocks stay disabled.
            assert_eq!(ratio(&plan.children[0], 0.8, true), 1.1);
        }

        for agg in [
            HashAggregate {
                agg_exprs: vec![sum],
                ..partial.clone()
            },
            HashAggregate {
                agg_exprs: vec![AggExpr::default()],
                ..partial.clone()
            },
            HashAggregate {
                agg_exprs: vec![AggExpr {
                    expr_struct: Some(AggExprStruct::Count(Count {
                        children: vec![Expr::default(), Expr::default()],
                    })),
                    ..Default::default()
                }],
                ..partial.clone()
            },
            HashAggregate {
                mode: AggregateMode::PartialMerge as i32,
                ..partial.clone()
            },
            HashAggregate {
                expr_modes: vec![AggregateMode::PartialMerge as i32],
                ..partial.clone()
            },
            HashAggregate {
                mode: 99,
                ..partial.clone()
            },
        ] {
            let plan = writer(agg);
            assert_eq!(ratio(&plan, 0.8, true), 1.1);
            // An eligible sibling or a Final parent must not hide the unsafe child.
            let mut nested = writer(HashAggregate {
                mode: AggregateMode::Final as i32,
                ..partial.clone()
            });
            nested.children[0].children = plan.children;
            assert_eq!(ratio(&nested, 0.8, true), 1.1);
        }
    }

    fn entry_count(thread_id: u64) -> usize {
        get_thread_memory_pools()
            .lock()
            .get(&thread_id)
            .map(HashMap::len)
            .unwrap_or(0)
    }

    #[test]
    fn thread_memory_pool_registration_is_scoped_and_deduplicates_base_pool() {
        let _guard = serial();
        const THREAD_ID: u64 = u64::MAX;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("test").register(&pool);
        reservation.grow(4096);
        let weak = Arc::downgrade(&pool);

        let first_wrapper: Arc<dyn MemoryPool> =
            Arc::new(LoggingMemoryPool::new(1, Arc::clone(&pool)));
        let second_wrapper: Arc<dyn MemoryPool> =
            Arc::new(LoggingMemoryPool::new(1, Arc::clone(&pool)));
        assert!(!Arc::ptr_eq(&first_wrapper, &second_wrapper));

        let first = ThreadMemoryPoolRegistration::new(THREAD_ID, 1, Arc::clone(&pool));
        let second = ThreadMemoryPoolRegistration::new(THREAD_ID, 2, Arc::clone(&pool));
        assert_eq!(entry_count(THREAD_ID), 2);
        assert_eq!(total_reserved_for_thread(THREAD_ID), pool.reserved());

        let metrics_result: Result<(), ()> = {
            let _registration = first;
            Err(())
        };
        assert!(metrics_result.is_err());
        assert_eq!(entry_count(THREAD_ID), 1);
        drop(second);
        assert_eq!(entry_count(THREAD_ID), 0);

        for context_id in 0..100 {
            let create_result: Result<(), ()> = {
                let _registration =
                    ThreadMemoryPoolRegistration::new(THREAD_ID, context_id, Arc::clone(&pool));
                Err(())
            };
            assert!(create_result.is_err());

            let metrics_result: Result<(), ()> = {
                let _registration =
                    ThreadMemoryPoolRegistration::new(THREAD_ID, context_id, Arc::clone(&pool));
                Err(())
            };
            assert!(metrics_result.is_err());
            assert_eq!(entry_count(THREAD_ID), 0);
        }

        drop(first_wrapper);
        drop(second_wrapper);
        drop(reservation);
        drop(pool);
        assert!(weak.upgrade().is_none());
    }

    /// `THREAD_MEMORY_POOLS` is process-wide and the crate's tests run in parallel, so any test
    /// that registers a pool perturbs another's view of the process-wide total. The tests below
    /// take this lock so their deltas are exact; without it they observe each other's pools.
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn reserving(bytes: usize) -> (Arc<dyn MemoryPool>, MemoryReservation) {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("test").register(&pool);
        reservation.grow(bytes);
        (pool, reservation)
    }

    fn total_reserved_across_threads() -> usize {
        total_reserved(None).across_threads
    }

    /// The property that makes this total comparable against a process-wide allocation counter,
    /// and the one summing the per-thread counters gets wrong: a pool shared by several contexts
    /// on several threads contributes its reservation once.
    #[test]
    fn a_shared_pool_is_counted_once_however_many_contexts_hold_it() {
        let _guard = serial();
        let before = total_reserved_across_threads();
        let (pool, reservation) = reserving(4096);

        let _first = ThreadMemoryPoolRegistration::new(11, 1, Arc::clone(&pool));
        assert_eq!(
            total_reserved_across_threads() - before,
            4096,
            "a registered pool's reservation must appear in the total"
        );

        // A second context in the same task, and a third on another thread, both register the
        // same pool. Summing the per-thread counters would report 4096 three times over.
        let _second = ThreadMemoryPoolRegistration::new(11, 2, Arc::clone(&pool));
        let _third = ThreadMemoryPoolRegistration::new(12, 3, Arc::clone(&pool));
        assert_eq!(
            total_reserved_across_threads() - before,
            4096,
            "a shared pool must be counted once, not once per holder"
        );
        assert_eq!(total_reserved_for_thread(11), 4096);
        assert_eq!(total_reserved_for_thread(12), 4096);

        drop(reservation);
    }

    /// The total has to cover every pool type, not just the task-shared ones: `greedy`,
    /// `fair_spill`, the `_global` variants and `unbounded` all bypass the task-shared registry,
    /// and reporting zero for them would make the analyzer call live reservations untracked.
    #[test]
    fn independent_pools_each_contribute_to_the_total() {
        let _guard = serial();
        let before = total_reserved_across_threads();
        let (first_pool, first_reservation) = reserving(4096);
        let (second_pool, second_reservation) = reserving(8192);

        let _first = ThreadMemoryPoolRegistration::new(13, 1, first_pool);
        let _second = ThreadMemoryPoolRegistration::new(13, 2, second_pool);
        assert_eq!(total_reserved_across_threads() - before, 4096 + 8192);

        drop(first_reservation);
        drop(second_reservation);
    }

    #[test]
    fn released_pools_leave_the_total() {
        let _guard = serial();
        let before = total_reserved_across_threads();
        let (pool, reservation) = reserving(8192);
        {
            let _registration = ThreadMemoryPoolRegistration::new(14, 1, pool);
            assert_eq!(total_reserved_across_threads() - before, 8192);
        }
        assert_eq!(
            total_reserved_across_threads(),
            before,
            "unregistering the last context must remove the pool from the total"
        );
        drop(reservation);
    }

    /// What the traced thread sees when a plan on another thread holds memory it knows nothing
    /// about, which is the case a tracing-gated registry got wrong: `tracing.enabled` is a session
    /// config, so an untraced plan can run alongside a traced one, and its reservation is part of
    /// the process-wide allocation the trace compares against. The per-thread figure stays local —
    /// that counter is for attribution — while the process total has to include the other thread.
    #[test]
    fn the_process_total_includes_pools_registered_on_other_threads() {
        let _guard = serial();
        let before = total_reserved_across_threads();
        let (traced_pool, traced_reservation) = reserving(20 * 1024 * 1024);
        let (untraced_pool, untraced_reservation) = reserving(100 * 1024 * 1024);

        let _traced = ThreadMemoryPoolRegistration::new(15, 1, traced_pool);
        let _untraced = ThreadMemoryPoolRegistration::new(16, 2, untraced_pool);

        let totals = total_reserved(Some(15));
        assert_eq!(
            totals.for_thread,
            20 * 1024 * 1024,
            "the per-thread counter attributes only this thread's pools"
        );
        assert_eq!(
            totals.across_threads - before,
            120 * 1024 * 1024,
            "the process total must cover the pool held by the other thread"
        );

        drop(traced_reservation);
        drop(untraced_reservation);
    }

    /// The periodic memory usage log counts every plan, and every pool once. Two plans of one task
    /// share a pool across threads, as a task-shared pool does, and a third plan has a pool of its
    /// own.
    #[test]
    fn memory_usage_counts_every_plan_and_every_pool_once() {
        let _guard = serial();
        let before = memory_usage();
        let (shared_pool, shared_reservation) = reserving(4096);
        let (own_pool, own_reservation) = reserving(8192);

        let _first = ThreadMemoryPoolRegistration::new(18, -6001, Arc::clone(&shared_pool));
        let _second = ThreadMemoryPoolRegistration::new(19, -6002, Arc::clone(&shared_pool));
        let third = ThreadMemoryPoolRegistration::new(18, -6003, own_pool);

        let during = memory_usage();
        assert_eq!(during.plans - before.plans, 3);
        assert_eq!(
            during.pools - before.pools,
            2,
            "a pool shared by two plans must be counted once"
        );
        assert_eq!(during.pools_reserved - before.pools_reserved, 4096 + 8192);

        drop(third);
        let after = memory_usage();
        assert_eq!(after.plans - before.plans, 2);
        assert_eq!(after.pools - before.pools, 1);
        assert_eq!(after.pools_reserved - before.pools_reserved, 4096);

        drop(shared_reservation);
        drop(own_reservation);
    }

    /// The memory usage log leaves overcommit out of the reservations it reports, because Spark's
    /// off-heap pool does not account for it, so the log counts it with the native memory that no
    /// pool tracks. Tracing's process total still reports everything the pools recorded.
    #[test]
    fn memory_usage_leaves_out_what_spark_did_not_grant() {
        use crate::execution::memory_pools::{
            create_memory_pool_with_fake_spark, MemoryPoolConfig, MemoryPoolType,
        };

        let _guard = serial();
        let before = memory_usage();
        let traced_before = total_reserved_across_threads();
        // A task's pool as `greedy_unified` creates it, where Spark grants at most 4096 bytes.
        let config = MemoryPoolConfig::new(MemoryPoolType::GreedyUnified, 0);
        let pool = create_memory_pool_with_fake_spark(&config, -6101, 4096);
        let _registration = ThreadMemoryPoolRegistration::new(21, -6101, Arc::clone(&pool));
        let reservation = MemoryConsumer::new("spill reader").register(&pool);

        // A spilled batch read back from disk is recorded in full, although Spark grants only 4096
        // of its 6144 bytes.
        reservation.grow(6144);
        assert_eq!(total_reserved_across_threads() - traced_before, 6144);
        assert_eq!(memory_usage().pools_reserved - before.pools_reserved, 4096);

        // Freeing memory repays the overcommit before anything goes back to Spark.
        reservation.shrink(2048);
        assert_eq!(memory_usage().pools_reserved - before.pools_reserved, 4096);
        reservation.shrink(1024);
        assert_eq!(memory_usage().pools_reserved - before.pools_reserved, 3072);
    }

    /// Stands in for a `CometFairMemoryPool` whose lock is held across a Spark acquire: it counts
    /// its reservation reads, and notes whether the registry lock was held during any of them.
    #[derive(Debug, Default)]
    struct RegistryProbePool {
        reads: std::sync::atomic::AtomicUsize,
        read_under_registry_lock: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for RegistryProbePool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RegistryProbePool")
        }
    }

    impl MemoryPool for RegistryProbePool {
        fn name(&self) -> &str {
            "RegistryProbePool"
        }

        fn grow(&self, _: &MemoryReservation, _: usize) {}

        fn shrink(&self, _: &MemoryReservation, _: usize) {}

        fn try_grow(&self, _: &MemoryReservation, _: usize) -> DataFusionResult<()> {
            Ok(())
        }

        fn reserved(&self) -> usize {
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if get_thread_memory_pools().try_lock().is_none() {
                self.read_under_registry_lock
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            4096
        }
    }

    /// No path may read a reservation while holding the registry lock; see `ThreadPoolMap`. The
    /// memory usage log reads from a thread running no plan, tracing reads from a plan's thread,
    /// and `releasePlan` unregisters on every plan, so all of them are covered. Unregistering must
    /// not read a reservation at all, since it runs whether or not anything is traced.
    #[test]
    fn reservations_are_read_outside_the_registry_lock() {
        use std::sync::atomic::Ordering::Relaxed;

        let _guard = serial();
        let before = memory_usage().pools_reserved;
        let probe = Arc::new(RegistryProbePool::default());
        let registration =
            ThreadMemoryPoolRegistration::new(20, -7001, Arc::clone(&probe) as Arc<dyn MemoryPool>);
        let second =
            ThreadMemoryPoolRegistration::new(20, -7002, Arc::clone(&probe) as Arc<dyn MemoryPool>);

        assert_eq!(memory_usage().pools_reserved - before, 4096);
        assert_eq!(total_reserved(Some(20)).for_thread, 4096);
        assert_eq!(total_reserved_for_thread(20), 4096);
        let reads = probe.reads.load(Relaxed);
        assert!(reads >= 3, "each reader should have read the probe");

        drop(registration);
        drop(second);
        assert_eq!(
            probe.reads.load(Relaxed),
            reads,
            "unregistering read a reservation"
        );
        assert!(
            !probe.read_under_registry_lock.load(Relaxed),
            "a pool's reservation was read while the registry lock was held"
        );
    }

    #[test]
    fn length_resolves_to_spark_length_for_string_and_binary() {
        use datafusion::physical_expr::expressions::{CastExpr, Column};
        use datafusion::physical_expr::ScalarFunctionExpr;
        use datafusion_comet_proto::spark_expression::data_type::DataTypeId;
        use datafusion_comet_proto::spark_expression::expr::ExprStruct;
        use datafusion_comet_proto::spark_expression::{BoundReference, ScalarFunc};

        let ctx = Arc::new(SessionContext::new());
        register_datafusion_spark_function(&ctx);
        let planner = PhysicalPlanner::new(Arc::clone(&ctx), 0);
        // The planner path: a uniform signature gets no coercion, so the input reaches the
        // kernel exactly as the scan produced it.
        for (type_id, input) in [
            (DataTypeId::String, DataType::Utf8),
            (DataTypeId::Bytes, DataType::Binary),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("arg0", input.clone(), true)]));
            for name in ["length", "char_length", "character_length"] {
                let expr = Expr {
                    expr_struct: Some(ExprStruct::ScalarFunc(ScalarFunc {
                        func: name.to_string(),
                        args: vec![Expr {
                            expr_struct: Some(ExprStruct::Bound(BoundReference {
                                index: 0,
                                datatype: Some(spark_expression::DataType {
                                    type_id: type_id as i32,
                                    type_info: None,
                                }),
                            })),
                            query_context: None,
                            expr_id: None,
                        }],
                        return_type: None,
                        fail_on_error: false,
                        null_short_circuit: false,
                    })),
                    query_context: None,
                    expr_id: None,
                };
                let physical = planner
                    .create_expr(&expr, Arc::clone(&schema))
                    .unwrap_or_else(|e| panic!("{name}({input}) failed to plan: {e}"));
                assert_eq!(
                    physical.data_type(&schema).unwrap(),
                    DataType::Int32,
                    "{name}({input})"
                );
                let func = physical
                    .downcast_ref::<ScalarFunctionExpr>()
                    .unwrap_or_else(|| panic!("{name}({input}) is not a scalar function"));
                assert_eq!(func.fun().name(), "length", "{name}({input})");
                assert!(
                    func.args()[0].downcast_ref::<Column>().is_some()
                        && func.args()[0].downcast_ref::<CastExpr>().is_none(),
                    "{name}({input}) should take the column without a cast"
                );
            }
        }
        // Wider and view encodings never come out of a Comet scan, but the kernel accepts them
        // with the same Int32 result should a future input path produce them.
        let udf = ctx.udf("length").unwrap();
        for input in [
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::LargeBinary,
            DataType::BinaryView,
        ] {
            let arg = Arc::new(Field::new("arg0", input.clone(), true));
            let ret = udf
                .return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[arg],
                    scalar_arguments: &[None],
                })
                .unwrap();
            assert_eq!(ret.data_type(), &DataType::Int32, "length({input})");
        }
    }

    fn single_worker_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    /// Fails when a wake is lost. The timeout keeps a lost wake from hanging the suite, but when
    /// its timer fires it polls `future` again, which can finish it, so this also fails when
    /// `future` took more than five seconds.
    async fn without_a_lost_wake<F: Future>(future: F) -> F::Output {
        let start = Instant::now();
        let output = tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("timed out: a wake was lost");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "took {elapsed:?}: a wake was lost, and only the timeout's timer woke the task"
        );
        output
    }

    #[test]
    fn next_batch_parks_while_the_stream_waits_on_native_io() {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let mut stream = futures::stream::once(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<_, DataFusionError>(batch)
        })
        .boxed();
        let mut pulls = 0;
        let next = single_worker_runtime()
            .block_on(without_a_lost_wake(next_batch(&mut stream, || {
                // Every JVM-fed scan already holds a batch, so the pull wakes nothing.
                pulls += 1;
                Ok(())
            })))
            .unwrap();
        assert!(next.is_some());
        assert!(
            pulls < 5,
            "the loop pulled {pulls} times during one 50 ms wait"
        );
    }

    #[test]
    fn next_batch_resumes_on_a_refill_and_stops_pulling_after_eof() {
        let mut scan =
            ScanExec::new(TEST_EXEC_CONTEXT_ID, None, "", vec![DataType::Int32]).unwrap();
        let mut stream = scan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let column: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let mut inputs = vec![InputBatch::new(vec![column], Some(3)), InputBatch::EOF].into_iter();
        let pulls = Cell::new(0);
        let mut pull = || {
            pulls.set(pulls.get() + 1);
            if let Some(input) = inputs.next() {
                scan.set_input_batch(input);
            }
            Ok::<(), CometError>(())
        };
        // Only the refill's wake gets the stream polled again.
        single_worker_runtime().block_on(without_a_lost_wake(async {
            let first = next_batch(&mut stream, &mut pull).await.unwrap();
            assert_eq!(first.unwrap().num_rows(), 3);
            assert_eq!(pulls.get(), 1);
            assert!(next_batch(&mut stream, &mut pull).await.unwrap().is_none());
            assert_eq!(pulls.get(), 2);
            assert!(next_batch(&mut stream, &mut pull).await.unwrap().is_none());
            assert_eq!(pulls.get(), 2);
        }));
    }

    /// A pull that runs another Comet plan on this thread, whose `block_on` parks until after the
    /// stream's native I/O has completed. The nested park takes the I/O's wake-up from the
    /// thread's parker, so `next_batch` has to have seen the wake some other way, or it parks
    /// until `without_a_lost_wake`'s timer wakes it.
    #[test]
    fn next_batch_polls_again_when_a_nested_block_on_took_the_wake_up() {
        let runtime = single_worker_runtime();
        let handle = runtime.handle().clone();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let mut stream = futures::stream::once(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, DataFusionError>(batch)
        })
        .boxed();
        let mut pulls = 0;
        let next = runtime
            .block_on(without_a_lost_wake(next_batch(&mut stream, || {
                pulls += 1;
                handle.block_on(tokio::time::sleep(Duration::from_millis(100)));
                Ok(())
            })))
            .unwrap();
        assert!(next.is_some());
        assert_eq!(pulls, 1);
    }

    fn empty_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    fn plan_stream(
        stream: impl Stream<Item = DataFusionResult<RecordBatch>> + Send + 'static,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream,
        ))
    }

    /// See issue #2453. A consumer that stops early can leave the producer waiting on its input,
    /// still holding the stream.
    #[test]
    fn stopping_a_batch_producer_drops_its_stream_while_it_waits_on_input() {
        let runtime = single_worker_runtime();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("sort").register(&pool);
        reservation.grow(4096);
        let mut first = Some(empty_batch());
        // Yields one batch, then waits for input that never arrives.
        let stream = futures::stream::poll_fn(move |_| {
            let _held = &reservation;
            match first.take() {
                Some(batch) => Poll::Ready(Some(Ok(batch))),
                None => Poll::Pending,
            }
        });

        let mut producer = BatchProducer::spawn(runtime.handle(), plan_stream(stream));
        assert!(producer.next_batch().unwrap().is_some());
        producer.stop().unwrap();
        assert_eq!(pool.reserved(), 0, "the stream outlived its producer");
    }

    /// The producer can be in the middle of producing a batch, such as one evaluating a JVM UDF,
    /// when the consumer stops.
    #[test]
    fn stopping_a_batch_producer_waits_for_the_poll_it_is_in() {
        let runtime = single_worker_runtime();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("sort").register(&pool);
        reservation.grow(4096);
        let (polling_tx, polling_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
        let mut polls = 0;
        // Produces its second batch without yielding, until the test lets it finish.
        let stream = futures::stream::poll_fn(move |_| {
            let _held = &reservation;
            polls += 1;
            if polls == 2 {
                polling_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
            }
            Poll::Ready(Some(Ok(empty_batch())))
        });

        let mut producer = BatchProducer::spawn(runtime.handle(), plan_stream(stream));
        assert!(producer.next_batch().unwrap().is_some());
        polling_rx.recv().unwrap();
        let finished = Arc::new(AtomicBool::new(false));
        let finishing = {
            let finished = Arc::clone(&finished);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                finished.store(true, Ordering::Release);
                finish_tx.send(()).unwrap();
            })
        };
        producer.stop().unwrap();
        assert!(
            finished.load(Ordering::Acquire),
            "stop returned while the producer was still polling the stream"
        );
        assert_eq!(pool.reserved(), 0, "the stream outlived its producer");
        finishing.join().unwrap();
    }

    /// Dropping a JVM-fed plan's stream happens inside `releasePlan`, so a panic there reaches
    /// the JVM. One in the producer's stream has to as well.
    #[test]
    fn stopping_a_batch_producer_returns_a_panic_from_dropping_its_stream() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("the stream failed to drop");
            }
        }

        let runtime = single_worker_runtime();
        let guard = PanicOnDrop;
        let stream = futures::stream::poll_fn(move |_| {
            let _guard = &guard;
            Poll::<Option<DataFusionResult<RecordBatch>>>::Pending
        });
        let producer = BatchProducer::spawn(runtime.handle(), plan_stream(stream));
        match producer.stop() {
            Err(CometError::Panic { msg }) => assert!(msg.contains("the stream failed to drop")),
            other => panic!("expected the panic, got {other:?}"),
        }
    }

    /// A sort's merge reads each sorted run through a task it spawns, and dropping the merge only
    /// aborts those tasks. Stopping the producer therefore does not return all of the plan's
    /// memory, and `releasePlan` also waits on the plan's pool.
    #[test]
    fn a_task_the_plan_spawned_keeps_its_memory_until_it_next_yields() {
        // One worker runs the spawned task, which blocks it, and the other polls the plan's stream.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        let plan_memory = Arc::new(PlanMemoryPool::new(
            Arc::new(UnboundedMemoryPool::default()),
        ));
        let pool = Arc::clone(&plan_memory) as Arc<dyn MemoryPool>;
        let reservation = MemoryConsumer::new("sorted run").register(&pool);
        reservation.grow(4096);
        let (working_tx, working_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
        let mut runs = tokio::task::JoinSet::new();
        runs.spawn_on(
            async move {
                let _held = reservation;
                // Stands in for sorting the run, which does not yield.
                working_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                futures::future::pending::<()>().await;
            },
            runtime.handle(),
        );
        working_rx.recv().unwrap();
        // The plan's stream owns the task, and aborts it when dropped.
        let stream = futures::stream::poll_fn(move |_| {
            let _runs = &runs;
            Poll::<Option<DataFusionResult<RecordBatch>>>::Pending
        });

        BatchProducer::spawn(runtime.handle(), plan_stream(stream))
            .stop()
            .unwrap();
        assert_eq!(
            plan_memory.wait_until_released(Instant::now()),
            4096,
            "the aborted task should still be sorting"
        );
        finish_tx.send(()).unwrap();
        assert_eq!(
            plan_memory.wait_until_released(Instant::now() + Duration::from_secs(10)),
            0
        );
    }

    /// Every Tokio worker can be tied up, for instance in Spark's `acquireMemory` waiting for
    /// memory the stopped plan holds. Stopping the producer must not need a free worker then, or
    /// neither task ever finishes.
    #[test]
    fn stopping_a_batch_producer_does_not_need_a_free_worker() {
        let runtime = single_worker_runtime();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("sort").register(&pool);
        reservation.grow(4096);
        let mut first = Some(empty_batch());
        let stream = futures::stream::poll_fn(move |_| {
            let _held = &reservation;
            match first.take() {
                Some(batch) => Poll::Ready(Some(Ok(batch))),
                None => Poll::Pending,
            }
        });
        let mut producer = BatchProducer::spawn(runtime.handle(), plan_stream(stream));
        assert!(producer.next_batch().unwrap().is_some());

        // Holds the only worker until the stopped plan's memory comes back, as another task
        // waiting in `acquireMemory` does. It gives up eventually so a failure cannot hang the
        // suite.
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        let waiting_on = Arc::clone(&pool);
        runtime.spawn(async move {
            waiting_tx.send(()).unwrap();
            let give_up = Instant::now() + Duration::from_secs(20);
            while waiting_on.reserved() > 0 && Instant::now() < give_up {
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        waiting_rx.recv().unwrap();

        let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || stopped_tx.send(producer.stop()).unwrap());
        stopped_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("stopping the producer waited for a free worker")
            .unwrap();
        assert_eq!(pool.reserved(), 0, "the stream outlived its producer");
    }

    /// One uncompressed native shuffle block holding `batch`.
    fn shuffle_block(batch: &RecordBatch) -> Vec<u8> {
        let mut block = b"NONE".to_vec();
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut block, batch.schema_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        block
    }

    fn int_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap()
    }

    /// Decodes `block` into freshly allocated C Data structs and imports the single column back.
    fn decode_one_column(
        block: &[u8],
        expected_types: Option<&[ArrowDataType]>,
    ) -> CometResult<(i64, ArrayRef)> {
        let mut ffi_array = Box::new(FFI_ArrowArray::empty());
        let mut ffi_schema = Box::new(FFI_ArrowSchema::empty());
        let array_addrs = [ffi_array.as_mut() as *mut FFI_ArrowArray as i64];
        let schema_addrs = [ffi_schema.as_mut() as *mut FFI_ArrowSchema as i64];
        let rows =
            unsafe { decode_shuffle_block(block, &array_addrs, &schema_addrs, expected_types)? };
        let data = unsafe { from_ffi(*ffi_array, &ffi_schema) }?;
        Ok((rows, arrow::array::make_array(data)))
    }

    #[test]
    fn decode_shuffle_block_exports_columns() {
        let batch = int_batch();
        let block = shuffle_block(&batch);

        let (rows, column) = decode_one_column(&block, None).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(column.as_ref(), batch.column(0).as_ref());

        let (rows, column) = decode_one_column(&block, Some(&[DataType::Int32])).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(column.as_ref(), batch.column(0).as_ref());
    }

    #[test]
    fn decode_shuffle_block_without_output_columns_counts_rows() {
        let block = shuffle_block(&int_batch());
        let rows = unsafe { decode_shuffle_block(&block, &[], &[], None) }.unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn decode_shuffle_block_rejects_bad_input() {
        let block = shuffle_block(&int_batch());
        // A remote block whose column type differs from the expected schema.
        assert!(decode_one_column(&block, Some(&[DataType::Utf8])).is_err());
        // A truncated block.
        assert!(decode_one_column(&block[..block.len() / 2], None).is_err());
        // An expected schema that is not a serialized `ShuffleScan`.
        let error = RemoteShuffleDecoder::try_new(&[0xff, 0xff, 0xff])
            .err()
            .expect("garbage schema is rejected");
        assert!(error
            .to_string()
            .starts_with("Comet Internal Error: Invalid expected remote shuffle schema"));
    }

    /// See issue #6294. A runtime cancels every task it has when it shuts down, which Comet's does
    /// when the executor stops. The producer's channel then closes as it does at the end of the
    /// stream, and the consumer must fail rather than end with the batches it has read so far.
    #[test]
    fn a_batch_producer_cancelled_mid_stream_is_an_error_not_the_end() {
        let runtime = single_worker_runtime();
        let mut first = Some(empty_batch());
        // Yields one batch, then waits for input that never arrives.
        let stream = futures::stream::poll_fn(move |_| match first.take() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => Poll::Pending,
        });
        let mut producer = BatchProducer::spawn(runtime.handle(), plan_stream(stream));
        assert!(producer.next_batch().unwrap().is_some());

        // Shuts the runtime down while the consumer waits for the next batch.
        let shutting_down = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            runtime.shutdown_timeout(Duration::from_secs(10));
        });
        match producer.next_batch() {
            Err(CometError::Internal(msg)) => assert!(msg.contains("cancelled"), "{msg}"),
            other => panic!("expected an error, got {other:?}"),
        }
        shutting_down.join().unwrap();
        producer.stop().unwrap();
    }

    /// The channel also closes once the stream has ended, and the consumer has then had every
    /// batch, even if the runtime shuts down before it takes the batches the producer sent.
    #[test]
    fn a_batch_producer_ends_cleanly_once_its_stream_has_ended() {
        let runtime = single_worker_runtime();
        let stream = futures::stream::iter([Ok(empty_batch()), Ok(empty_batch())]);
        let mut producer = BatchProducer::spawn(runtime.handle(), plan_stream(stream));
        // Both batches fit in the channel, so the task finishes without waiting for the consumer.
        runtime.block_on(&mut producer.task).unwrap();
        runtime.shutdown_timeout(Duration::from_secs(10));

        assert!(producer.next_batch().unwrap().is_some());
        assert!(producer.next_batch().unwrap().is_some());
        assert!(producer.next_batch().unwrap().is_none());
        producer.stop().unwrap();
    }

    #[test]
    fn shuffle_partition_offsets_requires_an_executed_shuffle_write() {
        let error = shuffle_partition_offsets(None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("before the plan has been executed"),
            "{error}"
        );

        let plan = datafusion::physical_plan::empty::EmptyExec::new(Arc::new(Schema::empty()));
        let error = shuffle_partition_offsets(Some(&plan)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("only available on a native shuffle write plan"),
            "{error}"
        );
    }

    #[test]
    fn shuffle_partition_offsets_after_the_write_is_drained() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion_comet_shuffle::{CometPartitioning, RoundRobinStrategy};

        let batch = int_batch();
        let num_partitions = 3;
        let dir = tempfile::tempdir().unwrap();
        let data_file = dir.path().join("data.out");
        let writer = ShuffleWriterExec::try_new(
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![batch.clone()]], batch.schema(), None).unwrap(),
            ))),
            CometPartitioning::RoundRobin(num_partitions, RoundRobinStrategy::default()),
            CompressionCodec::Zstd(1),
            data_file.to_str().unwrap().to_string(),
            false,
            1024 * 1024,
            None,
        )
        .unwrap();

        let error = shuffle_partition_offsets(Some(&writer)).unwrap_err();
        assert!(
            error.to_string().contains("not drained to completion"),
            "{error}"
        );

        let task_ctx = Arc::new(TaskContext::default());
        let stream = writer.execute(0, task_ctx).unwrap();
        Runtime::new()
            .unwrap()
            .block_on(datafusion::physical_plan::common::collect(stream))
            .unwrap();

        // One offset per partition plus the data file length.
        let offsets = shuffle_partition_offsets(Some(&writer)).unwrap();
        assert_eq!(offsets.len(), num_partitions + 1);
        assert_eq!(offsets[0], 0);
        assert!(offsets.windows(2).all(|pair| pair[0] <= pair[1]));
        let file_len = std::fs::metadata(&data_file).unwrap().len() as i64;
        assert_eq!(offsets[num_partitions], file_len);
    }

    /// Spark `UnsafeRow`s with one non-null `long` field each: an 8-byte null bitset, then the
    /// value.
    fn long_rows(values: &[i64]) -> Vec<[u8; 16]> {
        values
            .iter()
            .map(|value| {
                let mut row = [0_u8; 16];
                row[8..].copy_from_slice(&value.to_le_bytes());
                row
            })
            .collect()
    }

    /// Splits a shuffle data file into its blocks, each without the 8-byte length and the 8-byte
    /// field count, so it starts at the codec tag as `read_ipc_compressed` expects.
    fn shuffle_blocks(data: &[u8]) -> Vec<&[u8]> {
        let mut blocks = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let length = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
            blocks.push(&data[pos + 16..pos + 8 + length]);
            pos += 8 + length;
        }
        blocks
    }

    /// Writes `rows` (16-byte `long` rows) with `write_sorted_file` in batches of 2 rows,
    /// checksummed with `checksum_algo` if set, and returns the results and the file contents.
    fn write_long_rows(
        rows: &[[u8; 16]],
        sizes: &[i32],
        path: &std::path::Path,
        checksum_algo: Option<i32>,
        current_checksum: i64,
        codec: &str,
    ) -> CometResult<([i64; 3], Vec<u8>)> {
        let addresses: Vec<i64> = rows.iter().map(|row| row.as_ptr() as i64).collect();
        let results = unsafe {
            write_sorted_file(
                &addresses,
                sizes,
                &[DataType::Int64],
                path.to_str().unwrap().to_string(),
                1.0,
                2,
                checksum_algo.is_some(),
                checksum_algo.unwrap_or(0),
                current_checksum,
                codec,
                1,
                false,
            )
        }?;
        Ok((results, std::fs::read(path).unwrap()))
    }

    #[test]
    fn write_sorted_file_codecs() {
        let values = [1_i64, 2, 3, 4, 5];
        let rows = long_rows(&values);
        let sizes = vec![16_i32; rows.len()];
        let dir = tempfile::tempdir().unwrap();

        // Unknown codec names fall back to lz4.
        for (codec, tag) in [
            ("lz4", b"LZ4_"),
            ("zstd", b"ZSTD"),
            ("snappy", b"SNAP"),
            ("unknown", b"LZ4_"),
        ] {
            let path = dir.path().join(codec);
            let ([written, checksum, _], data) =
                write_long_rows(&rows, &sizes, &path, None, i64::MIN, codec).unwrap();
            assert_eq!(written, data.len() as i64, "{codec}");
            assert_eq!(checksum, i64::MIN, "no checksum without checksum_enabled");

            let blocks = shuffle_blocks(&data);
            assert_eq!(blocks.len(), 3, "{codec}: 5 rows in batches of 2");
            let mut decoded = Vec::new();
            for block in blocks {
                assert_eq!(&block[..4], tag, "{codec}");
                let batch = read_ipc_compressed(block).unwrap();
                let column = arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(
                    batch.column(0),
                );
                decoded.extend(column.values().iter().copied());
            }
            assert_eq!(decoded, values, "{codec}");
        }
    }

    /// Adler-32 of `data`, computed independently of the implementation under test.
    fn adler32(data: &[u8]) -> u32 {
        let (mut a, mut b) = (1_u32, 0_u32);
        for &byte in data {
            a = (a + byte as u32) % 65521;
            b = (b + a) % 65521;
        }
        b << 16 | a
    }

    #[test]
    fn write_sorted_file_checksums_the_written_bytes() {
        let rows = long_rows(&[1, 2, 3, 4, 5]);
        let sizes = vec![16_i32; rows.len()];
        let dir = tempfile::tempdir().unwrap();

        // CRC32 (0) starts from 0 but Adler-32 (1) from 1, so a missing current checksum
        // (`i64::MIN`) must start a fresh checksum rather than continue one from 0. Spark seeds
        // every partition with `Long.MIN_VALUE` and defaults to Adler-32.
        let algorithms = [(0, crc32fast::hash as fn(&[u8]) -> u32), (1, adler32)];
        for (algo, checksum_of) in algorithms {
            // Without a current checksum the checksum starts afresh and covers the file.
            let first_path = dir.path().join(format!("{algo}-first"));
            let ([_, first, _], first_data) =
                write_long_rows(&rows, &sizes, &first_path, Some(algo), i64::MIN, "lz4").unwrap();
            assert_eq!(first, checksum_of(&first_data) as i64, "algorithm {algo}");

            // A current checksum continues it, as across the spill files of one partition.
            let second_path = dir.path().join(format!("{algo}-second"));
            let ([_, second, _], second_data) =
                write_long_rows(&rows, &sizes, &second_path, Some(algo), first, "lz4").unwrap();
            let both = [first_data, second_data].concat();
            assert_eq!(second, checksum_of(&both) as i64, "algorithm {algo}");
        }
    }

    #[test]
    fn write_sorted_file_rejects_missing_row_sizes() {
        let rows = long_rows(&[1, 2]);
        let dir = tempfile::tempdir().unwrap();
        let error = write_long_rows(&rows, &[16], &dir.path().join("a"), None, i64::MIN, "lz4")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("1 row sizes for 2 row addresses"),
            "{error}"
        );
    }

    /// The page numbers `SorterRecords` stores rows under. They are not contiguous, so that a
    /// lookup that ignored the page number would read the wrong page.
    const SORTER_PAGE_NUMBERS: [usize; 2] = [3, 1];

    /// Rows stored the way Spark's sorter stores them: in pages, each record its length in 4
    /// bytes, then the row's 4-byte big-endian size from `UnsafeRowSerializer`, then the row.
    /// Rows alternate between two pages, and `pointers` holds the packed record pointer of
    /// each row, sorted by partition as `sort_row_partitions` sorts them.
    struct SorterRecords {
        _pages: Vec<Vec<u64>>,
        page_addresses: Vec<i64>,
        pointers: Vec<i64>,
    }

    impl SorterRecords {
        /// Each row must be whole 8-byte words, as an `UnsafeRow` is, so that every row stays
        /// aligned in its page.
        fn new<R: AsRef<[u8]>>(rows: &[R], partitions: &[usize]) -> Self {
            let mut page_bytes: Vec<Vec<u8>> = vec![Vec::new(); SORTER_PAGE_NUMBERS.len()];
            let mut pointers = Vec::new();
            for (i, (row, &partition)) in rows.iter().zip(partitions).enumerate() {
                let row = row.as_ref();
                let page = i % SORTER_PAGE_NUMBERS.len();
                let offset = page_bytes[page].len();
                page_bytes[page].extend_from_slice(&(4 + row.len() as i32).to_ne_bytes());
                page_bytes[page].extend_from_slice(&(row.len() as i32).to_be_bytes());
                page_bytes[page].extend_from_slice(row);
                pointers.push(
                    (partition as i64) << 40
                        | (SORTER_PAGE_NUMBERS[page] as i64) << 27
                        | offset as i64,
                );
            }
            let num_records = pointers.len();
            pointers.resize(2 * num_records, 0);
            sort_row_partitions(&mut pointers, num_records);
            pointers.truncate(num_records);

            // `u64` elements keep the rows 8-byte aligned, as in a Spark page.
            let pages: Vec<Vec<u64>> = page_bytes
                .iter()
                .map(|bytes| {
                    let mut page = vec![0_u64; bytes.len().div_ceil(8)];
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            bytes.as_ptr(),
                            page.as_mut_ptr() as *mut u8,
                            bytes.len(),
                        )
                    };
                    page
                })
                .collect();
            let mut page_addresses = vec![0_i64; SORTER_PAGE_NUMBERS.iter().max().unwrap() + 1];
            for (page, &number) in pages.iter().zip(&SORTER_PAGE_NUMBERS) {
                page_addresses[number] = page.as_ptr() as i64;
            }
            Self {
                _pages: pages,
                page_addresses,
                pointers,
            }
        }

        /// Writes the records with `write_sorted_partitions` in batches of 2 rows.
        fn write(
            &self,
            path: &std::path::Path,
            checksum_algo: i32,
            partition_checksums: &mut [i64],
            partition_lengths: &mut [i64],
            codec: &str,
        ) -> CometResult<i64> {
            unsafe {
                write_sorted_partitions(
                    &self.pointers,
                    &self.page_addresses,
                    4,
                    &[DataType::Int64],
                    path.to_str().unwrap(),
                    1.0,
                    2,
                    checksum_algo,
                    partition_checksums,
                    partition_lengths,
                    codec,
                    1,
                    false,
                )
            }
        }
    }

    /// The partition of row `i` of `long_rows(0..23)`: partitions 1, 3 and 6 of 7 get no rows,
    /// and every other one more than one 2-row batch.
    fn partition_of(i: usize) -> usize {
        [0, 2, 4, 5, 2, 0, 4][i % 7]
    }

    #[test]
    fn write_sorted_partitions_matches_writing_each_partition() {
        let values: Vec<i64> = (0..23).collect();
        let rows = long_rows(&values);
        let partitions: Vec<usize> = (0..rows.len()).map(partition_of).collect();
        let num_partitions = 7;
        let records = SorterRecords::new(&rows, &partitions);
        let dir = tempfile::tempdir().unwrap();

        // Every codec and checksum algorithm, and no checksum (-1) with no checksum array.
        for (codec, algo) in [("lz4", 1), ("zstd", 0), ("snappy", 2), ("lz4", -1)] {
            let name = format!("{codec}-{algo}");
            let path = dir.path().join(&name);
            let mut checksums = vec![i64::MIN; if algo == -1 { 0 } else { num_partitions }];
            let mut lengths = vec![0_i64; num_partitions];
            records
                .write(&path, algo, &mut checksums, &mut lengths, codec)
                .unwrap();

            // Writing each partition's rows, in the order the sorter keeps them, on its own.
            let mut expected = Vec::new();
            for partition in 0..num_partitions {
                let partition_rows: Vec<[u8; 16]> = rows
                    .iter()
                    .zip(&partitions)
                    .filter(|(_, &p)| p == partition)
                    .map(|(row, _)| *row)
                    .collect();
                if partition_rows.is_empty() {
                    assert_eq!(lengths[partition], 0, "{name}: partition {partition}");
                    if algo != -1 {
                        assert_eq!(checksums[partition], i64::MIN, "{name}: {partition}");
                    }
                    continue;
                }
                let sizes = vec![16_i32; partition_rows.len()];
                let ([written, checksum, _], data) = write_long_rows(
                    &partition_rows,
                    &sizes,
                    &dir.path().join(format!("{name}-{partition}")),
                    (algo != -1).then_some(algo),
                    i64::MIN,
                    codec,
                )
                .unwrap();
                assert_eq!(lengths[partition], written, "{name}: partition {partition}");
                if algo != -1 {
                    assert_eq!(checksums[partition], checksum, "{name}: {partition}");
                }
                expected.extend(data);
            }
            let data = std::fs::read(&path).unwrap();
            assert_eq!(data, expected, "{name}");

            // The blocks hold every row, partition by partition.
            let mut decoded = Vec::new();
            for block in shuffle_blocks(&data) {
                decoded.extend(long_values(&read_ipc_compressed(block).unwrap()));
            }
            let mut expected_values = values.clone();
            expected_values.sort_by_key(|&v| partition_of(v as usize));
            assert_eq!(decoded, expected_values, "{name}");
        }
    }

    /// Spark `UnsafeRow`s with one non-null string field each: an 8-byte null bitset, the
    /// field's offset and size, then the string padded to a word.
    fn string_rows(values: &[&str]) -> Vec<Vec<u8>> {
        values
            .iter()
            .map(|value| {
                let mut row = vec![0_u8; 16 + value.len().div_ceil(8) * 8];
                row[8..16].copy_from_slice(&(16_i64 << 32 | value.len() as i64).to_le_bytes());
                row[16..16 + value.len()].copy_from_slice(value.as_bytes());
                row
            })
            .collect()
    }

    #[test]
    fn write_sorted_partitions_matches_writing_each_partition_as_dictionaries_change() {
        // With a dictionary ratio of 1.5, a 2-row batch of two equal strings stays
        // dictionary-encoded and one of two different strings is written plain
        // (`builder_to_array`). So the block schema changes within partitions 0 and 3 and from
        // partition 0 to 2, and stays the same from partition 2 to 3, where the encoder keeps
        // its block writer.
        let inserted = [
            ("a", 0),
            ("e", 3),
            ("a", 0),
            ("d", 2),
            ("e", 3),
            ("b", 0),
            ("d", 2),
            ("f", 3),
            ("c", 0),
            ("g", 3),
        ];
        let (values, partitions): (Vec<&str>, Vec<usize>) = inserted.into_iter().unzip();
        let rows = string_rows(&values);
        let records = SorterRecords::new(&rows, &partitions);
        let num_partitions = 4;
        let mut expected_values = inserted.to_vec();
        expected_values.sort_by_key(|&(_, partition)| partition);
        let expected_values: Vec<&str> = expected_values.iter().map(|&(v, _)| v).collect();
        let dir = tempfile::tempdir().unwrap();

        // zstd stores the offsets of a plain string column as lengths, and of a dictionary one
        // as offsets, so a block writer kept for the wrong schema writes different bytes.
        for (codec, offsets_as_lengths) in [("lz4", false), ("zstd", true)] {
            let path = dir.path().join(codec);
            let mut checksums = vec![i64::MIN; num_partitions];
            let mut lengths = vec![0_i64; num_partitions];
            unsafe {
                write_sorted_partitions(
                    &records.pointers,
                    &records.page_addresses,
                    4,
                    &[DataType::Utf8],
                    path.to_str().unwrap(),
                    1.5,
                    2,
                    1,
                    &mut checksums,
                    &mut lengths,
                    codec,
                    1,
                    offsets_as_lengths,
                )
            }
            .unwrap();

            // Writing each partition's rows on its own, with an encoder of its own.
            let mut expected = Vec::new();
            for partition in 0..num_partitions {
                let (addresses, sizes): (Vec<i64>, Vec<i32>) = rows
                    .iter()
                    .zip(&partitions)
                    .filter(|(_, &p)| p == partition)
                    .map(|(row, _)| (row.as_ptr() as i64, row.len() as i32))
                    .unzip();
                if addresses.is_empty() {
                    assert_eq!(lengths[partition], 0, "{codec}: {partition}");
                    continue;
                }
                let partition_path = dir.path().join(format!("{codec}-{partition}"));
                let [written, checksum, _] = unsafe {
                    write_sorted_file(
                        &addresses,
                        &sizes,
                        &[DataType::Utf8],
                        partition_path.to_str().unwrap().to_string(),
                        1.5,
                        2,
                        true,
                        1,
                        i64::MIN,
                        codec,
                        1,
                        offsets_as_lengths,
                    )
                }
                .unwrap();
                assert_eq!(lengths[partition], written, "{codec}: {partition}");
                assert_eq!(checksums[partition], checksum, "{codec}: {partition}");
                expected.extend(std::fs::read(&partition_path).unwrap());
            }
            let data = std::fs::read(&path).unwrap();
            assert_eq!(data, expected, "{codec}");

            let mut dictionaries = Vec::new();
            let mut decoded = Vec::new();
            for block in shuffle_blocks(&data) {
                let batch = read_ipc_compressed(block).unwrap();
                let column = batch.column(0);
                dictionaries.push(matches!(column.data_type(), DataType::Dictionary(_, _)));
                let strings = arrow::compute::cast(column, &DataType::Utf8).unwrap();
                let strings = arrow::array::AsArray::as_string::<i32>(&strings);
                decoded.extend(strings.iter().map(|s| s.unwrap().to_string()));
            }
            assert_eq!(dictionaries, [true, false, true, true, false], "{codec}");
            assert_eq!(decoded, expected_values, "{codec}");
        }
    }

    #[test]
    fn write_sorted_partitions_continues_partition_checksums() {
        let rows = long_rows(&(0..23).collect::<Vec<_>>());
        let partitions: Vec<usize> = (0..rows.len()).map(partition_of).collect();
        let num_partitions = 7;
        let records = SorterRecords::new(&rows, &partitions);
        let dir = tempfile::tempdir().unwrap();

        // Two spills share the checksums, so after the second each partition's checksum covers
        // its bytes in both files, as Spark checksums the merged partition.
        let algorithms = [(0, crc32fast::hash as fn(&[u8]) -> u32), (1, adler32)];
        for (algo, checksum_of) in algorithms {
            let mut checksums = vec![i64::MIN; num_partitions];
            let mut spills = Vec::new();
            for spill in 0..2 {
                let path = dir.path().join(format!("{algo}-{spill}"));
                let mut lengths = vec![0_i64; num_partitions];
                records
                    .write(&path, algo, &mut checksums, &mut lengths, "lz4")
                    .unwrap();
                spills.push((std::fs::read(&path).unwrap(), lengths));
            }

            for partition in [0, 2, 4, 5] {
                let mut bytes = Vec::new();
                for (data, lengths) in &spills {
                    let start = lengths[..partition].iter().sum::<i64>() as usize;
                    bytes.extend_from_slice(&data[start..start + lengths[partition] as usize]);
                }
                assert_eq!(
                    checksums[partition],
                    checksum_of(&bytes) as i64,
                    "algorithm {algo}, partition {partition}"
                );
            }
        }
    }

    #[test]
    fn write_sorted_partitions_without_records() {
        let records = SorterRecords::new::<[u8; 16]>(&[], &[]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        let mut checksums = vec![i64::MIN; 3];
        let mut lengths = vec![0_i64; 3];
        records
            .write(&path, 1, &mut checksums, &mut lengths, "lz4")
            .unwrap();
        assert_eq!(lengths, [0, 0, 0]);
        assert_eq!(checksums, [i64::MIN; 3]);
        assert!(std::fs::read(&path).unwrap().is_empty());
    }

    #[test]
    fn write_sorted_partitions_rejects_bad_record_pointers() {
        let rows = long_rows(&[1, 2, 3]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad");
        let write = |records: &SorterRecords, num_partitions: usize| {
            records
                .write(&path, -1, &mut [], &mut vec![0; num_partitions], "lz4")
                .unwrap_err()
                .to_string()
        };

        let records = SorterRecords::new(&rows, &[0, 1, 2]);
        let error = write(&records, 2);
        assert!(error.contains("partition 2 of 2"), "{error}");

        let mut unsorted = SorterRecords::new(&rows, &[0, 1, 2]);
        unsorted.pointers.reverse();
        let error = write(&unsorted, 3);
        assert!(error.contains("not sorted by partition"), "{error}");

        let mut missing_page = SorterRecords::new(&rows, &[0, 1, 2]);
        missing_page.page_addresses[SORTER_PAGE_NUMBERS[1]] = 0;
        let error = write(&missing_page, 3);
        assert!(
            error.contains(&format!("page {}", SORTER_PAGE_NUMBERS[1])),
            "{error}"
        );

        let error = unsafe {
            write_sorted_partitions(
                &records.pointers,
                &records.page_addresses,
                2,
                &[DataType::Int64],
                path.to_str().unwrap(),
                1.0,
                2,
                -1,
                &mut [],
                &mut [0; 3],
                "lz4",
                1,
                false,
            )
        }
        .unwrap_err()
        .to_string();
        assert!(error.contains("record length size 2"), "{error}");
    }

    /// Exports `columns` as Arrow C Data structs and converts them with `columnar_to_row_convert`,
    /// returning the offsets, the lengths, and the row bytes.
    fn convert_exported(
        columns: &[ArrayRef],
        num_rows: i32,
    ) -> CometResult<(Vec<i32>, Vec<i32>, Vec<u8>)> {
        let mut ffi_arrays: Vec<Box<FFI_ArrowArray>> = columns
            .iter()
            .map(|column| Box::new(FFI_ArrowArray::new(&column.to_data())))
            .collect();
        let mut ffi_schemas: Vec<Box<FFI_ArrowSchema>> = columns
            .iter()
            .map(|column| Box::new(FFI_ArrowSchema::try_from(column.data_type()).unwrap()))
            .collect();
        let array_addrs: Vec<i64> = ffi_arrays
            .iter_mut()
            .map(|array| array.as_mut() as *mut FFI_ArrowArray as i64)
            .collect();
        let schema_addrs: Vec<i64> = ffi_schemas
            .iter_mut()
            .map(|schema| schema.as_mut() as *mut FFI_ArrowSchema as i64)
            .collect();
        let types = columns.iter().map(|c| c.data_type().clone()).collect();

        let mut ctx = ColumnarToRowContext::new(types, 8);
        let converted =
            unsafe { columnar_to_row_convert(&mut ctx, &array_addrs, &schema_addrs, num_rows) };
        // The call takes ownership of the exported structs, so they must not be released again.
        ffi_arrays.into_iter().for_each(std::mem::forget);
        ffi_schemas.into_iter().for_each(std::mem::forget);

        let (buffer, offsets, lengths) = converted?;
        let total = offsets
            .iter()
            .zip(lengths)
            .map(|(offset, length)| (offset + length) as usize)
            .max()
            .unwrap_or(0);
        let rows = unsafe { std::slice::from_raw_parts(buffer, total) }.to_vec();
        Ok((offsets.to_vec(), lengths.to_vec(), rows))
    }

    #[test]
    fn columnar_to_row_convert_imports_and_converts() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(arrow::array::StringArray::from(vec![
                Some("a"),
                Some("a longer string"),
                None,
            ])),
        ];
        let (offsets, lengths, rows) = convert_exported(&columns, 3).unwrap();

        let types = columns.iter().map(|c| c.data_type().clone()).collect();
        let mut expected_ctx = ColumnarToRowContext::new(types, 8);
        let (expected_buffer, expected_offsets, expected_lengths) =
            expected_ctx.convert(&columns, 3).unwrap();
        assert_eq!(offsets, expected_offsets);
        assert_eq!(lengths, expected_lengths);
        assert_eq!(rows, unsafe {
            std::slice::from_raw_parts(expected_buffer, rows.len())
        });
        // The string column makes the rows variable-length.
        assert_ne!(lengths[0], lengths[1]);
    }

    #[test]
    fn columnar_to_row_convert_rejects_mismatched_addresses() {
        let column: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let mut ffi_array = Box::new(FFI_ArrowArray::new(&column.to_data()));
        let array_addrs = [ffi_array.as_mut() as *mut FFI_ArrowArray as i64];
        let mut ctx = ColumnarToRowContext::new(vec![DataType::Int32], 8);
        // Rejected before any struct is imported, so the export is still released on drop.
        let error = unsafe { columnar_to_row_convert(&mut ctx, &array_addrs, &[], 2) }
            .expect_err("mismatched addresses are rejected");
        assert!(
            error
                .to_string()
                .contains("0 schema addresses for 1 array addresses"),
            "{error}"
        );
    }

    /// A range of `num_elements` longs from 0, which needs no JVM input and reserves no memory.
    fn range_operator(num_elements: i64) -> Operator {
        use datafusion_comet_proto::spark_operator::RangeScan;

        Operator {
            plan_id: 1,
            op_struct: Some(OpStruct::RangeScan(RangeScan {
                start: 0,
                step: 1,
                num_elements,
                num_slices: 1,
            })),
            ..Default::default()
        }
    }

    fn range_plan(num_elements: i64) -> Vec<u8> {
        use prost::Message;
        range_operator(num_elements).encode_to_vec()
    }

    /// A range of `num_elements` longs sorted in descending order. The sort buffers its whole
    /// input, so the plan holds a memory reservation while it emits its output.
    fn sorted_range_plan(num_elements: i64) -> Vec<u8> {
        use datafusion_comet_proto::spark_expression::{
            self, expr::ExprStruct, NullOrdering, SortDirection,
        };
        use datafusion_comet_proto::spark_operator::Sort;
        use prost::Message;

        let column = Expr {
            expr_struct: Some(ExprStruct::Bound(spark_expression::BoundReference {
                index: 0,
                // A Spark `long`.
                datatype: Some(spark_expression::DataType {
                    type_id: 4,
                    type_info: None,
                }),
            })),
            ..Default::default()
        };
        let descending = Expr {
            expr_struct: Some(ExprStruct::SortOrder(Box::new(
                spark_expression::SortOrder {
                    child: Some(Box::new(column)),
                    direction: SortDirection::Descending as i32,
                    null_ordering: NullOrdering::NullsLast as i32,
                },
            ))),
            ..Default::default()
        };
        Operator {
            plan_id: 2,
            children: vec![range_operator(num_elements)],
            op_struct: Some(OpStruct::Sort(Sort {
                sort_orders: vec![descending],
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn long_values(batch: &RecordBatch) -> Vec<i64> {
        arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(batch.column(0))
            .values()
            .to_vec()
    }

    /// Creates a plan without a JVM: no metrics node or JVM input, and a memory pool backed by a
    /// fake Spark.
    fn create_test_plan(serialized_plan: Vec<u8>) -> CometResult<Box<ExecutionContext>> {
        use crate::execution::memory_pools::create_memory_pool_with_fake_spark;
        use datafusion_comet_proto::spark_config::ConfigMap;
        use prost::Message;

        let configs = ConfigMap {
            entries: HashMap::from([(SPARK_EXECUTOR_CORES.to_string(), "2".to_string())]),
        };
        let inputs = PlanInputs {
            id: -6201,
            settings: prepare_plan_settings(&configs.encode_to_vec())?,
            serialized_plan,
            partition_count: 1,
            batch_size: 4,
            off_heap_mode: true,
            memory_pool_type: "greedy_unified".to_string(),
            memory_limit: 1 << 30,
            task_attempt_id: -6201,
            task_cpus: 1,
            local_dirs: vec![std::env::temp_dir().to_str().unwrap().to_string()],
            metrics_update_interval: 0,
            start: Instant::now(),
            jvm: PlanJvmRefs {
                metrics: None,
                input_sources: vec![],
                key_unwrapper: None,
                task_context: None,
                class_loader: None,
            },
        };
        create_execution_context(inputs, |config| {
            create_memory_pool_with_fake_spark(config, -6201, 1 << 30)
        })
    }

    #[test]
    fn execute_plan_drains_a_plan() {
        let _guard = serial();
        let mut exec_context = create_test_plan(range_plan(10)).unwrap();
        assert!(
            exec_context.root_op.is_none(),
            "operators are created on first execution"
        );

        let mut values = Vec::new();
        while let Some(batch) = execute_plan(&mut exec_context, 0, 0, &mut |_| Ok(())).unwrap() {
            assert!(
                batch.num_rows() <= 4,
                "batches follow the configured batch size"
            );
            values.extend(long_values(&batch));
        }
        assert_eq!(values, (0..10).collect::<Vec<i64>>());
        assert!(
            exec_context.batch_producer.is_some(),
            "a plan without JVM input runs on a Tokio task"
        );
        release_plan(exec_context, &mut |_| Ok(())).unwrap();
    }

    /// The sort holds a reservation while it emits its 10,000 sorted rows, 4 at a time.
    const SORTED_ROWS: i64 = 10_000;

    #[test]
    fn release_plan_returns_the_plans_memory() {
        let _guard = serial();
        let mut exec_context = create_test_plan(sorted_range_plan(SORTED_ROWS)).unwrap();

        // Without an update interval, metrics are published only when the plan is released.
        let mut publishes = 0;
        let batch = execute_plan(&mut exec_context, 0, 0, &mut |_| {
            publishes += 1;
            Ok(())
        })
        .unwrap()
        .expect("the sort emits a first batch");
        assert_eq!(long_values(&batch), [9999, 9998, 9997, 9996]);
        assert_eq!(publishes, 0);

        // Released mid-stream, as when Spark stops reading early, with the sort still holding
        // its buffered input.
        let plan_memory = Arc::clone(&exec_context.plan_memory);
        assert!(plan_memory.reserved() > 0, "the sort holds a reservation");
        release_plan(exec_context, &mut |ctx| {
            assert!(
                ctx.root_op.is_some(),
                "the final metrics are of the executed plan"
            );
            publishes += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(publishes, 1);
        assert_eq!(plan_memory.reserved(), 0);
    }

    #[test]
    fn release_plan_reports_a_failed_metrics_publish() {
        let _guard = serial();
        let mut exec_context = create_test_plan(sorted_range_plan(SORTED_ROWS)).unwrap();
        execute_plan(&mut exec_context, 0, 0, &mut |_| Ok(())).unwrap();
        let plan_memory = Arc::clone(&exec_context.plan_memory);
        assert!(plan_memory.reserved() > 0, "the sort holds a reservation");

        let error = release_plan(exec_context, &mut |_| {
            Err(CometError::Internal("metrics node is gone".to_string()))
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("metrics node is gone"),
            "{error}"
        );
        // The plan is still released.
        assert_eq!(plan_memory.reserved(), 0);
    }

    #[test]
    fn create_execution_context_rejects_bad_input() {
        let _guard = serial();
        let error = create_test_plan(vec![0xff, 0xff, 0xff])
            .err()
            .expect("a malformed plan is rejected");
        assert!(error.to_string().contains("deserialize"), "{error}");
    }

    struct NoopPusher;

    impl ShufflePartitionPusher for NoopPusher {
        fn push_partition_data(
            &self,
            _partition_id: i32,
            _data: &[u8],
        ) -> datafusion::common::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn register_shuffle_partition_pusher_once_before_execution() {
        let _guard = serial();
        let mut exec_context = create_test_plan(range_plan(3)).unwrap();
        register_shuffle_partition_pusher(&mut exec_context, || Ok(Arc::new(NoopPusher))).unwrap();
        assert!(exec_context.shuffle_partition_pusher.is_some());

        // The pusher is built only once the plan is known to accept it.
        let error = register_shuffle_partition_pusher(&mut exec_context, || {
            panic!("a rejected pusher must not be built")
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("already been registered"),
            "{error}"
        );

        let mut exec_context = create_test_plan(range_plan(3)).unwrap();
        execute_plan(&mut exec_context, 0, 0, &mut |_| Ok(())).unwrap();
        let error = register_shuffle_partition_pusher(&mut exec_context, || {
            panic!("a rejected pusher must not be built")
        })
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot be registered after native execution starts"),
            "{error}"
        );
        release_plan(exec_context, &mut |_| Ok(())).unwrap();
    }
}
