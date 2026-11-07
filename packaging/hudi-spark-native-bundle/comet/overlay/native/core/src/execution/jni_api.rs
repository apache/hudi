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

use super::{serde, utils::SparkArrowConvert};
use crate::{
    errors::{try_unwrap_or_throw, CometError, CometResult},
    execution::{
        metrics::utils::update_comet_metric, planner::PhysicalPlanner, serde::to_arrow_datatype,
        shuffle::spark_unsafe::row::process_sorted_row_partition, sort::RdxSort,
    },
    jvm_bridge::{JVMClasses, JavaShufflePartitionPusher, ShufflePartitionPusher},
};
use std::collections::HashSet;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::disk_manager::DiskManagerMode;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::{
    execution::disk_manager::DiskManagerBuilder,
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

use crate::execution::memory_pools::{create_memory_pool, parse_memory_pool_config};
use crate::execution::operators::{ScanExec, ShuffleScanExec};
use crate::execution::shuffle::{
    decode_remote_shuffle_batch, read_ipc_compressed, CompressionCodec, ShuffleWriterExec,
};
use crate::execution::spark_plan::SparkPlan;

use crate::execution::tracing::{
    get_thread_id, log_memory_usage, trace_begin, trace_end, with_trace, POOL_TOTAL_METRIC,
};

use crate::execution::memory_pools::logging_pool::LoggingMemoryPool;
use crate::execution::spark_config::{
    SparkConfig, COMET_DEBUG_ENABLED, COMET_DEBUG_MEMORY,
    COMET_EXEC_AGGREGATE_SKIP_PARTIAL_ENABLED, COMET_EXPLAIN_NATIVE_ENABLED,
    COMET_MAX_TEMP_DIRECTORY_SIZE, COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED,
    COMET_TRACING_ENABLED, SPARK_EXECUTOR_CORES,
};
use crate::parquet::encryption_support::{CometEncryptionFactory, ENCRYPTION_FACTORY_ID};
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
    /// plans share it.
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
        pools_reserved: sum_reserved(&snapshot.all_pools),
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
/// The runtime is moved out of the global slot and shut down in the background so the
/// calling (JNI) thread is not blocked waiting for worker threads to finish. Any handles
/// previously returned by [`get_runtime`] will start failing their spawns once the runtime
/// is gone, so this must only be called when no native execution is in flight.
///
/// Must not be called from within the runtime's own worker threads, otherwise the shutdown
/// would deadlock/panic.
pub fn release_runtime() {
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
        OpStruct::ContribScan(_) => "ContribScan",
        OpStruct::WindowGroupLimit(_) => "WindowGroupLimit",
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
    /// Receives batches from a spawned tokio task (async I/O path)
    pub batch_receiver: Option<mpsc::Receiver<DataFusionResult<RecordBatch>>>,
    /// Native metrics
    pub metrics: Arc<Global<JObject<'static>>>,
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
        // Deserialize Spark configs
        let bytes = env.convert_byte_array(serialized_spark_configs)?;
        let spark_configs = serde::deserialize_config(bytes.as_slice())?;
        let spark_config: HashMap<String, String> = spark_configs.entries.into_iter().collect();

        // Initialize the tokio runtime with spark.executor.cores as the default
        // worker thread count, falling back to 1 if not set.
        let executor_cores = spark_config.get_usize(SPARK_EXECUTOR_CORES, 1);
        init_runtime(executor_cores);

        // Access Comet configs
        let debug_native = spark_config.get_bool(COMET_DEBUG_ENABLED);
        let explain_native = spark_config.get_bool(COMET_EXPLAIN_NATIVE_ENABLED);
        let tracing_enabled = spark_config.get_bool(COMET_TRACING_ENABLED);
        let max_temp_directory_size =
            spark_config.get_u64(COMET_MAX_TEMP_DIRECTORY_SIZE, 100 * 1024 * 1024 * 1024);
        let logging_memory_pool = spark_config.get_bool(COMET_DEBUG_MEMORY);

        with_trace("createPlan", tracing_enabled, || {
            // Init JVM classes
            JVMClasses::init(env);

            let start = Instant::now();

            // Deserialize query plan
            let bytes = env.convert_byte_array(serialized_query)?;
            let spark_plan = serde::deserialize_op(bytes.as_slice())?;

            let metrics = Arc::new(jni_new_global_ref!(env, metrics_node)?);

            // Get the global references of input sources
            let mut input_sources = vec![];
            let num_inputs = iterators.len(env)?;
            for i in 0..num_inputs {
                let input_source = iterators.get_element(env, i)?;
                let input_source = Arc::new(jni_new_global_ref!(env, input_source)?);
                input_sources.push(input_source);
            }

            // Create DataFusion memory pool
            let task_memory_manager =
                Arc::new(jni_new_global_ref!(env, comet_task_memory_manager_obj)?);

            let memory_pool_type = memory_pool_type.try_to_string(env)?;
            let memory_pool_config = parse_memory_pool_config(
                off_heap_mode != JNI_FALSE,
                memory_pool_type,
                memory_limit,
            )?;
            let memory_pool =
                create_memory_pool(&memory_pool_config, task_memory_manager, task_attempt_id);

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

            let memory_pool = if logging_memory_pool {
                Arc::new(LoggingMemoryPool::new(task_attempt_id as u64, memory_pool))
            } else {
                memory_pool
            };

            // Get local directories for storing spill files
            let num_local_dirs = local_dirs.len(env)?;
            let mut local_dirs_vec = vec![];
            for i in 0..num_local_dirs {
                let local_dir = local_dirs.get_element(env, i)?;
                let local_dir = unsafe { JString::from_raw(&*env, local_dir.into_raw()) };
                let local_dir = local_dir.try_to_string(env)?;
                local_dirs_vec.push(local_dir);
            }

            // We need to keep the session context alive. Some session state like temporary
            // dictionaries are stored in session context. If it is dropped, the temporary
            // dictionaries will be dropped as well.
            let session = prepare_datafusion_session_context(
                batch_size as usize,
                memory_pool,
                local_dirs_vec,
                max_temp_directory_size,
                task_cpus as usize,
                &spark_config,
                &spark_plan,
            )?;

            let plan_creation_time = start.elapsed();

            let metrics_update_interval = if metrics_update_interval > 0 {
                Some(Duration::from_millis(metrics_update_interval as u64))
            } else {
                None
            };

            // Handle key unwrapper for encrypted files
            if !key_unwrapper_obj.is_null() {
                let encryption_factory = CometEncryptionFactory {
                    key_unwrapper: Arc::new(jni_new_global_ref!(env, key_unwrapper_obj)?),
                };
                session.runtime_env().register_parquet_encryption_factory(
                    ENCRYPTION_FACTORY_ID,
                    Arc::new(encryption_factory),
                );
            }

            let session = Arc::new(session);

            let tracing_event_name = if tracing_enabled {
                build_tracing_event_name(&spark_plan)
            } else {
                String::new()
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

            let exec_context = Box::new(ExecutionContext {
                id,
                spark_plan,
                partition_count: partition_count as usize,
                root_op: None,
                scans: vec![],
                shuffle_scans: vec![],
                input_sources,
                stream: None,
                batch_receiver: None,
                metrics,
                metrics_update_interval,
                metrics_last_update_time: Instant::now(),
                plan_creation_time,
                session_ctx: session,
                debug_native,
                explain_native,
                tracing_enabled,
                rust_thread_id,
                tracing_memory_metric_name: format!(
                    "thread_{rust_thread_id}_comet_memory_reserved"
                ),
                tracing_event_name,
                task_context,
                class_loader,
                shuffle_partition_pusher: None,
                memory_pool_registration,
            });

            Ok(Box::into_raw(exec_context) as i64)
        })
    })
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
        if exec_context.root_op.is_some() {
            return Err(CometError::Internal(
                "Remote shuffle callback cannot be registered after native execution starts"
                    .to_string(),
            ));
        }

        if exec_context.shuffle_partition_pusher.is_some() {
            return Err(CometError::Internal(
                "Remote shuffle callback has already been registered for this task".to_string(),
            ));
        }

        let pusher = JavaShufflePartitionPusher::try_new(env, &callback)?;
        exec_context.shuffle_partition_pusher = Some(Arc::new(pusher));
        Ok(())
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
                // PartialMerge is represented as native Partial, but consumes states, not rows.
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
    let disk_manager = DiskManagerBuilder::default()
        .with_mode(DiskManagerMode::Directories(paths))
        .with_max_temp_directory_size(max_temp_directory_size);
    let mut rt_config = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(disk_manager)
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
    let num_cols = array_addrs.len(env)?;

    let array_addrs = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
    let array_addrs = &*array_addrs;

    let schema_addrs = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
    let schema_addrs = &*schema_addrs;

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

    Ok(num_rows as jlong)
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
            let exec_context_id = exec_context.id;

            // Initialize the execution stream.
            // Because we don't know if input arrays are dictionary-encoded when we create
            // query plan, we need to defer stream initialization to first time execution.
            if exec_context.root_op.is_none() {
                let start = Instant::now();
                let planner =
                    PhysicalPlanner::new(Arc::clone(&exec_context.session_ctx), partition)
                        .with_exec_id(exec_context_id)
                        .with_sql_text_pool(&exec_context.spark_plan)
                        .with_task_context(exec_context.task_context.clone())
                        .with_class_loader(exec_context.class_loader.clone())
                        .with_shuffle_partition_pusher(
                            exec_context.shuffle_partition_pusher.clone(),
                        );
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
                    //
                    // Channel capacity of 2 allows the producer to work one batch
                    // ahead while the consumer processes the current one via JNI,
                    // without buffering excessive memory. Increasing this would
                    // trade memory for latency hiding if JNI/FFI overhead dominates;
                    // decreasing to 1 would serialize production and consumption.
                    let (tx, rx) = mpsc::channel(2);
                    let mut stream = stream;
                    get_runtime().spawn(async move {
                        let result = std::panic::AssertUnwindSafe(async {
                            while let Some(batch) = stream.next().await {
                                if tx.send(batch).await.is_err() {
                                    break;
                                }
                            }
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
                    exec_context.batch_receiver = Some(rx);
                } else {
                    exec_context.stream = Some(stream);
                }
                exec_context.root_op = Some(root_op);
            } else {
                // Pull input batches
                pull_input_batches(exec_context)?;
            }

            if let Some(rx) = &mut exec_context.batch_receiver {
                match rx.blocking_recv() {
                    Some(Ok(batch)) => {
                        update_metrics(env, exec_context)?;
                        return prepare_output(
                            env,
                            array_addrs,
                            schema_addrs,
                            batch,
                            exec_context.debug_native,
                        );
                    }
                    Some(Err(e)) => {
                        return Err(e.into());
                    }
                    None => {
                        log_plan_metrics(exec_context, stage_id, partition);
                        return Ok(-1);
                    }
                }
            }

            // ScanExec path: JVM-fed scans return `Pending` until `pull_input_batches` refills
            // them and wakes the stream. A poll that is still pending, with nothing having woken
            // the stream by the end of the pull, waits on native I/O, and `next_batch` parks
            // until it completes.
            let mut stream = exec_context.stream.take().unwrap();
            let next = get_runtime().block_on(next_batch(&mut stream, || {
                pull_input_batches(exec_context)?;
                update_metrics_on_interval(env, exec_context)
            }));
            exec_context.stream = Some(stream);
            let next = next?;
            update_metrics_on_interval(env, exec_context)?;
            match next {
                Some(batch) => prepare_output(
                    env,
                    array_addrs,
                    schema_addrs,
                    batch,
                    exec_context.debug_native,
                ),
                None => {
                    log_plan_metrics(exec_context, stage_id, partition);
                    Ok(-1)
                }
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

        // Reclaim ownership of the context up front so that it is always freed, even if updating
        // metrics below fails. Dropping it releases the memory pool and every JNI global ref the
        // context holds.
        let mut execution_context: Box<ExecutionContext> =
            Box::from_raw(exec_context as *mut ExecutionContext);

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

        // Flush metrics last, as it is the only fallible step here.
        update_metrics(env, &mut execution_context)
    })
}

/// Runs `update_metrics` once the configured interval has passed and, with tracing on, samples
/// this thread's pool reservation at the same cadence.
fn update_metrics_on_interval(
    env: &mut Env,
    exec_context: &mut ExecutionContext,
) -> CometResult<()> {
    let Some(interval) = exec_context.metrics_update_interval else {
        return Ok(());
    };
    let now = Instant::now();
    if now - exec_context.metrics_last_update_time < interval {
        return Ok(());
    }
    update_metrics(env, exec_context)?;
    exec_context.metrics_last_update_time = now;
    if exec_context.tracing_enabled {
        log_memory_usage(
            &exec_context.tracing_memory_metric_name,
            total_reserved_for_thread(exec_context.rust_thread_id) as u64,
        );
    }
    Ok(())
}

fn update_metrics(env: &mut Env, exec_context: &mut ExecutionContext) -> CometResult<()> {
    if let Some(native_query) = &exec_context.root_op {
        let metrics = exec_context.metrics.as_obj();
        update_comet_metric(env, metrics, native_query)
    } else {
        Ok(())
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

        let root_op = context.root_op.as_ref().ok_or_else(|| {
            CometError::Internal(
                "Cannot read shuffle partition offsets before the plan has been executed"
                    .to_string(),
            )
        })?;

        // `ExecutionPlan` has `Any` as a supertrait but no `as_any` method of its own, so upcast
        // the trait object before downcasting to the writer.
        let writer = (root_op.native_plan.as_ref() as &dyn std::any::Any)
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

        let long_array = env.new_long_array(offsets.len())?;
        long_array.set_region(env, 0, offsets)?;
        Ok(long_array.into_raw())
    })
}

/// Used by Comet shuffle external sorter to write sorted records to disk.
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
    tracing_enabled: jboolean,
) -> jlongArray {
    try_unwrap_or_throw(&e, |env| unsafe {
        with_trace(
            "writeSortedFileNative",
            tracing_enabled != JNI_FALSE,
            || {
                let data_types = convert_datatype_arrays(env, serialized_datatypes)?;

                let row_num = row_addresses.len(env)?;
                let row_addresses = row_addresses.get_elements(env, ReleaseMode::NoCopyBack)?;

                let row_sizes = row_sizes.get_elements(env, ReleaseMode::NoCopyBack)?;

                let row_addresses_ptr = row_addresses.as_ptr();
                let row_sizes_ptr = row_sizes.as_ptr();

                let output_path: String = file_path.try_to_string(env).unwrap();

                let current_checksum = if current_checksum == i64::MIN {
                    // Initial checksum is not available.
                    None
                } else {
                    Some(current_checksum as u32)
                };

                let compression_codec: String = compression_codec.try_to_string(env).unwrap();

                let compression_codec = match compression_codec.as_str() {
                    "zstd" => CompressionCodec::Zstd(compression_level),
                    "lz4" => CompressionCodec::Lz4Frame,
                    "snappy" => CompressionCodec::Snappy,
                    _ => CompressionCodec::Lz4Frame,
                };

                let (written_bytes, checksum, encode_nanos) = process_sorted_row_partition(
                    row_num,
                    batch_size as usize,
                    row_addresses_ptr,
                    row_sizes_ptr,
                    &data_types,
                    output_path,
                    prefer_dictionary_ratio,
                    checksum_enabled,
                    checksum_algo,
                    current_checksum,
                    &compression_codec,
                )?;

                let checksum = if let Some(checksum) = checksum {
                    checksum as i64
                } else {
                    // Spark checksums (CRC32 or Adler32) are both u32, so we use i64::MIN to indicate
                    // checksum is not available.
                    i64::MIN
                };

                // results[0] = bytes written, results[1] = checksum, results[2] = encode nanos
                let long_array = env.new_long_array(3)?;
                long_array.set_region(env, 0, &[written_bytes, checksum, encode_nanos])?;

                Ok(long_array.into_raw())
            },
        )
    })
}

#[no_mangle]
/// Used by Comet shuffle external sorter to sort in-memory row partition ids.
pub extern "system" fn Java_org_apache_comet_Native_sortRowPartitionsNative(
    e: EnvUnowned,
    _class: JClass,
    address: jlong,
    size: jlong,
    tracing_enabled: jboolean,
) {
    try_unwrap_or_throw(&e, |_| {
        with_trace(
            "sortRowPartitionsNative",
            tracing_enabled != JNI_FALSE,
            || {
                // SAFETY: JVM unsafe memory allocation is aligned with long.
                debug_assert!(address != 0, "sortRowPartitionsNative: null address");
                debug_assert!(size >= 0, "sortRowPartitionsNative: negative size {size}");
                debug_assert_eq!(
                    (address as usize) % std::mem::align_of::<i64>(),
                    0,
                    "sortRowPartitionsNative: address not aligned to i64"
                );
                let array =
                    unsafe { std::slice::from_raw_parts_mut(address as *mut i64, size as usize) };
                array.rdxsort();
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
            decode_shuffle_block(env, byte_buffer, length, array_addrs, schema_addrs, None)
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
        let schema = ShuffleScan::decode(bytes.as_slice()).map_err(|error| {
            CometError::Internal(format!("Invalid expected remote shuffle schema: {error}"))
        })?;
        let decoder = RemoteShuffleDecoder {
            expected_types: schema.fields.iter().map(to_arrow_datatype).collect(),
        };
        Ok(Box::into_raw(Box::new(decoder)) as jlong)
    })
}

/// Immutable decoding state owned by one JVM remote shuffle iterator, not shared across tasks.
struct RemoteShuffleDecoder {
    expected_types: Vec<ArrowDataType>,
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
            decode_shuffle_block(
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

fn decode_shuffle_block(
    env: &mut Env,
    byte_buffer: JByteBuffer,
    length: jint,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    expected_types: Option<&[ArrowDataType]>,
) -> CometResult<jlong> {
    let raw_pointer = env.get_direct_buffer_address(&byte_buffer)?;
    let length = length as usize;
    let slice: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };
    let batch = if let Some(expected_types) = expected_types {
        // Reject incompatible logical types, then decode dictionaries before JVM import. The
        // JVM importer supports fewer dictionary key/value layouts than the shuffle writer.
        decode_remote_shuffle_batch(slice, expected_types)?
    } else {
        read_ipc_compressed(slice)?
    };
    prepare_output(env, array_addrs, schema_addrs, batch, false)
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

        let num_cols = array_addrs.len(env)?;

        // Get array and schema addresses
        let array_addrs_elements =
            unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
        let schema_addrs_elements =
            unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };

        // Import Arrow arrays from FFI
        let mut arrays = Vec::with_capacity(num_cols);
        for i in 0..num_cols {
            let array_ptr = array_addrs_elements[i] as *mut FFI_ArrowArray;
            let schema_ptr = schema_addrs_elements[i] as *mut FFI_ArrowSchema;

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
        let (buffer_ptr, offsets, lengths) = ctx.convert(&arrays, num_rows as usize)?;

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
}
