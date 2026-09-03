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
    jvm_bridge::JVMClasses,
};
use std::collections::HashSet;

use arrow::array::{Array, RecordBatch, UInt32Array};
use arrow::compute::{take, TakeOptions};
use arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::execution::disk_manager::DiskManagerMode;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::{
    execution::disk_manager::DiskManagerBuilder,
    physical_plan::{display::DisplayableExecutionPlan, SendableRecordBatchStream},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_comet_proto::spark_operator::Operator;
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
use datafusion_spark::function::string::luhn_check::SparkLuhnCheck;
use datafusion_spark::function::string::space::SparkSpace;
use datafusion_spark::function::string::substring::SparkSubstring;
use datafusion_spark::function::url::try_url_decode::TryUrlDecode as SparkTryUrlDecode;
use datafusion_spark::function::url::url_decode::UrlDecode as SparkUrlDecode;
use datafusion_spark::function::url::url_encode::UrlEncode as SparkUrlEncode;
use futures::poll;
use futures::stream::StreamExt;
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
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{sync::Arc, task::Poll};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc;

use crate::execution::memory_pools::{
    create_memory_pool, handle_task_shared_pool_release, parse_memory_pool_config, MemoryPoolConfig,
};
use crate::execution::operators::{ScanExec, ShuffleScanExec};
use crate::execution::shuffle::{read_ipc_compressed, CompressionCodec};
use crate::execution::spark_plan::SparkPlan;

use crate::execution::tracing::{
    get_thread_id, log_memory_usage, trace_begin, trace_end, with_trace,
};

use crate::execution::memory_pools::logging_pool::LoggingMemoryPool;
use crate::execution::spark_config::{
    SparkConfig, COMET_DEBUG_ENABLED, COMET_DEBUG_MEMORY, COMET_EXPLAIN_NATIVE_ENABLED,
    COMET_MAX_TEMP_DIRECTORY_SIZE, COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED,
    COMET_TRACING_ENABLED, SPARK_EXECUTOR_CORES,
};
use crate::parquet::encryption_support::{CometEncryptionFactory, ENCRYPTION_FACTORY_ID};
use datafusion_comet_proto::spark_operator::operator::OpStruct;
use log::info;
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

/// Registry of active memory pools per Rust thread ID.
/// Used to sum memory reservations across all contexts on the same thread for tracing.
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

/// Unregister a context's pool and return the remaining total reserved for the thread.
fn unregister_and_total(thread_id: u64, context_id: i64) -> usize {
    let mut map = get_thread_memory_pools().lock();
    if let Some(pools) = map.get_mut(&thread_id) {
        pools.remove(&context_id);
        if pools.is_empty() {
            map.remove(&thread_id);
            return 0;
        }
        let mut seen = HashSet::new();
        return pools
            .values()
            .filter_map(|p| {
                let ptr = Arc::as_ptr(p) as *const ();
                seen.insert(ptr).then(|| p.reserved())
            })
            .sum::<usize>();
    }
    0
}

fn total_reserved_for_thread(thread_id: u64) -> usize {
    let map = get_thread_memory_pools().lock();
    map.get(&thread_id)
        .map(|pools| {
            // Deduplicate pools that share the same underlying allocation
            // (e.g. task-shared pools registered by multiple execution contexts)
            let mut seen = HashSet::new();
            pools
                .values()
                .filter_map(|p| {
                    let ptr = Arc::as_ptr(p) as *const ();
                    seen.insert(ptr).then(|| p.reserved())
                })
                .sum::<usize>()
        })
        .unwrap_or(0)
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
        .build()
        .expect("Failed to create Tokio runtime")
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

/// Returns a short name for an OpStruct variant.
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
        OpStruct::HudiScan(_) => "HudiScan",
        OpStruct::ParquetWriter(_) => "ParquetWriter",
        OpStruct::Explode(_) => "Explode",
        OpStruct::CsvScan(_) => "CsvScan",
        OpStruct::ShuffleScan(_) => "ShuffleScan",
        OpStruct::BroadcastNestedLoopJoin(_) => "BroadcastNestedLoopJoin",
        OpStruct::Sample(_) => "Sample",
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
    /// Task attempt id
    pub task_attempt_id: i64,
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
    /// Counter to avoid checking time on every poll iteration (reduces syscalls)
    pub poll_count_since_metrics_check: u32,
    /// The time it took to create the native plan and configure the context
    pub plan_creation_time: Duration,
    /// DataFusion SessionContext
    pub session_ctx: Arc<SessionContext>,
    /// Whether to enable additional debugging checks & messages
    pub debug_native: bool,
    /// Whether to write native plans with metrics to stdout
    pub explain_native: bool,
    /// Memory pool config
    pub memory_pool_config: MemoryPoolConfig,
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
    memory_limit_per_task: jlong,
    task_attempt_id: jlong,
    task_cpus: jlong,
    key_unwrapper_obj: JObject,
    task_context_obj: JObject,
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
                memory_limit_per_task,
            )?;
            let memory_pool =
                create_memory_pool(&memory_pool_config, task_memory_manager, task_attempt_id);

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

            // Register this context's memory pool so we can sum all pools
            // on the same thread when emitting tracing metrics.
            let rust_thread_id = get_thread_id();
            if tracing_enabled {
                register_memory_pool(
                    rust_thread_id,
                    id,
                    Arc::clone(&session.runtime_env().memory_pool),
                );
            }

            let tracing_event_name = if tracing_enabled {
                build_tracing_event_name(&spark_plan)
            } else {
                String::new()
            };

            // Capture the driving Spark task's TaskContext as a JNI global reference when
            // non-null. The `Arc<Global<JObject>>` releases its global ref on drop, so cleanup
            // is automatic when the ExecutionContext drops.
            let task_context = if !task_context_obj.is_null() {
                Some(Arc::new(jni_new_global_ref!(env, task_context_obj)?))
            } else {
                None
            };

            let exec_context = Box::new(ExecutionContext {
                id,
                task_attempt_id,
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
                poll_count_since_metrics_check: 0,
                plan_creation_time,
                session_ctx: session,
                debug_native,
                explain_native,
                memory_pool_config,
                tracing_enabled,
                rust_thread_id,
                tracing_memory_metric_name: format!(
                    "thread_{rust_thread_id}_comet_memory_reserved"
                ),
                tracing_event_name,
                task_context,
            });

            Ok(Box::into_raw(exec_context) as i64)
        })
    })
}

/// Configure DataFusion session context.
fn prepare_datafusion_session_context(
    batch_size: usize,
    memory_pool: Arc<dyn MemoryPool>,
    local_dirs: Vec<String>,
    max_temp_directory_size: u64,
    task_cpus: usize,
    spark_config: &HashMap<String, String>,
) -> CometResult<SessionContext> {
    let paths = local_dirs.into_iter().map(PathBuf::from).collect();
    let disk_manager = DiskManagerBuilder::default()
        .with_mode(DiskManagerMode::Directories(paths))
        .with_max_temp_directory_size(max_temp_directory_size);
    let mut rt_config = RuntimeEnvBuilder::new().with_disk_manager_builder(disk_manager);
    rt_config = rt_config.with_memory_pool(memory_pool);

    let mut session_config = SessionConfig::new()
        .with_target_partitions(task_cpus)
        // This DataFusion context is within the scope of an executing Spark Task. We want to set
        // its internal parallelism to the number of CPUs allocated to Spark Tasks. This can be
        // modified by changing spark.task.cpus in the Spark config.
        .with_batch_size(batch_size)
        // DataFusion partial aggregates can emit duplicate rows so we disable the
        // skip partial aggregation feature because this is not compatible with Spark's
        // use of partial aggregates.
        .set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            // this is the threshold of number of groups / number of rows and the
            // maximum value is 1.0, so we set the threshold a little higher just
            // to be safe
            &ScalarValue::Float64(Some(1.1)),
        );

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

            if array_ref.offset() != 0 {
                // https://github.com/apache/datafusion-comet/issues/2051
                // Bug with non-zero offset FFI, so take to a new array which will have an offset of 0.
                // We expect this to be a cold code path, hence the check_bounds: true and assert_eq.
                let indices = UInt32Array::from((0..num_rows as u32).collect::<Vec<u32>>());
                let new_array = take(
                    array_ref,
                    &indices,
                    Some(TakeOptions { check_bounds: true }),
                )?;

                assert_eq!(new_array.offset(), 0);

                new_array
                    .to_data()
                    .move_to_spark(array_addrs[i], schema_addrs[i])?;
            } else {
                array_ref
                    .to_data()
                    .move_to_spark(array_addrs[i], schema_addrs[i])?;
            }
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
                        .with_task_context(exec_context.task_context.clone());
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

            // ScanExec path: busy-poll to interleave JVM batch pulls with stream polling
            get_runtime().block_on(async {
                loop {
                    let next_item = exec_context.stream.as_mut().unwrap().next();
                    let poll_output = poll!(next_item);

                    // Only check time/tracing every 100 polls to reduce overhead
                    exec_context.poll_count_since_metrics_check += 1;
                    if exec_context.poll_count_since_metrics_check >= 100 {
                        exec_context.poll_count_since_metrics_check = 0;
                        if let Some(interval) = exec_context.metrics_update_interval {
                            let now = Instant::now();
                            if now - exec_context.metrics_last_update_time >= interval {
                                update_metrics(env, exec_context)?;
                                exec_context.metrics_last_update_time = now;
                            }
                        }
                        if exec_context.tracing_enabled {
                            log_memory_usage(
                                &exec_context.tracing_memory_metric_name,
                                total_reserved_for_thread(exec_context.rust_thread_id) as u64,
                            );
                        }
                    }

                    match poll_output {
                        Poll::Ready(Some(output)) => {
                            return prepare_output(
                                env,
                                array_addrs,
                                schema_addrs,
                                output?,
                                exec_context.debug_native,
                            );
                        }
                        Poll::Ready(None) => {
                            log_plan_metrics(exec_context, stage_id, partition);
                            return Ok(-1);
                        }
                        Poll::Pending => {
                            // JNI call to pull batches from JVM into ScanExec operators.
                            // block_in_place lets tokio move other tasks off this worker
                            // while we wait for JVM data.
                            tokio::task::block_in_place(|| pull_input_batches(exec_context))?;
                        }
                    }
                }
            })
        });

        if exec_context.tracing_enabled {
            #[cfg(feature = "jemalloc")]
            log_jemalloc_usage();
            log_memory_usage(
                &exec_context.tracing_memory_metric_name,
                total_reserved_for_thread(exec_context.rust_thread_id) as u64,
            );
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
        let execution_context = get_execution_context(exec_context);

        // Update metrics
        update_metrics(env, execution_context)?;

        handle_task_shared_pool_release(
            execution_context.memory_pool_config.pool_type,
            execution_context.task_attempt_id,
        );

        // Unregister this context's pool and emit the remaining total for the thread
        if execution_context.tracing_enabled {
            let remaining =
                unregister_and_total(execution_context.rust_thread_id, execution_context.id);
            log_memory_usage(
                &execution_context.tracing_memory_metric_name,
                remaining as u64,
            );
        }

        let _: Box<ExecutionContext> = Box::from_raw(execution_context);
        Ok(())
    })
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
            let raw_pointer = env.get_direct_buffer_address(&byte_buffer)?;
            let length = length as usize;
            let slice: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };
            let batch = read_ipc_compressed(slice)?;
            prepare_output(env, array_addrs, schema_addrs, batch, false)
        })
    })
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

            arrays.push(arrow::array::make_array(array_data));
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
