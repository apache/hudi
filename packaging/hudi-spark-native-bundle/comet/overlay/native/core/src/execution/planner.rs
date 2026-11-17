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

//! Converts Spark physical plan to DataFusion physical plan

mod aggregate;
mod explode;
pub mod expression_registry;
mod join;
pub mod macros;
mod merge_rows;
pub mod operator_registry;
mod shuffle;
mod window;
mod write;

// Glue that wires the optional Delta integration into core's plan-tree builder.
// Compiled only under `--features contrib-delta`; default builds carry zero Delta
// surface. The bulk of the Delta logic lives in the `comet-contrib-delta` crate --
// this module is just the bridge that reaches into core's `pub(crate)` planner
// helpers (`create_expr`, `init_datasource_exec`, `prepare_object_store_with_configs`)
// and calls into that crate.
#[cfg(feature = "contrib-delta")]
mod delta_scan;
// JVM-planned Delta sibling of the kernel handler above; see delta_spark_scan.rs.
#[cfg(feature = "delta")]
mod delta_spark_scan;
#[cfg(feature = "contrib-lance")]
mod lance_scan;

use crate::execution::operators::init_csv_datasource_exec;
use crate::execution::operators::{HudiFileSliceSpec, HudiScanExec};
use crate::execution::operators::IcebergScanExec;
use crate::execution::operators::SortedInputExec;
use crate::execution::operators::TopKReaderFilterExec;
#[cfg(feature = "python-udf")]
use crate::execution::operators::{ArrowPythonUdfExec, ArrowPythonUdfSpec};
use crate::execution::{
    operators::{ExecutionError, ScanExec, ShuffleScanExec},
    planner::expression_registry::ExpressionRegistry,
    planner::operator_registry::OperatorRegistry,
    serde::{to_arrow_datatype, to_arrow_field},
};
use crate::jvm_bridge::{jni_call, JVMClasses, OutputStreamSink, ShufflePartitionPusher};
use arrow::compute::CastOptions;
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit, DECIMAL128_MAX_PRECISION,
};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef},
    config::ConfigOptions,
    execution::FunctionRegistry,
    logical_expr::Operator as DataFusionOperator,
    physical_expr::{
        expressions::{BinaryExpr, CastExpr, Column, IsNullExpr, Literal as DataFusionLiteral},
        PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
    },
    physical_plan::{
        empty::EmptyExec, limit::LocalLimitExec, sorts::sort::SortExec, ExecutionPlan,
    },
    prelude::SessionContext,
};
use datafusion_comet_operators::{range_exec, range_sort_expr, ExpandExec, SampleExec};
use datafusion_comet_spark_expr::{
    create_comet_physical_fun, create_comet_physical_fun_with_eval_mode, BinaryOutputStyle,
    BloomFilterMightContain, CheckedBinaryExpr, CsvWriteOptions, EvalMode, SparkArraysZipFunc,
    Subquery, ToCsv,
};
use datafusion_datasource::TableSchema;
use iceberg::expr::Bind;

use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::spark_plan::SparkPlan;
use crate::parquet::objectstore::s3_blob_fs_support::normalize_object_store_url;
use crate::parquet::parquet_support::{
    object_store_url_key, prepare_object_store_with_configs, ObjectStoreBackend,
};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{ReturnFieldArgs, ScalarUDF, TypeSignature};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::LexOrdering;

use crate::parquet::parquet_exec::init_datasource_exec;
use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalYearMonthArray, ListArray,
    NullArray, StringBuilder, TimestampMicrosecondArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion_comet_proto::spark_expression::ListLiteral;
use datafusion_comet_proto::spark_operator::SparkFilePartition;
use datafusion_comet_proto::{
    spark_expression::{self, expr::ExprStruct, literal::Value, Expr, ScalarFunc},
    spark_operator::{self, operator::OpStruct, Operator},
};
use datafusion_comet_spark_expr::{
    coerce_to_common_type, create_case_when, create_if_expr, decimal128_add_sub_width,
    jvm_udf::JvmScalarUdfExpr, spark_in_list, ArrayInsert, AtLeastNNonNulls, Cast, CheckOverflow,
    CreateNamedStruct, DecimalRescaleCheckOverflow, FloatOperands, GetArrayStructFields,
    GetStructField, IfExpr, ListExtract, NormalizeNaNAndZero, NormalizeNestedFloats,
    NullShortCircuit, SparkCastOptions, ToJson, UnboundColumn, WideDecimalBinaryExpr,
    WideDecimalOp,
};
use itertools::Itertools;
use jni::objects::{Global, JObject};
use log::warn;
use num::{BigInt, ToPrimitive};
use object_store::path::Path;
use std::{collections::HashMap, sync::Arc};

pub type PlanCreationResult =
    Result<(Vec<ScanExec>, Vec<ShuffleScanExec>, Arc<SparkPlan>), ExecutionError>;

/// Return a copy of a `Map` type with only the outer entries `value` field marked nullable, keeping
/// the key field non-nullable and every nested key/value type byte-for-byte unchanged. Any non-`Map`
/// type is returned unchanged.
///
/// This is the shallow counterpart of `make_all_fields_nullable` (in `planner/aggregate.rs`), used
/// for `map_entries`. `map_entries` reuses the input map's entries array as its output list values
/// but declares that element's `value` field nullable (`Struct(key non-null, value nullable)`), deriving both nested
/// types verbatim from the input. Arrow's `GenericListArray::try_new` compares the declared element
/// field's type against the reused values array's type in full, so the ONLY field that can mismatch
/// is the entries `value` field's own `nullable` flag; widening just that field is sufficient.
/// Recursing into nested types (as `make_all_fields_nullable` does) would additionally flip, say, a
/// nested `Map`'s `valueContainsNull`, which then diverges from the Spark-serialized `return_type`
/// and makes a downstream `make_array` see unequal map types and panic.
fn widen_map_entry_value_nullable(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 && !kv[1].is_nullable() => {
                let new_value = Arc::new(kv[1].as_ref().clone().with_nullable(true));
                let new_kv: Fields = vec![Arc::clone(&kv[0]), new_value].into();
                let new_entries = Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(new_kv)),
                );
                DataType::Map(new_entries, *sorted)
            }
            _ => data_type.clone(),
        },
        other => other.clone(),
    }
}

#[derive(Default)]
pub struct BinaryExprOptions {
    pub is_integral_div: bool,
    /// See `MathExpr.check_divide_overflow` in expr.proto
    pub check_divide_overflow: bool,
}

pub const TEST_EXEC_CONTEXT_ID: i64 = -1;

/// The query planner for converting Spark query plans to DataFusion query plans.
#[derive(Clone)]
pub struct PhysicalPlanner {
    // The execution context id of this planner.
    exec_context_id: i64,
    partition: i32,
    session_ctx: Arc<SessionContext>,
    query_context_registry: Arc<datafusion_comet_spark_expr::QueryContextMap>,
    /// SQL texts referenced by `QueryContext.sql_text_idx`, taken from the root operator's
    /// `sql_text_pool`. Held as `Arc`s so that every context in the plan shares one allocation
    /// rather than each cloning the full query text out of the proto. Empty when the plan was
    /// serialized without interning, in which case contexts carry `sql_text` inline.
    sql_text_pool: Vec<Arc<String>>,
    /// Captured at `createPlan` time on `ExecutionContext`; see that struct for the
    /// propagation rationale. `None` when no driving Spark task is available.
    task_context: Option<Arc<Global<JObject<'static>>>>,
    /// Context `ClassLoader` of the driving Spark task thread, captured at `createPlan` time on
    /// `ExecutionContext`; see that struct for the propagation rationale. `None` when no driving
    /// Spark task is available.
    class_loader: Option<Arc<Global<JObject<'static>>>>,
    /// Task-owned destination for remote shuffle blocks, registered on the driving Spark task
    /// thread before native planning. Only explicit RSS destinations may use it.
    shuffle_partition_pusher: Option<Arc<dyn ShufflePartitionPusher>>,
    /// The output stream of the file the plan's Parquet writer writes, when the JVM created that
    /// file, registered on the driving Spark task thread before native planning. Only a writer
    /// planned to write through the JVM may use it.
    output_stream: Option<Arc<dyn OutputStreamSink>>,
    /// How comparisons treat floating-point operands. `Raw` only while planning a scan's data
    /// filters; see [`Self::create_data_filter`].
    float_operands: FloatOperands,
    /// Whether a boundary that would cast a timestamp's timezone label to the declared one fails
    /// instead. Test-only; see `spark.comet.testing.strictTimestampLabels.enabled`.
    strict_timestamp_labels: bool,
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new(Arc::new(SessionContext::new()), 0)
    }
}

impl PhysicalPlanner {
    pub fn new(session_ctx: Arc<SessionContext>, partition: i32) -> Self {
        Self {
            exec_context_id: TEST_EXEC_CONTEXT_ID,
            session_ctx,
            partition,
            query_context_registry: datafusion_comet_spark_expr::create_query_context_map(),
            sql_text_pool: vec![],
            task_context: None,
            class_loader: None,
            shuffle_partition_pusher: None,
            output_stream: None,
            float_operands: FloatOperands::Normalize,
            strict_timestamp_labels: false,
        }
    }

    /// Load the SQL text pool from the root operator of the plan about to be planned. Must be
    /// called with the *root* operator: the JVM only populates the pool there, and
    /// `QueryContext.sql_text_idx` values are indices into it.
    pub fn with_sql_text_pool(mut self, root: &Operator) -> Self {
        self.sql_text_pool = root
            .sql_text_pool
            .iter()
            .map(|text| Arc::new(text.clone()))
            .collect();
        self
    }

    /// Register the `QueryContext` carried by a serialized expression, if any, so that native ANSI
    /// errors raised by it can render Spark's `== SQL ... ==` block. Shared by `create_expr` and
    /// `create_agg_expr`.
    fn register_query_context(
        &self,
        expr_id: Option<u64>,
        ctx_proto: Option<&spark_expression::QueryContext>,
    ) {
        if let (Some(expr_id), Some(ctx_proto)) = (expr_id, ctx_proto) {
            self.query_context_registry
                .register(expr_id, self.build_query_context(ctx_proto));
        }
    }

    /// Resolve a serialized `QueryContext` into a `QueryContext`, sharing the pooled SQL text
    /// when the context refers to the pool by index. Falls back to the inline `sql_text` for
    /// plans serialized without interning -- the paths that serialize an operator directly rather
    /// than through `convertBlock` (e.g. `CometNativeWriteExec`, the native shuffle writer) and
    /// the bare-`Expr` Parquet filter path, which has no root operator to hang a pool on.
    fn build_query_context(
        &self,
        ctx_proto: &spark_expression::QueryContext,
    ) -> datafusion_comet_spark_expr::QueryContext {
        let pooled = ctx_proto
            .sql_text_idx
            .map(|idx| match usize::try_from(idx).ok() {
                Some(idx) => self.sql_text_pool.get(idx).map(Arc::clone),
                None => None,
            });

        let sql_text: Arc<String> = match pooled {
            Some(Some(text)) => text,
            // An index we cannot resolve means the plan was interned against a pool this planner
            // never saw, so `sql_text` is empty and the error block would silently come out blank.
            // Warn rather than fail: a degraded error message is better than a failed query.
            Some(None) => {
                warn!(
                    "QueryContext sql_text_idx {:?} is out of range for a SQL text pool of {} \
                     entries; SQL context will be missing from any error raised by this expression",
                    ctx_proto.sql_text_idx,
                    self.sql_text_pool.len()
                );
                Arc::new(ctx_proto.sql_text.clone())
            }
            None => Arc::new(ctx_proto.sql_text.clone()),
        };

        datafusion_comet_spark_expr::QueryContext::new(
            sql_text,
            ctx_proto.start_index,
            ctx_proto.stop_index,
            ctx_proto.object_type.clone(),
            ctx_proto.object_name.clone(),
            ctx_proto.line,
            ctx_proto.start_position,
        )
    }

    pub fn with_exec_id(mut self, exec_context_id: i64) -> Self {
        self.exec_context_id = exec_context_id;
        self
    }

    pub fn with_strict_timestamp_labels(mut self, strict: bool) -> Self {
        self.strict_timestamp_labels = strict;
        self
    }

    pub(crate) fn strict_timestamp_labels(&self) -> bool {
        self.strict_timestamp_labels
    }

    /// Attach a propagated Spark `TaskContext` global reference. Called by the JNI `executePlan`
    /// entry with whatever was captured at `createPlan` time. The planner clones this `Option`
    /// into every `JvmScalarUdfExpr` it builds.
    pub fn with_task_context(
        mut self,
        task_context: Option<Arc<Global<JObject<'static>>>>,
    ) -> Self {
        self.task_context = task_context;
        self
    }

    /// Attach the driving Spark task thread's context `ClassLoader` as a global reference. Mirrors
    /// `with_task_context`: called by the JNI `executePlan` entry with whatever was captured at
    /// `createPlan` time, and cloned into every `JvmScalarUdfExpr` the planner builds.
    pub fn with_class_loader(
        mut self,
        class_loader: Option<Arc<Global<JObject<'static>>>>,
    ) -> Self {
        self.class_loader = class_loader;
        self
    }

    /// Attach the remote-shuffle callback owned by the Spark task that created this plan.
    ///
    /// The callback is retained by any RSS shuffle writer in the physical plan and must not be
    /// supplied to a legacy or explicitly local shuffle writer.
    pub fn with_shuffle_partition_pusher(
        mut self,
        shuffle_partition_pusher: Option<Arc<dyn ShufflePartitionPusher>>,
    ) -> Self {
        self.shuffle_partition_pusher = shuffle_partition_pusher;
        self
    }

    /// Attach the output stream of the file the plan's Parquet writer writes, for a file the
    /// Spark task that created this plan created through the JVM.
    ///
    /// Only a writer planned to write through the JVM takes it, and such a writer requires it.
    pub fn with_output_stream(mut self, output_stream: Option<Arc<dyn OutputStreamSink>>) -> Self {
        self.output_stream = output_stream;
        self
    }

    /// How comparisons treat floating-point operands.
    pub fn float_operands(&self) -> FloatOperands {
        self.float_operands
    }

    /// Return session context of this planner.
    pub fn session_ctx(&self) -> &Arc<SessionContext> {
        &self.session_ctx
    }

    /// Return partition id of this planner.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Build the native parquet `DataSourceExec` shared by the parquet-backed scan arms
    /// (NativeScan and, behind the `delta` feature, DeltaScan): schema conversion, data-filter
    /// binding, object-store setup, file-group construction, and `init_datasource_exec`.
    /// Arm-specific concerns (file-list decoding, deletion-vector handling) stay in the arms.
    /// `rebase_from_file_metadata` opts the scan into per-file datetime calendar-rebase
    /// resolution (see `datetime_rebase.rs`): the Delta arm passes true, while NativeScan
    /// passes false to keep its documented no-rebase behavior (#5010).
    /// `datetime_rebase_mode_in_read` / `int96_rebase_mode_in_read` carry the session's
    /// effective read modes for files whose footer metadata does not decide the policy;
    /// they are only consulted when `rebase_from_file_metadata` is true (empty means
    /// EXCEPTION, the conservative refuse-ancient posture).
    #[allow(clippy::too_many_arguments)]
    fn build_parquet_scan_plan(
        &self,
        plan_id: u32,
        common: &spark_operator::NativeScanCommon,
        object_store_url: ObjectStoreUrl,
        object_store_backend: ObjectStoreBackend,
        files: Vec<PartitionedFile>,
        rebase_from_file_metadata: bool,
        datetime_rebase_mode_in_read: &str,
        int96_rebase_mode_in_read: &str,
    ) -> Result<Arc<SparkPlan>, ExecutionError> {
        let data_schema = convert_spark_types_to_arrow_schema(common.data_schema.as_slice());
        let required_schema: SchemaRef =
            convert_spark_types_to_arrow_schema(common.required_schema.as_slice());
        let partition_schema: SchemaRef =
            convert_spark_types_to_arrow_schema(common.partition_schema.as_slice());
        let projection_vector: Vec<usize> = common
            .projection_vector
            .iter()
            .map(|offset| *offset as usize)
            .collect();

        // Check if this partition has any files (bucketed scan with bucket pruning may have
        // empty partitions; a fully-pruned Delta partition likewise).
        if files.is_empty() {
            // Match the nonempty scan's projected table schema, including partition
            // and constant metadata columns. Fused operators bind against this schema
            // even when the bucket has no rows.
            let partition_fields: Vec<FieldRef> = partition_schema
                .fields()
                .iter()
                .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), f.is_nullable())))
                .collect();
            let table_schema = TableSchema::builder(data_schema)
                .with_table_partition_cols(partition_fields)
                .build();
            let output_schema = Arc::new(table_schema.table_schema().project(&projection_vector)?);
            let empty_exec = Arc::new(EmptyExec::new(output_schema));
            return Ok(Arc::new(SparkPlan::new(plan_id, empty_exec, vec![])));
        }

        // data_filters may reference partition columns and constant metadata columns
        // (e.g. `_metadata.file_size`), which the Parquet reader appends after
        // required_schema's columns once partition_values are projected into the
        // batch. Bind against the combined schema so `Bound` indices resolve
        // correctly -- Scala's `exprToProto(filter, scan.output)`
        // (CometNativeScan.scala) numbers columns against that same ordering.
        let data_filters: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> =
            if common.data_filters.is_empty() {
                Ok(vec![])
            } else {
                let filter_schema: SchemaRef = Arc::new(Schema::new(
                    required_schema
                        .fields()
                        .iter()
                        .chain(partition_schema.fields().iter())
                        .cloned()
                        .collect::<Vec<FieldRef>>(),
                ));
                let float_operands = self.data_filter_float_operands();
                common
                    .data_filters
                    .iter()
                    .map(|expr| {
                        self.create_data_filter(expr, Arc::clone(&filter_schema), float_operands)
                    })
                    .collect()
            };

        let default_values = self.parse_default_values(common, &required_schema)?;

        let file_groups: Vec<Vec<PartitionedFile>> = vec![files];

        let scan = init_datasource_exec(
            required_schema,
            Some(data_schema),
            Some(partition_schema),
            object_store_url,
            object_store_backend,
            file_groups,
            Some(projection_vector),
            if common.has_data_filters || !common.data_filters.is_empty() {
                Some(data_filters?)
            } else {
                None
            },
            default_values,
            common.session_timezone.as_str(),
            common.case_sensitive,
            common.return_null_struct_if_all_fields_missing,
            common.allow_type_promotion,
            common.allow_timestamp_ltz_to_ntz,
            self.session_ctx(),
            common.encryption_enabled,
            common.use_field_id,
            common.require_field_ids,
            rebase_from_file_metadata,
            datetime_rebase_mode_in_read,
            int96_rebase_mode_in_read,
        )?;
        Ok(Arc::new(SparkPlan::new(plan_id, scan, vec![])))
    }

    /// Register the scan's object store and convert its proto file list into DataFusion
    /// [`PartitionedFile`]. Shared by the NativeScan and DeltaScan arms; empty partitions
    /// yield an empty file list (handled by `build_parquet_scan_plan`).
    fn prepare_scan_store_and_files(
        &self,
        common: &spark_operator::NativeScanCommon,
        partition_files: &SparkFilePartition,
    ) -> Result<(ObjectStoreUrl, ObjectStoreBackend, Vec<PartitionedFile>), ExecutionError> {
        let one_file = match partition_files.partitioned_file.first() {
            Some(f) => f.file_path.clone(),
            None => {
                // Empty partition: no store to resolve; the URL and backend are unused because
                // the file group is empty and build_parquet_scan_plan returns EmptyExec.
                return Ok((
                    ObjectStoreUrl::local_filesystem(),
                    ObjectStoreBackend::Local,
                    vec![],
                ));
            }
        };
        let object_store_options: HashMap<String, String> = common
            .object_store_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let (object_store_url, _, object_store_backend) = prepare_object_store_with_configs(
            self.session_ctx.runtime_env(),
            one_file,
            &object_store_options,
        )?;
        let files = self.get_partitioned_files(partition_files, &object_store_options)?;
        Ok((object_store_url, object_store_backend, files))
    }

    /// Parse a scan's serialized default values (for columns missing in older files) into the
    /// map consumed by the SchemaMapper. Shared by the NativeScan and DeltaScan arms.
    fn parse_default_values(
        &self,
        common: &spark_operator::NativeScanCommon,
        required_schema: &SchemaRef,
    ) -> Result<Option<HashMap<Column, ScalarValue>>, ExecutionError> {
        if common.default_values.len() != common.default_values_indexes.len() {
            return Err(GeneralError(
                "Scan default values and indexes have different lengths".to_string(),
            ));
        }
        if common.default_values.is_empty() {
            return Ok(None);
        }
        common
            .default_values
            .iter()
            .zip(&common.default_values_indexes)
            .map(|(expr, offset)| {
                let idx = usize::try_from(*offset)
                    .map_err(|_| GeneralError(format!("Invalid scan default index {offset}")))?;
                let field = required_schema.fields().get(idx).ok_or_else(|| {
                    GeneralError(format!("Scan default index {idx} is outside schema"))
                })?;
                let value = self.create_default_value(expr, Arc::clone(required_schema))?;
                Ok((Column::new(field.name(), idx), value))
            })
            .collect::<Result<HashMap<_, _>, ExecutionError>>()
            .map(Some)
    }

    /// get DataFusion PartitionedFiles from a Spark FilePartition. The scan reads a partition
    /// through a single object store, so every file must resolve to the same one.
    fn get_partitioned_files(
        &self,
        partition: &SparkFilePartition,
        object_store_options: &HashMap<String, String>,
    ) -> Result<Vec<PartitionedFile>, ExecutionError> {
        let mut files = Vec::with_capacity(partition.partitioned_file.len());
        // Create an empty input schema for partition values because they are all literals.
        let empty_schema = Arc::new(Schema::empty());
        let mut partition_store: Option<(String, bool)> = None;
        partition.partitioned_file.iter().try_for_each(|file| {
            assert!(file.start + file.length <= file.file_size);

            let mut partitioned_file = PartitionedFile::new_with_range(
                String::new(), // Dummy file path.
                file.file_size as u64,
                file.start,
                file.start + file.length,
            );

            // Apply the same alias/s3a scheme rewrite the object store construction uses, so the
            // object-store key we hand DataFusion is stripped of the bucket prefix. Skipping this
            // would leave `bucket/key` as the object key, and path-style S3 GETs would double the
            // bucket (`<endpoint>/bucket/bucket/key`).
            let normalized = normalize_object_store_url(&file.file_path, object_store_options)?;
            let store = object_store_url_key(&normalized);
            match &partition_store {
                None => partition_store = Some(store),
                Some(first) if *first != store => {
                    let describe = |(key, is_hdfs): &(String, bool)| {
                        if *is_hdfs {
                            format!("{key} (libhdfs)")
                        } else {
                            key.clone()
                        }
                    };
                    return Err(GeneralError(format!(
                        "A native scan partition holds files from two object stores ({} and {}); \
                         each partition is read through one store",
                        describe(first),
                        describe(&store)
                    )));
                }
                Some(_) => {}
            }
            let url = normalized.url;
            let path = Path::from_url_path(url.path()).map_err(|e| GeneralError(e.to_string()))?;
            partitioned_file.object_meta.location = path;

            // Process partition values
            let partition_values: Result<Vec<_>, _> = file
                .partition_values
                .iter()
                .map(|partition_value| {
                    let literal =
                        self.create_expr(partition_value, Arc::<Schema>::clone(&empty_schema))?;
                    literal
                        .downcast_ref::<DataFusionLiteral>()
                        .ok_or_else(|| {
                            GeneralError("Expected literal of partition value".to_string())
                        })
                        .map(|literal| literal.value().clone())
                })
                .collect();
            let partition_values = partition_values?;

            partitioned_file.partition_values = partition_values;

            files.push(partitioned_file);
            Ok::<(), ExecutionError>(())
        })?;

        Ok(files)
    }

    /// Create a DataFusion physical expression from Spark physical expression
    pub(crate) fn create_expr(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        self.register_query_context(spark_expr.expr_id, spark_expr.query_context.as_ref());

        // Try to use the modular registry first - this automatically handles any registered expression types
        if ExpressionRegistry::global().can_handle(spark_expr) {
            return ExpressionRegistry::global().create_expr(spark_expr, input_schema, self);
        }

        // Fall back to the original monolithic match for other expressions
        match spark_expr.expr_struct.as_ref().unwrap() {
            ExprStruct::Bound(bound) => {
                let idx = bound.index as usize;
                if idx >= input_schema.fields().len() {
                    return Err(GeneralError(format!(
                        "Column index {idx} is out of bound. Schema: {input_schema}"
                    )));
                }
                let field = input_schema.field(idx);
                Ok(Arc::new(Column::new(field.name().as_str(), idx)))
            }
            ExprStruct::Unbound(unbound) => {
                let data_type = to_arrow_datatype(unbound.datatype.as_ref().unwrap());
                Ok(Arc::new(UnboundColumn::new(
                    unbound.name.as_str(),
                    data_type,
                )))
            }
            ExprStruct::Literal(literal) => {
                let data_type = to_arrow_datatype(literal.datatype.as_ref().unwrap());
                let scalar_value = if literal.is_null {
                    match data_type {
                        DataType::Boolean => ScalarValue::Boolean(None),
                        DataType::Int8 => ScalarValue::Int8(None),
                        DataType::Int16 => ScalarValue::Int16(None),
                        DataType::Int32 => ScalarValue::Int32(None),
                        DataType::Int64 => ScalarValue::Int64(None),
                        DataType::Float32 => ScalarValue::Float32(None),
                        DataType::Float64 => ScalarValue::Float64(None),
                        DataType::Utf8 => ScalarValue::Utf8(None),
                        DataType::Date32 => ScalarValue::Date32(None),
                        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                            ScalarValue::TimestampMicrosecond(None, timezone)
                        }
                        DataType::Binary => ScalarValue::Binary(None),
                        DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, p, s),
                        DataType::Struct(fields) => ScalarStructBuilder::new_null(fields),
                        DataType::Map(f, s) => DataType::Map(f, s).try_into()?,
                        DataType::List(f) => DataType::List(f).try_into()?,
                        DataType::Null => ScalarValue::Null,
                        DataType::Time64(TimeUnit::Nanosecond) => {
                            ScalarValue::Time64Nanosecond(None)
                        }
                        DataType::Interval(IntervalUnit::YearMonth) => {
                            ScalarValue::IntervalYearMonth(None)
                        }
                        DataType::Duration(TimeUnit::Microsecond) => {
                            ScalarValue::DurationMicrosecond(None)
                        }
                        DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
                            ScalarValue::IntervalMonthDayNano(None)
                        }
                        dt => {
                            return Err(GeneralError(format!("{dt:?} is not supported in Comet")))
                        }
                    }
                } else {
                    match literal.value.as_ref().unwrap() {
                        Value::BoolVal(value) => ScalarValue::Boolean(Some(*value)),
                        Value::ByteVal(value) => ScalarValue::Int8(Some(*value as i8)),
                        Value::ShortVal(value) => ScalarValue::Int16(Some(*value as i16)),
                        Value::IntVal(value) => match data_type {
                            DataType::Int32 => ScalarValue::Int32(Some(*value)),
                            DataType::Date32 => ScalarValue::Date32(Some(*value)),
                            DataType::Interval(IntervalUnit::YearMonth) => {
                                ScalarValue::IntervalYearMonth(Some(*value))
                            }
                            dt => {
                                return Err(GeneralError(format!(
                                    "Expected either 'Int32', 'Date32', or 'Interval(YearMonth)' for IntVal, but found {dt:?}"
                                )))
                            }
                        },
                        Value::LongVal(value) => match data_type {
                            DataType::Int64 => ScalarValue::Int64(Some(*value)),
                            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                                ScalarValue::TimestampMicrosecond(Some(*value), None)
                            }
                            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                                ScalarValue::TimestampMicrosecond(Some(*value), Some(tz))
                            }
                            DataType::Duration(TimeUnit::Microsecond) => {
                                ScalarValue::DurationMicrosecond(Some(*value))
                            }
                            DataType::Time64(TimeUnit::Nanosecond) => {
                                ScalarValue::Time64Nanosecond(Some(*value))
                            }
                            dt => {
                                return Err(GeneralError(format!(
                                    "Expected 'Int64', 'Timestamp', 'Duration(Microsecond)', or 'Time64(Nanosecond)' for LongVal, but found {dt:?}"
                                )))
                            }
                        },
                        Value::FloatVal(value) => ScalarValue::Float32(Some(*value)),
                        Value::DoubleVal(value) => ScalarValue::Float64(Some(*value)),
                        Value::StringVal(value) => ScalarValue::Utf8(Some(value.clone())),
                        Value::BytesVal(value) => ScalarValue::Binary(Some(value.clone())),
                        Value::DecimalVal(value) => {
                            let big_integer = BigInt::from_signed_bytes_be(value);
                            let integer = big_integer.to_i128().ok_or_else(|| {
                                GeneralError(format!(
                                    "Cannot parse {big_integer:?} as i128 for Decimal literal"
                                ))
                            })?;

                            match data_type {
                                DataType::Decimal128(p, s) => {
                                    ScalarValue::Decimal128(Some(integer), p, s)
                                }
                                dt => {
                                    return Err(GeneralError(format!(
                                        "Decimal literal's data type should be Decimal128 but got {dt:?}"
                                    )))
                                }
                            }
                        }
                        Value::ListVal(values) => {
                            if let DataType::List(_) = data_type {
                                SingleRowListArrayBuilder::new(literal_to_array_ref(data_type, values.clone())?).build_list_scalar()
                            } else {
                                return Err(GeneralError(format!(
                                    "Expected DataType::List but got {data_type:?}"
                                )));
                            }
                        }
                    }
                };
                Ok(Arc::new(DataFusionLiteral::new(scalar_value)))
            }
            ExprStruct::Cast(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;

                // Look up query context from registry if expr_id is present
                let query_context = spark_expr.expr_id.and_then(|expr_id| {
                    let registry = &self.query_context_registry;
                    registry.get(expr_id)
                });

                let mut cast_options = SparkCastOptions::new_with_version(
                    eval_mode,
                    &expr.timezone,
                    expr.is_spark4_plus,
                );
                cast_options.decimal_zero_any_exponent = expr.decimal_zero_any_exponent;
                Ok(Arc::new(Cast::new(
                    child,
                    datatype,
                    cast_options,
                    spark_expr.expr_id,
                    query_context,
                )))
            }
            ExprStruct::CheckOverflow(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let fail_on_error = expr.fail_on_error;
                let query_context = spark_expr.expr_id.and_then(|expr_id| {
                    let registry = &self.query_context_registry;
                    registry.get(expr_id)
                });

                // WideDecimalBinaryExpr already handles overflow — skip redundant check
                // but only if its output type matches CheckOverflow's declared type. A binary
                // expression with query context is wrapped in CheckedBinaryExpr, so look
                // through any such wrappers.
                let is_wide_decimal = unwrap_checked(&child)
                    .downcast_ref::<WideDecimalBinaryExpr>()
                    .is_some();
                if is_wide_decimal {
                    let child_type = child.data_type(&input_schema)?;
                    if child_type == data_type {
                        return if query_context.is_some()
                            && child.downcast_ref::<CheckedBinaryExpr>().is_none()
                        {
                            Ok(Arc::new(CheckedBinaryExpr::new(child, query_context)))
                        } else {
                            Ok(child)
                        };
                    }
                }

                // Fuse Cast(Decimal128→Decimal128) + CheckOverflow into single rescale+check
                // Only fuse when the Cast target type matches the CheckOverflow output type.
                // Spark 3.4+ does not currently emit this shape, but keep its errors typed and
                // contextualized in case a future serializer makes the fusion reachable.
                if let Some(cast) = child.downcast_ref::<Cast>() {
                    if let (
                        DataType::Decimal128(p_out, s_out),
                        Ok(DataType::Decimal128(_p_in, s_in)),
                    ) = (&data_type, cast.child.data_type(&input_schema))
                    {
                        let cast_target = cast.data_type(&input_schema)?;
                        if cast_target == data_type {
                            let fused: Arc<dyn PhysicalExpr> =
                                Arc::new(DecimalRescaleCheckOverflow::new(
                                    Arc::clone(&cast.child),
                                    s_in,
                                    *p_out,
                                    *s_out,
                                    fail_on_error,
                                ));
                            return if query_context.is_some() {
                                Ok(Arc::new(CheckedBinaryExpr::new(fused, query_context)))
                            } else {
                                Ok(fused)
                            };
                        }
                    }
                }

                // Generated child protos may not carry an expression id of their own, so retain
                // the outer CheckOverflow context as a fallback for bare child SparkErrors.
                let child = if query_context.is_some()
                    && child.downcast_ref::<CheckedBinaryExpr>().is_none()
                {
                    Arc::new(CheckedBinaryExpr::new(child, query_context.clone()))
                        as Arc<dyn PhysicalExpr>
                } else {
                    child
                };
                Ok(Arc::new(CheckOverflow::new(
                    child,
                    data_type,
                    fail_on_error,
                    spark_expr.expr_id,
                    query_context,
                )))
            }
            ExprStruct::ScalarFunc(expr) => {
                let func = self.create_scalar_function_expr(expr, input_schema);
                match expr.func.as_ref() {
                    // DataFusion 49 hardcodes return type for MD5 built in function as UTF8View
                    // which is not yet supported in Comet
                    // Converting forcibly to UTF8. To be removed after UTF8View supported
                    "md5" => Ok(Arc::new(Cast::new(
                        func?,
                        DataType::Utf8,
                        SparkCastOptions::new_without_timezone(EvalMode::Try),
                        None,
                        None,
                    ))),
                    _ => func,
                }
            }
            ExprStruct::CaseWhen(case_when) => {
                let when_then_pairs = case_when
                    .when
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .zip(
                        case_when
                            .then
                            .iter()
                            .map(|then| self.create_expr(then, Arc::clone(&input_schema))),
                    )
                    .try_fold(Vec::new(), |mut acc, (a, b)| {
                        acc.push((a?, b?));
                        Ok::<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>, ExecutionError>(
                            acc,
                        )
                    })?;

                let else_phy_expr = match &case_when.else_expr {
                    None => None,
                    Some(_) => Some(self.create_expr(
                        case_when.else_expr.as_ref().unwrap(),
                        Arc::clone(&input_schema),
                    )?),
                };

                create_case_when(when_then_pairs, else_phy_expr, &input_schema)
                    .map_err(|e| e.into())
            }
            ExprStruct::In(expr) => {
                let value =
                    self.create_expr(expr.in_value.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let list = expr
                    .lists
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                spark_in_list(value, list, expr.negated, input_schema.as_ref())
                    .map_err(|e| e.into())
            }
            ExprStruct::If(expr) => {
                let if_expr =
                    self.create_expr(expr.if_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let true_expr =
                    self.create_expr(expr.true_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let false_expr =
                    self.create_expr(expr.false_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                create_if_expr(if_expr, true_expr, false_expr, &input_schema).map_err(|e| e.into())
            }
            ExprStruct::NormalizeNanAndZero(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(NormalizeNaNAndZero::new(data_type, child)))
            }
            ExprStruct::PreciseTimestampConversion(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                // Spark's PreciseTimestampConversion reinterprets the value between
                // Timestamp (microseconds) and Long without any conversion. Arrow's cast kernel
                // performs exactly this zero-cost reinterpret between Timestamp(µs) and Int64,
                // so we use DataFusion's CastExpr (arrow semantics) rather than Comet's
                // Spark-compatible Cast (which would scale by 1_000_000).
                Ok(Arc::new(CastExpr::new(child, data_type, None)))
            }
            ExprStruct::Subquery(expr) => {
                let id = expr.id;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(Subquery::new(self.exec_context_id, id, data_type)))
            }
            ExprStruct::BloomFilterMightContain(expr) => {
                let bloom_filter_expr = self.create_expr(
                    expr.bloom_filter.as_ref().unwrap(),
                    Arc::clone(&input_schema),
                )?;

                // We only provide the values as argument, the bloom filter is created only in plan time.
                let value_expr = self.create_expr(expr.value.as_ref().unwrap(), input_schema)?;
                let args = vec![value_expr];
                let udf =
                    ScalarUDF::new_from_impl(BloomFilterMightContain::try_new(bloom_filter_expr)?);

                let field_ref = Arc::new(Field::new("might_contain", DataType::Boolean, true));
                let expr: ScalarFunctionExpr = ScalarFunctionExpr::new(
                    "might_contain",
                    Arc::new(udf),
                    args,
                    field_ref,
                    Arc::new(ConfigOptions::default()),
                );
                Ok(Arc::new(expr))
            }
            ExprStruct::CreateNamedStruct(expr) => {
                let values = expr
                    .values
                    .iter()
                    .map(|expr| self.create_expr(expr, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;
                let names = expr.names.clone();
                Ok(Arc::new(CreateNamedStruct::new(values, names)))
            }
            ExprStruct::GetStructField(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                Ok(Arc::new(GetStructField::new(child, expr.ordinal as usize)))
            }
            ExprStruct::ToJson(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(ToJson::new(
                    child,
                    &expr.timezone,
                    expr.ignore_null_fields,
                )))
            }
            ExprStruct::ToPrettyString(expr) => {
                let mut spark_cast_options = SparkCastOptions::new(EvalMode::Try, &expr.timezone);
                let null_string = "NULL";
                spark_cast_options.null_string = null_string.to_string();
                spark_cast_options.binary_output_style =
                    from_protobuf_binary_output_style(expr.binary_output_style).ok();
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let cast = Arc::new(Cast::new(
                    Arc::clone(&child),
                    DataType::Utf8,
                    spark_cast_options,
                    None,
                    None,
                ));
                Ok(Arc::new(IfExpr::new(
                    Arc::new(IsNullExpr::new(child)),
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(
                        null_string.to_string(),
                    )))),
                    cast,
                )))
            }
            ExprStruct::ListExtract(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let ordinal =
                    self.create_expr(expr.ordinal.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let default_value = expr
                    .default_value
                    .as_ref()
                    .map(|e| self.create_expr(e, Arc::clone(&input_schema)))
                    .transpose()?;
                let list_extract: Arc<dyn PhysicalExpr> = Arc::new(ListExtract::new(
                    child,
                    ordinal,
                    default_value,
                    expr.one_based,
                    expr.fail_on_error,
                    spark_expr.expr_id,
                    Arc::clone(&self.query_context_registry),
                ));
                if expr.null_short_circuit {
                    Ok(NullShortCircuit::wrap(list_extract, &input_schema)?)
                } else {
                    Ok(list_extract)
                }
            }
            ExprStruct::GetArrayStructFields(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;

                Ok(Arc::new(GetArrayStructFields::new(
                    child,
                    expr.ordinal as usize,
                )))
            }
            ExprStruct::ArrayInsert(expr) => {
                let src_array_expr = self.create_expr(
                    expr.src_array_expr.as_ref().unwrap(),
                    Arc::clone(&input_schema),
                )?;
                let pos_expr =
                    self.create_expr(expr.pos_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let item_expr =
                    self.create_expr(expr.item_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                Ok(Arc::new(ArrayInsert::new(
                    src_array_expr,
                    pos_expr,
                    item_expr,
                    expr.legacy_negative_index,
                )))
            }
            ExprStruct::ToCsv(expr) => {
                let csv_struct_expr =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let options = expr.options.clone().unwrap();
                let csv_write_options = CsvWriteOptions::new(
                    options.delimiter,
                    options.quote,
                    options.escape,
                    options.null_value,
                    options.quote_all,
                    options.ignore_leading_white_space,
                    options.ignore_trailing_white_space,
                );
                Ok(Arc::new(ToCsv::new(
                    csv_struct_expr,
                    &options.timezone,
                    csv_write_options,
                )))
            }
            ExprStruct::AtLeastNNonNulls(expr) => {
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(AtLeastNNonNulls::new(expr.n, children)))
            }
            ExprStruct::ArraysZip(expr) => {
                if expr.values.is_empty() {
                    return Err(GeneralError(
                        "arrays_zip requires at least one argument".to_string(),
                    ));
                }

                let children = expr
                    .values
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(SparkArraysZipFunc::new(
                    children,
                    expr.names.clone(),
                )))
            }
            ExprStruct::JvmScalarUdf(udf) => {
                let args = udf
                    .args
                    .iter()
                    .map(|e| self.create_expr(e, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;
                let return_type =
                    to_arrow_datatype(udf.return_type.as_ref().ok_or_else(|| {
                        GeneralError("JvmScalarUdf missing return_type".to_string())
                    })?);
                // Invariant: task_context and class_loader are propagated for every
                // JvmScalarUdfExpr built during normal execution. The TEST_EXEC_CONTEXT_ID path is
                // the only context in which they may legitimately be None (unit tests, direct
                // native driver runs).
                debug_assert!(
                    (self.task_context.is_some() && self.class_loader.is_some())
                        || self.exec_context_id == TEST_EXEC_CONTEXT_ID,
                    "task_context and class_loader must be set for non-test execution"
                );
                Ok(Arc::new(JvmScalarUdfExpr::new(
                    udf.class_name.clone(),
                    args,
                    return_type,
                    udf.return_nullable,
                    self.task_context.clone(),
                    self.class_loader.clone(),
                    self.partition,
                    self.exec_context_id,
                )))
            }
            ExprStruct::NativeScalarUdf(call) => {
                let arg_exprs: Vec<Arc<dyn PhysicalExpr>> = call
                    .args
                    .iter()
                    .map(|e| self.create_expr(e, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                // The driver checked this when the function was registered, but the path arrives
                // in the plan, so this side applies the policy too.
                let policy = self
                    .session_ctx
                    .state_ref()
                    .read()
                    .config()
                    .get_extension::<crate::execution::c_udf::policy::NativeUdfPolicy>();
                if let Some(policy) = policy {
                    policy
                        .check(&call.library_path)
                        .map_err(|e| GeneralError(e.to_string()))?;
                }

                let lib = crate::execution::c_udf::cache::get_or_load(&call.library_path).map_err(
                    |e| GeneralError(format!("native UDF load '{}': {e}", call.library_path)),
                )?;

                let loaded = lib
                    .udfs
                    .iter()
                    .find(|u| u.name == call.name)
                    .ok_or_else(|| {
                        GeneralError(format!(
                            "native UDF '{}' not found in '{}'",
                            call.name, call.library_path
                        ))
                    })?;

                let udf = Arc::new(ScalarUDF::new_from_shared_impl(Arc::clone(
                    &loaded.udf_impl,
                )));

                let return_type =
                    to_arrow_datatype(call.return_type.as_ref().ok_or_else(|| {
                        GeneralError("NativeScalarUdf missing return_type".into())
                    })?);

                // The declared return type comes from the JVM-side `CometNativeUDF.register` call
                // and is what Spark planned against; the kernel's own `return_field` is what will
                // actually be produced. If they disagree, fail here with both types named rather
                // than letting it surface later as a bare type assertion mid-execution.
                let arg_types = arg_exprs
                    .iter()
                    .map(|e| e.data_type(input_schema.as_ref()))
                    .collect::<Result<Vec<_>, _>>()?;
                let kernel_return_type = loaded.udf_impl.return_type(&arg_types)?;
                if !crate::execution::c_udf::return_types_compatible(
                    &return_type,
                    &kernel_return_type,
                ) {
                    return Err(GeneralError(format!(
                        "native UDF '{}' was registered as returning {return_type} but its \
                         return_field reports {kernel_return_type} for argument types {arg_types:?}. \
                         Make the type passed to CometNativeUDF.register match what the UDF returns. \
                         Note that a timestamp's timezone, a decimal's precision and scale, and \
                         struct field names all have to match exactly; Spark's TimestampType is \
                         Timestamp(Microsecond, Some(\"UTC\")) and TimestampNTZType is \
                         Timestamp(Microsecond, None).",
                        call.name
                    )));
                }

                // Promise DataFusion the kernel's type in the form other expressions producing it
                // use, rather than as the kernel reports it: list and map child fields carry
                // Comet's canonical names, and every nested field is nullable. An operator that
                // combines this column with one from another expression, such as `if`, needs the
                // two types to agree, and cannot narrow a nullable field to match a non-nullable
                // one. The adapter conforms each result to this type.
                let return_field = Arc::new(Field::new(
                    &call.name,
                    crate::execution::c_udf::promised_return_type(&kernel_return_type),
                    true,
                ));
                let expr = Arc::new(ScalarFunctionExpr::new(
                    &call.name,
                    udf,
                    arg_exprs,
                    return_field,
                    Arc::new(ConfigOptions::default()),
                ));
                Ok(expr)
            }
            expr => Err(GeneralError(format!("Not implemented: {expr:?}"))),
        }
    }

    /// Create a data filter that a scan pushes into the Parquet reader, with float operands
    /// treated as [`Self::data_filter_float_operands`] decides.
    fn create_data_filter(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        float_operands: FloatOperands,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let planner = Self {
            float_operands,
            ..self.clone()
        };
        planner.create_expr(spark_expr, input_schema)
    }

    /// How a scan's data filters treat float operands. The Parquet reader prunes row groups and
    /// pages with them, and with row-level pushdown (`pushdown_filters`) it also drops the rows
    /// they reject, which Spark's Filter above the scan then never sees.
    ///
    /// Without row-level pushdown the filters only prune, and pruning only recognizes a column
    /// compared with a literal, so that shape stays a plain comparison of the raw column
    /// ([`FloatOperands::Raw`]). With it, every float comparison follows Spark's ordering and
    /// gives up pruning: a raw column would drop a stored NaN that Spark matches, such as one with
    /// the sign bit set, which Arrow orders below every other value.
    fn data_filter_float_operands(&self) -> FloatOperands {
        if self
            .session_ctx
            .copied_config()
            .options()
            .execution
            .parquet
            .pushdown_filters
        {
            FloatOperands::Normalize
        } else {
            FloatOperands::Raw
        }
    }

    /// Normalize floating-point comparison keys without changing output values: a `FLOAT` or
    /// `DOUBLE` key, and an array or struct key with a float at any depth, whose order Arrow
    /// otherwise takes from the raw bits. Sort, Window, and WindowGroupLimit must use identical
    /// expressions so DataFusion can recognize the ordering of window partition keys.
    fn create_normalized_key_expr(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let child = self.create_expr(spark_expr, Arc::clone(&input_schema))?;
        let child = NormalizeNaNAndZero::wrap_if_needed(child, input_schema.as_ref())?;
        Ok(NormalizeNestedFloats::wrap_if_needed(
            child,
            input_schema.as_ref(),
        )?)
    }

    /// Only constant literals are supported as scan defaults.
    fn create_default_value(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
    ) -> Result<ScalarValue, ExecutionError> {
        let expr = self.create_expr(spark_expr, Arc::clone(&input_schema))?;
        if let Some(literal) = expr.downcast_ref::<DataFusionLiteral>() {
            return Ok(literal.value().clone());
        }
        Err(GeneralError("Expected a literal scan default".to_string()))
    }

    /// Create a DataFusion physical sort expression from Spark physical expression
    fn create_sort_expr<'a>(
        &'a self,
        spark_expr: &'a Expr,
        input_schema: SchemaRef,
    ) -> Result<PhysicalSortExpr, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            ExprStruct::SortOrder(expr) => {
                let child =
                    self.create_normalized_key_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let descending = expr.direction == 1;
                let nulls_first = expr.null_ordering == 0;

                let options = SortOptions {
                    descending,
                    nulls_first,
                };

                Ok(PhysicalSortExpr {
                    expr: child,
                    options,
                })
            }
            expr => Err(GeneralError(format!("{expr:?} isn't a SortOrder"))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_binary_expr(
        &self,
        spark_expr: &Expr,
        left: &Expr,
        right: &Expr,
        return_type: Option<&spark_expression::DataType>,
        op: DataFusionOperator,
        input_schema: SchemaRef,
        eval_mode: EvalMode,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        self.create_binary_expr_with_options(
            spark_expr,
            left,
            right,
            return_type,
            op,
            input_schema,
            BinaryExprOptions::default(),
            eval_mode,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_binary_expr_with_options(
        &self,
        spark_expr: &Expr,
        left: &Expr,
        right: &Expr,
        return_type: Option<&spark_expression::DataType>,
        op: DataFusionOperator,
        input_schema: SchemaRef,
        options: BinaryExprOptions,
        eval_mode: EvalMode,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        // Look up query context from registry if expr_id is present
        let query_context = spark_expr.expr_id.and_then(|expr_id| {
            let registry = &self.query_context_registry;
            registry.get(expr_id)
        });

        let left = self.create_expr(left, Arc::clone(&input_schema))?;
        let right = self.create_expr(right, Arc::clone(&input_schema))?;
        let required_return_type = || {
            return_type
                .map(to_arrow_datatype)
                .ok_or_else(|| GeneralError(format!("Missing return type for {op:?} expression")))
        };
        match (
            &op,
            left.data_type(&input_schema),
            right.data_type(&input_schema),
        ) {
            // `decimal128_add_sub_width` computes the width in `i16`; see its doc comment for
            // why a `u8` computation panicked on a negative scale in a debug build.
            (
                DataFusionOperator::Plus | DataFusionOperator::Minus | DataFusionOperator::Multiply,
                Ok(DataType::Decimal128(p1, s1)),
                Ok(DataType::Decimal128(p2, s2)),
            ) if ((op == DataFusionOperator::Plus || op == DataFusionOperator::Minus)
                && decimal128_add_sub_width(p1, s1, p2, s2) >= DECIMAL128_MAX_PRECISION as i16)
                || (op == DataFusionOperator::Multiply
                    && p1 as i16 + p2 as i16 >= DECIMAL128_MAX_PRECISION as i16) =>
            {
                let data_type = required_return_type()?;
                let (p_out, s_out) = match &data_type {
                    DataType::Decimal128(p, s) => (*p, *s),
                    dt => {
                        return Err(ExecutionError::GeneralError(format!(
                            "Expected Decimal128 return type, got {dt:?}"
                        )))
                    }
                };
                let wide_op = match op {
                    DataFusionOperator::Plus => WideDecimalOp::Add,
                    DataFusionOperator::Minus => WideDecimalOp::Subtract,
                    DataFusionOperator::Multiply => WideDecimalOp::Multiply,
                    _ => unreachable!(),
                };
                let expr: Arc<dyn PhysicalExpr> = Arc::new(WideDecimalBinaryExpr::new(
                    left, right, wide_op, p_out, s_out, eval_mode,
                ));
                if query_context.is_some() {
                    Ok(Arc::new(CheckedBinaryExpr::new(expr, query_context)))
                } else {
                    Ok(expr)
                }
            }
            (
                DataFusionOperator::Divide,
                Ok(DataType::Decimal128(_p1, _s1)),
                Ok(DataType::Decimal128(_p2, _s2)),
            ) => {
                let data_type = required_return_type()?;
                let func_name = if options.is_integral_div {
                    // Decimal256 division in Arrow may overflow, so we still need this variant of decimal_div.
                    // Otherwise, we may be able to reuse the previous case-match instead of here,
                    // see more: https://github.com/apache/datafusion-comet/pull/1428#discussion_r1972648463
                    "decimal_integral_div"
                } else {
                    "decimal_div"
                };
                // check_divide_overflow rides in the generic fail_on_error slot; only
                // decimal_integral_div consumes it
                let fun_expr = create_comet_physical_fun_with_eval_mode(
                    func_name,
                    data_type.clone(),
                    &self.session_ctx.state(),
                    Some(options.check_divide_overflow),
                    eval_mode,
                )?;
                let expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
                    func_name,
                    fun_expr,
                    vec![left, right],
                    Arc::new(Field::new(func_name, data_type, true)),
                    Arc::new(ConfigOptions::default()),
                ));
                if query_context.is_some() {
                    Ok(Arc::new(CheckedBinaryExpr::new(expr, query_context)))
                } else {
                    Ok(expr)
                }
            }
            // Date +/- Int8/Int16/Int32: DataFusion 52's arrow-arith kernels only
            // support Date32 +/- Interval types, not raw integers. Use the Spark
            // date_add / date_sub UDFs which handle Int8/Int16/Int32 directly.
            (
                DataFusionOperator::Plus,
                Ok(DataType::Date32),
                Ok(DataType::Int8 | DataType::Int16 | DataType::Int32),
            ) => {
                let udf = Arc::new(ScalarUDF::new_from_impl(
                    datafusion_spark::function::datetime::date_add::SparkDateAdd::new(),
                ));
                Ok(Arc::new(ScalarFunctionExpr::new(
                    "date_add",
                    udf,
                    vec![left, right],
                    Arc::new(Field::new("date_add", DataType::Date32, true)),
                    Arc::new(ConfigOptions::default()),
                )))
            }
            (
                DataFusionOperator::Minus,
                Ok(DataType::Date32),
                Ok(DataType::Int8 | DataType::Int16 | DataType::Int32),
            ) => {
                let udf = Arc::new(ScalarUDF::new_from_impl(
                    datafusion_spark::function::datetime::date_sub::SparkDateSub::new(),
                ));
                Ok(Arc::new(ScalarFunctionExpr::new(
                    "date_sub",
                    udf,
                    vec![left, right],
                    Arc::new(Field::new("date_sub", DataType::Date32, true)),
                    Arc::new(ConfigOptions::default()),
                )))
            }
            _ => {
                let data_type = required_return_type()?;
                if [EvalMode::Try, EvalMode::Ansi].contains(&eval_mode)
                    && (data_type.is_integer()
                        || (data_type.is_floating() && op == DataFusionOperator::Divide))
                {
                    let op_str = match op {
                        DataFusionOperator::Plus => "checked_add",
                        DataFusionOperator::Minus => "checked_sub",
                        DataFusionOperator::Multiply => "checked_mul",
                        DataFusionOperator::Divide => "checked_div",
                        _ => {
                            return Err(GeneralError(format!(
                                "{op:?} is not supported in {eval_mode:?} mode for {data_type}"
                            )));
                        }
                    };
                    let fun_expr = create_comet_physical_fun_with_eval_mode(
                        op_str,
                        data_type.clone(),
                        &self.session_ctx.state(),
                        None,
                        eval_mode,
                    )?;
                    let scalar_expr = Arc::new(ScalarFunctionExpr::new(
                        op_str,
                        fun_expr,
                        vec![left, right],
                        Arc::new(Field::new(op_str, data_type, true)),
                        Arc::new(ConfigOptions::default()),
                    ));

                    if query_context.is_some() {
                        Ok(Arc::new(CheckedBinaryExpr::new(scalar_expr, query_context)))
                    } else {
                        Ok(scalar_expr)
                    }
                } else {
                    Ok(Arc::new(BinaryExpr::new(left, op, right)))
                }
            }
        }
    }

    /// DataFusion's nested comparison kernel (`apply_cmp_for_nested`) requires both operands to
    /// have identical data types, including nested field nullability, whereas Spark comparisons
    /// ignore nullability. When a comparison's operands are nested types that differ only in
    /// nullability (e.g. a higher-order `transform` produces `List(non-null Struct)` while the
    /// other side is `List(nullable Struct)`), cast both to their nullability-union type so the
    /// kernel accepts them. Non-comparison ops and non-nested or already-matching types are left
    /// untouched.
    pub fn reconcile_nested_comparison_types(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        op: &DataFusionOperator,
        input_schema: &SchemaRef,
    ) -> (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>) {
        use DataFusionOperator::*;
        let is_cmp = matches!(
            op,
            Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom
        );
        if !is_cmp {
            return (left, right);
        }
        let (lt, rt) = match (left.data_type(input_schema), right.data_type(input_schema)) {
            (Ok(lt), Ok(rt)) => (lt, rt),
            _ => return (left, right),
        };
        // Only nested types route through `apply_cmp_for_nested`; primitives coerce fine.
        let nested = matches!(
            lt,
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
                | DataType::Map(_, _)
        );
        if !nested || lt.equals_datatype(&rt) {
            return (left, right);
        }
        // `Field::try_merge` unions nullability recursively while preserving structure (and the
        // Map/list invariants). Bail out unchanged if the structures are genuinely incompatible.
        let mut merged = Field::new("c", lt.clone(), true);
        if merged
            .try_merge(&Field::new("c", rt.clone(), true))
            .is_err()
        {
            return (left, right);
        }
        let target = merged.data_type().clone();
        let cast_to_target = |e: Arc<dyn PhysicalExpr>, dt: &DataType| -> Arc<dyn PhysicalExpr> {
            if dt.equals_datatype(&target) {
                e
            } else {
                Arc::new(CastExpr::new(e, target.clone(), None))
            }
        };
        (cast_to_target(left, &lt), cast_to_target(right, &rt))
    }

    /// Create a DataFusion physical plan from Spark physical plan. There is a level of
    /// abstraction where a tree of SparkPlan nodes is returned. There is a 1:1 mapping from a
    /// protobuf Operator (that represents a Spark operator) to a native SparkPlan struct. We
    /// need this 1:1 mapping so that we can report metrics back to Spark. The native execution
    /// plan that is generated for each Operator is sometimes a single ExecutionPlan, but in some
    /// cases we generate a tree of ExecutionPlans and we need to collect metrics for all of these
    /// plans so we store references to them in the SparkPlan struct.
    ///
    /// `inputs` is a vector of input source IDs. It is used to create `ScanExec`s. Each `ScanExec`
    /// will be assigned a unique ID from `inputs` and the ID will be used to identify the input
    /// source at JNI API.
    ///
    /// Note that `ScanExec` will pull initial input batch during initialization. It is because we
    /// need to know the exact schema (not only data type but also dictionary-encoding) at
    /// `ScanExec`s. It is because some DataFusion operators, e.g., `ProjectionExec`, gets child
    /// operator schema during initialization and uses it later for `RecordBatch`. We may be
    /// able to get rid of it once `RecordBatch` relaxes schema check.
    ///
    /// Note that we return created `Scan`s which will be kept at JNI API. JNI calls will use it to
    /// feed in new input batch from Spark JVM side.
    pub(crate) fn create_plan<'a>(
        &'a self,
        spark_plan: &'a Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
    ) -> PlanCreationResult {
        // Try to use the modular registry first - this automatically handles any registered operator types
        if OperatorRegistry::global().can_handle(spark_plan) {
            return OperatorRegistry::global().create_plan(
                spark_plan,
                inputs,
                partition_count,
                self,
            );
        }

        // Fall back to the original monolithic match for other operators
        let children = &spark_plan.children;
        match spark_plan.op_struct.as_ref().unwrap() {
            #[cfg(feature = "python-udf")]
            OpStruct::ArrowPythonUdf(udf) => {
                if children.len() != 1 {
                    return Err(ExecutionError::GeneralError(
                        "ArrowPythonUdf requires one child".to_string(),
                    ));
                }
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;
                let specs = udf
                    .functions
                    .iter()
                    .map(|function| {
                        let args = function
                            .args
                            .iter()
                            .map(|arg| self.create_expr(arg, child.schema()))
                            .collect::<Result<Vec<_>, _>>()?;
                        let return_type = to_arrow_datatype(function.return_type.as_ref().ok_or_else(|| {
                            ExecutionError::GeneralError(
                                "ArrowPythonUdf missing return type".to_string(),
                            )
                        })?);
                        Ok(ArrowPythonUdfSpec {
                            command: function.command.clone(),
                            args,
                            arg_names: function.arg_names.clone(),
                            return_type,
                            return_name: function.return_name.clone(),
                            python_version: function.python_version.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, ExecutionError>>()?;
                let native_plan = Arc::new(ArrowPythonUdfExec::try_new(
                    Arc::clone(&child.native_plan),
                    specs,
                    udf.max_records_per_batch,
                    udf.max_bytes_per_batch,
                    udf.accept_array_like_results,
                )?);
                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, native_plan, vec![child])),
                ))
            }
            #[cfg(not(feature = "python-udf"))]
            OpStruct::ArrowPythonUdf(_) => Err(ExecutionError::GeneralError(
                "Native Arrow Python UDF support was not compiled in; rebuild with --features python-udf"
                    .to_string(),
            )),
            OpStruct::Filter(filter) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;
                let predicate =
                    self.create_expr(filter.predicate.as_ref().unwrap(), child.schema())?;

                let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
                    predicate,
                    Arc::clone(&child.native_plan),
                )?);

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, filter, vec![child])),
                ))
            }
            OpStruct::Limit(limit) => {
                assert_eq!(children.len(), 1);
                let num = limit.limit;
                let offset: i32 = limit.offset;
                if num != -1 && offset > num {
                    return Err(GeneralError(format!(
                        "Invalid limit/offset combination: [{num}. {offset}]"
                    )));
                }
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;
                let limit: Arc<dyn ExecutionPlan> = if offset == 0 {
                    Arc::new(LocalLimitExec::new(
                        Arc::clone(&child.native_plan),
                        num as usize,
                    ))
                } else {
                    let fetch = if num == -1 {
                        None
                    } else {
                        Some((num - offset) as usize)
                    };
                    Arc::new(GlobalLimitExec::new(
                        Arc::clone(&child.native_plan),
                        offset as usize,
                        fetch,
                    ))
                };
                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, limit, vec![child])),
                ))
            }
            OpStruct::RangeScan(range) => {
                // A leaf with no JVM input: each task produces its own partition of the range.
                let source: Arc<dyn ExecutionPlan> = Arc::new(range_exec(
                    range.start,
                    range.step,
                    range.num_elements,
                    range.num_slices,
                    self.partition,
                    self.session_ctx.copied_config().batch_size(),
                )?);
                // The range's order is real, and Spark's `RangeExec` reports it, so declare it
                // for the operators above, a hash join in particular: see `SortedInputExec`.
                let ordering = LexOrdering::new([range_sort_expr(range.step)]);
                let source: Arc<dyn ExecutionPlan> = match ordering {
                    Some(ordering) => Arc::new(SortedInputExec::new(source, ordering)),
                    None => source,
                };
                Ok((
                    vec![],
                    vec![],
                    Arc::new(SparkPlan::new(spark_plan.plan_id, source, vec![])),
                ))
            }
            OpStruct::Sample(sample) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;
                // Spark seeds a fresh sampler per partition with `seed + partitionIndex`.
                let seed = sample.seed.wrapping_add(self.partition().into());
                let sample_exec: Arc<dyn ExecutionPlan> = Arc::new(SampleExec::new(
                    Arc::clone(&child.native_plan),
                    sample.lower_bound,
                    sample.upper_bound,
                    seed,
                ));
                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, sample_exec, vec![child])),
                ))
            }
            OpStruct::Sort(sort) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;

                let exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = sort
                    .sort_orders
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, child.schema()))
                    .collect();

                let fetch = sort.fetch.map(|num| num as usize);

                let sort_plan = SortExec::new(
                    LexOrdering::new(exprs?).unwrap(),
                    Arc::clone(&child.native_plan),
                )
                .with_fetch(fetch);
                let mut sort_exec: Arc<dyn ExecutionPlan> = if sort.dynamic_filter_enabled {
                    match TopKReaderFilterExec::try_new(
                        &sort_plan,
                        self.session_ctx.copied_config().options(),
                    )? {
                        Some(wrapper) => Arc::new(wrapper),
                        None => Arc::new(sort_plan),
                    }
                } else {
                    Arc::new(sort_plan)
                };

                if let Some(skip) = sort.skip.filter(|&n| n > 0).map(|n| n as usize) {
                    sort_exec = Arc::new(GlobalLimitExec::new(sort_exec, skip, None));
                }

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        sort_exec,
                        vec![Arc::clone(&child)],
                    )),
                ))
            }
            OpStruct::NativeScan(scan) => {
                // Extract common data and single partition's file list
                // Per-partition injection happens in Scala before sending to native
                let common = scan
                    .common
                    .as_ref()
                    .ok_or_else(|| GeneralError("NativeScan missing common data".into()))?;

                let partition_files = scan
                    .file_partition
                    .as_ref()
                    .ok_or_else(|| GeneralError("NativeScan missing file_partition".into()))?;

                let (object_store_url, object_store_backend, files) =
                    self.prepare_scan_store_and_files(common, partition_files)?;
                let scan = self.build_parquet_scan_plan(
                    spark_plan.plan_id,
                    common,
                    object_store_url,
                    object_store_backend,
                    files,
                    false,
                    "",
                    "",
                )?;
                Ok((vec![], vec![], scan))
            }
            OpStruct::CsvScan(scan) => {
                let data_schema = convert_spark_types_to_arrow_schema(scan.data_schema.as_slice());
                let partition_schema =
                    convert_spark_types_to_arrow_schema(scan.partition_schema.as_slice());
                let projection_vector: Vec<usize> =
                    scan.projection_vector.iter().map(|i| *i as usize).collect();
                let object_store_options: HashMap<String, String> = scan
                    .object_store_options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let partition = usize::try_from(self.partition)
                    .ok()
                    .and_then(|p| scan.file_partitions.get(p))
                    .ok_or_else(|| {
                        GeneralError(format!(
                            "CsvScan has no file partition {} ({} partitions)",
                            self.partition,
                            scan.file_partitions.len()
                        ))
                    })?;
                // Build the store from this partition's files; other partitions may read
                // from a different one.
                let object_store_url = match partition.partitioned_file.first() {
                    Some(file) => {
                        prepare_object_store_with_configs(
                            self.session_ctx.runtime_env(),
                            file.file_path.clone(),
                            &object_store_options,
                        )?
                        .0
                    }
                    None => ObjectStoreUrl::local_filesystem(),
                };
                let files = self.get_partitioned_files(partition, &object_store_options)?;
                let file_groups: Vec<Vec<PartitionedFile>> = vec![files];
                let scan = init_csv_datasource_exec(
                    object_store_url,
                    file_groups,
                    data_schema,
                    partition_schema,
                    projection_vector,
                    &scan.csv_options.clone().unwrap(),
                )?;
                Ok((
                    vec![],
                    vec![],
                    Arc::new(SparkPlan::new(spark_plan.plan_id, scan, vec![])),
                ))
            }
            OpStruct::Scan(scan) => {
                let data_types = scan.fields.iter().map(to_arrow_datatype).collect_vec();

                // If it is not test execution context for unit test, we should have at least one
                // input source
                if self.exec_context_id != TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
                    return Err(GeneralError("No input for scan".to_string()));
                }

                // Consumes the first input source for the scan. The Java side passes an
                // `org.apache.arrow.c.ArrowArrayStream` whose `memoryAddress` points at the C
                // struct; native takes ownership via `ArrowArrayStreamReader::from_raw`.
                let input_source = if self.exec_context_id == TEST_EXEC_CONTEXT_ID
                    && inputs.is_empty()
                {
                    // For unit test, we will set input batch to scan directly by `set_input_batch`.
                    None
                } else {
                    let java_stream = inputs.remove(0);
                    let address: i64 = JVMClasses::with_env(|env| unsafe {
                        jni_call!(env, arrow_array_stream(java_stream.as_obj()).memory_address() -> i64)
                    })?;
                    let reader = unsafe {
                        ArrowArrayStreamReader::from_raw(address as *mut FFI_ArrowArrayStream)
                    }
                    .map_err(|e| {
                        GeneralError(format!("Failed to import ArrowArrayStream from JVM: {e}"))
                    })?;
                    Some(Arc::new(std::sync::Mutex::new(reader)))
                };

                // The `ScanExec` operator will take actual arrays from Spark during execution
                let scan =
                    ScanExec::new(self.exec_context_id, input_source, &scan.source, data_types)?
                        .with_strict_timestamp_labels(self.strict_timestamp_labels);

                Ok((
                    vec![scan.clone()],
                    vec![],
                    Arc::new(SparkPlan::new(spark_plan.plan_id, Arc::new(scan), vec![])),
                ))
            }
            OpStruct::HudiScan(scan) => {
                let common = scan
                    .common
                    .as_ref()
                    .ok_or_else(|| GeneralError("HudiScan missing common data".into()))?;
                let required_schema =
                    convert_spark_types_to_arrow_schema(common.required_schema.as_slice());
                let options: HashMap<String, String> = common
                    .options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let file_slices = scan
                    .file_slices
                    .iter()
                    .map(|s| HudiFileSliceSpec {
                        base_file_path: s.base_file_path.clone(),
                        log_file_paths: s.log_file_paths.clone(),
                    })
                    .collect();
                let hudi_scan = HudiScanExec::new(
                    common.table_base_uri.clone(),
                    options,
                    required_schema,
                    file_slices,
                );
                Ok((
                    vec![],
                    vec![],
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        Arc::new(hudi_scan),
                        vec![],
                    )),
                ))
            }
            OpStruct::IcebergScan(scan) => {
                // Extract common data and single partition's file tasks
                // Per-partition injection happens in Scala before sending to native
                let common = scan
                    .common
                    .as_ref()
                    .ok_or_else(|| GeneralError("IcebergScan missing common data".into()))?;

                let required_schema =
                    convert_spark_types_to_arrow_schema(common.required_schema.as_slice());
                let catalog_properties: HashMap<String, String> = common
                    .catalog_properties
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                // Passed RAW, like every other Iceberg location (see `data_file_path` in
                // `parse_file_scan_tasks_from_common`). `storage_factory_for` routes an opted-in
                // alias to the S3 backend by scheme, so no URL rewrite is needed here.
                let metadata_location = common.metadata_location.clone();
                let catalog_name = common.catalog_name.clone();
                let tasks = parse_file_scan_tasks_from_common(common, &scan.file_scan_tasks)?;
                let data_file_concurrency_limit = common.data_file_concurrency_limit as usize;

                let iceberg_scan = IcebergScanExec::new(
                    metadata_location,
                    required_schema,
                    catalog_properties,
                    catalog_name,
                    tasks,
                    data_file_concurrency_limit,
                )?;

                Ok((
                    vec![],
                    vec![],
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        Arc::new(iceberg_scan),
                        vec![],
                    )),
                ))
            }
            OpStruct::ContribScan(contrib) => {
                // Extension point for optional, out-of-tree contrib scans (Delta, Lance, ...). The
                // concrete scan message is packed into the `ContribScan` envelope on the JVM side;
                // here we route by `type_url` to whichever contrib was compiled in. Core names no
                // specific contrib: the type_url match + decode lives entirely inside each
                // contrib's gated module, so a default build carries zero contrib surface. The arm
                // itself is unconditional so a default build that receives a contrib-shaped plan
                // from a misconfigured driver gets a clear error instead of a "no match" decode
                // failure.
                #[cfg(feature = "contrib-delta")]
                if let Some(result) = delta_scan::try_plan_contrib_scan(self, spark_plan, contrib) {
                    return result;
                }
                #[cfg(feature = "delta")]
                if let Some(result) =
                    delta_spark_scan::try_plan_contrib_scan(self, spark_plan, contrib)
                {
                    return result;
                }
                #[cfg(feature = "contrib-lance")]
                if let Some(result) = lance_scan::try_plan_contrib_scan(self, spark_plan, contrib) {
                    return result;
                }
                Err(GeneralError(format!(
                    "Received a contrib_scan operator (type_url: {}) but core was built without a \
                     contrib that handles it. Rebuild with the matching Maven profile and Cargo \
                     feature.",
                    contrib.type_url
                )))
            }
            OpStruct::Expand(expand) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;

                let mut projections = vec![];
                let mut projection = vec![];

                expand.project_list.iter().try_for_each(|expr| {
                    let expr = self.create_expr(expr, child.schema())?;
                    projection.push(expr);

                    if projection.len() == expand.num_expr_per_project as usize {
                        projections.push(projection.clone());
                        projection = vec![];
                    }

                    Ok::<(), ExecutionError>(())
                })?;

                let schema = ExpandExec::build_schema(&projections, &child.schema())?;

                // `Expand` operator keeps the input batch and expands it to multiple output
                // batches. However, `ScanExec` will reuse input arrays for the next
                // input batch. Therefore, we need to copy the input batch to avoid
                // the data corruption. Note that we only need to copy the input batch
                // if the child operator is `ScanExec`, because other operators after `ScanExec`
                // will create new arrays for the output batch.
                let input = Arc::clone(&child.native_plan);
                let expand = Arc::new(ExpandExec::new(projections, input, schema));
                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, expand, vec![child])),
                ))
            }
            _ => Err(GeneralError(format!(
                "Unsupported or unregistered operator type: {:?}",
                spark_plan.op_struct
            ))),
        }
    }

    fn create_scalar_function_expr(
        &self,
        expr: &ScalarFunc,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let args = expr
            .args
            .iter()
            .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
            .collect::<Result<Vec<_>, _>>()?;

        let fun_name = &expr.func;
        // `map_entries` needs its argument's entry `value` field widened to nullable first (only
        // that outer field). See `widen_map_entry_value_nullable`.
        let args = if fun_name == "map_entries" {
            args.into_iter()
                .map(|arg| {
                    Self::coerce_child_to(arg, &input_schema, widen_map_entry_value_nullable)
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?
        } else if fun_name == "greatest" || fun_name == "least" {
            // Spark compares struct arguments field by field by position, while DataFusion's
            // coercion below would match fields by name. See `coerce_to_common_type`.
            coerce_to_common_type(args, &input_schema)?
        } else {
            args
        };
        let input_expr_types = args
            .iter()
            .map(|x| x.data_type(input_schema.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

        let (data_type, coerced_input_types) =
            match expr.return_type.as_ref().map(to_arrow_datatype) {
                Some(t) => (t, input_expr_types.clone()),
                None => {
                    let fun_name = match fun_name.as_ref() {
                        "read_side_padding" => "rpad", // use the same return type as rpad
                        other => other,
                    };
                    let func = self.session_ctx.udf(fun_name)?;

                    // Type coercion strategy:
                    //
                    // In DF52, Comet used coerce_types() which returns NotImplemented
                    // for most UDFs, so input types were kept unchanged. In DF53,
                    // fields_with_udf() runs full coercion which aggressively promotes
                    // types (e.g. Utf8 to Utf8View via Variadic signatures, Int32 to Int64
                    // via Exact signatures). This breaks Comet's native implementations.
                    //
                    // Strategy:
                    // 1. Try coerce_types() — only UDFs that explicitly implement it
                    //    will return Ok. Same as DF52 behavior.
                    // 2. For "well-supported" signatures (Coercible, String, Numeric,
                    //    Comparable), use fields_with_udf(). These preserve input types
                    //    (e.g. Utf8 stays Utf8, not promoted to Utf8View).
                    // 3. For all other signatures (Variadic, Exact, etc.), keep original
                    //    types unchanged. Same as DF52 behavior.
                    let coerced_types = match func.coerce_types(&input_expr_types) {
                        Ok(types) => types,
                        Err(_) if needs_fields_coercion(&func.signature().type_signature) => {
                            let input_fields: Vec<_> = input_expr_types
                                .iter()
                                .enumerate()
                                .map(|(i, dt)| {
                                    Arc::new(Field::new(format!("arg{i}"), dt.clone(), true))
                                })
                                .collect();
                            let arg_fields = fields_with_udf(&input_fields, func.as_ref())?;
                            arg_fields.iter().map(|f| f.data_type().clone()).collect()
                        }
                        Err(_) => input_expr_types.clone(),
                    };

                    let arg_fields: Vec<_> = coerced_types
                        .iter()
                        .enumerate()
                        .map(|(i, dt)| Arc::new(Field::new(format!("arg{i}"), dt.clone(), true)))
                        .collect();

                    // TODO this should try and find scalar
                    let arguments = args
                        .iter()
                        .map(|e| e.as_ref().downcast_ref::<Literal>().map(|lit| lit.value()))
                        .collect::<Vec<_>>();

                    let args = ReturnFieldArgs {
                        arg_fields: &arg_fields,
                        scalar_arguments: &arguments,
                    };

                    let data_type = Arc::clone(&func.inner().return_field_from_args(args)?)
                        .data_type()
                        .clone();

                    (data_type, coerced_types)
                }
            };

        let fun_expr = create_comet_physical_fun(
            fun_name,
            data_type.clone(),
            &self.session_ctx.state(),
            Some(expr.fail_on_error),
        )?;

        let args = args
            .into_iter()
            .zip(input_expr_types.into_iter().zip(coerced_input_types))
            .map(|(expr, (from_type, to_type))| {
                if from_type != to_type {
                    Arc::new(CastExpr::new(
                        expr,
                        to_type,
                        Some(CastOptions {
                            safe: false,
                            ..Default::default()
                        }),
                    ))
                } else {
                    expr
                }
            })
            .collect::<Vec<_>>();

        let scalar_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            fun_name,
            fun_expr,
            args.to_vec(),
            Arc::new(Field::new(fun_name, data_type.clone(), true)),
            Arc::new(ConfigOptions::default()),
        ));
        let scalar_expr = if expr.null_short_circuit {
            NullShortCircuit::wrap(scalar_expr, &input_schema)?
        } else {
            scalar_expr
        };

        // DF53 changed some UDFs (e.g. md5) to return StringViewArray at execution
        // time (apache/datafusion#20045). Comet does not yet support view types, so
        // cast the result back to the non-view variant.
        let scalar_expr = match data_type {
            DataType::Utf8View => Arc::new(CastExpr::new(
                scalar_expr,
                DataType::Utf8,
                Some(CastOptions {
                    safe: false,
                    ..Default::default()
                }),
            )) as Arc<dyn PhysicalExpr>,
            DataType::BinaryView => Arc::new(CastExpr::new(
                scalar_expr,
                DataType::Binary,
                Some(CastOptions {
                    safe: false,
                    ..Default::default()
                }),
            )) as Arc<dyn PhysicalExpr>,
            _ => scalar_expr,
        };

        Ok(scalar_expr)
    }

    /// Casts `child` so its type matches `widen(child_type)`, wrapping it in a `CastExpr` only when
    /// the widened type differs. Used for the `map_entries` argument widening (with
    /// `widen_map_entry_value_nullable`). Unlike `coerce_collect_child_nullability`, which casts
    /// unconditionally as a normalization barrier for aggregate accumulators, this casts only when
    /// the type actually changes. That is sufficient for the scalar `map_entries`, whose reused
    /// entries array only needs its declared element type to line up.
    fn coerce_child_to(
        child: Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
        widen: impl Fn(&DataType) -> DataType,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let child_type = child.data_type(schema.as_ref())?;
        let widened = widen(&child_type);
        if child_type.equals_datatype(&widened) {
            Ok(child)
        } else {
            Ok(Arc::new(CastExpr::new(child, widened, None)))
        }
    }
}

/// Strip any [`CheckedBinaryExpr`] wrappers so callers can inspect the expression
/// they decorate, however many context-wrapping layers the planner emitted.
fn unwrap_checked(expr: &Arc<dyn PhysicalExpr>) -> &Arc<dyn PhysicalExpr> {
    let mut current = expr;
    while let Some(checked) = current.downcast_ref::<CheckedBinaryExpr>() {
        current = checked.child();
    }
    current
}

pub fn from_protobuf_eval_mode(value: i32) -> Result<EvalMode, prost::UnknownEnumValue> {
    match spark_expression::EvalMode::try_from(value)? {
        spark_expression::EvalMode::Legacy => Ok(EvalMode::Legacy),
        spark_expression::EvalMode::Try => Ok(EvalMode::Try),
        spark_expression::EvalMode::Ansi => Ok(EvalMode::Ansi),
    }
}

/// Convert Comet's `SparkStructField` protos to an Arrow [`SchemaRef`].
///
/// `pub(crate)` so the optional `contrib-delta` scan module
/// (`planner/delta_scan.rs`) can reuse the exact same Spark→Arrow schema
/// mapping that the core scan path uses, keeping the two in lockstep. Kept
/// crate-private — it is not part of any public API.
pub(crate) fn convert_spark_types_to_arrow_schema(
    spark_types: &[spark_operator::SparkStructField],
) -> SchemaRef {
    let arrow_fields = spark_types
        .iter()
        .map(|spark_type| {
            let field = to_arrow_field(
                String::clone(&spark_type.name),
                spark_type.data_type.as_ref().unwrap(),
                spark_type.nullable,
            );
            if spark_type.metadata.is_empty() {
                field
            } else {
                let mut metadata = spark_type.metadata.clone();
                metadata.extend(field.metadata().clone());
                field.with_metadata(metadata)
            }
        })
        .collect_vec();
    let arrow_schema: SchemaRef = Arc::new(Schema::new(arrow_fields));
    arrow_schema
}

/// Converts a protobuf PartitionValue to an iceberg Literal.
///
fn partition_value_to_literal(
    proto_value: &spark_operator::PartitionValue,
) -> Result<Option<iceberg::spec::Literal>, ExecutionError> {
    use spark_operator::iceberg_literal::Value;

    let Some(lit) = proto_value.literal.as_ref() else {
        return Err(GeneralError(
            "PartitionValue has no literal set".to_string(),
        ));
    };

    if lit.is_null {
        return Ok(None);
    }

    let literal = match &lit.value {
        Some(Value::BoolVal(v)) => iceberg::spec::Literal::bool(*v),
        Some(Value::IntVal(v)) => iceberg::spec::Literal::int(*v),
        Some(Value::LongVal(v)) => iceberg::spec::Literal::long(*v),
        Some(Value::FloatVal(v)) => iceberg::spec::Literal::float(*v),
        Some(Value::DoubleVal(v)) => iceberg::spec::Literal::double(*v),
        Some(Value::DateVal(v)) => iceberg::spec::Literal::date(*v),
        Some(Value::TimestampVal(v)) => iceberg::spec::Literal::timestamp(*v),
        Some(Value::TimestampTzVal(v)) => iceberg::spec::Literal::timestamptz(*v),
        Some(Value::StringVal(s)) => iceberg::spec::Literal::string(s.clone()),
        Some(Value::DecimalVal(d)) => {
            iceberg::spec::Literal::decimal(decimal_bytes_to_i128(&d.unscaled)?)
        }
        Some(Value::UuidVal(bytes)) => {
            if bytes.len() != 16 {
                return Err(GeneralError(format!(
                    "Invalid UUID bytes length: {} (expected 16)",
                    bytes.len()
                )));
            }
            let uuid = uuid::Uuid::from_slice(bytes)
                .map_err(|e| GeneralError(format!("Failed to parse UUID: {}", e)))?;
            iceberg::spec::Literal::uuid(uuid)
        }
        Some(Value::FixedVal(bytes)) => iceberg::spec::Literal::fixed(bytes.to_vec()),
        Some(Value::BinaryVal(bytes)) => iceberg::spec::Literal::binary(bytes.to_vec()),
        None => {
            return Err(GeneralError(
                "IcebergLiteral has no value set and is_null is false".to_string(),
            ));
        }
    };

    Ok(Some(literal))
}

/// Decodes an unscaled decimal (two's-complement big-endian) into i128.
fn decimal_bytes_to_i128(bytes: &[u8]) -> Result<i128, ExecutionError> {
    if bytes.len() > 16 {
        return Err(GeneralError(format!(
            "Decimal bytes too large: {} bytes (max 16 for i128)",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 16];
    let offset = 16 - bytes.len();
    buf[offset..].copy_from_slice(bytes);
    // Sign-extend negative values.
    if !bytes.is_empty() && (bytes[0] & 0x80) != 0 {
        for byte in buf.iter_mut().take(offset) {
            *byte = 0xFF;
        }
    }
    Ok(i128::from_be_bytes(buf))
}

/// Converts a protobuf PartitionData to an iceberg Struct.
///
/// Uses the existing Struct::from_iter() API from iceberg-rust to construct the struct
/// from the list of partition values.
/// This can potentially be upstreamed to iceberg_rust
fn partition_data_to_struct(
    proto_partition: &spark_operator::PartitionData,
) -> Result<iceberg::spec::Struct, ExecutionError> {
    let literals: Vec<Option<iceberg::spec::Literal>> = proto_partition
        .values
        .iter()
        .map(partition_value_to_literal)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(iceberg::spec::Struct::from_iter(literals))
}

/// Converts protobuf FileScanTasks from Scala into iceberg-rust FileScanTask objects.
///
/// Each task contains a residual predicate that is used for row-group level filtering
/// during Parquet scanning.
///
/// This function uses deduplication pools from the IcebergScanCommon to avoid redundant
/// parsing of schemas, partition specs, partition types, name mappings, and other repeated data.
fn parse_file_scan_tasks_from_common(
    proto_common: &spark_operator::IcebergScanCommon,
    proto_tasks: &[spark_operator::IcebergFileScanTask],
) -> Result<Vec<iceberg::scan::FileScanTask>, ExecutionError> {
    // Parse each unique schema once, not once per task
    let schema_cache: Vec<Arc<iceberg::spec::Schema>> = proto_common
        .schema_pool
        .iter()
        .map(|json| {
            serde_json::from_str(json).map(Arc::new).map_err(|e| {
                ExecutionError::GeneralError(format!(
                    "Failed to parse schema JSON from pool: {}",
                    e
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let partition_spec_cache: Vec<Option<Arc<iceberg::spec::PartitionSpec>>> = proto_common
        .partition_spec_pool
        .iter()
        .map(|json| {
            serde_json::from_str::<iceberg::spec::PartitionSpec>(json)
                .ok()
                .map(Arc::new)
        })
        .collect();

    let name_mapping_cache: Vec<Option<Arc<iceberg::spec::NameMapping>>> = proto_common
        .name_mapping_pool
        .iter()
        .map(|json| {
            serde_json::from_str::<iceberg::spec::NameMapping>(json)
                .ok()
                .map(Arc::new)
        })
        .collect();

    // Flat pool of unique delete files. A delete file applies to many data files under Iceberg's
    // default partition delete granularity, so DeleteFileList entries reference this pool by index
    // rather than embedding copies.
    let delete_file_pool: Vec<iceberg::scan::FileScanTaskDeleteFile> = proto_common
        .delete_file_pool
        .iter()
        .map(|del| {
            let file_type = match del.content_type.as_str() {
                "POSITION_DELETES" => iceberg::spec::DataContentType::PositionDeletes,
                "EQUALITY_DELETES" => iceberg::spec::DataContentType::EqualityDeletes,
                other => {
                    return Err(GeneralError(format!(
                        "Invalid delete content type '{}'",
                        other
                    )))
                }
            };

            // Puffin selects iceberg-rust's deletion-vector reader; Parquet the ordinary
            // delete-file reader. Only the formats iceberg-rust can read reach here, because
            // CometScanRule falls back to Spark for any other delete format.
            let file_format = match del.file_format.as_str() {
                "PARQUET" => iceberg::spec::DataFileFormat::Parquet,
                "PUFFIN" => iceberg::spec::DataFileFormat::Puffin,
                other => {
                    return Err(GeneralError(format!(
                        "Invalid delete file format '{}'",
                        other
                    )))
                }
            };

            let file_path = proto_common
                .delete_file_path_pool
                .get(del.file_path_idx as usize)
                .ok_or_else(|| {
                    GeneralError(format!(
                        "Invalid file_path_idx: {} (pool size: {})",
                        del.file_path_idx,
                        proto_common.delete_file_path_pool.len()
                    ))
                })?
                .clone();

            // iceberg-rust holds these counts and offsets as `u64`, so a negative one is rejected
            // here, where the proto's `int64` becomes iceberg-rust's type.
            let non_negative = |value: Option<i64>, what: &str| {
                value.map(u64::try_from).transpose().map_err(|_| {
                    GeneralError(format!("Delete file '{file_path}' has a negative {what}"))
                })
            };
            // Required for deletion vectors: iceberg-rust checks it against the number of
            // positions decoded from the blob and errors if it is absent.
            let record_count = non_negative(del.record_count, "record count")?;
            let content_offset = non_negative(del.content_offset, "content offset")?;
            let content_size_in_bytes = non_negative(del.content_size_in_bytes, "content size")?;

            iceberg::scan::FileScanTaskDeleteFile::builder()
                // Passed RAW, like `data_file_path` below (same exact-string delete-matching
                // constraint -- see there).
                .with_file_path(file_path)
                .with_file_type(file_type)
                .with_file_format(file_format)
                // Not serialized; filled in by IcebergScanExec::fill_delete_file_sizes.
                .with_file_size_in_bytes(0)
                .with_partition_spec_id(del.partition_spec_id)
                .with_equality_ids(if del.equality_ids.is_empty() {
                    None
                } else {
                    Some(del.equality_ids.clone())
                })
                // Deletion-vector coordinates, which the serde sets only when file_format is
                // PUFFIN. referenced_data_file names the data file the vector applies to; the other
                // two locate the deletion-vector-v1 blob in its Puffin file. file_format above is
                // the discriminator, since Iceberg also populates referencedDataFile on
                // file-scoped Parquet position deletes.
                .with_referenced_data_file(del.referenced_data_file.clone())
                .with_content_offset(content_offset)
                .with_content_size_in_bytes(content_size_in_bytes)
                .with_record_count(record_count)
                // Plaintext StandardKeyMetadata forwarded verbatim from the JVM; decoded by
                // iceberg-rust with no KMS unwrap. None for unencrypted delete files.
                .with_key_metadata(del.key_metadata.clone().map(Vec::into_boxed_slice))
                // Rejects a deletion vector that lacks its referenced data file, either blob
                // coordinate or its record count.
                .build()
                .map_err(|e| GeneralError(format!("Invalid Iceberg delete file: {e}")))
        })
        .collect::<Result<Vec<_>, ExecutionError>>()?;

    let delete_files_cache: Vec<Vec<iceberg::scan::FileScanTaskDeleteFile>> = proto_common
        .delete_files_pool
        .iter()
        .map(|list| {
            list.delete_file_indices
                .iter()
                .map(|&idx| {
                    delete_file_pool.get(idx as usize).cloned().ok_or_else(|| {
                        GeneralError(format!(
                            "Invalid delete_file_index: {} (pool size: {})",
                            idx,
                            delete_file_pool.len()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, ExecutionError>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Compute unified partition type from the partition_type_pool.
    // Each entry is a StructType JSON with the resolved field types for one partition spec.
    // Merge all specs into a single unified type, matching Iceberg Java's
    // Partitioning.buildPartitionProjectionType()/iceberg-rust's own
    // compute_unified_partition_type(): process specs by spec_id descending (so the newest
    // spec's field name wins when two specs share a field id), then sort the merged fields
    // ascending by field id. The ascending sort matters beyond cosmetics: it fixes the
    // physical Arrow struct field order for `_partition`, which must match the field order
    // Spark's analyzer bound `_partition.<field>` accesses to (also computed via
    // Partitioning.partitionType(), spec-descending + ascending-sort) -- a mismatch would
    // read the wrong value under the right field name instead of erroring.
    //
    // partition_type_pool is index-aligned with partition_spec_pool (see the .proto comment),
    // so partition_spec_cache[i].spec_id() gives the spec_id for partition_type_pool[i].
    //
    // NOTE: The resolved field types here come from Iceberg Java's PartitionSpec.partitionType()
    // (via Scala reflection), which already applied Transform::result_type() per field. Because
    // we only have resolved types (not the original Transform), we cannot replicate one further
    // nuance of the reference algorithms: preferring a non-void transform's type over an older
    // void transform's for the same field id. In practice this only affects a field whose
    // partition source column was later dropped from the schema, which Comet already filters out
    // upstream (see the "unknown type" filtering in serializePartitionData).
    let unified_partition_type = {
        let mut indexed_types = proto_common
            .partition_type_pool
            .iter()
            .enumerate()
            .map(|(idx, type_json)| {
                let struct_type = serde_json::from_str::<iceberg::spec::StructType>(type_json)
                    .map_err(|e| {
                        ExecutionError::GeneralError(format!(
                            "Failed to deserialize partition type JSON from pool: {e}"
                        ))
                    })?;
                // Recover spec_id from the index-aligned spec JSON's "spec-id" field directly,
                // rather than from partition_spec_cache: a spec that fails to deserialize into a
                // PartitionSpec still has its "spec-id" in the JSON, so the merge below stays
                // correctly ordered. The JVM serializer maps a transform name iceberg-rust cannot
                // parse to "unknown" (see IcebergReflection.Transforms.forNative), so this is
                // defensive rather than a path a supported table reaches. Falling back to idx keeps
                // ordering deterministic if the field is somehow absent.
                let spec_id = proto_common
                    .partition_spec_pool
                    .get(idx)
                    .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                    .and_then(|v| v.get("spec-id").and_then(serde_json::Value::as_i64))
                    .map(|id| id as i32)
                    .unwrap_or(idx as i32);
                Ok((spec_id, struct_type))
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        indexed_types.sort_by_key(|(spec_id, _)| std::cmp::Reverse(*spec_id));

        let mut seen_field_ids = std::collections::HashSet::new();
        let mut struct_fields: Vec<iceberg::spec::NestedFieldRef> = Vec::new();

        for (_, struct_type) in &indexed_types {
            for field in struct_type.fields() {
                if seen_field_ids.insert(field.id) {
                    struct_fields.push(Arc::clone(field));
                }
            }
        }

        struct_fields.sort_by_key(|f| f.id);

        iceberg::spec::StructType::new(struct_fields)
    };

    let unified_partition_type_arc = Arc::new(unified_partition_type);

    let results: Result<Vec<_>, _> = proto_tasks
        .iter()
        .map(|proto_task| {
            let schema_ref = Arc::clone(
                schema_cache
                    .get(proto_task.schema_idx as usize)
                    .ok_or_else(|| {
                        ExecutionError::GeneralError(format!(
                            "Invalid schema_idx: {} (pool size: {})",
                            proto_task.schema_idx,
                            schema_cache.len()
                        ))
                    })?,
            );

            let data_file_format = iceberg::spec::DataFileFormat::Parquet;

            let deletes = if let Some(idx) = proto_task.delete_files_idx {
                delete_files_cache
                    .get(idx as usize)
                    .ok_or_else(|| {
                        ExecutionError::GeneralError(format!(
                            "Invalid delete_files_idx: {} (pool size: {})",
                            idx,
                            delete_files_cache.len()
                        ))
                    })?
                    .clone()
            } else {
                vec![]
            };

            let bound_predicate = if let Some(idx) = proto_task.residual_idx {
                proto_common
                    .residual_pool
                    .get(idx as usize)
                    .and_then(iceberg_predicate_to_predicate)
                    .and_then(|pred| {
                        // The residual predicate only drives row-group pruning; the post-scan
                        // filter still enforces correctness. iceberg-rust cannot bind a datum
                        // whose type has no conversion to the column type, so on a bind failure
                        // we skip pushdown rather than fail the scan.
                        // rewrite_not first: iceberg-rust's scan evaluators require negation-normal
                        // form and reject a bare NOT (e.g. from a `NOT (a = b)` residual).
                        match pred.rewrite_not().bind(Arc::clone(&schema_ref), true) {
                            Ok(bound) => Some(bound),
                            Err(e) => {
                                log::warn!("Skipping Iceberg predicate pushdown; bind failed: {e}");
                                None
                            }
                        }
                    })
            } else {
                None
            };

            let partition = if let Some(partition_data_idx) = proto_task.partition_data_idx {
                let partition_data_proto = proto_common
                    .partition_data_pool
                    .get(partition_data_idx as usize)
                    .ok_or_else(|| {
                        ExecutionError::GeneralError(format!(
                            "Invalid partition_data_idx: {} (pool size: {})",
                            partition_data_idx,
                            proto_common.partition_data_pool.len()
                        ))
                    })?;

                match partition_data_to_struct(partition_data_proto) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        return Err(ExecutionError::GeneralError(format!(
                            "Failed to deserialize partition data: {}",
                            e
                        )))
                    }
                }
            } else {
                None
            };

            let partition_spec = proto_task
                .partition_spec_idx
                .and_then(|idx| partition_spec_cache.get(idx as usize))
                .and_then(|opt| opt.clone());

            let name_mapping = proto_task
                .name_mapping_idx
                .and_then(|idx| name_mapping_cache.get(idx as usize))
                .and_then(|opt| opt.clone());

            let project_field_ids = proto_common
                .project_field_ids_pool
                .get(proto_task.project_field_ids_idx as usize)
                .ok_or_else(|| {
                    ExecutionError::GeneralError(format!(
                        "Invalid project_field_ids_idx: {} (pool size: {})",
                        proto_task.project_field_ids_idx,
                        proto_common.project_field_ids_pool.len()
                    ))
                })?
                .field_ids
                .clone();

            let unified_partition_type_for_task = if project_field_ids
                .contains(&iceberg::metadata_columns::RESERVED_FIELD_ID_PARTITION)
            {
                Some(Arc::clone(&unified_partition_type_arc))
            } else {
                None
            };

            // `FileScanTask`'s fields are private as of iceberg-rust 665c64e, so the task is
            // constructed through its builder. `build()` runs the task's validation (partition
            // vs. partition-spec consistency), surfaced here as a GeneralError.
            iceberg::scan::FileScanTask::builder()
                .with_file_size_in_bytes(proto_task.file_size_in_bytes)
                .with_start(proto_task.start)
                .with_length(proto_task.length)
                .with_record_count(proto_task.record_count)
                // RAW data-file path -- do NOT rewrite the alias to s3://. iceberg-rust matches
                // positional deletes by comparing this against the path recorded inside the delete
                // file, so changing the scheme drops deletes. The S3 backend opens a raw alias path
                // fine (bucket from host, key sliced verbatim), and `metadata_location` above
                // already selected the storage factory.
                .with_data_file_path(proto_task.data_file_path.clone())
                .with_data_file_format(data_file_format)
                .with_schema(schema_ref)
                .with_project_field_ids(project_field_ids)
                .with_predicate(bound_predicate)
                .with_deletes(deletes)
                .with_partition(partition)
                .with_partition_spec(partition_spec)
                .with_name_mapping(name_mapping)
                .with_unified_partition_type(unified_partition_type_for_task)
                .with_case_sensitive(false)
                // Plaintext StandardKeyMetadata forwarded verbatim from the JVM; decoded by
                // iceberg-rust with no KMS unwrap. None for unencrypted data files.
                .with_key_metadata(proto_task.key_metadata.clone().map(Vec::into_boxed_slice))
                .build()
                .map_err(|e| {
                    ExecutionError::GeneralError(format!("Invalid Iceberg scan task: {e}"))
                })
        })
        .collect();

    results
}

fn from_protobuf_binary_output_style(
    value: i32,
) -> Result<BinaryOutputStyle, prost::UnknownEnumValue> {
    match spark_expression::BinaryOutputStyle::try_from(value)? {
        spark_expression::BinaryOutputStyle::Utf8 => Ok(BinaryOutputStyle::Utf8),
        spark_expression::BinaryOutputStyle::Basic => Ok(BinaryOutputStyle::Basic),
        spark_expression::BinaryOutputStyle::Base64 => Ok(BinaryOutputStyle::Base64),
        spark_expression::BinaryOutputStyle::Hex => Ok(BinaryOutputStyle::Hex),
        spark_expression::BinaryOutputStyle::HexDiscrete => Ok(BinaryOutputStyle::HexDiscrete),
    }
}

fn literal_to_array_ref(
    data_type: DataType,
    list_literal: ListLiteral,
) -> Result<ArrayRef, ExecutionError> {
    let nulls = &list_literal.null_mask;
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::new(nulls.len()))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::new(
            BooleanBuffer::from(list_literal.boolean_values),
            Some(nulls.clone().into()),
        ))),
        DataType::Int8 => Ok(Arc::new(Int8Array::new(
            list_literal
                .byte_values
                .iter()
                .map(|&x| x as i8)
                .collect::<Vec<_>>()
                .into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Int16 => Ok(Arc::new(Int16Array::new(
            list_literal
                .short_values
                .iter()
                .map(|&x| x as i16)
                .collect::<Vec<_>>()
                .into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Int32 => Ok(Arc::new(Int32Array::new(
            list_literal.int_values.into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::new(
            list_literal.long_values.into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Float32 => Ok(Arc::new(Float32Array::new(
            list_literal.float_values.into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::new(
            list_literal.double_values.into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::new(
            list_literal.int_values.into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Interval(IntervalUnit::YearMonth) => Ok(Arc::new(IntervalYearMonthArray::new(
            list_literal.int_values.into(),
            Some(nulls.clone().into()),
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(Arc::new(TimestampMicrosecondArray::new(
                list_literal.long_values.into(),
                Some(nulls.clone().into()),
            )))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => Ok(Arc::new(
            TimestampMicrosecondArray::new(
                list_literal.long_values.into(),
                Some(nulls.clone().into()),
            )
            .with_timezone(Arc::clone(&tz)),
        )),
        DataType::Binary => {
            // Using a builder as it is cumbersome to create BinaryArray from a vector with nulls
            // and calculate correct offsets
            let item_capacity = list_literal.bytes_values.len();
            let data_capacity = list_literal
                .bytes_values
                .first()
                .map(|s| s.len() * item_capacity)
                .unwrap_or(0);
            let mut arr = BinaryBuilder::with_capacity(item_capacity, data_capacity);

            for (i, v) in list_literal.bytes_values.into_iter().enumerate() {
                if nulls[i] {
                    arr.append_value(v);
                } else {
                    arr.append_null();
                }
            }

            Ok(Arc::new(arr.finish()))
        }
        DataType::Utf8 => {
            // Using a builder as it is cumbersome to create StringArray from a vector with nulls
            // and calculate correct offsets
            let item_capacity = list_literal.string_values.len();
            let data_capacity = list_literal
                .string_values
                .first()
                .map(|s| s.len() * item_capacity)
                .unwrap_or(0);
            let mut arr = StringBuilder::with_capacity(item_capacity, data_capacity);

            for (i, v) in list_literal.string_values.into_iter().enumerate() {
                if nulls[i] {
                    arr.append_value(v);
                } else {
                    arr.append_null();
                }
            }

            Ok(Arc::new(arr.finish()))
        }
        DataType::Decimal128(p, s) => Ok(Arc::new(
            Decimal128Array::new(
                list_literal
                    .decimal_values
                    .into_iter()
                    .map(|v| {
                        let big_integer = BigInt::from_signed_bytes_be(&v);
                        big_integer.to_i128().ok_or_else(|| {
                            GeneralError(format!(
                                "Cannot parse {big_integer:?} as i128 for Decimal literal"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
                Some(nulls.clone().into()),
            )
            .with_precision_and_scale(p, s)?,
        )),
        // list of primitive types
        DataType::List(f) if !matches!(f.data_type(), DataType::List(_)) => {
            literal_to_array_ref(f.data_type().clone(), list_literal)
        }
        DataType::List(ref f) => {
            let dt = f.data_type().clone();

            // Build offsets and collect non-null child arrays
            let mut offsets = Vec::with_capacity(list_literal.list_values.len() + 1);
            // Offsets starts with 0
            offsets.push(0i32);
            let mut child_arrays: Vec<ArrayRef> = Vec::new();

            for (i, child_literal) in list_literal.list_values.iter().enumerate() {
                // Check if the current child literal is non-null and not the empty array
                if list_literal.null_mask[i] && *child_literal != ListLiteral::default() {
                    // Non-null entry: process the child array
                    let child_array = literal_to_array_ref(dt.clone(), child_literal.clone())?;
                    let len = child_array.len() as i32;
                    offsets.push(offsets.last().unwrap() + len);
                    child_arrays.push(child_array);
                } else {
                    // Null entry: just repeat the last offset (empty slot)
                    offsets.push(*offsets.last().unwrap());
                }
            }

            // Concatenate all non-null child arrays' values into one array
            let output_array = if !child_arrays.is_empty() {
                let child_refs: Vec<&dyn Array> = child_arrays.iter().map(|a| a.as_ref()).collect();
                arrow::compute::concat(&child_refs)?
            } else {
                // All entries are null or empty. Build the empty values through the recursion a
                // populated entry takes, so they match a populated sibling's values: one list
                // level below `dt` rather than `dt` itself, e.g. [[[]], [[1]]], and with the
                // nullable fields every rebuilt level gets rather than the ones `dt` declares,
                // e.g. a folded [[[]], [[[1]]]] whose arrays Spark declares non-nullable. Either
                // difference keeps this level from being concatenated with the sibling.
                literal_to_array_ref(dt, ListLiteral::default())?
            };

            // Create and return the parent ListArray
            Ok(Arc::new(ListArray::new(
                FieldRef::from(Field::new("item", output_array.data_type().clone(), true)),
                OffsetBuffer::new(offsets.into()),
                output_array,
                Some(NullBuffer::from(list_literal.null_mask.clone())),
            )))
        }
        dt => Err(GeneralError(format!(
            "DataType::List literal does not support {dt:?} type"
        ))),
    }
}

// ============================================================================
// Iceberg Residual Predicate Conversion
// ============================================================================

/// Converts a serialized Iceberg residual predicate into an iceberg-rust `Predicate` for row-group
/// pruning. This is only a pruning hint -- the post-scan CometFilter enforces correctness -- so any
/// node or literal that cannot be represented degrades to `None` (no pushdown) rather than an
/// error. Mirrors the proto 1:1 onto iceberg-rust's `Reference` builders.
fn iceberg_predicate_to_predicate(
    proto: &spark_operator::IcebergPredicate,
) -> Option<iceberg::expr::Predicate> {
    use iceberg::expr::{Predicate, Reference};
    use spark_operator::iceberg_predicate::Node;
    use spark_operator::IcebergPredicateOperator as Op;

    match proto.node.as_ref()? {
        Node::Unary(u) => {
            let column = Reference::new(u.column.clone());
            match u.op() {
                Op::IsNull => Some(column.is_null()),
                Op::NotNull => Some(column.is_not_null()),
                Op::IsNan => Some(column.is_nan()),
                Op::NotNan => Some(column.is_not_nan()),
                _ => None,
            }
        }
        Node::Binary(b) => {
            let column = Reference::new(b.column.clone());
            let datum = iceberg_literal_to_datum(b.value.as_ref()?)?;
            match b.op() {
                Op::Eq => Some(column.equal_to(datum)),
                Op::NotEq => Some(column.not_equal_to(datum)),
                Op::LessThan => Some(column.less_than(datum)),
                Op::LessThanOrEq => Some(column.less_than_or_equal_to(datum)),
                Op::GreaterThan => Some(column.greater_than(datum)),
                Op::GreaterThanOrEq => Some(column.greater_than_or_equal_to(datum)),
                Op::StartsWith => Some(column.starts_with(datum)),
                Op::NotStartsWith => Some(column.not_starts_with(datum)),
                _ => None,
            }
        }
        Node::Set(s) => {
            // Only IN prunes from stats; not_in is inherently unprunable, so it is never emitted.
            if s.op() != Op::In {
                return None;
            }
            let column = Reference::new(s.column.clone());
            let datums = s
                .values
                .iter()
                .map(iceberg_literal_to_datum)
                .collect::<Option<Vec<_>>>()?;
            Some(column.is_in(datums))
        }
        // And/Or are whole-or-nothing: both children must convert. Dropping a child is unsafe.
        // A dropped disjunct strengthens an Or directly; a dropped conjunct is safe for a bare And
        // but not under a NOT, where De Morgan turns And into Or (`NOT(A AND B)` = `NOT A OR NOT B`)
        // and dropping strengthens it, wrongly pruning row groups. Since Not is only pushed to
        // negation-normal form after decode (rewrite_not below), the Not(And(..)) shape does reach
        // here, so the And arm cannot assume positive position. Degrading to no-pushdown is always
        // safe; the post-scan CometFilter is exact. See `combine_logical` for the parity invariant.
        Node::And(l) => {
            let left = l.left.as_deref().and_then(iceberg_predicate_to_predicate);
            let right = l.right.as_deref().and_then(iceberg_predicate_to_predicate);
            combine_logical(left, right, Predicate::and)
        }
        Node::Or(o) => {
            let left = o.left.as_deref().and_then(iceberg_predicate_to_predicate);
            let right = o.right.as_deref().and_then(iceberg_predicate_to_predicate);
            combine_logical(left, right, Predicate::or)
        }
        Node::Not(child) => iceberg_predicate_to_predicate(child).map(|p| !p),
    }
}

/// Combines the two children of a logical residual node (And/Or), returning `None` unless both
/// converted. A missing child is not expected: the Scala serde emits a logical node only when both
/// children convert, and Rust decodes every node it emits, so both sides always convert for a
/// well-formed residual. A `None` here therefore means the two serde paths have diverged (e.g. a
/// new `IcebergLiteral` variant on the serialize side without a matching decode arm), so it trips a
/// debug-only assertion and degrades to no-pushdown in release. See [`iceberg_predicate_to_predicate`]
/// for why a partial logical node must never be pushed.
fn combine_logical(
    left: Option<iceberg::expr::Predicate>,
    right: Option<iceberg::expr::Predicate>,
    combine: fn(iceberg::expr::Predicate, iceberg::expr::Predicate) -> iceberg::expr::Predicate,
) -> Option<iceberg::expr::Predicate> {
    match (left, right) {
        (Some(l), Some(r)) => Some(combine(l, r)),
        _ => {
            debug_assert!(
                false,
                "Iceberg residual logical node with an unconvertible child: the Scala serde emits \
                 both children and Rust decodes every node it emits, so the two serde paths have \
                 diverged"
            );
            None
        }
    }
}

/// Converts a serialized `IcebergLiteral` into an iceberg-rust `Datum` for predicate pushdown.
/// Returns `None` for null and for byte-array-backed types (decimal/uuid/fixed/binary), which
/// iceberg-rust cannot use in the page index yet; the driver does not emit those for predicates,
/// so this only guards against unexpected input. Correctness is unaffected -- a dropped literal
/// just skips pushdown.
fn iceberg_literal_to_datum(lit: &spark_operator::IcebergLiteral) -> Option<iceberg::spec::Datum> {
    use spark_operator::iceberg_literal::Value;

    if lit.is_null {
        return None;
    }

    match lit.value.as_ref()? {
        Value::BoolVal(v) => Some(iceberg::spec::Datum::bool(*v)),
        Value::IntVal(v) => Some(iceberg::spec::Datum::int(*v)),
        Value::LongVal(v) => Some(iceberg::spec::Datum::long(*v)),
        Value::FloatVal(v) => Some(iceberg::spec::Datum::float(*v)),
        Value::DoubleVal(v) => Some(iceberg::spec::Datum::double(*v)),
        Value::DateVal(v) => Some(iceberg::spec::Datum::date(*v)),
        Value::TimestampVal(v) => Some(iceberg::spec::Datum::timestamp_micros(*v)),
        Value::TimestampTzVal(v) => Some(iceberg::spec::Datum::timestamptz_micros(*v)),
        Value::StringVal(v) => Some(iceberg::spec::Datum::string(v.clone())),
        Value::DecimalVal(_) | Value::UuidVal(_) | Value::FixedVal(_) | Value::BinaryVal(_) => None,
    }
}

/// Returns true for signature types that need fields_with_udf() for coercion.
///
/// "Well-supported" signatures (Coercible, String, Numeric, Comparable) preserve
/// input types naturally (e.g. Utf8 stays Utf8) and need fields_with_udf() because
/// they don't implement coerce_types(). Other signatures (Variadic, Exact, etc.)
/// should keep original types to match DF52 behavior and avoid unwanted promotions
/// like Utf8 to Utf8View or Int32 to Int64.
fn needs_fields_coercion(sig: &TypeSignature) -> bool {
    match sig {
        TypeSignature::Coercible(_)
        | TypeSignature::String(_)
        | TypeSignature::Numeric(_)
        | TypeSignature::Comparable(_) => true,
        TypeSignature::OneOf(sigs) => sigs.iter().any(needs_fields_coercion),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    mod empty_native_scan;
    mod multi_store_scan;

    use futures::{poll, StreamExt};
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::Poll,
    };

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Float32Array, Float64Array, Int32Array, Int8Array,
        ListArray, RecordBatch, StringArray,
    };
    use arrow::datatypes::{DataType, Field, FieldRef, Fields, IntervalUnit, Schema, SchemaRef};
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::common::ScalarValue;
    use datafusion::config::TableParquetOptions;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
    };
    use datafusion::error::DataFusionError;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::logical_expr::{AggregateUDF, EmitTo};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::windows::get_ordered_partition_by_indices;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionConfig;
    use datafusion::{assert_batches_eq, physical_plan::common::collect, prelude::SessionContext};
    use datafusion_comet_spark_expr::{CometCollectList, CometCollectSet};
    use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
    use parquet::variant::VariantType;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use crate::execution::{
        operators::{InputBatch, ScanExec},
        planner::PhysicalPlanner,
    };

    use crate::execution::operators::ExecutionError;
    use crate::execution::planner::{
        convert_spark_types_to_arrow_schema, literal_to_array_ref,
        parse_file_scan_tasks_from_common,
    };
    use crate::execution::shuffle::CometPartitioning;
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
    use datafusion_comet_operators::{PartitionedRankLimitExec, WindowFnKind};
    use datafusion_comet_proto::spark_expression::expr::ExprStruct;
    use datafusion_comet_proto::spark_expression::ListLiteral;
    use datafusion_comet_proto::{
        spark_expression::expr::ExprStruct::*,
        spark_expression::Expr,
        spark_expression::{self, literal},
        spark_operator,
        spark_operator::{operator::OpStruct, Operator},
        spark_partitioning::{
            partitioning::PartitioningStruct, BoundaryRow, Partitioning, RangePartition,
        },
    };
    use datafusion_comet_spark_expr::EvalMode;

    #[test]
    fn scan_default_rejects_struct_expressions() {
        let planner = PhysicalPlanner::new(Arc::new(SessionContext::new()), 0);
        let storage = DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]));
        let field = Field::new("v", storage.clone(), true).with_extension_type(VariantType);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let bytes = |value| Expr {
            expr_struct: Some(ExprStruct::Literal(spark_expression::Literal {
                value: Some(literal::Value::BytesVal(value)),
                datatype: Some(spark_expression::DataType {
                    type_id: spark_expression::data_type::DataTypeId::Bytes as i32,
                    type_info: None,
                }),
                is_null: false,
            })),
            ..Default::default()
        };
        let value = spark_expression::CreateNamedStruct {
            names: vec!["value".to_string(), "metadata".to_string()],
            values: vec![bytes(vec![0]), bytes(vec![1, 0, 0])],
        };
        let default_expr = |value| Expr {
            expr_struct: Some(ExprStruct::CreateNamedStruct(value)),
            ..Default::default()
        };
        assert!(planner
            .create_default_value(&default_expr(value), schema)
            .is_err());
    }

    #[test]
    fn spark_variant_schema_preserves_field_metadata() {
        let schema = convert_spark_types_to_arrow_schema(&[spark_operator::SparkStructField {
            name: "v".to_string(),
            data_type: Some(spark_expression::DataType {
                type_id: spark_expression::data_type::DataTypeId::Variant as i32,
                type_info: None,
            }),
            nullable: true,
            metadata: std::collections::HashMap::from([(
                "source".to_string(),
                "spark".to_string(),
            )]),
        }]);

        let field = schema.field(0);
        assert!(field.has_valid_extension_type::<VariantType>());
        assert_eq!(field.metadata().get("source"), Some(&"spark".to_string()));
    }

    /// Pack a `DeltaSparkScan` into the generic `ContribScan` envelope exactly as the
    /// contrib jar does on the JVM side.
    fn delta_spark_envelope(scan: spark_operator::DeltaSparkScan) -> Operator {
        use prost::Message;
        Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::ContribScan(spark_operator::ContribScan {
                type_url: "type.googleapis.com/comet.contrib.delta_spark.DeltaSparkScan".into(),
                value: scan.encode_to_vec(),
            })),
        }
    }

    fn create_sort_order(index: i32, type_id: i32, descending: bool, nulls_first: bool) -> Expr {
        Expr {
            expr_struct: Some(SortOrder(Box::new(spark_expression::SortOrder {
                child: Some(Box::new(Expr {
                    expr_struct: Some(Bound(spark_expression::BoundReference {
                        index,
                        datatype: Some(spark_expression::DataType {
                            type_id,
                            type_info: None,
                        }),
                    })),
                    ..Default::default()
                })),
                direction: i32::from(descending),
                null_ordering: i32::from(!nulls_first),
            }))),
            ..Default::default()
        }
    }

    fn floating_sort_batches() -> Vec<(i32, RecordBatch)> {
        // Include distinct quiet/signaling NaNs of both signs. Construct them directly:
        // round-tripping through Parquet or a string literal can canonicalize the payload.
        let floats: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(f32::NAN),
            Some(f32::from_bits(0xffc0_0042)),
            Some(f32::from_bits(0x7fc0_1234)),
            Some(0.0),
            Some(-0.0),
            Some(f32::NEG_INFINITY),
            Some(1.0),
            Some(f32::INFINITY),
            Some(f32::from_bits(0xff80_0001)),
            Some(f32::from_bits(0x7f80_0001)),
            Some(0.0),
            Some(-0.0),
            None,
            Some(-1.0),
        ]));
        let doubles: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::from_bits(0xfff8_0000_0000_0042)),
            Some(f64::from_bits(0x7ff8_0000_0000_1234)),
            Some(0.0),
            Some(-0.0),
            Some(f64::NEG_INFINITY),
            Some(1.0),
            Some(f64::INFINITY),
            Some(f64::from_bits(0xfff0_0000_0000_0001)),
            Some(f64::from_bits(0x7ff0_0000_0000_0001)),
            Some(0.0),
            Some(-0.0),
            None,
            Some(-1.0),
        ]));
        [(5, floats), (6, doubles)]
            .into_iter()
            .map(|(type_id, values)| {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("ord", values.data_type().clone(), true),
                    Field::new("suffix", DataType::Int32, false),
                    Field::new("id", DataType::Int32, false),
                ]));
                let ids = Int32Array::from_iter_values(0..values.len() as i32);
                let suffix = Int32Array::from(vec![1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1]);
                let batch =
                    RecordBatch::try_new(schema, vec![values, Arc::new(suffix), Arc::new(ids)])
                        .unwrap();
                (type_id, batch)
            })
            .collect()
    }

    /// `floating_sort_batches` with each key wrapped in a one-element list and in a one-field
    /// struct, which have to order and tie exactly as the bare key does. A null key becomes a
    /// null list or struct.
    fn nested_floating_sort_batches() -> Vec<(i32, RecordBatch)> {
        use arrow::array::StructArray;
        use arrow::buffer::OffsetBuffer;
        floating_sort_batches()
            .into_iter()
            .flat_map(|(type_id, batch)| {
                let values = Arc::clone(batch.column(0));
                let nulls = values.nulls().cloned();
                let element = Field::new("item", values.data_type().clone(), true);
                let list: ArrayRef = Arc::new(ListArray::new(
                    Arc::new(element),
                    OffsetBuffer::from_lengths(vec![1; values.len()]),
                    Arc::clone(&values),
                    nulls.clone(),
                ));
                let fields = Fields::from(vec![Field::new("v", values.data_type().clone(), true)]);
                let record: ArrayRef = Arc::new(StructArray::new(fields, vec![values], nulls));
                [list, record].into_iter().map(move |key| {
                    let mut fields: Vec<FieldRef> = batch.schema().fields().to_vec();
                    fields[0] = Arc::new(Field::new("ord", key.data_type().clone(), true));
                    let mut columns = batch.columns().to_vec();
                    columns[0] = key;
                    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns);
                    (type_id, batch.unwrap())
                })
            })
            .collect()
    }

    #[test]
    fn floating_window_partition_keys_preserve_ordering() {
        let planner = PhysicalPlanner::default();
        for (type_id, batch) in floating_sort_batches() {
            for already_normalized in [false, true] {
                let mut partition_sort = create_sort_order(0, type_id, false, true);
                let Some(SortOrder(sort_order)) = partition_sort.expr_struct.as_mut() else {
                    unreachable!();
                };
                if already_normalized {
                    sort_order.child = Some(Box::new(Expr {
                        expr_struct: Some(NormalizeNanAndZero(Box::new(
                            spark_expression::NormalizeNaNAndZero {
                                child: sort_order.child.take(),
                                datatype: Some(spark_expression::DataType {
                                    type_id,
                                    type_info: None,
                                }),
                            },
                        ))),
                        ..Default::default()
                    }));
                }
                // A key can arrive as Spark's normalization expression or as a
                // materialized floating column. Both must match the sort prefix.
                let partition_expr = planner
                    .create_normalized_key_expr(sort_order.child.as_ref().unwrap(), batch.schema())
                    .unwrap();
                let sort_expr = planner
                    .create_sort_expr(&partition_sort, batch.schema())
                    .unwrap();
                let input =
                    MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)
                        .unwrap();
                let sorted: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
                    LexOrdering::new(vec![sort_expr.clone()]).unwrap(),
                    input,
                ));
                let limited: Arc<dyn ExecutionPlan> = Arc::new(
                    PartitionedRankLimitExec::try_new(
                        Arc::clone(&sorted),
                        vec![PhysicalSortExpr {
                            expr: Arc::clone(&partition_expr),
                            options: sort_expr.options,
                        }],
                        vec![],
                        1,
                        WindowFnKind::RowNumber,
                    )
                    .unwrap(),
                );
                for input in [sorted, limited] {
                    assert_eq!(
                        get_ordered_partition_by_indices(&[Arc::clone(&partition_expr)], &input)
                            .unwrap(),
                        vec![0],
                        "type={type_id}, already_normalized={already_normalized}",
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn floating_sort_keys_preserve_window_group_limit_peers() {
        let planner = PhysicalPlanner::default();
        let context = SessionContext::new_with_config(SessionConfig::new().with_batch_size(3));
        // Floats nested in a list or a struct must rank exactly as the bare floats do.
        let batches = floating_sort_batches()
            .into_iter()
            .chain(nested_floating_sort_batches());
        for (type_id, batch) in batches {
            for (descending, kind, fetch, expected) in [
                (true, WindowFnKind::Rank, 3, vec![0, 1, 2, 8, 9]),
                (true, WindowFnKind::DenseRank, 2, vec![0, 1, 2, 8, 9]),
                (true, WindowFnKind::RowNumber, 2, vec![8, 9]),
                (false, WindowFnKind::Rank, 3, vec![5, 10, 11, 13]),
                (false, WindowFnKind::DenseRank, 3, vec![5, 10, 11, 13]),
                (false, WindowFnKind::RowNumber, 4, vec![5, 10, 11, 13]),
            ] {
                let order_keys = [
                    create_sort_order(0, type_id, descending, false),
                    create_sort_order(1, 3, false, false),
                ]
                .iter()
                .map(|expr| planner.create_sort_expr(expr, batch.schema()).unwrap())
                .collect::<Vec<_>>();
                let input = MemorySourceConfig::try_new_exec(
                    &[vec![
                        batch.slice(0, 4),
                        batch.slice(4, 4),
                        batch.slice(8, 6),
                    ]],
                    batch.schema(),
                    None,
                )
                .unwrap();
                let sorted = Arc::new(SortExec::new(
                    LexOrdering::new(order_keys.clone()).unwrap(),
                    input,
                ));
                let limit =
                    PartitionedRankLimitExec::try_new(sorted, vec![], order_keys, fetch, kind)
                        .unwrap();
                let output = collect(limit.execute(0, context.task_ctx()).unwrap())
                    .await
                    .unwrap();
                let mut ids = vec![];
                for out in &output {
                    let out_ids = out.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
                    for row in 0..out.num_rows() {
                        let id = out_ids.value(row);
                        ids.push(id);
                        // ScalarValue float equality compares raw bits, including NaN
                        // payloads and the zero sign. Only comparison keys may change.
                        assert_eq!(
                            ScalarValue::try_from_array(out.column(0), row).unwrap(),
                            ScalarValue::try_from_array(batch.column(0), id as usize).unwrap(),
                        );
                    }
                }
                ids.sort_unstable();
                assert_eq!(
                    ids,
                    expected,
                    "type={type_id}, key={}, descending={descending}, {kind:?}",
                    batch.schema().field(0).data_type()
                );
                // The zero peer group and the second NaN peer group straddle size-3
                // sort output batches. Tie state must survive those boundaries.
                assert!(output.iter().all(|b| b.num_rows() <= 3));
                if expected.len() > 3 {
                    assert!(output.len() > 1);
                }
            }
        }
    }

    #[test]
    fn floating_range_boundaries_match_sort_keys() {
        let planner = PhysicalPlanner::default();
        for (type_id, batch) in floating_sort_batches() {
            for descending in [false, true] {
                for nulls_first in [false, true] {
                    // Use -0 and a negative payload NaN, while the input contains both
                    // zero signs and several NaN representations equivalent in Spark.
                    let boundary_indices = if descending { [1, 4] } else { [4, 1] };
                    let boundary_rows = boundary_indices
                        .iter()
                        .map(|&index| {
                            let value = match ScalarValue::try_from_array(batch.column(0), index)
                                .unwrap()
                            {
                                ScalarValue::Float32(Some(value)) => {
                                    literal::Value::FloatVal(value)
                                }
                                ScalarValue::Float64(Some(value)) => {
                                    literal::Value::DoubleVal(value)
                                }
                                _ => unreachable!(),
                            };
                            BoundaryRow {
                                partition_bounds: vec![Expr {
                                    expr_struct: Some(Literal(spark_expression::Literal {
                                        value: Some(value),
                                        datatype: Some(spark_expression::DataType {
                                            type_id,
                                            type_info: None,
                                        }),
                                        is_null: false,
                                    })),
                                    ..Default::default()
                                }],
                            }
                        })
                        .collect();
                    let partitioning = Partitioning {
                        partitioning_struct: Some(PartitioningStruct::RangePartition(
                            RangePartition {
                                sort_orders: vec![create_sort_order(
                                    0,
                                    type_id,
                                    descending,
                                    nulls_first,
                                )],
                                num_partitions: 3,
                                boundary_rows,
                            },
                        )),
                    };
                    let CometPartitioning::RangePartitioning(ordering, _, converter, boundaries) =
                        planner
                            .create_partitioning(&partitioning, batch.schema())
                            .unwrap()
                    else {
                        panic!("expected range partitioning");
                    };
                    let columns = ordering
                        .iter()
                        .map(|expr| {
                            expr.expr
                                .evaluate(&batch)
                                .unwrap()
                                .into_array(batch.num_rows())
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    let rows = converter.convert_columns(&columns).unwrap();
                    for index in [3, 4, 10, 11] {
                        assert_eq!(rows.row(index), boundaries[usize::from(descending)].row());
                    }
                    for index in [0, 1, 2, 8, 9] {
                        assert_eq!(rows.row(index), boundaries[usize::from(!descending)].row());
                    }
                    // Match the range shuffle writer's binary-search routing.
                    let partitions = rows
                        .iter()
                        .map(|row| boundaries.partition_point(|bound| bound.row() <= row))
                        .collect::<Vec<_>>();
                    let null_partition = if nulls_first { 0 } else { 2 };
                    let expected = if descending {
                        vec![1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 2, 2, null_partition, 2]
                    } else {
                        vec![2, 2, 2, 1, 1, 0, 1, 1, 2, 2, 1, 1, null_partition, 0]
                    };
                    assert_eq!(
                        partitions, expected,
                        "type={type_id}, descending={descending}, nulls_first={nulls_first}",
                    );
                }
            }
        }
    }

    #[test]
    fn delta_scan_errors_without_delta_feature() {
        let op = delta_spark_envelope(spark_operator::DeltaSparkScan {
            common: None,
            delta_common: None,
            file_partition: None,
        });
        let planner = PhysicalPlanner::default();
        let err = planner.create_plan(&op, &mut vec![], 1).unwrap_err();
        let msg = format!("{err}");
        #[cfg(not(feature = "delta"))]
        assert!(
            msg.contains("built without a contrib that handles it"),
            "expected mismatched-build error, got: {msg}"
        );
        #[cfg(feature = "delta")]
        assert!(
            msg.contains("missing common data"),
            "expected missing-common-data error for an empty DeltaSparkScan, got: {msg}"
        );
    }

    #[cfg(feature = "delta")]
    fn delta_scan_op(files: Vec<spark_operator::DeltaSparkPartitionedFile>) -> Operator {
        delta_spark_envelope(spark_operator::DeltaSparkScan {
            common: Some(Default::default()),
            delta_common: None,
            file_partition: Some(spark_operator::DeltaSparkFilePartition {
                partitioned_file: files,
            }),
        })
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_scan_rejects_dv_without_source() {
        let op = delta_scan_op(vec![spark_operator::DeltaSparkPartitionedFile {
            file: Some(spark_operator::SparkPartitionedFile {
                file_path: "file:///tmp/f.parquet".into(),
                start: 0,
                length: 0,
                file_size: 0,
                partition_values: vec![],
            }),
            dv: Some(spark_operator::DeltaSparkDvDescriptor {
                storage_type: "u".into(),
                absolute_path: None,
                inline_data: None,
                offset: Some(1),
                size_in_bytes: 1,
                cardinality: 1,
            }),
            // (file_path carries a scheme because store resolution now precedes
            // the DV handling)
        }]);
        let err = PhysicalPlanner::default()
            .create_plan(&op, &mut vec![], 1)
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("neither inline data nor a path"),
            "expected malformed-descriptor error, got: {msg}"
        );
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_scan_rejects_missing_inner_file() {
        let op = delta_scan_op(vec![spark_operator::DeltaSparkPartitionedFile {
            file: None,
            dv: None,
        }]);
        let err = PhysicalPlanner::default()
            .create_plan(&op, &mut vec![], 1)
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("missing inner file"),
            "expected missing-inner-file error, got: {msg}"
        );
    }

    #[test]
    fn test_unpack_dictionary_primitive() {
        let op_scan = Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![spark_expression::DataType {
                    type_id: 3, // Int32
                    type_info: None,
                }],
                source: "".to_string(),
            })),
        };

        let op = create_filter(op_scan, 3);
        let planner = PhysicalPlanner::default();
        let row_count = 100;

        // Create a dictionary array with 100 values, and use it as input to the execution.
        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = Int32Array::from(vec![0, 1, 2, 3]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

        let (mut scans, _shuffle_scans, datafusion_plan) =
            planner.create_plan(&op, &mut vec![], 1).unwrap();
        scans[0].set_input_batch(input_batch);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let mut eof_sent = false;
            let mut got_result = false;
            loop {
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), row_count / 4);
                        // dictionary should be unpacked
                        assert!(matches!(batch.column(0).data_type(), DataType::Int32));
                        got_result = true;
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    Poll::Pending => {
                        // Stream needs more input (e.g. FilterExec's batch coalescer
                        // is accumulating). Send EOF to flush.
                        if !eof_sent {
                            scans[0].set_input_batch(InputBatch::EOF);
                            eof_sent = true;
                        }
                    }
                }
            }
            assert!(got_result, "Expected at least one result batch");
        });
    }

    const STRING_TYPE_ID: i32 = 7;

    #[test]
    fn test_unpack_dictionary_string() {
        let op_scan = Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![spark_expression::DataType {
                    type_id: STRING_TYPE_ID, // String
                    type_info: None,
                }],
                source: "".to_string(),
            })),
        };

        let lit = spark_expression::Literal {
            value: Some(literal::Value::StringVal("foo".to_string())),
            datatype: Some(spark_expression::DataType {
                type_id: STRING_TYPE_ID,
                type_info: None,
            }),
            is_null: false,
        };

        let op = create_filter_literal(op_scan, STRING_TYPE_ID, lit);
        let planner = PhysicalPlanner::default();

        let row_count = 100;

        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = StringArray::from(vec!["foo", "bar", "hello", "comet"]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

        let (mut scans, _shuffle_scans, datafusion_plan) =
            planner.create_plan(&op, &mut vec![], 1).unwrap();

        // Scan's schema is determined by the input batch, so we need to set it before execution.
        scans[0].set_input_batch(input_batch);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let mut eof_sent = false;
            let mut got_result = false;
            loop {
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), row_count / 4);
                        // string/binary should no longer be packed with dictionary
                        assert!(matches!(batch.column(0).data_type(), DataType::Utf8));
                        got_result = true;
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    Poll::Pending => {
                        // Stream needs more input (e.g. FilterExec's batch coalescer
                        // is accumulating). Send EOF to flush.
                        if !eof_sent {
                            scans[0].set_input_batch(InputBatch::EOF);
                            eof_sent = true;
                        }
                    }
                }
            }
            assert!(got_result, "Expected at least one result batch");
        });
    }

    #[tokio::test()]
    #[allow(clippy::field_reassign_with_default)]
    async fn to_datafusion_filter() {
        let op_scan = create_scan();
        let op = create_filter(op_scan, 0);
        let planner = PhysicalPlanner::default();

        let (mut scans, _shuffle_scans, datafusion_plan) =
            planner.create_plan(&op, &mut vec![], 1).unwrap();

        let scan = &mut scans[0];
        scan.set_input_batch(InputBatch::EOF);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let stream = datafusion_plan
            .native_plan
            .execute(0, Arc::clone(&task_ctx))
            .unwrap();
        let output = collect(stream).await.unwrap();
        assert!(output.is_empty());
    }

    #[tokio::test()]
    async fn from_datafusion_error_to_comet() {
        let err_msg = "exec error";
        let err = datafusion::common::DataFusionError::Execution(err_msg.to_string());
        let comet_err: ExecutionError = err.into();
        assert_eq!(comet_err.to_string(), "Error from DataFusion: exec error.");
    }

    // Creates a filter operator which takes an `Int32Array` and selects rows that are equal to
    // `value`.
    fn create_filter(child_op: spark_operator::Operator, value: i32) -> spark_operator::Operator {
        let lit = spark_expression::Literal {
            value: Some(literal::Value::IntVal(value)),
            datatype: Some(spark_expression::DataType {
                type_id: 3,
                type_info: None,
            }),
            is_null: false,
        };

        create_filter_literal(child_op, 3, lit)
    }

    fn create_filter_literal(
        child_op: spark_operator::Operator,
        type_id: i32,
        lit: spark_expression::Literal,
    ) -> spark_operator::Operator {
        let left = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(spark_expression::DataType {
                    type_id,
                    type_info: None,
                }),
            })),
            query_context: None,
            expr_id: None,
        };
        let right = spark_expression::Expr {
            expr_struct: Some(Literal(lit)),
            query_context: None,
            expr_id: None,
        };

        let expr = spark_expression::Expr {
            expr_struct: Some(Eq(Box::new(spark_expression::BinaryExpr {
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
            query_context: None,
            expr_id: None,
        };

        Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![child_op],
            op_struct: Some(OpStruct::Filter(spark_operator::Filter {
                predicate: Some(expr),
                ..Default::default()
            })),
        }
    }

    #[test]
    fn spark_plan_metrics_filter() {
        let op_scan = create_scan();
        let op = create_filter(op_scan, 0);
        let planner = PhysicalPlanner::default();

        let (_scans, _shuffle_scans, filter_exec) =
            planner.create_plan(&op, &mut vec![], 1).unwrap();

        assert_eq!("FilterExec", filter_exec.native_plan.name());
        assert_eq!(1, filter_exec.children.len());
        assert_eq!(0, filter_exec.additional_native_plans.len());
    }

    /// Comparisons follow Spark's float ordering without normalizing their operands, except in the
    /// data filters that a scan pushes into the Parquet reader, where pruning has to see a plain
    /// comparison of the column.
    #[test]
    fn scan_data_filters_compare_float_columns_directly() {
        use datafusion::physical_expr::expressions::BinaryExpr;
        use datafusion_comet_spark_expr::SparkComparison;
        let double = spark_expression::DataType {
            type_id: 6,
            type_info: None,
        };
        let operand = |expr_struct| Expr {
            expr_struct: Some(expr_struct),
            query_context: None,
            expr_id: None,
        };
        let expr = operand(Gt(Box::new(spark_expression::BinaryExpr {
            left: Some(Box::new(operand(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(double.clone()),
            })))),
            right: Some(Box::new(operand(Literal(spark_expression::Literal {
                value: Some(literal::Value::DoubleVal(500.0)),
                datatype: Some(double),
                is_null: false,
            })))),
        })));
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Float64, true)]));
        let planner = PhysicalPlanner::default();
        let comparison = planner.create_expr(&expr, Arc::clone(&schema)).unwrap();
        let comparison = comparison
            .downcast_ref::<SparkComparison>()
            .expect("a Spark comparison");
        assert!(comparison.left().downcast_ref::<Column>().is_some());
        let data_filter = planner
            .create_data_filter(&expr, schema, planner.data_filter_float_operands())
            .unwrap();
        let data_filter = data_filter
            .downcast_ref::<BinaryExpr>()
            .expect("a plain comparison");
        assert!(data_filter.left().downcast_ref::<Column>().is_some());
    }

    /// With row-level pushdown the Parquet reader drops the rows a data filter rejects, so the
    /// filter compares the column in Spark's ordering rather than as it is. A raw column would drop
    /// a stored NaN whose bits differ from the normalized literal, and a stored NaN with the sign
    /// bit set under any ordering comparison, both of which Spark matches.
    #[test]
    fn scan_data_filters_follow_spark_ordering_with_row_level_pushdown() {
        use arrow::array::{AsArray, BooleanArray};
        use datafusion_comet_spark_expr::{FloatOperands, SparkComparison};
        let double = spark_expression::DataType {
            type_id: 6,
            type_info: None,
        };
        let operand = |expr_struct| Expr {
            expr_struct: Some(expr_struct),
            query_context: None,
            expr_id: None,
        };
        let column_and = |value: f64| {
            Box::new(spark_expression::BinaryExpr {
                left: Some(Box::new(operand(Bound(spark_expression::BoundReference {
                    index: 0,
                    datatype: Some(double.clone()),
                })))),
                right: Some(Box::new(operand(Literal(spark_expression::Literal {
                    value: Some(literal::Value::DoubleVal(value)),
                    datatype: Some(double.clone()),
                    is_null: false,
                })))),
            })
        };
        // Spark folds `-double('NaN')` into a literal with the sign bit set.
        let negative_nan = f64::from_bits(0xfff8_0000_0000_0000);
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![
                negative_nan,
                f64::NAN,
                1.0,
            ]))],
        )
        .unwrap();
        let config =
            SessionConfig::new().set_bool("datafusion.execution.parquet.pushdown_filters", true);
        let planner = PhysicalPlanner::new(Arc::new(SessionContext::new_with_config(config)), 0);
        assert_eq!(
            planner.data_filter_float_operands(),
            FloatOperands::Normalize
        );
        for (expr, expected) in [
            (operand(Eq(column_and(negative_nan))), [true, true, false]),
            (operand(Gt(column_and(0.0))), [true, true, true]),
        ] {
            let data_filter = planner
                .create_data_filter(
                    &expr,
                    Arc::clone(&schema),
                    planner.data_filter_float_operands(),
                )
                .unwrap();
            assert!(data_filter.downcast_ref::<SparkComparison>().is_some());
            let matched = data_filter.evaluate(&batch).unwrap().into_array(3).unwrap();
            assert_eq!(matched.as_boolean(), &BooleanArray::from(expected.to_vec()));
        }
    }

    #[test]
    fn spark_plan_metrics_hash_join() {
        let op_scan = create_scan();
        let op_join = Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![op_scan.clone(), op_scan.clone()],
            op_struct: Some(OpStruct::HashJoin(spark_operator::HashJoin {
                left_join_keys: vec![create_bound_reference(0)],
                right_join_keys: vec![create_bound_reference(0)],
                join_type: 0,
                condition: None,
                build_side: 0,
                null_aware_anti_join: false,
                dynamic_filter_enabled: false,
                streamed_sort_orders: vec![],
            })),
        };

        let planner = PhysicalPlanner::default();

        let (_scans, _shuffle_scans, hash_join_exec) =
            planner.create_plan(&op_join, &mut vec![], 1).unwrap();

        assert_eq!("HashJoinExec", hash_join_exec.native_plan.name());
        assert_eq!(2, hash_join_exec.children.len());
        assert_eq!("ScanExec", hash_join_exec.children[0].native_plan.name());
        assert_eq!("ScanExec", hash_join_exec.children[1].native_plan.name());
    }

    /// The hash join in `plan`, under the projection that `swap_inputs` adds when Spark builds
    /// on the right.
    fn planned_hash_join(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> &datafusion::physical_plan::joins::HashJoinExec {
        let join = if plan.is::<datafusion::physical_plan::projection::ProjectionExec>() {
            plan.children()[0]
        } else {
            plan
        };
        join.downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>()
            .expect("a hash join")
    }

    fn sorted_outer_hash_join(
        join_type: spark_operator::JoinType,
        build_side: spark_operator::BuildSide,
        children: Vec<Operator>,
    ) -> Operator {
        Operator {
            children,
            op_struct: Some(OpStruct::HashJoin(spark_operator::HashJoin {
                left_join_keys: vec![create_bound_reference(0)],
                right_join_keys: vec![create_bound_reference(0)],
                join_type: join_type as i32,
                build_side: build_side as i32,
                streamed_sort_orders: vec![create_sort_order(0, 3, false, true)],
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn hash_join_declares_streamed_order_on_probe_input() {
        // The two outer joins Comet sends the streamed side's order for both run as DataFusion's
        // right join, whose probe input is its right one, whichever side Spark builds on.
        for (join_type, build_side) in [
            (
                spark_operator::JoinType::RightOuter,
                spark_operator::BuildSide::BuildLeft,
            ),
            (
                spark_operator::JoinType::LeftOuter,
                spark_operator::BuildSide::BuildRight,
            ),
        ] {
            let operator =
                sorted_outer_hash_join(join_type, build_side, vec![create_scan(), create_scan()]);
            let (_, _, planned) = PhysicalPlanner::default()
                .create_plan(&operator, &mut vec![], 1)
                .unwrap();
            let join = planned_hash_join(&planned.native_plan);
            assert_eq!(&datafusion::common::JoinType::Right, join.join_type());
            assert_eq!("ScanExec", join.left().name());
            let probe = join.right();
            let sorted = probe
                .downcast_ref::<crate::execution::operators::SortedInputExec>()
                .expect("the probe input declares the streamed side's order");
            assert_eq!("ScanExec", sorted.children()[0].name());
            assert_eq!(
                "col_0@0 ASC",
                probe.properties().output_ordering().unwrap().to_string()
            );
            // The join keeps the order in its own output, so the operators above it see it too.
            assert!(planned.native_plan.properties().output_ordering().is_some());
        }
    }

    #[test]
    fn hash_join_declares_order_through_operators_that_keep_it() {
        let projection = Operator {
            children: vec![create_scan()],
            op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                project_list: vec![create_bound_reference(0)],
            })),
            ..Default::default()
        };
        let operator = sorted_outer_hash_join(
            spark_operator::JoinType::RightOuter,
            spark_operator::BuildSide::BuildLeft,
            vec![create_scan(), projection],
        );
        let (_, _, planned) = PhysicalPlanner::default()
            .create_plan(&operator, &mut vec![], 1)
            .unwrap();
        let probe = planned_hash_join(&planned.native_plan).right();
        assert_eq!("SortedInputExec", probe.name());
        assert_eq!("ProjectionExec", probe.children()[0].name());
    }

    /// A hash join over two scans, for use as the probe input of the join under test.
    fn hash_join_over_scans(
        join_type: spark_operator::JoinType,
        build_side: spark_operator::BuildSide,
        null_aware_anti_join: bool,
    ) -> Operator {
        Operator {
            children: vec![create_scan(), create_scan()],
            op_struct: Some(OpStruct::HashJoin(spark_operator::HashJoin {
                left_join_keys: vec![create_bound_reference(0)],
                right_join_keys: vec![create_bound_reference(0)],
                join_type: join_type as i32,
                build_side: build_side as i32,
                null_aware_anti_join,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn hash_join_declares_order_through_joins_that_keep_probe_order() {
        // An inner join emits its matches probe row by probe row, and a right semi, anti or mark
        // join emits its probe indices in ascending order. None of them depends on the probe
        // declaring an order, so the order Spark reports for the outer join's streamed side
        // still holds above them.
        use datafusion::common::JoinType as DF;
        use spark_operator::BuildSide::{BuildLeft, BuildRight};
        use spark_operator::JoinType::{Existence, Inner, LeftAnti, LeftSemi};
        for (join_type, build_side, planned_type) in [
            (Inner, BuildLeft, DF::Inner),
            (Inner, BuildRight, DF::Inner),
            (LeftSemi, BuildRight, DF::RightSemi),
            (LeftAnti, BuildRight, DF::RightAnti),
            (Existence, BuildRight, DF::RightMark),
        ] {
            let operator = sorted_outer_hash_join(
                spark_operator::JoinType::RightOuter,
                BuildLeft,
                vec![
                    create_scan(),
                    hash_join_over_scans(join_type, build_side, false),
                ],
            );
            let (_, _, planned) = PhysicalPlanner::default()
                .create_plan(&operator, &mut vec![], 1)
                .unwrap();
            let probe = planned_hash_join(&planned.native_plan).right();
            assert_eq!(
                "SortedInputExec",
                probe.name(),
                "{join_type:?} {build_side:?}"
            );
            let declared = planned_hash_join(probe.children()[0]);
            assert_eq!(&planned_type, declared.join_type());
        }
    }

    #[test]
    fn hash_join_declares_no_order_over_an_operator_that_may_move_rows() {
        use spark_operator::BuildSide::{BuildLeft, BuildRight};
        use spark_operator::JoinType::{FullOuter, LeftAnti, LeftSemi, RightOuter};
        for (probe_input, reason) in [
            // Comet builds a null-aware anti join on Spark's streamed side, so its output is not
            // in the order Spark reports for it.
            (
                hash_join_over_scans(LeftAnti, BuildRight, true),
                "a null-aware anti join",
            ),
            // A right join whose probe declares no order moves its unmatched rows, although
            // DataFusion reports it as keeping its probe order.
            (
                hash_join_over_scans(RightOuter, BuildLeft, false),
                "a right outer join",
            ),
            // The remaining types keep no probe order at all: a left semi join built on the left
            // emits build rows, and a full outer join emits its unmatched build rows last.
            (
                hash_join_over_scans(LeftSemi, BuildLeft, false),
                "a left semi join",
            ),
            (
                hash_join_over_scans(FullOuter, BuildLeft, false),
                "a full outer join",
            ),
        ] {
            let operator =
                sorted_outer_hash_join(RightOuter, BuildLeft, vec![create_scan(), probe_input]);
            let (_, _, planned) = PhysicalPlanner::default()
                .create_plan(&operator, &mut vec![], 1)
                .unwrap();
            let probe = planned_hash_join(&planned.native_plan).right();
            assert_eq!("HashJoinExec", probe.name(), "{reason}");
        }
    }

    #[test]
    fn hash_join_keeps_probe_input_that_declares_an_order() {
        // A probe input that already declares an order, such as a sort in the native plan, is
        // left as it is.
        let sort = Operator {
            children: vec![create_scan()],
            op_struct: Some(OpStruct::Sort(spark_operator::Sort {
                sort_orders: vec![create_sort_order(0, 3, false, true)],
                ..Default::default()
            })),
            ..Default::default()
        };
        let operator = sorted_outer_hash_join(
            spark_operator::JoinType::RightOuter,
            spark_operator::BuildSide::BuildLeft,
            vec![create_scan(), sort],
        );
        let (_, _, planned) = PhysicalPlanner::default()
            .create_plan(&operator, &mut vec![], 1)
            .unwrap();
        assert_eq!(
            "SortExec",
            planned_hash_join(&planned.native_plan).right().name()
        );
    }

    #[test]
    fn declared_order_reaches_a_scan_of_one_file_group() {
        // A scan of files reads the files of its one file group one after the other, so the order
        // Spark reports for it, a sorted bucketed table's, holds. With more than one file group,
        // DataFusion may hand a file to another partition.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let scan = |groups: usize| -> Arc<dyn ExecutionPlan> {
            let source = Arc::new(ParquetSource::new(Arc::clone(&schema))) as Arc<dyn FileSource>;
            let file_groups = (0..groups)
                .map(|i| FileGroup::new(vec![PartitionedFile::new(format!("f{i}.parquet"), 8)]))
                .collect();
            let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
                .with_file_groups(file_groups)
                .build();
            Arc::new(DataSourceExec::new(Arc::new(config)))
        };
        assert!(PhysicalPlanner::keeps_source_order(&scan(1)));
        assert!(!PhysicalPlanner::keeps_source_order(&scan(2)));
    }

    #[tokio::test]
    async fn spark_plan_dynamic_filter_preserves_metrics() {
        use crate::execution::metrics::utils::to_native_metric_node;

        for build_side in [
            spark_operator::BuildSide::BuildLeft,
            spark_operator::BuildSide::BuildRight,
        ] {
            for enabled in [false, true] {
                let session = Arc::new(SessionContext::new());
                let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
                let operator = Operator {
                    children: vec![create_scan(), create_scan()],
                    op_struct: Some(OpStruct::HashJoin(spark_operator::HashJoin {
                        left_join_keys: vec![create_bound_reference(0)],
                        right_join_keys: vec![create_bound_reference(0)],
                        join_type: spark_operator::JoinType::Inner as i32,
                        build_side: build_side as i32,
                        dynamic_filter_enabled: enabled,
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let (mut scans, _, planned) =
                    planner.create_plan(&operator, &mut vec![], 1).unwrap();
                let build = vec![10, 20, 20, 90];
                let probe = (0..100).collect::<Vec<_>>();
                let inputs = if build_side == spark_operator::BuildSide::BuildLeft {
                    [build, probe]
                } else {
                    [probe, build]
                };
                let mut inputs = inputs.map(|values| {
                    values
                        .chunks(2)
                        .map(|chunk| {
                            InputBatch::Batch(
                                vec![Arc::new(Int32Array::from(chunk.to_vec())) as ArrayRef],
                                chunk.len(),
                            )
                        })
                        .chain(std::iter::once(InputBatch::EOF))
                        .collect::<Vec<_>>()
                        .into_iter()
                });
                let stream = planned.native_plan.execute(0, session.task_ctx()).unwrap();
                let batches = collect_with_inputs(stream, &mut scans, &mut inputs).await;
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
                let metrics = to_native_metric_node(&planned).unwrap();
                assert_eq!(metrics.children.len(), 2);
                assert_eq!(metrics.metrics["output_rows"], 4);
                assert_eq!(metrics.metrics["build_input_rows"], 4);
                if enabled {
                    assert_eq!(metrics.metrics["input_rows"], 3);
                    assert_eq!(metrics.metrics["dynamic_filter_join_rows_evaluated"], 100);
                    assert_eq!(metrics.metrics["dynamic_filter_join_rows_pruned"], 97);
                    assert_eq!(metrics.metrics["dynamic_filter_join_rows_bypassed"], 0);
                } else {
                    assert_eq!(metrics.metrics["input_rows"], 100);
                    assert!(!metrics
                        .metrics
                        .contains_key("dynamic_filter_join_rows_pruned"));
                }
            }
        }
    }

    #[tokio::test]
    async fn window_cast_projection_reports_window_time() {
        use crate::execution::metrics::utils::to_native_metric_node;
        use spark_operator::{
            lower_window_frame_bound::LowerFrameBoundStruct,
            upper_window_frame_bound::UpperFrameBoundStruct, CurrentRow, LowerWindowFrameBound,
            UnboundedPreceding, UpperWindowFrameBound, WindowExpr, WindowFrame, WindowFrameType,
            WindowSpecDefinition,
        };

        let session = Arc::new(SessionContext::new());
        let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
        // `row_number` returns UInt64 natively and Spark expects Int32, so the planner adds a
        // casting projection above the window.
        let row_number = WindowExpr {
            built_in_window_function: Some(Expr {
                expr_struct: Some(ScalarFunc(spark_expression::ScalarFunc {
                    func: "row_number".to_string(),
                    ..Default::default()
                })),
                query_context: None,
                expr_id: None,
            }),
            spec: Some(WindowSpecDefinition {
                frame_specification: Some(WindowFrame {
                    frame_type: WindowFrameType::Rows as i32,
                    lower_bound: Some(LowerWindowFrameBound {
                        lower_frame_bound_struct: Some(LowerFrameBoundStruct::UnboundedPreceding(
                            UnboundedPreceding {},
                        )),
                    }),
                    upper_bound: Some(UpperWindowFrameBound {
                        upper_frame_bound_struct: Some(UpperFrameBoundStruct::CurrentRow(
                            CurrentRow {},
                        )),
                    }),
                }),
                ..Default::default()
            }),
            result_type: Some(create_proto_datatype()),
            ..Default::default()
        };
        let operator = Operator {
            children: vec![create_scan()],
            op_struct: Some(OpStruct::Window(Box::new(spark_operator::Window {
                window_expr: vec![row_number],
                order_by_list: vec![Expr {
                    expr_struct: Some(SortOrder(Box::new(spark_expression::SortOrder {
                        child: Some(Box::new(create_bound_reference(0))),
                        direction: 0,
                        null_ordering: 0,
                    }))),
                    query_context: None,
                    expr_id: None,
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };
        let (mut scans, _, planned) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
        assert_eq!(planned.additional_native_plans.len(), 1);

        let rows = 100;
        let mut inputs = [vec![
            InputBatch::Batch(
                vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<_>>())) as ArrayRef],
                rows as usize,
            ),
            InputBatch::EOF,
        ]
        .into_iter()];
        let stream = planned.native_plan.execute(0, session.task_ctx()).unwrap();
        let batches = collect_with_inputs(stream, &mut scans, &mut inputs).await;
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            rows as usize
        );

        let elapsed = |plan: &Arc<dyn ExecutionPlan>| {
            plan.metrics()
                .and_then(|m| m.elapsed_compute())
                .unwrap_or(0) as i64
        };
        let window_time = elapsed(&planned.additional_native_plans[0]);
        let projection_time = elapsed(&planned.native_plan);
        assert!(window_time > 0);

        let metrics = to_native_metric_node(&planned).unwrap();
        // Output rows come from the projection only, and the time covers both operators.
        assert_eq!(metrics.metrics["output_rows"], rows as i64);
        assert_eq!(
            metrics.metrics["elapsed_compute"],
            window_time + projection_time
        );
    }

    #[test]
    fn projection_rolls_up_same_plan_id_child_metrics() {
        let op_scan = Operator {
            plan_id: 1,
            ..create_scan()
        };
        let hash_agg = Operator {
            plan_id: 2,
            sql_text_pool: vec![],
            children: vec![op_scan],
            op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
                grouping_exprs: vec![create_bound_reference(0)],
                agg_exprs: vec![],
                mode: spark_operator::AggregateMode::Partial as i32,
                expr_modes: vec![],
                initial_input_buffer_offset: 0,
                ordered_by_grouping_keys: false,
            })),
        };
        let projection = Operator {
            plan_id: 2,
            sql_text_pool: vec![],
            children: vec![hash_agg],
            op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                project_list: vec![create_bound_reference(0)],
            })),
        };

        let planner = PhysicalPlanner::default();
        let (_scans, _shuffle_scans, projection_exec) =
            planner.create_plan(&projection, &mut vec![], 1).unwrap();

        assert_eq!("ProjectionExec", projection_exec.native_plan.name());
        assert_eq!(1, projection_exec.additional_native_plans.len());
        assert_eq!(
            "AggregateExec",
            projection_exec.additional_native_plans[0].name()
        );
        assert_eq!(1, projection_exec.children.len());
        assert_eq!("ScanExec", projection_exec.children[0].native_plan.name());
    }

    #[tokio::test]
    async fn keyless_aggregate_without_functions_emits_one_empty_row() {
        use arrow::array::RecordBatchOptions;

        let scan = Operator {
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![],
                source: String::new(),
            })),
            ..Default::default()
        };
        let schema = Arc::new(Schema::empty());
        let rows = |num_rows| {
            RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            )
            .unwrap()
        };
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::new(ctx), 0);

        // Spark serializes the stage below its exchange as Partial and the one above as Final.
        // Both emit one row with no columns per partition, even for an empty input.
        for mode in [
            spark_operator::AggregateMode::Partial,
            spark_operator::AggregateMode::Final,
        ] {
            let aggregate = Operator {
                plan_id: 1,
                children: vec![scan.clone()],
                op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
                    mode: mode as i32,
                    ..Default::default()
                })),
                ..Default::default()
            };
            let (_, _, planned) = planner.create_plan(&aggregate, &mut vec![], 1).unwrap();
            assert_eq!("ProjectionExec", planned.native_plan.name());
            assert_eq!(0, planned.schema().fields().len());
            assert_eq!(1, planned.additional_native_plans.len());
            assert_eq!("AggregateExec", planned.additional_native_plans[0].name());
            assert_eq!(1, planned.children.len());
            assert_eq!("ScanExec", planned.children[0].native_plan.name());

            for batches in [vec![rows(3), rows(2)], vec![]] {
                let results = collect_over_batches(&planned, batches, Arc::clone(&task_ctx)).await;
                assert_eq!(1, results.iter().map(RecordBatch::num_rows).sum::<usize>());
            }
        }
    }

    #[tokio::test]
    async fn sort_aggregate_output_is_ordered_by_grouping_keys() {
        use arrow::datatypes::Int32Type;
        use spark_expression::data_type::{data_type_info::DatatypeStruct, DataTypeInfo, ListInfo};

        let array_type = spark_expression::DataType {
            type_id: 14,
            type_info: Some(Box::new(DataTypeInfo {
                datatype_struct: Some(DatatypeStruct::List(Box::new(ListInfo {
                    element_type: Some(Box::new(create_proto_datatype())),
                    contains_null: true,
                    element_field_id: None,
                }))),
            })),
        };
        let key = Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(array_type.clone()),
            })),
            ..Default::default()
        };
        let scan = Operator {
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![array_type],
                source: String::new(),
            })),
            ..Default::default()
        };
        let sort = Operator {
            children: vec![scan.clone()],
            op_struct: Some(OpStruct::Sort(spark_operator::Sort {
                sort_orders: vec![Expr {
                    expr_struct: Some(SortOrder(Box::new(spark_expression::SortOrder {
                        child: Some(Box::new(key.clone())),
                        direction: spark_expression::SortDirection::Ascending as i32,
                        null_ordering: spark_expression::NullOrdering::NullsFirst as i32,
                    }))),
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        let aggregate = |child: Operator| Operator {
            plan_id: 1,
            children: vec![child],
            op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
                grouping_exprs: vec![key.clone()],
                mode: spark_operator::AggregateMode::Final as i32,
                ordered_by_grouping_keys: true,
                ..Default::default()
            })),
            ..Default::default()
        };
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::new(ctx), 0);

        // DataFusion sees a sort in the same native plan and emits the groups in input order.
        let (_, _, planned) = planner
            .create_plan(&aggregate(sort), &mut vec![], 1)
            .unwrap();
        assert_eq!("AggregateExec", planned.native_plan.name());

        // An input ordered outside the native plan, as a cached sorted relation is, is not visible
        // to DataFusion. NULL and an empty list hash alike, so its vectorized grouping would emit
        // the empty list after [1] if the planner did not sort the output.
        let (_, _, planned) = planner
            .create_plan(&aggregate(scan), &mut vec![], 1)
            .unwrap();
        assert_eq!("SortExec", planned.native_plan.name());
        assert_eq!(1, planned.additional_native_plans.len());
        assert_eq!("AggregateExec", planned.additional_native_plans[0].name());

        let keys = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            None,
            Some(vec![]),
            Some(vec![Some(1)]),
        ])) as ArrayRef;
        let batch =
            RecordBatch::try_new(planned.children[0].schema(), vec![Arc::clone(&keys)]).unwrap();
        let results = collect_over_batches(&planned, vec![batch], task_ctx).await;
        let output = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
        assert_eq!(keys.as_ref(), output.column(0).as_ref());
    }

    #[test]
    fn sort_aggregate_float_key_streams_over_native_sort() {
        // A native sort normalizes a float key, so the aggregate above it must group on the
        // normalized key too for DataFusion to see the ordering and skip the output sort.
        let double_type = spark_expression::DataType {
            type_id: 6,
            type_info: None,
        };
        let key = Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(double_type.clone()),
            })),
            ..Default::default()
        };
        let op = Operator {
            plan_id: 1,
            children: vec![Operator {
                children: vec![Operator {
                    op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                        fields: vec![double_type],
                        source: String::new(),
                    })),
                    ..Default::default()
                }],
                op_struct: Some(OpStruct::Sort(spark_operator::Sort {
                    sort_orders: vec![Expr {
                        expr_struct: Some(SortOrder(Box::new(spark_expression::SortOrder {
                            child: Some(Box::new(key.clone())),
                            direction: spark_expression::SortDirection::Ascending as i32,
                            null_ordering: spark_expression::NullOrdering::NullsFirst as i32,
                        }))),
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            }],
            op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
                grouping_exprs: vec![key],
                mode: spark_operator::AggregateMode::Final as i32,
                ordered_by_grouping_keys: true,
                ..Default::default()
            })),
            ..Default::default()
        };
        let planner = PhysicalPlanner::default();
        let (_, _, planned) = planner.create_plan(&op, &mut vec![], 1).unwrap();
        assert_eq!("AggregateExec", planned.native_plan.name());
    }

    #[tokio::test]
    async fn partial_merge_runs_as_single_and_emits_merged_states() {
        use arrow::array::{BooleanArray, Int64Array};
        use datafusion::assert_batches_sorted_eq;
        use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
        use spark_expression::agg_expr::ExprStruct as AggExprStruct;

        // A Spark PartialMerge of max(v), count(v) and first(v) grouped by k, over k and their
        // states. DataFusion's first has no GroupsAccumulator without an ordering.
        let int64 = create_int64_datatype();
        let boolean = spark_expression::DataType {
            type_id: 0,
            type_info: None,
        };
        let v = create_unbound_int64("v");
        let partial_merge = spark_operator::AggregateMode::PartialMerge as i32;
        let aggregate = |mode: i32| Operator {
            plan_id: 1,
            children: vec![Operator {
                op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                    fields: vec![
                        int64.clone(),
                        int64.clone(),
                        int64.clone(),
                        int64.clone(),
                        boolean.clone(),
                    ],
                    source: String::new(),
                })),
                ..Default::default()
            }],
            op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
                grouping_exprs: vec![Expr {
                    expr_struct: Some(Bound(spark_expression::BoundReference {
                        index: 0,
                        datatype: Some(int64.clone()),
                    })),
                    ..Default::default()
                }],
                agg_exprs: vec![
                    create_max_agg(&v),
                    create_count_agg(&v),
                    spark_expression::AggExpr {
                        expr_struct: Some(AggExprStruct::First(spark_expression::First {
                            child: Some(v.clone()),
                            datatype: Some(int64.clone()),
                            ignore_nulls: false,
                        })),
                        ..Default::default()
                    },
                ],
                mode,
                expr_modes: vec![partial_merge; 3],
                initial_input_buffer_offset: 1,
                ..Default::default()
            })),
            ..Default::default()
        };
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::new(ctx), 0);

        // DataFusion's Single spills rather than emit a key twice, and the projection above it
        // unpacks the states into the columns that the Partial it replaces emitted.
        let (_, _, planned) = planner
            .create_plan(&aggregate(partial_merge), &mut vec![], 1)
            .unwrap();
        assert_eq!("ProjectionExec", planned.native_plan.name());
        assert_eq!(1, planned.additional_native_plans.len());
        let single = planned.additional_native_plans[0]
            .downcast_ref::<AggregateExec>()
            .unwrap();
        assert_eq!(&AggregateMode::Single, single.mode());
        let (_, _, partial) = planner
            .create_plan(
                &aggregate(spark_operator::AggregateMode::Partial as i32),
                &mut vec![],
                1,
            )
            .unwrap();
        assert_eq!("AggregateExec", partial.native_plan.name());
        assert_eq!(partial.native_plan.schema(), planned.native_plan.schema());

        let batch = RecordBatch::try_new(
            planned.children[0].schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1, 2, 3])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(5),
                    Some(7),
                    None,
                    Some(1),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 0, 4])),
                Arc::new(Int64Array::from(vec![
                    Some(100),
                    None,
                    Some(101),
                    Some(200),
                    None,
                ])),
                Arc::new(BooleanArray::from(vec![true, false, true, true, false])),
            ],
        )
        .unwrap();
        let results = collect_over_batches(&planned, vec![batch], task_ctx).await;
        assert_batches_sorted_eq!(
            [
                "+-------+------------+--------------+--------------------+---------------------------+",
                "| col_0 | max[value] | count[count] | first[first_value] | first[first_value_is_set] |",
                "+-------+------------+--------------+--------------------+---------------------------+",
                "| 1     | 10         | 4            | 100                | true                      |",
                "| 2     | 5          | 2            | 200                | true                      |",
                "| 3     | 1          | 4            |                    | false                     |",
                "+-------+------------+--------------+--------------------+---------------------------+",
            ],
            &results
        );
    }

    #[tokio::test]
    async fn partial_merge_over_sorted_input_runs_as_final() {
        use arrow::array::Int64Array;
        use datafusion::assert_batches_sorted_eq;
        use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
        use datafusion::physical_plan::InputOrderMode;
        use spark_expression::agg_expr::ExprStruct as AggExprStruct;

        // A Spark PartialMerge grouped by (g, k) over a native sort on g, which DataFusion 55's
        // Single would run with a GroupedHashAggregateStream that can fail to spill (#6880).
        let int64 = create_int64_datatype();
        let v = create_unbound_int64("v");
        let key = |index: i32| Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index,
                datatype: Some(int64.clone()),
            })),
            ..Default::default()
        };
        let partial_merge = spark_operator::AggregateMode::PartialMerge as i32;
        let aggregate = |sort_keys: &[i32],
                         state_fields: Vec<spark_expression::DataType>,
                         agg_exprs: Vec<spark_expression::AggExpr>| {
            Operator {
                plan_id: 1,
                children: vec![Operator {
                    children: vec![Operator {
                        op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                            fields: [vec![int64.clone(), int64.clone()], state_fields].concat(),
                            source: String::new(),
                        })),
                        ..Default::default()
                    }],
                    op_struct: Some(OpStruct::Sort(spark_operator::Sort {
                        sort_orders: sort_keys
                            .iter()
                            .map(|&index| create_sort_order(index, 4, false, true))
                            .collect(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
                    grouping_exprs: vec![key(0), key(1)],
                    expr_modes: vec![partial_merge; agg_exprs.len()],
                    agg_exprs,
                    mode: partial_merge,
                    initial_input_buffer_offset: 2,
                    ..Default::default()
                })),
                ..Default::default()
            }
        };
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::new(ctx), 0);

        // Over native max and count states, DataFusion's Final runs an ordered aggregate that
        // spills, and the projection above it unpacks the states as it does above Single.
        let max_count = |sort_keys: &[i32]| {
            aggregate(
                sort_keys,
                vec![int64.clone(), int64.clone()],
                vec![create_max_agg(&v), create_count_agg(&v)],
            )
        };
        let (_, _, planned) = planner
            .create_plan(&max_count(&[0]), &mut vec![], 1)
            .unwrap();
        assert_eq!("ProjectionExec", planned.native_plan.name());
        let final_agg = planned.additional_native_plans[0]
            .downcast_ref::<AggregateExec>()
            .unwrap();
        assert_eq!(&AggregateMode::Final, final_agg.mode());
        assert_eq!(
            &InputOrderMode::PartiallySorted(vec![0]),
            final_agg.input_order_mode()
        );

        let input_schema = planned.children[0].schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 1, 2, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 1, 2])),
                Arc::new(Int64Array::from(vec![
                    Some(4),
                    Some(10),
                    None,
                    Some(9),
                    Some(3),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 0, 3, 4])),
            ],
        )
        .unwrap();
        let results = collect_over_batches(&planned, vec![batch], Arc::clone(&task_ctx)).await;
        assert_batches_sorted_eq!(
            [
                "+-------+-------+------------+--------------+",
                "| col_0 | col_1 | max[value] | count[count] |",
                "+-------+-------+------------+--------------+",
                "| 1     | 1     |            | 0            |",
                "| 1     | 2     | 10         | 6            |",
                "| 2     | 1     | 9          | 4            |",
                "+-------+-------+------------+--------------+",
            ],
            &results
        );

        // Final reads the keys from the leading columns and each state from the columns after
        // them, so a PartialMerge whose input is laid out any other way stays Single. So does one
        // over input sorted on every key, where Final has no spill path to offer.
        let with_hash_agg =
            |mut op: Operator, edit: &dyn Fn(&mut spark_operator::HashAggregate)| {
                if let Some(OpStruct::HashAgg(agg)) = op.op_struct.as_mut() {
                    edit(agg);
                }
                op
            };
        // Spark's partial collect_list hands over its buffers as binary, which Final would spill
        // as list states into binary columns.
        let collect_list = aggregate(
            &[0],
            vec![spark_expression::DataType {
                type_id: 8,
                type_info: None,
            }],
            vec![spark_expression::AggExpr {
                expr_struct: Some(AggExprStruct::CollectList(spark_expression::CollectList {
                    child: Some(v.clone()),
                    datatype: Some(int64.clone()),
                })),
                ..Default::default()
            }],
        );
        let swapped_keys = with_hash_agg(max_count(&[0]), &|agg| {
            agg.grouping_exprs = vec![key(1), key(0)];
        });
        let states_after_one_key = with_hash_agg(max_count(&[0]), &|agg| {
            agg.initial_input_buffer_offset = 1;
        });
        let extra_column = aggregate(
            &[0],
            vec![int64.clone(), int64.clone(), int64.clone()],
            vec![create_max_agg(&v), create_count_agg(&v)],
        );
        for (case, op) in [
            ("collect_list buffers", collect_list),
            ("swapped keys", swapped_keys),
            ("states after one key", states_after_one_key),
            ("a column after the states", extra_column),
            ("sorted on every key", max_count(&[0, 1])),
        ] {
            let (_, _, planned) = planner.create_plan(&op, &mut vec![], 1).unwrap();
            let single = planned.additional_native_plans[0]
                .downcast_ref::<AggregateExec>()
                .unwrap();
            assert_eq!(&AggregateMode::Single, single.mode(), "{case}");
            assert_ne!(&InputOrderMode::Linear, single.input_order_mode(), "{case}");
        }
    }

    #[tokio::test]
    async fn projection_prunes_filter_output_and_preserves_metrics() {
        use crate::execution::metrics::utils::to_native_metric_node;
        use datafusion::physical_plan::filter::FilterExec;

        // Duplicate outputs must reuse a filtered column, even when they outnumber the inputs.
        for (indices, output) in [
            (vec![], Some(vec![])),
            (vec![2], Some(vec![2])),
            (vec![2, 1, 2], Some(vec![1, 2])),
            (vec![2, 1, 2, 1, 2], Some(vec![1, 2])),
            (vec![0, 1, 2, 3], None),
            (vec![3, 2, 1, 0], None),
        ] {
            for project_id in [2, 3] {
                let scan = Operator {
                    plan_id: 1,
                    op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                        fields: vec![create_proto_datatype(); 4],
                        source: String::new(),
                    })),
                    ..Default::default()
                };
                let filter = Operator {
                    plan_id: 2,
                    ..create_filter(scan, 1)
                };
                let project = Operator {
                    plan_id: project_id,
                    children: vec![filter],
                    op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                        project_list: indices.iter().map(|&i| create_bound_reference(i)).collect(),
                    })),
                    ..Default::default()
                };
                let planner = PhysicalPlanner::default();
                let (mut scans, _, planned) =
                    planner.create_plan(&project, &mut vec![], 1).unwrap();
                let filter_plan = &planned.native_plan.children()[0];
                let filter = filter_plan.downcast_ref::<FilterExec>().unwrap();
                assert_eq!(filter.projection().as_deref(), output.as_deref());
                assert_eq!(filter.input().schema().fields().len(), 4);
                for (index, field) in planned.schema().fields().iter().enumerate() {
                    assert_eq!(field.name(), &format!("col_{index}"));
                }
                let columns = vec![
                    Arc::new(Int32Array::from(vec![1, 0, 1])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![20, 21, 22])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![30, 31, 32])) as ArrayRef,
                ];
                let mut inputs = [vec![
                    InputBatch::Batch(columns.clone(), 3),
                    InputBatch::Batch(columns, 3),
                    InputBatch::EOF,
                ]
                .into_iter()];
                let stream = planned
                    .native_plan
                    .execute(0, SessionContext::new().task_ctx())
                    .unwrap();
                let mut rows = 0;
                for batch in collect_with_inputs(stream, &mut scans, &mut inputs).await {
                    assert_eq!(batch.num_columns(), indices.len());
                    for row in 0..batch.num_rows() {
                        for (column, &source) in indices.iter().enumerate() {
                            let values = batch
                                .column(column)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();
                            let expected = if source == 0 {
                                1
                            } else {
                                source * 10 + ((rows + row) % 2) as i32 * 2
                            };
                            assert_eq!(values.value(row), expected);
                        }
                    }
                    rows += batch.num_rows();
                }
                assert_eq!(rows, 4);
                let metrics = to_native_metric_node(&planned).unwrap();
                if project_id == 2 {
                    assert_eq!(planned.additional_native_plans.len(), 1);
                    assert!(Arc::ptr_eq(
                        filter_plan,
                        &planned.additional_native_plans[0]
                    ));
                    // A single Spark node must not count the filter and project rows twice.
                    assert_eq!(metrics.metrics["output_rows"], 4);
                } else {
                    assert_eq!(metrics.metrics["output_rows"], 4);
                    assert_eq!(metrics.children[0].metrics["output_rows"], 4);
                    assert!(metrics.children[0].metrics["elapsed_compute"] > 0);
                }
            }
        }
    }

    fn create_bound_reference(index: i32) -> Expr {
        Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index,
                datatype: Some(create_proto_datatype()),
            })),
            query_context: None,
            expr_id: None,
        }
    }

    /// Drives `stream` to completion, feeding each scan its next input batch whenever the stream
    /// is pending and that scan has none, and returns the output batches.
    async fn collect_with_inputs(
        mut stream: SendableRecordBatchStream,
        scans: &mut [ScanExec],
        inputs: &mut [std::vec::IntoIter<InputBatch>],
    ) -> Vec<RecordBatch> {
        let mut batches = vec![];
        while let Some(batch) = futures::future::poll_fn(|cx| {
            let result = stream.poll_next_unpin(cx);
            if result.is_pending() {
                for (scan, input) in scans.iter_mut().zip(inputs.iter_mut()) {
                    if scan.batch.try_lock().unwrap().is_none() {
                        if let Some(batch) = input.next() {
                            scan.set_input_batch(batch);
                            cx.waker().wake_by_ref();
                        }
                    }
                }
            }
            result
        })
        .await
        {
            batches.push(batch.unwrap());
        }
        batches
    }

    fn create_scan() -> Operator {
        Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![create_proto_datatype()],
                source: "".to_string(),
            })),
        }
    }

    fn create_proto_datatype() -> spark_expression::DataType {
        spark_expression::DataType {
            type_id: 3,
            type_info: None,
        }
    }

    fn create_int64_datatype() -> spark_expression::DataType {
        spark_expression::DataType {
            type_id: 4,
            type_info: None,
        }
    }

    /// An unbound reference to the Int64 column `name`, as a Spark aggregate function's child.
    fn create_unbound_int64(name: &str) -> Expr {
        Expr {
            expr_struct: Some(Unbound(spark_expression::UnboundReference {
                name: name.to_string(),
                datatype: Some(create_int64_datatype()),
            })),
            ..Default::default()
        }
    }

    fn create_max_agg(child: &Expr) -> spark_expression::AggExpr {
        spark_expression::AggExpr {
            expr_struct: Some(spark_expression::agg_expr::ExprStruct::Max(
                spark_expression::Max {
                    child: Some(child.clone()),
                    datatype: Some(create_int64_datatype()),
                },
            )),
            ..Default::default()
        }
    }

    fn create_count_agg(child: &Expr) -> spark_expression::AggExpr {
        spark_expression::AggExpr {
            expr_struct: Some(spark_expression::agg_expr::ExprStruct::Count(
                spark_expression::Count {
                    children: vec![child.clone()],
                },
            )),
            ..Default::default()
        }
    }

    /// Runs `planned` with `batches` standing in for its single `ScanExec` and returns the
    /// output batches.
    async fn collect_over_batches(
        planned: &crate::execution::spark_plan::SparkPlan,
        batches: Vec<RecordBatch>,
        task_ctx: Arc<datafusion::execution::TaskContext>,
    ) -> Vec<RecordBatch> {
        use datafusion::common::tree_node::{Transformed, TreeNode};

        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[batches], planned.children[0].schema(), None)
                .unwrap();
        let native_plan = Arc::clone(&planned.native_plan)
            .transform_up(|node| {
                Ok(if node.name() == "ScanExec" {
                    Transformed::yes(Arc::clone(&input))
                } else {
                    Transformed::no(node)
                })
            })
            .unwrap()
            .data;
        collect(native_plan.execute(0, task_ctx).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn explode_evaluates_array_once_per_batch() {
        use arrow::datatypes::Int32Type;
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::logical_expr::{create_udf, Volatility};
        use datafusion::physical_plan::projection::ProjectionExec;
        use spark_expression::data_type::{data_type_info::DatatypeStruct, DataTypeInfo, ListInfo};

        let array_type = spark_expression::DataType {
            type_id: 14,
            type_info: Some(Box::new(DataTypeInfo {
                datatype_struct: Some(DatatypeStruct::List(Box::new(ListInfo {
                    element_type: Some(Box::new(create_proto_datatype())),
                    contains_null: true,
                    element_field_id: None,
                }))),
            })),
        };
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10), None]),
            Some(vec![]),
            None,
            Some(vec![Some(20)]),
        ])) as ArrayRef;

        for outer in [false, true] {
            for position in [false, true] {
                for computed in [false, true] {
                    let calls = Arc::new(AtomicUsize::new(0));
                    let counter = Arc::clone(&calls);
                    let ctx = SessionContext::new();
                    ctx.register_udf(create_udf(
                        "counted_array",
                        vec![arrays.data_type().clone()],
                        arrays.data_type().clone(),
                        Volatility::Immutable,
                        Arc::new(move |args| {
                            counter.fetch_add(1, Ordering::Relaxed);
                            Ok(args[0].clone())
                        }),
                    ));
                    let task_ctx = ctx.task_ctx();
                    let planner = PhysicalPlanner::new(Arc::new(ctx), 0);
                    let bound = Expr {
                        expr_struct: Some(Bound(spark_expression::BoundReference {
                            index: 0,
                            datatype: Some(array_type.clone()),
                        })),
                        ..Default::default()
                    };
                    let child_expr = if computed {
                        Expr {
                            expr_struct: Some(ScalarFunc(spark_expression::ScalarFunc {
                                func: "counted_array".to_string(),
                                args: vec![bound.clone()],
                                ..Default::default()
                            })),
                            ..Default::default()
                        }
                    } else {
                        bound.clone()
                    };
                    let op = Operator {
                        children: vec![Operator {
                            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                                fields: vec![array_type.clone()],
                                source: String::new(),
                            })),
                            ..Default::default()
                        }],
                        op_struct: Some(OpStruct::Explode(spark_operator::Explode {
                            child: Some(child_expr),
                            outer,
                            position,
                            project_list: vec![bound],
                        })),
                        ..Default::default()
                    };
                    let (_, _, plan) = planner.create_plan(&op, &mut vec![], 1).unwrap();
                    let schema = plan.children[0].schema();
                    let batch = RecordBatch::try_new(schema, vec![Arc::clone(&arrays)]).unwrap();
                    let input: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
                        &[vec![batch.clone(), batch.clone()]],
                        batch.schema(),
                        None,
                    )
                    .unwrap();
                    let mut projections = 0;
                    // Replace only the JNI-fed scan, keeping the planner's generated operators.
                    let native_plan = Arc::clone(&plan.native_plan)
                        .transform_up(|node| {
                            projections += usize::from(node.is::<ProjectionExec>());
                            Ok(if node.name() == "ScanExec" {
                                Transformed::yes(Arc::clone(&input))
                            } else {
                                Transformed::no(node)
                            })
                        })
                        .unwrap()
                        .data;
                    let results = collect(native_plan.execute(0, task_ctx).unwrap())
                        .await
                        .unwrap();
                    let context =
                        format!("outer={outer}, position={position}, computed={computed}");
                    assert_eq!(
                        calls.load(Ordering::Relaxed),
                        if computed { 2 } else { 0 },
                        "{context}"
                    );
                    // The array is pre-projected only to share one evaluation between the
                    // `pos` and `value` references, so only a computed child needs it. `outer`
                    // does not, since it is now a `UnnestOptions` mode rather than a wrapper
                    // expression around the child.
                    assert_eq!(
                        projections,
                        1 + usize::from(position && computed),
                        "{context}"
                    );
                    let expected_values = if outer {
                        vec![Some(10), None, None, None, Some(20)]
                    } else {
                        vec![Some(10), None, Some(20)]
                    };
                    let values: Vec<_> = results
                        .iter()
                        .flat_map(|batch| {
                            batch
                                .column(batch.num_columns() - 1)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .iter()
                        })
                        .collect();
                    assert_eq!(values, expected_values.repeat(2), "{context}");
                    if position {
                        let expected_positions = if outer {
                            vec![Some(0), Some(1), None, None, Some(0)]
                        } else {
                            vec![Some(0), Some(1), Some(0)]
                        };
                        let positions: Vec<_> = results
                            .iter()
                            .flat_map(|batch| {
                                batch
                                    .column(1)
                                    .as_any()
                                    .downcast_ref::<Int32Array>()
                                    .unwrap()
                                    .iter()
                            })
                            .collect();
                        assert_eq!(positions, expected_positions.repeat(2), "{context}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_create_array() {
        let session_ctx = SessionContext::new();
        session_ctx.register_udf(ScalarUDF::from(
            datafusion_functions_nested::make_array::MakeArray::new(),
        ));
        let task_ctx = session_ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::from(session_ctx), 0);

        // Create a plan for
        // ProjectionExec: expr=[make_array(col_0@0) as col_0]
        // ScanExec: source=[CometScan parquet  (unknown)], schema=[col_0: Int32]
        let op_scan = Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                ],
                source: "".to_string(),
            })),
        };

        let array_col = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
            query_context: None,
            expr_id: None,
        };

        let array_col_1 = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 1,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
            query_context: None,
            expr_id: None,
        };

        let projection = Operator {
            children: vec![op_scan],
            plan_id: 0,
            sql_text_pool: vec![],
            op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                project_list: vec![spark_expression::Expr {
                    expr_struct: Some(ExprStruct::ScalarFunc(spark_expression::ScalarFunc {
                        func: "make_array".to_string(),
                        args: vec![array_col, array_col_1],
                        return_type: None,
                        fail_on_error: false,
                        null_short_circuit: false,
                    })),
                    query_context: None,
                    expr_id: None,
                }],
            })),
        };

        let (mut scans, _shuffle_scans, datafusion_plan) =
            planner.create_plan(&projection, &mut vec![], 1).unwrap();

        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        // Separate thread to send the EOF signal once we've processed the only input batch
        runtime.spawn(async move {
            let a = Int32Array::from(vec![0, 3]);
            let b = Int32Array::from(vec![1, 4]);
            let c = Int32Array::from(vec![2, 5]);
            let input_batch1 = InputBatch::Batch(vec![Arc::new(a), Arc::new(b), Arc::new(c)], 2);
            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), 2);
                        let expected = [
                            "+--------+",
                            "| col_0  |",
                            "+--------+",
                            "| [0, 1] |",
                            "| [3, 4] |",
                            "+--------+",
                        ];
                        assert_batches_eq!(expected, &[batch]);
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    #[test]
    fn test_array_repeat() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let planner = PhysicalPlanner::new(Arc::from(session_ctx), 0);

        // Mock scan operator with 3 INT32 columns
        let op_scan = Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 3, // Int32
                        type_info: None,
                    },
                ],
                source: "".to_string(),
            })),
        };

        // Mock expression to read a INT32 column with position 0
        let array_col = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
            query_context: None,
            expr_id: None,
        };

        // Mock expression to read a INT32 column with position 1
        let array_col_1 = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 1,
                datatype: Some(spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }),
            })),
            query_context: None,
            expr_id: None,
        };

        // Make a projection operator with array_repeat(array_col, array_col_1)
        let projection = Operator {
            children: vec![op_scan],
            plan_id: 0,
            sql_text_pool: vec![],
            op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                project_list: vec![spark_expression::Expr {
                    expr_struct: Some(ExprStruct::ScalarFunc(spark_expression::ScalarFunc {
                        func: "array_repeat".to_string(),
                        args: vec![array_col, array_col_1],
                        return_type: None,
                        fail_on_error: false,
                        null_short_circuit: false,
                    })),
                    query_context: None,
                    expr_id: None,
                }],
            })),
        };

        // Create a physical plan
        let (mut scans, _shuffle_scans, datafusion_plan) =
            planner.create_plan(&projection, &mut vec![], 1).unwrap();

        // Start executing the plan in a separate thread
        // The plan waits for incoming batches and emitting result as input comes
        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        // create async channel
        let (tx, mut rx) = mpsc::channel(1);

        // Send data as input to the plan being executed in a separate thread
        runtime.spawn(async move {
            // create data batch
            // 0, 1, 2
            // 3, 4, 5
            // 6, null, null
            let a = Int32Array::from(vec![Some(0), Some(3), Some(6)]);
            let b = Int32Array::from(vec![Some(1), Some(4), None]);
            let c = Int32Array::from(vec![Some(2), Some(5), None]);
            let input_batch1 = InputBatch::Batch(vec![Arc::new(a), Arc::new(b), Arc::new(c)], 3);
            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        // Wait for the plan to finish executing and assert the result
        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        let expected = [
                            "+--------------+",
                            "| col_0        |",
                            "+--------------+",
                            "| [0]          |",
                            "| [3, 3, 3, 3] |",
                            "|              |",
                            "+--------------+",
                        ];
                        assert_batches_eq!(expected, &[batch]);
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    /// Executes a `test_data_query` SQL query
    /// and saves the result into a temp folder using parquet format
    /// Read the file back to the memory using a custom schema
    async fn make_parquet_data(
        test_data_query: &str,
        read_schema: Schema,
    ) -> Result<RecordBatch, DataFusionError> {
        let session_ctx = SessionContext::new();

        // generate test data in the temp folder
        let tmp_dir = TempDir::new()?;
        let test_path = tmp_dir.path().to_str().unwrap();

        let plan = session_ctx
            .sql(test_data_query)
            .await?
            .create_physical_plan()
            .await?;

        // Write a parquet file into temp folder
        session_ctx.write_parquet(plan, test_path, None).await?;

        // Register all parquet with temp data as file groups
        let mut file_groups: Vec<FileGroup> = vec![];
        for entry in std::fs::read_dir(test_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                if let Some(path_str) = path.to_str() {
                    file_groups.push(FileGroup::new(vec![PartitionedFile::from_path(
                        path_str.into(),
                    )?]));
                }
            }
        }

        let source = Arc::new(
            ParquetSource::new(Arc::new(read_schema.clone()))
                .with_table_parquet_options(TableParquetOptions::new()),
        ) as Arc<dyn FileSource>;

        let spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC");

        let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
            SparkPhysicalExprAdapterFactory::new(spark_parquet_options, None),
        );

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let file_scan_config = FileScanConfigBuilder::new(object_store_url, source)
            .with_expr_adapter(Some(expr_adapter_factory))
            .with_file_groups(file_groups)
            .build();

        // Run native read
        let scan = Arc::new(DataSourceExec::new(Arc::new(file_scan_config.clone())));
        let result: Vec<_> = scan.execute(0, session_ctx.task_ctx())?.collect().await;
        Ok(result.first().unwrap().as_ref().unwrap().clone())
    }

    /*
    Testing a nested types scenario

    select arr[0].a, arr[0].c from (
        select array(named_struct('a', 1, 'b', 'n', 'c', 'x')) arr)
     */
    #[tokio::test]
    async fn test_nested_types_list_of_struct_by_index() -> Result<(), DataFusionError> {
        let test_data =
            "select make_array(named_struct('a', cast(1 as int), 'b', 'n', 'c', 'x')) c0";

        // Define schema Comet reads with
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::List(
                Field::new(
                    "element",
                    DataType::Struct(Fields::from(vec![
                        Field::new("a", DataType::Int32, true),
                        Field::new("c", DataType::Utf8, true),
                    ] as Vec<Field>)),
                    true,
                )
                .into(),
            ),
            true,
        )]));

        let actual = make_parquet_data(test_data, required_schema).await?;

        let expected = [
            "+----------------+",
            "| c0             |",
            "+----------------+",
            "| [{a: 1, c: x}] |",
            "+----------------+",
        ];
        assert_batches_eq!(expected, &[actual]);

        Ok(())
    }

    /*
    Testing a nested types scenario map[struct, struct]

    select map_keys(m).b from (
        select map(named_struct('a', 1, 'b', 'n', 'c', 'x'), named_struct('a', 1, 'b', 'n', 'c', 'x')) m
     */
    #[tokio::test]
    async fn test_nested_types_map_keys() -> Result<(), DataFusionError> {
        let test_data = "select map([named_struct('a', 1, 'b', 'n', 'c', 'x')], [named_struct('a', 2, 'b', 'm', 'c', 'y')]) c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new(
                            "key",
                            DataType::Struct(Fields::from(vec![Field::new(
                                "b",
                                DataType::Utf8,
                                true,
                            )])),
                            false,
                        ),
                        Field::new(
                            "value",
                            DataType::Struct(Fields::from(vec![
                                Field::new("a", DataType::Int64, true),
                                Field::new("b", DataType::Utf8, true),
                                Field::new("c", DataType::Utf8, true),
                            ])),
                            true,
                        ),
                    ] as Vec<Field>)),
                    false,
                )
                .into(),
                false,
            ),
            true,
        )]));

        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = [
            "+------------------------------+",
            "| c0                           |",
            "+------------------------------+",
            "| {{b: n}: {a: 2, b: m, c: y}} |",
            "+------------------------------+",
        ];
        assert_batches_eq!(expected, std::slice::from_ref(&actual));

        Ok(())
    }

    // Read struct using schema where schema fields do not overlap with
    // struct fields
    #[tokio::test]
    async fn test_nested_types_extract_missing_struct_names_non_overlap(
    ) -> Result<(), DataFusionError> {
        let test_data = "select named_struct('a', 1, 'b', 'abc') c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Struct(Fields::from(vec![
                Field::new("c", DataType::Int64, true),
                Field::new("d", DataType::Utf8, true),
            ])),
            true,
        )]));
        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = ["+----+", "| c0 |", "+----+", "|    |", "+----+"];
        assert_batches_eq!(expected, &[actual]);
        Ok(())
    }

    // Read struct using custom schema to read just a single field from the struct
    #[tokio::test]
    async fn test_nested_types_extract_missing_struct_names_single_field(
    ) -> Result<(), DataFusionError> {
        let test_data = "select named_struct('a', 1, 'b', 'abc') c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)])),
            true,
        )]));
        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = [
            "+--------+",
            "| c0     |",
            "+--------+",
            "| {a: 1} |",
            "+--------+",
        ];
        assert_batches_eq!(expected, &[actual]);
        Ok(())
    }

    // Read struct using custom schema to handle a missing field
    #[tokio::test]
    async fn test_nested_types_extract_missing_struct_names_missing_field(
    ) -> Result<(), DataFusionError> {
        let test_data = "select named_struct('a', 1, 'b', 'abc') c0";
        let required_schema = Schema::new(Fields::from(vec![Field::new(
            "c0",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("x", DataType::Int64, true),
            ])),
            true,
        )]));
        let actual = make_parquet_data(test_data, required_schema).await?;
        let expected = [
            "+-------------+",
            "| c0          |",
            "+-------------+",
            "| {a: 1, x: } |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &[actual]);
        Ok(())
    }

    /// A nested list whose children are all empty must decode to the same element type as a
    /// populated sibling, so the two can be concatenated: `[[[]], [[1]]]`.
    #[test]
    fn test_literal_to_list_with_empty_nested_children() -> Result<(), DataFusionError> {
        for leaf_type in [DataType::Interval(IntervalUnit::YearMonth), DataType::Int32] {
            let data = ListLiteral {
                list_values: vec![
                    // [[]]
                    ListLiteral {
                        list_values: vec![ListLiteral::default()],
                        null_mask: vec![true],
                        ..Default::default()
                    },
                    // [[1]]
                    ListLiteral {
                        list_values: vec![ListLiteral {
                            int_values: vec![1],
                            null_mask: vec![true],
                            ..Default::default()
                        }],
                        null_mask: vec![true],
                        ..Default::default()
                    },
                ],
                null_mask: vec![true, true],
                ..Default::default()
            };
            let inner = DataType::List(Arc::new(Field::new("item", leaf_type.clone(), true)));
            let middle = DataType::List(Arc::new(Field::new("item", inner.clone(), true)));
            let outer = DataType::List(Arc::new(Field::new("item", middle.clone(), true)));

            let array = literal_to_array_ref(outer, data)?;

            // The outer list's values: two lists whose elements are `inner` lists.
            assert_eq!(array.data_type(), &middle, "{leaf_type}");
            let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(lists.len(), 2);

            let first = lists.value(0);
            let first = first.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(first.len(), 1);
            assert!(first.is_valid(0));
            assert_eq!(first.value(0).len(), 0);

            let second = lists.value(1);
            let second = second.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(second.len(), 1);
            assert_eq!(second.value(0).len(), 1);
            assert_eq!(second.value(0).data_type(), &leaf_type);
        }
        Ok(())
    }

    /// The empty and populated children must also agree on nested field nullability. Spark folds
    /// `array(array(array()), array(array(array(INTERVAL '1' MONTH))))` into a literal whose
    /// arrays all declare non-nullable elements, while a populated level is rebuilt with nullable
    /// fields: `[[[]], [[[1]]]]`.
    #[test]
    fn test_literal_to_list_with_empty_nested_children_non_nullable() -> Result<(), DataFusionError>
    {
        let list_of = |element: DataType, nullable: bool| {
            DataType::List(Arc::new(Field::new("item", element, nullable)))
        };
        for leaf_type in [DataType::Interval(IntervalUnit::YearMonth), DataType::Int32] {
            let data = ListLiteral {
                list_values: vec![
                    // [[]]
                    ListLiteral {
                        list_values: vec![ListLiteral::default()],
                        null_mask: vec![true],
                        ..Default::default()
                    },
                    // [[[1]]]
                    ListLiteral {
                        list_values: vec![ListLiteral {
                            list_values: vec![ListLiteral {
                                int_values: vec![1],
                                null_mask: vec![true],
                                ..Default::default()
                            }],
                            null_mask: vec![true],
                            ..Default::default()
                        }],
                        null_mask: vec![true],
                        ..Default::default()
                    },
                ],
                null_mask: vec![true, true],
                ..Default::default()
            };
            let mut declared = leaf_type.clone();
            for _ in 0..4 {
                declared = list_of(declared, false);
            }

            let array = literal_to_array_ref(declared, data)?;

            // The outer list's values: two lists of lists of `leaf_type` lists, nullable at every
            // level whichever child decoded them.
            let expected = list_of(list_of(list_of(leaf_type.clone(), true), true), true);
            assert_eq!(array.data_type(), &expected, "{leaf_type}");
            let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(lists.len(), 2);

            let first = lists.value(0);
            let first = first.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(first.len(), 1);
            assert!(first.is_valid(0));
            assert_eq!(first.value(0).len(), 0);

            let second = lists.value(1);
            let second = second.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(second.len(), 1);
            let innermost = second.value(0);
            let innermost = innermost.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(innermost.len(), 1);
            assert_eq!(innermost.value(0).len(), 1);
            assert_eq!(innermost.value(0).data_type(), &leaf_type);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_literal_to_list() -> Result<(), DataFusionError> {
        /*
           [
               [
                   [1, 2, 3],
                   [4, 5, 6],
                   [7, 8, 9, null],
                   [],
                   null
               ],
               [
                   [10, null, 12]
               ],
               null,
               []
          ]
        */
        let data = ListLiteral {
            list_values: vec![
                ListLiteral {
                    list_values: vec![
                        ListLiteral {
                            int_values: vec![1, 2, 3],
                            null_mask: vec![true, true, true],
                            ..Default::default()
                        },
                        ListLiteral {
                            int_values: vec![4, 5, 6],
                            null_mask: vec![true, true, true],
                            ..Default::default()
                        },
                        ListLiteral {
                            int_values: vec![7, 8, 9, 0],
                            null_mask: vec![true, true, true, false],
                            ..Default::default()
                        },
                        ListLiteral {
                            ..Default::default()
                        },
                        ListLiteral {
                            ..Default::default()
                        },
                    ],
                    null_mask: vec![true, true, true, false, true],
                    ..Default::default()
                },
                ListLiteral {
                    list_values: vec![ListLiteral {
                        int_values: vec![10, 0, 11],
                        null_mask: vec![true, false, true],
                        ..Default::default()
                    }],
                    null_mask: vec![true],
                    ..Default::default()
                },
                ListLiteral {
                    ..Default::default()
                },
                ListLiteral {
                    ..Default::default()
                },
            ],
            null_mask: vec![true, true, false, true],
            ..Default::default()
        };

        let nested_type = DataType::List(FieldRef::from(Field::new(
            "item",
            DataType::List(
                Field::new(
                    "item",
                    DataType::List(
                        Field::new(
                            "item",
                            DataType::Int32,
                            true, // Int32 nullable
                        )
                        .into(),
                    ),
                    true, // inner list nullable
                )
                .into(),
            ),
            true, // outer list nullable
        )));

        let array = literal_to_array_ref(nested_type, data)?;

        // Top-level should be ListArray<ListArray<Int32>>
        let list_outer = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_outer.len(), 4);

        // First outer element: ListArray<Int32>
        let first_elem = list_outer.value(0);
        let list_inner = first_elem.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_inner.len(), 5);

        // Inner values
        let v0 = list_inner.value(0);
        let vals0 = v0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals0.values(), &[1, 2, 3]);

        let v1 = list_inner.value(1);
        let vals1 = v1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals1.values(), &[4, 5, 6]);

        let v2 = list_inner.value(2);
        let vals2 = v2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals2.values(), &[7, 8, 9, 0]);

        // Second outer element
        let second_elem = list_outer.value(1);
        let list_inner2 = second_elem.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_inner2.len(), 1);

        let v3 = list_inner2.value(0);
        let vals3 = v3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals3.values(), &[10, 0, 11]);

        Ok(())
    }

    /// Null slots arrive as empty bytes with a false null mask entry, and must decode next to
    /// valid values: `[123.45, null, -0.01]`.
    #[test]
    fn test_literal_to_decimal_list_with_nulls() -> Result<(), ExecutionError> {
        let data = ListLiteral {
            decimal_values: vec![
                12345i128.to_be_bytes().to_vec(),
                vec![],
                (-1i128).to_be_bytes().to_vec(),
            ],
            null_mask: vec![true, false, true],
            ..Default::default()
        };

        let array = literal_to_array_ref(DataType::Decimal128(38, 2), data)?;

        let decimals = array
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(decimals.data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(decimals.len(), 3);
        assert_eq!(decimals.value(0), 12345);
        assert!(decimals.is_null(1));
        assert_eq!(decimals.value(2), -1);
        Ok(())
    }

    /// A decimal list value that does not fit in i128 must return an error instead of panicking.
    #[test]
    fn test_literal_to_decimal_list_out_of_range() {
        // 2^128 needs 17 bytes and exceeds i128::MAX.
        let mut too_large = vec![0u8; 17];
        too_large[0] = 0x01;
        let data = ListLiteral {
            decimal_values: vec![12345i128.to_be_bytes().to_vec(), too_large],
            null_mask: vec![true, true],
            ..Default::default()
        };

        match literal_to_array_ref(DataType::Decimal128(38, 2), data) {
            Err(ExecutionError::GeneralError(msg)) => {
                assert!(msg.contains("Cannot parse"), "unexpected message: {msg}")
            }
            other => panic!("expected GeneralError, got {other:?}"),
        }
    }

    /// Builds two nullable columns of the same type and bound references to them, as inputs for
    /// `create_binary_expr`.
    fn binary_expr_inputs(
        arrow_type: DataType,
        proto_type: spark_expression::DataType,
    ) -> (SchemaRef, Expr, Expr) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", arrow_type.clone(), true),
            Field::new("b", arrow_type, true),
        ]));
        let bound = |index| Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index,
                datatype: Some(proto_type.clone()),
            })),
            expr_id: None,
            query_context: None,
        };
        (schema, bound(0), bound(1))
    }

    fn int32_proto_type() -> spark_expression::DataType {
        spark_expression::DataType {
            type_id: 3, // INT32
            type_info: None,
        }
    }

    fn decimal_proto_type(precision: i32, scale: i32) -> spark_expression::DataType {
        use spark_expression::data_type::{
            data_type_info::DatatypeStruct, DataTypeInfo, DecimalInfo,
        };
        spark_expression::DataType {
            type_id: 10, // DECIMAL
            type_info: Some(Box::new(DataTypeInfo {
                datatype_struct: Some(DatatypeStruct::Decimal(DecimalInfo { precision, scale })),
            })),
        }
    }

    /// Checked integer arithmetic only covers `+ - * /`. Any other operator in Try or Ansi mode
    /// must return an error instead of panicking.
    #[test]
    fn test_checked_binary_expr_unsupported_operator() {
        use datafusion::logical_expr::Operator as DataFusionOperator;

        let planner = PhysicalPlanner::default();
        let (schema, left, right) = binary_expr_inputs(DataType::Int32, int32_proto_type());

        for eval_mode in [EvalMode::Try, EvalMode::Ansi] {
            let result = planner.create_binary_expr(
                &left,
                &left,
                &right,
                Some(&int32_proto_type()),
                DataFusionOperator::Modulo,
                Arc::clone(&schema),
                eval_mode,
            );
            match result {
                Err(ExecutionError::GeneralError(msg)) => assert!(
                    msg.contains("Modulo") && msg.contains(&format!("{eval_mode:?}")),
                    "unexpected message: {msg}"
                ),
                Err(e) => panic!("expected GeneralError for {eval_mode:?}, got {e:?}"),
                Ok(expr) => panic!("expected an error for {eval_mode:?}, got {expr:?}"),
            }
        }
    }

    /// `return_type` is optional on the wire. A binary expression without one must return an
    /// error instead of panicking, in each arm that reads it.
    #[test]
    fn test_binary_expr_missing_return_type() {
        use datafusion::logical_expr::Operator as DataFusionOperator;

        let planner = PhysicalPlanner::default();
        let cases = [
            // Generic arm.
            (
                DataType::Int32,
                int32_proto_type(),
                DataFusionOperator::Plus,
            ),
            // Wide decimal arm: 38 integer digits reach DECIMAL128_MAX_PRECISION for Plus.
            (
                DataType::Decimal128(38, 0),
                decimal_proto_type(38, 0),
                DataFusionOperator::Plus,
            ),
            // Decimal Divide arm.
            (
                DataType::Decimal128(38, 0),
                decimal_proto_type(38, 0),
                DataFusionOperator::Divide,
            ),
        ];

        for (arrow_type, proto_type, op) in cases {
            let (schema, left, right) = binary_expr_inputs(arrow_type.clone(), proto_type);
            let result = planner.create_binary_expr(
                &left,
                &left,
                &right,
                None,
                op,
                schema,
                EvalMode::Legacy,
            );
            match result {
                Err(ExecutionError::GeneralError(msg)) => assert!(
                    msg.contains("Missing return type"),
                    "unexpected message for {op:?} on {arrow_type}: {msg}"
                ),
                Err(e) => panic!("expected GeneralError for {op:?} on {arrow_type}, got {e:?}"),
                Ok(expr) => panic!("expected an error for {op:?} on {arrow_type}, got {expr:?}"),
            }
        }
    }

    /// Test that reproduces the "Cast error: Casting from Int8 to Date32 not supported" error
    /// that occurs when performing date subtraction with Int8 (TINYINT) values.
    /// This corresponds to the Scala test "date_sub with int arrays" in CometExpressionSuite.
    ///
    /// The error occurs because DataFusion's BinaryExpr tries to cast Int8 to Date32
    /// when evaluating date - int8, but this cast is not supported.
    #[test]
    fn test_date_sub_with_int8_cast_error() {
        use arrow::array::Date32Array;

        let planner = PhysicalPlanner::default();
        let row_count = 3;

        // Create a Scan operator with Date32 (DATE) and Int8 (TINYINT) columns
        let op_scan = Operator {
            plan_id: 0,
            sql_text_pool: vec![],
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![
                    spark_expression::DataType {
                        type_id: 12, // DATE (Date32)
                        type_info: None,
                    },
                    spark_expression::DataType {
                        type_id: 1, // INT8 (TINYINT)
                        type_info: None,
                    },
                ],
                source: "".to_string(),
            })),
        };

        // Create bound reference for the DATE column (index 0)
        let date_col = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(spark_expression::DataType {
                    type_id: 12, // DATE
                    type_info: None,
                }),
            })),
            expr_id: None,
            query_context: None,
        };

        // Create bound reference for the INT8 column (index 1)
        let int8_col = spark_expression::Expr {
            expr_struct: Some(Bound(spark_expression::BoundReference {
                index: 1,
                datatype: Some(spark_expression::DataType {
                    type_id: 1, // INT8
                    type_info: None,
                }),
            })),
            expr_id: None,
            query_context: None,
        };

        // Create a Subtract expression: date_col - int8_col
        // This is equivalent to the SQL: SELECT _20 - _2 FROM tbl (date_sub operation)
        // In the protobuf, subtract uses MathExpr type
        let subtract_expr = spark_expression::Expr {
            expr_struct: Some(ExprStruct::Subtract(Box::new(spark_expression::MathExpr {
                left: Some(Box::new(date_col)),
                right: Some(Box::new(int8_col)),
                return_type: Some(spark_expression::DataType {
                    type_id: 12, // DATE - result should be DATE
                    type_info: None,
                }),
                eval_mode: 0, // Legacy mode
                check_divide_overflow: false,
            }))),
            expr_id: None,
            query_context: None,
        };

        // Create a projection operator with the subtract expression
        let projection = Operator {
            children: vec![op_scan],
            plan_id: 1,
            sql_text_pool: vec![],
            op_struct: Some(OpStruct::Projection(spark_operator::Projection {
                project_list: vec![subtract_expr],
            })),
        };

        // Create the physical plan
        let (mut scans, _shuffle_scans, datafusion_plan) =
            planner.create_plan(&projection, &mut vec![], 1).unwrap();

        // Create test data: Date32 and Int8 columns
        let date_array = Date32Array::from(vec![Some(19000), Some(19001), Some(19002)]);
        let int8_array = Int8Array::from(vec![Some(1i8), Some(2i8), Some(3i8)]);

        // Set input batch for the scan
        let input_batch =
            InputBatch::Batch(vec![Arc::new(date_array), Arc::new(int8_array)], row_count);
        scans[0].set_input_batch(input_batch);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = datafusion_plan.native_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        // Separate thread to send the EOF signal once we've processed the only input batch
        runtime.spawn(async move {
            // Create test data again for the second batch
            let date_array = Date32Array::from(vec![Some(19000), Some(19001), Some(19002)]);
            let int8_array = Int8Array::from(vec![Some(1i8), Some(2i8), Some(3i8)]);
            let input_batch1 =
                InputBatch::Batch(vec![Arc::new(date_array), Arc::new(int8_array)], row_count);
            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(result)) => {
                        // We expect success - the Int8 should be automatically cast to Int32
                        assert!(
                            result.is_ok(),
                            "Expected success for date - int8 operation but got error: {:?}",
                            result.unwrap_err()
                        );

                        let batch = result.unwrap();
                        assert_eq!(batch.num_rows(), row_count);

                        // The result should be Date32 type
                        assert_eq!(batch.column(0).data_type(), &DataType::Date32);

                        // Verify the values: 19000-1=18999, 19001-2=18999, 19002-3=18999
                        let date_array = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .unwrap();
                        assert_eq!(date_array.value(0), 18999); // 19000 - 1
                        assert_eq!(date_array.value(1), 18999); // 19001 - 2
                        assert_eq!(date_array.value(2), 18999); // 19002 - 3
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    /// `struct<flag: Boolean, items: List<Struct<n: Int32>>>`, with the leaf nullability
    /// controlled by `nullable`: a non-nullable leaf both directly in the struct and buried
    /// under a list-of-struct.
    fn collect_agg_struct_type(nullable: bool) -> DataType {
        let item = DataType::Struct(Fields::from(vec![Field::new(
            "n",
            DataType::Int32,
            nullable,
        )]));
        DataType::Struct(Fields::from(vec![
            Field::new("flag", DataType::Boolean, nullable),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", item, true))),
                true,
            ),
        ]))
    }

    /// A struct array whose actual type has non-nullable leaves.
    fn collect_agg_struct_array() -> ArrayRef {
        use arrow::array::{BooleanArray, StructArray};
        use arrow::buffer::OffsetBuffer;

        let n = Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef;
        let item_fields = Fields::from(vec![Field::new("n", DataType::Int32, false)]);
        let items_values = Arc::new(StructArray::new(item_fields.clone(), vec![n], None));
        let items = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(item_fields), true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            items_values,
            None,
        )) as ArrayRef;
        let flag = Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef;
        let DataType::Struct(fields) = collect_agg_struct_type(false) else {
            unreachable!()
        };
        Arc::new(StructArray::new(fields, vec![flag, items], None))
    }

    fn collect_agg_schema(data_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("s", data_type, true)]))
    }

    /// Builds `func` over `child` against `plan_schema`, feeds it a batch built from
    /// `batch_schema`, and returns (type declared by `state_fields()`, type of the state the
    /// ungrouped `Accumulator` emits, type of the state the `GroupsAccumulator` emits). The
    /// declared type is compared against the emitted one by `RecordBatch::try_new` inside
    /// `GroupedHashAggregateStream::emit`.
    fn collect_agg_declared_vs_produced(
        child: &Arc<dyn PhysicalExpr>,
        plan_schema: &SchemaRef,
        batch_schema: &SchemaRef,
        func: AggregateUDF,
    ) -> (DataType, DataType, DataType) {
        let agg = PhysicalPlanner::create_aggr_func_expr(
            "collect",
            Arc::clone(plan_schema),
            vec![Arc::clone(child)],
            func,
        )
        .unwrap();
        let declared = agg.state_fields().unwrap()[0].data_type().clone();

        let batch =
            RecordBatch::try_new(Arc::clone(batch_schema), vec![collect_agg_struct_array()])
                .unwrap();
        let arg = child
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let mut acc = agg.create_accumulator().unwrap();
        acc.update_batch(&[Arc::clone(&arg)]).unwrap();
        let produced = acc.state().unwrap()[0].data_type();

        let mut groups = agg.create_groups_accumulator().unwrap();
        groups.update_batch(&[arg], &[0, 0], None, 1).unwrap();
        let grouped = groups.state(EmitTo::All).unwrap()[0].data_type().clone();

        (declared, produced, grouped)
    }

    /// `collect_list` / `collect_set` derive their accumulator state type from the declared
    /// argument type but can emit the runtime type, so a plan that declares nullable leaves while
    /// the arriving batch carries non-nullable ones drifts, and the drift reaches `AggregateExec`
    /// and fails its output-batch validation. `coerce_collect_child_nullability` normalizes the
    /// argument to the declared type before it reaches the accumulator.
    ///
    /// DataFusion 55's `ScalarValue::new_list` fix removed the drift for `collect_set`, which
    /// rebuilds its state list honouring `data_type`. `collect_list` still rebuilds from the
    /// runtime array and drifts, so the coercion remains necessary. This test pins that
    /// `collect_list` still drifts without the coercion, and that the coercion normalizes both.
    ///
    /// The grouped path never drifts: `CollectListGroupsAccumulator` /
    /// `CollectSetGroupsAccumulator` normalize every array they take in to the declared element
    /// type, so their state matches the declaration with or without the coercion.
    #[test]
    fn test_collect_agg_absorbs_nested_nullability_drift() {
        let plan_schema = collect_agg_schema(collect_agg_struct_type(true));
        let batch_schema = collect_agg_schema(collect_agg_struct_type(false));
        let raw = Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>;
        let coerced =
            PhysicalPlanner::coerce_collect_child_nullability(Arc::clone(&raw), &plan_schema)
                .unwrap();

        for func in [
            AggregateUDF::new_from_impl(CometCollectSet::new()),
            AggregateUDF::new_from_impl(CometCollectList::new()),
        ] {
            // Without the coercion, collect_list still drifts (declared nullable leaves vs
            // produced non-null ones). collect_set no longer drifts after DataFusion 55's new_list
            // fix, so only assert the drift for the func that still exhibits it.
            let (declared, produced, grouped) =
                collect_agg_declared_vs_produced(&raw, &plan_schema, &batch_schema, func.clone());
            if func.name() == "collect_list" {
                assert!(
                    !declared.equals_datatype(&produced),
                    "expected drift for {}: declared {declared}, produced {produced}",
                    func.name()
                );
            }
            assert!(
                declared.equals_datatype(&grouped),
                "grouped state drifted for {}: declared {declared}, produced {grouped}",
                func.name()
            );

            // With the coercion, the argument is normalized to the declared type before it
            // reaches the accumulator, so both funcs emit a state matching the declared type.
            let (declared, produced, grouped) =
                collect_agg_declared_vs_produced(&coerced, &plan_schema, &batch_schema, func);
            assert!(
                declared.equals_datatype(&produced),
                "declared {declared} != produced {produced}"
            );
            assert!(
                declared.equals_datatype(&grouped),
                "declared {declared} != grouped {grouped}"
            );
        }
    }

    /// The coercion widens every nested field, so the declared element type is the
    /// all-nullable variant of the argument's declared type.
    #[test]
    fn test_collect_agg_coercion_widens_all_nested_fields() {
        let plan_schema = collect_agg_schema(collect_agg_struct_type(false));
        let raw = Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>;
        let coerced = PhysicalPlanner::coerce_collect_child_nullability(raw, &plan_schema).unwrap();
        assert_eq!(
            coerced.data_type(plan_schema.as_ref()).unwrap(),
            collect_agg_struct_type(true)
        );
    }

    /// Primitive arguments carry no field-level nullability, so no cast is inserted.
    #[test]
    fn test_collect_agg_no_coercion_for_primitive_child() {
        let plan_schema = collect_agg_schema(DataType::Int32);
        let raw = Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>;
        let coerced =
            PhysicalPlanner::coerce_collect_child_nullability(Arc::clone(&raw), &plan_schema)
                .unwrap();
        assert!(Arc::ptr_eq(&raw, &coerced));
    }

    /// Normalizing an already-matching typed-null scalar or array must preserve its shared
    /// storage, and both scalar collect accumulators must discard the null and return an empty
    /// list. Duplicate field names with different types detect accidental name-based struct
    /// conversion; matching runtime types must take DataFusion's identity cast path instead.
    #[test]
    fn test_collect_agg_preserves_matching_nested_nulls() {
        use datafusion::logical_expr::ColumnarValue;
        use datafusion::physical_expr::expressions::Literal;

        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "a",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
                true,
            ),
        ]));
        let null_list = Arc::new(ListArray::new_null(
            Arc::new(Field::new("item", struct_type, true)),
            1,
        ));
        let null_array = Arc::clone(&null_list) as ArrayRef;
        let schema = collect_agg_schema(null_array.data_type().clone());
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&null_array)]).unwrap();

        for raw in [
            Arc::new(Literal::new(ScalarValue::List(Arc::clone(&null_list))))
                as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
        ] {
            let child = PhysicalPlanner::coerce_collect_child_nullability(raw, &schema).unwrap();
            let value = child.evaluate(&batch).unwrap();
            match &value {
                ColumnarValue::Scalar(ScalarValue::List(list)) => {
                    assert!(Arc::ptr_eq(list, &null_list));
                }
                ColumnarValue::Array(array) => assert!(Arc::ptr_eq(array, &null_array)),
                other => panic!("expected a list scalar or array, got {other:?}"),
            }
            let arg = value.into_array(batch.num_rows()).unwrap();

            for func in [
                AggregateUDF::new_from_impl(CometCollectList::new()),
                AggregateUDF::new_from_impl(CometCollectSet::new()),
            ] {
                let agg = PhysicalPlanner::create_aggr_func_expr(
                    "collect",
                    Arc::clone(&schema),
                    vec![Arc::clone(&child)],
                    func,
                )
                .unwrap();
                let mut acc = agg.create_accumulator().unwrap();
                acc.update_batch(&[Arc::clone(&arg)]).unwrap();
                assert_eq!(
                    acc.state().unwrap()[0].data_type(),
                    *agg.state_fields().unwrap()[0].data_type()
                );
                let ScalarValue::List(result) = acc.evaluate().unwrap() else {
                    panic!("expected a list result");
                };
                assert_eq!(result.len(), 1);
                assert!(!result.is_null(0));
                assert!(result.value(0).is_empty());
            }
        }
    }

    #[test]
    fn test_metadata_field_id_constants_match_iceberg_rust() {
        // These constants are duplicated in Scala (CometIcebergNativeScan.MetadataFieldIds)
        // as Int.MaxValue - 1 and Int.MaxValue - 5. This test ensures the Rust constants
        // haven't drifted, which would cause a silent mismatch with the Scala side.
        assert_eq!(
            iceberg::metadata_columns::RESERVED_FIELD_ID_FILE,
            i32::MAX - 1,
            "RESERVED_FIELD_ID_FILE must be i32::MAX - 1 to match Scala MetadataFieldIds"
        );
        assert_eq!(
            iceberg::metadata_columns::RESERVED_FIELD_ID_PARTITION,
            i32::MAX - 5,
            "RESERVED_FIELD_ID_PARTITION must be i32::MAX - 5 to match Scala MetadataFieldIds"
        );
    }

    #[test]
    fn test_unified_partition_type_merges_specs_by_descending_spec_id() {
        use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Type};

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "category", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        let schema_arc = Arc::new(iceberg_schema);

        let identity_field = |source_id: i32, field_id: i32, name: &str| {
            iceberg::spec::UnboundPartitionField::builder()
                .source_ids(vec![source_id])
                .field_id(field_id)
                .name(name)
                .transform(iceberg::spec::Transform::Identity)
                .build()
                .expect("unbound field")
        };

        // Spec 0 (older): field 1000 named "region_old".
        let spec0 = PartitionSpec::builder(Arc::clone(&schema_arc))
            .with_spec_id(0)
            .add_unbound_field(identity_field(2, 1000, "region_old"))
            .expect("add field")
            .build()
            .expect("build spec0");

        // Spec 1 (newer): same field id 1000 renamed to "region_new", plus a new field 2000.
        let spec1 = PartitionSpec::builder(Arc::clone(&schema_arc))
            .with_spec_id(1)
            .add_unbound_field(identity_field(2, 1000, "region_new"))
            .expect("add field")
            .add_unbound_field(identity_field(3, 2000, "category"))
            .expect("add field")
            .build()
            .expect("build spec1");

        let spec0_type_json =
            serde_json::to_string(&spec0.partition_type(&schema_arc).expect("partition_type"))
                .expect("serialize");
        let spec1_type_json =
            serde_json::to_string(&spec1.partition_type(&schema_arc).expect("partition_type"))
                .expect("serialize");
        let spec0_json = serde_json::to_string(&spec0).expect("serialize spec0");
        let spec1_json = serde_json::to_string(&spec1).expect("serialize spec1");

        let schema_json = serde_json::to_string(schema_arc.as_ref()).expect("serialize schema");

        // Pool insertion order deliberately does NOT match spec_id order: the newer spec
        // (spec_id 1) is inserted first, at index 0, and the older spec (spec_id 0) second, at
        // index 1. If the merge relied on pool-insertion order instead of spec_id, this would
        // produce the wrong result (region_old kept instead of region_new).
        let proto_common = spark_operator::IcebergScanCommon {
            schema_pool: vec![schema_json],
            partition_type_pool: vec![spec1_type_json, spec0_type_json],
            partition_spec_pool: vec![spec1_json, spec0_json],
            project_field_ids_pool: vec![spark_operator::ProjectFieldIdList {
                field_ids: vec![iceberg::metadata_columns::RESERVED_FIELD_ID_PARTITION],
            }],
            ..Default::default()
        };

        // No partition_spec_idx/partition_data_idx: this test exercises unified-type merging,
        // which is driven by the type/spec pools rather than the task's own spec. `FileScanTask`
        // validation rejects a partitioned spec without matching partition data, and the JVM
        // serializer always sends spec and data together, so a spec-without-data task never
        // occurs in practice.
        let proto_task = spark_operator::IcebergFileScanTask {
            data_file_path: "file:///tmp/data.parquet".to_string(),
            file_size_in_bytes: 100,
            schema_idx: 0,
            project_field_ids_idx: 0,
            ..Default::default()
        };

        let tasks =
            parse_file_scan_tasks_from_common(&proto_common, &[proto_task]).expect("parse tasks");
        assert_eq!(tasks.len(), 1);

        let unified = tasks[0]
            .unified_partition_type()
            .expect("unified_partition_type must be set when _partition is projected");

        let fields = unified.fields();
        assert_eq!(
            fields.len(),
            2,
            "expected fields 1000 and 2000, got {fields:?}"
        );
        // Ascending by field id: 1000 before 2000.
        assert_eq!(fields[0].id, 1000);
        assert_eq!(fields[1].id, 2000);
        // Newest spec (spec_id 1) wins the name for field 1000, despite being inserted into the
        // pool before spec_id 0's entry.
        assert_eq!(fields[0].name, "region_new");
        assert_eq!(fields[1].name, "category");
    }

    /// iceberg-rust holds a deletion vector's record count and blob coordinates as `u64`, so the
    /// planner rejects a negative one from the proto rather than build the delete file with it. A
    /// deletion vector without a record count reaches iceberg-rust's builder, whose error must
    /// come back through the `Invalid Iceberg delete file` mapping.
    #[test]
    fn test_delete_file_pool_rejects_invalid_deletion_vectors() {
        let deletion_vector = spark_operator::IcebergDeleteFile {
            file_path_idx: 0,
            content_type: "POSITION_DELETES".to_string(),
            file_format: "PUFFIN".to_string(),
            referenced_data_file: Some("file:///tmp/data.parquet".to_string()),
            content_offset: Some(4),
            content_size_in_bytes: Some(40),
            record_count: Some(2),
            ..Default::default()
        };
        let parse = |delete: spark_operator::IcebergDeleteFile| {
            let proto_common = spark_operator::IcebergScanCommon {
                delete_file_path_pool: vec!["file:///tmp/dv.puffin".to_string()],
                delete_file_pool: vec![delete],
                ..Default::default()
            };
            parse_file_scan_tasks_from_common(&proto_common, &[]).map(|_| ())
        };

        assert!(parse(deletion_vector.clone()).is_ok());
        for (delete, expected) in [
            (
                spark_operator::IcebergDeleteFile {
                    content_offset: Some(-1),
                    ..deletion_vector.clone()
                },
                "negative content offset",
            ),
            (
                spark_operator::IcebergDeleteFile {
                    content_size_in_bytes: Some(-1),
                    ..deletion_vector.clone()
                },
                "negative content size",
            ),
            (
                spark_operator::IcebergDeleteFile {
                    record_count: Some(-1),
                    ..deletion_vector.clone()
                },
                "negative record count",
            ),
            (
                spark_operator::IcebergDeleteFile {
                    record_count: None,
                    ..deletion_vector.clone()
                },
                "is missing record_count",
            ),
        ] {
            let error = parse(delete).expect_err(expected).to_string();
            assert!(
                error.contains(expected),
                "expected '{expected}' in: {error}"
            );
        }
    }

    #[test]
    fn test_unified_partition_type_tolerates_unparseable_spec() {
        // A spec whose transform iceberg-rust cannot deserialize still has to yield a spec_id for
        // the unified-type merge, so recovering it must not require the full spec to parse. The JVM
        // serializer no longer emits such a spec (see
        // test_unknown_transform_spec_builds_task_with_partition_data), so this pins the defensive
        // path: the merge succeeds rather than erroring.
        let schema_json = serde_json::to_string(
            &iceberg::spec::Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )
                .into()])
                .build()
                .expect("schema"),
        )
        .expect("serialize schema");

        // A spec JSON whose transform iceberg-rust cannot deserialize, paired with an empty type
        // entry. No partition data: `FileScanTask` validation rejects partition values without a
        // spec, and the unparseable spec deserializes to None.
        let unparseable_spec_json = r#"{"spec-id":7,"fields":[{"source-id":1,"field-id":1000,"name":"x","transform":"totally_unknown[9]"}]}"#;
        let empty_type_json = r#"{"type":"struct","fields":[]}"#;

        let proto_common = spark_operator::IcebergScanCommon {
            schema_pool: vec![schema_json],
            partition_type_pool: vec![empty_type_json.to_string()],
            partition_spec_pool: vec![unparseable_spec_json.to_string()],
            project_field_ids_pool: vec![spark_operator::ProjectFieldIdList {
                field_ids: vec![iceberg::metadata_columns::RESERVED_FIELD_ID_PARTITION],
            }],
            ..Default::default()
        };

        let proto_task = spark_operator::IcebergFileScanTask {
            data_file_path: "file:///tmp/data.parquet".to_string(),
            file_size_in_bytes: 100,
            schema_idx: 0,
            partition_spec_idx: Some(0),
            project_field_ids_idx: 0,
            ..Default::default()
        };

        let tasks = parse_file_scan_tasks_from_common(&proto_common, &[proto_task])
            .expect("parse tasks must not error on an unparseable spec");
        assert_eq!(tasks.len(), 1);
        assert!(
            tasks[0]
                .unified_partition_type()
                .map(|t| t.fields().is_empty())
                .unwrap_or(true),
            "unified partition type should have no fields for an all-unknown-transform spec"
        );
    }

    /// Regression for TestForwardCompatibility.testSparkCanReadUnknownTransform. Iceberg Java
    /// reports a transform it doesn't know (written by a newer Iceberg) under its original name,
    /// e.g. `zero`, and the field's partition type as `string` -- so the field is NOT dropped from
    /// the spec, and the task carries a real partition value for it. If that name reached
    /// iceberg-rust verbatim the spec would fail to deserialize, leaving partition values with no
    /// spec, which `FileScanTask::build()` rejects with "Non-empty FileScanTask partition requires
    /// a partition spec". `IcebergReflection.Transforms.forNative` maps it to `unknown`, which
    /// iceberg-rust parses; this pins that the resulting task builds and keeps its spec.
    #[test]
    fn test_unknown_transform_spec_builds_task_with_partition_data() {
        let schema_json = serde_json::to_string(
            &iceberg::spec::Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    iceberg::spec::NestedField::optional(
                        1,
                        "id",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                    )
                    .into(),
                    iceberg::spec::NestedField::optional(
                        2,
                        "data",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .expect("schema"),
        )
        .expect("serialize schema");

        // What the JVM serializer emits for TestForwardCompatibility's UNKNOWN_SPEC: the `zero`
        // transform normalized to `unknown`, and the partition type Iceberg Java resolved for it
        // (string, from UnknownTransform.getResultType).
        let spec_json = r#"{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"name":"id_zero","transform":"unknown"}]}"#;
        let type_json = r#"{"type":"struct","fields":[{"id":1000,"name":"id_zero","required":false,"type":"string"}]}"#;

        let proto_common = spark_operator::IcebergScanCommon {
            schema_pool: vec![schema_json],
            partition_type_pool: vec![type_json.to_string()],
            partition_spec_pool: vec![spec_json.to_string()],
            partition_data_pool: vec![spark_operator::PartitionData {
                values: vec![spark_operator::PartitionValue {
                    field_id: 1000,
                    literal: Some(spark_operator::IcebergLiteral {
                        value: Some(spark_operator::iceberg_literal::Value::StringVal(
                            "0".to_string(),
                        )),
                        ..Default::default()
                    }),
                }],
            }],
            project_field_ids_pool: vec![spark_operator::ProjectFieldIdList {
                field_ids: vec![1, 2, iceberg::metadata_columns::RESERVED_FIELD_ID_PARTITION],
            }],
            ..Default::default()
        };

        let proto_task = spark_operator::IcebergFileScanTask {
            data_file_path: "file:///tmp/data.parquet".to_string(),
            file_size_in_bytes: 100,
            schema_idx: 0,
            partition_spec_idx: Some(0),
            partition_data_idx: Some(0),
            project_field_ids_idx: 0,
            ..Default::default()
        };

        let tasks =
            parse_file_scan_tasks_from_common(&proto_common, std::slice::from_ref(&proto_task))
                .expect("an unknown-transform spec must still build a scan task");
        assert_eq!(tasks.len(), 1);

        let spec = tasks[0]
            .partition_spec()
            .expect("partition spec must survive an unknown transform");
        assert_eq!(spec.spec_id(), 0);
        assert_eq!(spec.fields().len(), 1);
        assert_eq!(
            spec.fields()[0].transform,
            iceberg::spec::Transform::Unknown
        );
        assert_eq!(
            tasks[0].partition().map(|p| p.fields().len()),
            Some(1),
            "the partition value must be carried alongside the spec"
        );

        // The `_partition` column reads its field types from the JVM-supplied partition type, which
        // agrees with Transform::Unknown's own result type (string).
        let unified = tasks[0]
            .unified_partition_type()
            .expect("unified_partition_type must be set when _partition is projected");
        assert_eq!(unified.fields().len(), 1);
        assert_eq!(unified.fields()[0].id, 1000);
        assert_eq!(
            *unified.fields()[0].field_type,
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)
        );

        // Pin the failure the normalization avoids: the raw Iceberg Java transform name is the same
        // input in every other respect and does not survive `FileScanTask` validation.
        let raw_spec_json = spec_json.replace(r#""transform":"unknown""#, r#""transform":"zero""#);
        assert_ne!(raw_spec_json, spec_json, "the replace must have matched");
        let raw_common = spark_operator::IcebergScanCommon {
            partition_spec_pool: vec![raw_spec_json],
            ..proto_common
        };
        let err = parse_file_scan_tasks_from_common(&raw_common, &[proto_task])
            .expect_err("an unnormalized transform name must not build a task");
        assert!(
            err.to_string().contains("Non-empty FileScanTask partition"),
            "unexpected error: {err}"
        );
    }

    /// The planner promises DataFusion a native UDF's return type with every nested field nullable,
    /// whatever the kernel reports, and every batch the UDF produces has that type. `make_struct_c`
    /// reports its struct's field `a` as non-nullable for a registration that declares it nullable.
    #[test]
    fn native_scalar_udf_promises_nullable_nested_fields() {
        use crate::execution::c_udf::test_support::{test_udfs_path, BUILD_HINT};
        use datafusion_comet_proto::spark_expression::data_type::{
            data_type_info::DatatypeStruct, DataTypeInfo, StructInfo,
        };

        let int_type = |type_id| spark_expression::DataType {
            type_id,
            type_info: None,
        };
        let declared = spark_expression::DataType {
            type_id: 16, // STRUCT
            type_info: Some(Box::new(DataTypeInfo {
                datatype_struct: Some(DatatypeStruct::Struct(StructInfo {
                    field_names: vec!["a".to_string()],
                    field_datatypes: vec![int_type(3)], // INT32
                    field_nullable: vec![true],
                    field_metadata: vec![],
                })),
            })),
        };
        let call = Expr {
            expr_struct: Some(NativeScalarUdf(spark_expression::NativeScalarUdf {
                name: "make_struct_c".to_string(),
                library_path: test_udfs_path().to_string_lossy().into_owned(),
                args: vec![Expr {
                    expr_struct: Some(Bound(spark_expression::BoundReference {
                        index: 0,
                        datatype: Some(int_type(4)), // INT64
                    })),
                    ..Default::default()
                }],
                return_type: Some(declared),
                deterministic: true,
            })),
            ..Default::default()
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let expr = PhysicalPlanner::default()
            .create_expr(&call, Arc::clone(&schema))
            .expect(BUILD_HINT);

        let promised = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));
        assert_eq!(expr.data_type(&schema).unwrap(), promised);

        let ids: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![Some(1), None]));
        let batch = RecordBatch::try_new(schema, vec![ids]).unwrap();
        let out = expr.evaluate(&batch).unwrap().into_array(2).unwrap();
        assert_eq!(out.data_type(), &promised);
    }

    /// The executor applies the native UDF policy to the library path in the plan, whatever the
    /// driver that built the plan did.
    #[test]
    fn native_scalar_udf_load_follows_the_session_policy() {
        use crate::execution::c_udf::policy::NativeUdfPolicy;
        use crate::execution::c_udf::test_support::{test_udfs_path, BUILD_HINT};

        let int64 = || spark_expression::DataType {
            type_id: 4, // INT64
            type_info: None,
        };
        let library = test_udfs_path();
        let call = Expr {
            expr_struct: Some(NativeScalarUdf(spark_expression::NativeScalarUdf {
                name: "add_one_c".to_string(),
                library_path: library.to_string_lossy().into_owned(),
                args: vec![Expr {
                    expr_struct: Some(Bound(spark_expression::BoundReference {
                        index: 0,
                        datatype: Some(int64()),
                    })),
                    ..Default::default()
                }],
                return_type: Some(int64()),
                deterministic: true,
            })),
            ..Default::default()
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let plan_with = |policy: NativeUdfPolicy| {
            let config = SessionConfig::new().with_extension(Arc::new(policy));
            PhysicalPlanner::new(Arc::new(SessionContext::new_with_config(config)), 0)
                .create_expr(&call, Arc::clone(&schema))
        };

        plan_with(NativeUdfPolicy::default()).expect(BUILD_HINT);

        let err = plan_with(NativeUdfPolicy::new(false, "")).unwrap_err();
        assert!(err.to_string().contains("disabled"), "unexpected: {err}");

        let elsewhere = tempfile::tempdir().unwrap();
        let err = plan_with(NativeUdfPolicy::new(
            true,
            elsewhere.path().to_str().unwrap(),
        ))
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("spark.comet.nativeUdf.allowedPaths"),
            "unexpected: {err}"
        );

        let allowed = library.parent().unwrap().to_str().unwrap().to_string();
        plan_with(NativeUdfPolicy::new(true, &allowed)).expect(BUILD_HINT);
    }
}
