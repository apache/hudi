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

pub mod expression_registry;
pub mod macros;
pub mod operator_registry;

use crate::execution::operators::init_csv_datasource_exec;
use crate::execution::operators::AlignedArrowStreamReader;
use crate::execution::operators::IcebergScanExec;
use crate::execution::operators::{HudiFileSliceSpec, HudiScanExec};
use crate::execution::{
    expressions::list_empty_to_null::ListEmptyToNullExpr,
    expressions::list_positions::ListPositionsExpr,
    expressions::subquery::Subquery,
    operators::{
        ExecutionError, ExpandExec, ParquetCompression, ParquetWriterExec, SampleExec, ScanExec,
        ShuffleScanExec,
    },
    planner::expression_registry::ExpressionRegistry,
    planner::operator_registry::OperatorRegistry,
    serde::to_arrow_datatype,
    shuffle::{SchemaAlignExec, ShuffleWriterExec},
};
use crate::jvm_bridge::{jni_call, JVMClasses};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit, DECIMAL128_MAX_PRECISION};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::functions_aggregate::bit_and_or_xor::{bit_and_udaf, bit_or_udaf, bit_xor_udaf};
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::functions_aggregate::min_max::min_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::InputOrderMode;
use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef},
    common::DataFusionError,
    config::ConfigOptions,
    execution::FunctionRegistry,
    functions_aggregate::first_last::{FirstValue, LastValue},
    logical_expr::Operator as DataFusionOperator,
    physical_expr::{
        expressions::{
            in_list, BinaryExpr, CaseExpr, CastExpr, Column, IsNullExpr,
            Literal as DataFusionLiteral,
        },
        PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
    },
    physical_plan::{
        aggregates::{AggregateMode as DFAggregateMode, PhysicalGroupBy},
        empty::EmptyExec,
        joins::{utils::JoinFilter, HashJoinExec, PartitionMode, SortMergeJoinExec},
        limit::LocalLimitExec,
        projection::ProjectionExec,
        sorts::sort::SortExec,
        ExecutionPlan,
    },
    prelude::SessionContext,
};
use datafusion_comet_spark_expr::{
    create_comet_physical_fun, create_comet_physical_fun_with_eval_mode, BinaryOutputStyle,
    BloomFilterAgg, BloomFilterMightContain, CsvWriteOptions, EvalMode, SparkArraysZipFunc,
    SparkBloomFilterVersion, SparkPercentile, SumInteger, ToCsv,
};
use datafusion_spark::function::aggregate::collect::{SparkCollectList, SparkCollectSet};
use iceberg::expr::Bind;

use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::shuffle::{CometPartitioning, CompressionCodec};
use crate::execution::spark_plan::SparkPlan;
use crate::parquet::parquet_support::prepare_object_store_with_configs;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    JoinType as DFJoinType, NullEquality, ScalarValue,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion::logical_expr::{
    AggregateUDF, ReturnFieldArgs, ScalarUDF, TypeSignature, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::expressions::{Literal, StatsType};
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_expr::LexOrdering;

use crate::parquet::parquet_exec::init_datasource_exec;
use arrow::array::{
    new_empty_array, Array, ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray,
    NullArray, StringBuilder, TimestampMicrosecondArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::UnnestOptions;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion_comet_proto::spark_expression::ListLiteral;
use datafusion_comet_proto::spark_operator::SparkFilePartition;
use datafusion_comet_proto::{
    spark_expression::{
        self, agg_expr::ExprStruct as AggExprStruct, expr::ExprStruct, literal::Value, AggExpr,
        Expr, ScalarFunc,
    },
    spark_operator::{
        self, lower_window_frame_bound::LowerFrameBoundStruct, operator::OpStruct,
        upper_window_frame_bound::UpperFrameBoundStruct, AggregateMode as ProtoAggregateMode,
        BuildSide, CompressionCodec as SparkCompressionCodec, JoinType, Operator, WindowFrameType,
    },
    spark_partitioning::{partitioning::PartitioningStruct, Partitioning as SparkPartitioning},
};
use datafusion_comet_spark_expr::{
    jvm_udf::JvmScalarUdfExpr, ApproxPercentile, ArrayInsert, Avg, AvgDecimal, Cast, CheckOverflow,
    Correlation, Covariance, CreateNamedStruct, DecimalRescaleCheckOverflow, GetArrayStructFields,
    GetStructField, HllPlusPlus, IfExpr, ListExtract, NormalizeNaNAndZero, SparkCastOptions,
    Stddev, SumDecimal, ToJson, UnboundColumn, Variance, WideDecimalBinaryExpr, WideDecimalOp,
};
use itertools::Itertools;
use jni::objects::{Global, JObject};
use log::warn;
use num::{BigInt, ToPrimitive};
use object_store::path::Path;
use std::cmp::max;
use std::{collections::HashMap, sync::Arc};
use url::Url;

// For clippy error on type_complexity.
type PhyAggResult = Result<Vec<AggregateFunctionExpr>, ExecutionError>;
type PhyExprResult = Result<Vec<(Arc<dyn PhysicalExpr>, String)>, ExecutionError>;
type PartitionPhyExprResult = Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError>;
pub type PlanCreationResult =
    Result<(Vec<ScanExec>, Vec<ShuffleScanExec>, Arc<SparkPlan>), ExecutionError>;

struct JoinParameters {
    pub left: Arc<SparkPlan>,
    pub right: Arc<SparkPlan>,
    pub join_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    pub join_filter: Option<JoinFilter>,
    pub join_type: DFJoinType,
}

/// Return a copy of `data_type` with every nested field marked nullable. Map key fields are left
/// non-nullable to preserve Arrow's map invariant. Primitive types are returned unchanged.
fn make_all_fields_nullable(data_type: &DataType) -> DataType {
    fn nullable_field(field: &Field, nullable: bool) -> FieldRef {
        Arc::new(
            Field::new(
                field.name(),
                make_all_fields_nullable(field.data_type()),
                nullable,
            )
            .with_metadata(field.metadata().clone()),
        )
    }
    match data_type {
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|f| nullable_field(f, true)).collect())
        }
        DataType::List(field) => DataType::List(nullable_field(field, true)),
        DataType::LargeList(field) => DataType::LargeList(nullable_field(field, true)),
        DataType::FixedSizeList(field, len) => {
            DataType::FixedSizeList(nullable_field(field, true), *len)
        }
        DataType::Map(entries, sorted) => {
            // Map entries are a non-nullable struct of {key (non-null), value}. Recurse into the
            // key/value types but keep the key field non-nullable per Arrow's map invariant.
            match entries.data_type() {
                DataType::Struct(kv) => {
                    let new_kv = kv
                        .iter()
                        .enumerate()
                        .map(|(i, f)| nullable_field(f, i != 0))
                        .collect();
                    let new_entries = Field::new(
                        entries.name(),
                        DataType::Struct(new_kv),
                        entries.is_nullable(),
                    )
                    .with_metadata(entries.metadata().clone());
                    DataType::Map(Arc::new(new_entries), *sorted)
                }
                _ => data_type.clone(),
            }
        }
        other => other.clone(),
    }
}

/// If `expr` evaluates to `Timestamp(_, Some(_))` against `schema`, wrap it in a
/// metadata-only cast to `Timestamp(_, None)`. This is required because
/// DataFusion's `SortMergeJoinExec` comparator only supports timezone-less
/// timestamp types, while Spark's `TimestampType` serializes as
/// `Timestamp(µs, "UTC")`. The cast preserves ordering on the same time unit.
fn strip_timestamp_tz(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    match expr.data_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => Ok(Arc::new(CastExpr::new(
            expr,
            DataType::Timestamp(unit, None),
            None,
        ))),
        _ => Ok(expr),
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

    /// Return session context of this planner.
    pub fn session_ctx(&self) -> &Arc<SessionContext> {
        &self.session_ctx
    }

    /// Return partition id of this planner.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// get DataFusion PartitionedFiles from a Spark FilePartition
    fn get_partitioned_files(
        &self,
        partition: &SparkFilePartition,
    ) -> Result<Vec<PartitionedFile>, ExecutionError> {
        let mut files = Vec::with_capacity(partition.partitioned_file.len());
        partition.partitioned_file.iter().try_for_each(|file| {
            assert!(file.start + file.length <= file.file_size);

            let mut partitioned_file = PartitionedFile::new_with_range(
                String::new(), // Dummy file path.
                file.file_size as u64,
                file.start,
                file.start + file.length,
            );

            // Spark sends the path over as URL-encoded, parse that first.
            let url =
                Url::parse(file.file_path.as_ref()).map_err(|e| GeneralError(e.to_string()))?;
            // Convert that to a Path object to use in the PartitionedFile.
            let path = Path::from_url_path(url.path()).map_err(|e| GeneralError(e.to_string()))?;
            partitioned_file.object_meta.location = path;

            // Process partition values
            // Create an empty input schema for partition values because they are all literals.
            let empty_schema = Arc::new(Schema::empty());
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
                        DataType::Duration(TimeUnit::Microsecond) => {
                            ScalarValue::DurationMicrosecond(None)
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
                            dt => {
                                return Err(GeneralError(format!(
                                    "Expected either 'Int32' or 'Date32' for IntVal, but found {dt:?}"
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
                            dt => {
                                return Err(GeneralError(format!(
                                    "Expected 'Int64', 'Timestamp', or 'Duration(Microsecond)' for LongVal, but found {dt:?}"
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

                Ok(Arc::new(Cast::new(
                    child,
                    datatype,
                    SparkCastOptions::new_with_version(
                        eval_mode,
                        &expr.timezone,
                        expr.allow_incompat,
                        expr.is_spark4_plus,
                    ),
                    spark_expr.expr_id,
                    query_context,
                )))
            }
            ExprStruct::CheckOverflow(expr) => {
                let child =
                    self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let fail_on_error = expr.fail_on_error;

                // WideDecimalBinaryExpr already handles overflow — skip redundant check
                // but only if its output type matches CheckOverflow's declared type
                if child.downcast_ref::<WideDecimalBinaryExpr>().is_some() {
                    let child_type = child.data_type(&input_schema)?;
                    if child_type == data_type {
                        return Ok(child);
                    }
                }

                // Fuse Cast(Decimal128→Decimal128) + CheckOverflow into single rescale+check
                // Only fuse when the Cast target type matches the CheckOverflow output type
                if let Some(cast) = child.downcast_ref::<Cast>() {
                    if let (
                        DataType::Decimal128(p_out, s_out),
                        Ok(DataType::Decimal128(_p_in, s_in)),
                    ) = (&data_type, cast.child.data_type(&input_schema))
                    {
                        let cast_target = cast.data_type(&input_schema)?;
                        if cast_target == data_type {
                            return Ok(Arc::new(DecimalRescaleCheckOverflow::new(
                                Arc::clone(&cast.child),
                                s_in,
                                *p_out,
                                *s_out,
                                fail_on_error,
                            )));
                        }
                    }
                }

                // Look up query context from registry if expr_id is present
                let query_context = spark_expr.expr_id.and_then(|expr_id| {
                    let registry = &self.query_context_registry;
                    registry.get(expr_id)
                });

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
                    // DataFusion map_extract returns array of struct entries even if lookup by key
                    // Apache Spark wants a single value, so wrap the result into additional list extraction
                    "map_extract" => Ok(Arc::new(ListExtract::new(
                        func?,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                        None,
                        true,
                        false,
                        None, // No expr_id for internal map_extract wrapper
                        Arc::clone(&self.query_context_registry),
                    ))),
                    // DataFusion 49 hardcodes return type for MD5 built in function as UTF8View
                    // which is not yet supported in Comet
                    // Converting forcibly to UTF8. To be removed after UTF8View supported
                    "md5" => Ok(Arc::new(Cast::new(
                        func?,
                        DataType::Utf8,
                        SparkCastOptions::new_without_timezone(EvalMode::Try, true),
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

                create_case_expr(when_then_pairs, else_phy_expr, &input_schema)
            }
            ExprStruct::In(expr) => {
                let value =
                    self.create_expr(expr.in_value.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let list = expr
                    .lists
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                in_list(value, list, &expr.negated, input_schema.as_ref()).map_err(|e| e.into())
            }
            ExprStruct::If(expr) => {
                let if_expr =
                    self.create_expr(expr.if_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let true_expr =
                    self.create_expr(expr.true_expr.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let false_expr =
                    self.create_expr(expr.false_expr.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(IfExpr::new(if_expr, true_expr, false_expr)))
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
                let mut spark_cast_options =
                    SparkCastOptions::new(EvalMode::Try, &expr.timezone, true);
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
                Ok(Arc::new(ListExtract::new(
                    child,
                    ordinal,
                    default_value,
                    expr.one_based,
                    expr.fail_on_error,
                    spark_expr.expr_id,
                    Arc::clone(&self.query_context_registry),
                )))
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
                // Invariant: task_context is propagated for every JvmScalarUdfExpr built during
                // normal execution. The TEST_EXEC_CONTEXT_ID path is the only context in which
                // task_context may legitimately be None (unit tests, direct native driver runs).
                debug_assert!(
                    self.task_context.is_some() || self.exec_context_id == TEST_EXEC_CONTEXT_ID,
                    "task_context must be set for non-test execution"
                );
                Ok(Arc::new(JvmScalarUdfExpr::new(
                    udf.class_name.clone(),
                    args,
                    return_type,
                    udf.return_nullable,
                    self.task_context.clone(),
                )))
            }
            expr => Err(GeneralError(format!("Not implemented: {expr:?}"))),
        }
    }

    /// Create a DataFusion physical sort expression from Spark physical expression
    fn create_sort_expr<'a>(
        &'a self,
        spark_expr: &'a Expr,
        input_schema: SchemaRef,
    ) -> Result<PhysicalSortExpr, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            ExprStruct::SortOrder(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
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
        match (
            &op,
            left.data_type(&input_schema),
            right.data_type(&input_schema),
        ) {
            (
                DataFusionOperator::Plus | DataFusionOperator::Minus | DataFusionOperator::Multiply,
                Ok(DataType::Decimal128(p1, s1)),
                Ok(DataType::Decimal128(p2, s2)),
            ) if ((op == DataFusionOperator::Plus || op == DataFusionOperator::Minus)
                && max(s1, s2) as u8 + max(p1 - s1 as u8, p2 - s2 as u8)
                    >= DECIMAL128_MAX_PRECISION)
                || (op == DataFusionOperator::Multiply && p1 + p2 >= DECIMAL128_MAX_PRECISION) =>
            {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
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
                Ok(Arc::new(WideDecimalBinaryExpr::new(
                    left, right, wide_op, p_out, s_out, eval_mode,
                )))
            }
            (
                DataFusionOperator::Divide,
                Ok(DataType::Decimal128(_p1, _s1)),
                Ok(DataType::Decimal128(_p2, _s2)),
            ) => {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
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
                Ok(Arc::new(ScalarFunctionExpr::new(
                    func_name,
                    fun_expr,
                    vec![left, right],
                    Arc::new(Field::new(func_name, data_type, true)),
                    Arc::new(ConfigOptions::default()),
                )))
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
                let data_type = return_type.map(to_arrow_datatype).unwrap();
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
                            todo!("ANSI mode for Operator yet to be implemented!");
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

                    // Wrap with CheckedBinaryExpr to add query_context to errors
                    use crate::execution::expressions::arithmetic::CheckedBinaryExpr;
                    Ok(Arc::new(CheckedBinaryExpr::new(scalar_expr, query_context)))
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
            OpStruct::HashAgg(agg) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;

                let group_exprs: PhyExprResult = agg
                    .grouping_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, child.schema())
                            .map(|r| (r, format!("col_{idx}")))
                    })
                    .collect();
                let group_by = PhysicalGroupBy::new_single(group_exprs?);
                let schema = child.schema();

                let proto_mode = ProtoAggregateMode::try_from(agg.mode).map_err(|_| {
                    ExecutionError::GeneralError(format!(
                        "Unsupported aggregate mode: {}",
                        agg.mode
                    ))
                })?;
                let mode = match proto_mode {
                    ProtoAggregateMode::Partial => DFAggregateMode::Partial,
                    ProtoAggregateMode::Final => DFAggregateMode::Final,
                    // PartialMerge: Partial + MergeAsPartial
                    ProtoAggregateMode::PartialMerge => DFAggregateMode::Partial,
                };

                // Check if any expression uses PartialMerge mode. When present,
                // those expressions are wrapped with MergeAsPartial to get merge
                // semantics inside a Partial-mode AggregateExec.
                let partial_merge_value = ProtoAggregateMode::PartialMerge as i32;
                let has_partial_merge = proto_mode == ProtoAggregateMode::PartialMerge
                    || agg.expr_modes.contains(&partial_merge_value);

                let agg_exprs: PhyAggResult = agg
                    .agg_exprs
                    .iter()
                    .map(|expr| self.create_agg_expr(expr, Arc::clone(&schema)))
                    .collect();

                let aggr_expr: Vec<Arc<AggregateFunctionExpr>> = if has_partial_merge {
                    // Wrap PartialMerge expressions with MergeAsPartial.
                    // State fields in the child's output start at initial_input_buffer_offset.
                    let mut state_offset = agg.initial_input_buffer_offset as usize;
                    let per_expr_modes: Vec<i32> = if !agg.expr_modes.is_empty() {
                        agg.expr_modes.clone()
                    } else {
                        vec![agg.mode; agg.agg_exprs.len()]
                    };

                    agg_exprs?
                        .into_iter()
                        .enumerate()
                        .map(|(idx, expr)| {
                            if per_expr_modes[idx] == partial_merge_value {
                                // PartialMerge: wrap with MergeAsPartial
                                let state_fields = expr
                                    .state_fields()
                                    .map_err(|e| ExecutionError::GeneralError(e.to_string()))?;
                                let num_state_fields = state_fields.len();

                                let state_cols: Vec<Arc<dyn PhysicalExpr>> = (0..num_state_fields)
                                    .map(|i| {
                                        let col_idx = state_offset + i;
                                        let field = schema.field(col_idx);
                                        Arc::new(Column::new(field.name(), col_idx))
                                            as Arc<dyn PhysicalExpr>
                                    })
                                    .collect();
                                state_offset += num_state_fields;

                                let merge_udf =
                                    crate::execution::merge_as_partial::MergeAsPartialUDF::new(
                                        &expr,
                                    )
                                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))?;
                                let merge_udf_arc = Arc::new(
                                    datafusion::logical_expr::AggregateUDF::new_from_impl(
                                        merge_udf,
                                    ),
                                );

                                let merge_expr =
                                    AggregateExprBuilder::new(merge_udf_arc, state_cols)
                                        .schema(Arc::clone(&schema))
                                        .alias(format!("col_{idx}"))
                                        .with_ignore_nulls(expr.ignore_nulls())
                                        .with_distinct(expr.is_distinct())
                                        .build()
                                        .map_err(|e| {
                                            ExecutionError::DataFusionError(e.to_string())
                                        })?;

                                Ok(Arc::new(merge_expr))
                            } else {
                                Ok(Arc::new(expr))
                            }
                        })
                        .collect::<Result<Vec<_>, ExecutionError>>()?
                } else {
                    agg_exprs?.into_iter().map(Arc::new).collect()
                };

                // Build per-aggregate filter expressions from the FILTER (WHERE ...) clause.
                // Filters are only present in Partial mode; Final/PartialMerge always get None.
                let filter_exprs: Result<Vec<Option<Arc<dyn PhysicalExpr>>>, ExecutionError> = agg
                    .agg_exprs
                    .iter()
                    .map(|expr| {
                        if let Some(f) = expr.filter.as_ref() {
                            self.create_expr(f, Arc::clone(&schema)).map(Some)
                        } else {
                            Ok(None)
                        }
                    })
                    .collect();

                let aggregate: Arc<dyn ExecutionPlan> = Arc::new(
                    datafusion::physical_plan::aggregates::AggregateExec::try_new(
                        mode,
                        group_by,
                        aggr_expr,
                        filter_exprs?,
                        Arc::clone(&child.native_plan),
                        Arc::clone(&schema),
                    )?,
                );

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, aggregate, vec![child])),
                ))
            }

            OpStruct::BroadcastNestedLoopJoin(bnlj) => {
                let (join_params, scans, shuffle_scans) = self.parse_join_parameters(
                    inputs,
                    children,
                    &[],
                    &[],
                    bnlj.join_type,
                    &bnlj.condition,
                    partition_count,
                )?;

                let left = Arc::clone(&join_params.left.native_plan);
                let right = Arc::clone(&join_params.right.native_plan);

                let nested_loop_join = Arc::new(NestedLoopJoinExec::try_new(
                    left,
                    right,
                    join_params.join_filter,
                    &join_params.join_type,
                    None,
                )?);

                if bnlj.build_side == BuildSide::BuildRight as i32 {
                    let swapped_join = nested_loop_join.as_ref().swap_inputs()?;
                    let mut additional_native_plans = vec![];
                    if swapped_join.is::<ProjectionExec>() {
                        additional_native_plans.push(Arc::clone(swapped_join.children()[0]));
                    }
                    Ok((
                        scans,
                        shuffle_scans,
                        Arc::new(SparkPlan::new_with_additional(
                            spark_plan.plan_id,
                            swapped_join,
                            vec![join_params.left, join_params.right],
                            additional_native_plans,
                        )),
                    ))
                } else {
                    Ok((
                        scans,
                        shuffle_scans,
                        Arc::new(SparkPlan::new(
                            spark_plan.plan_id,
                            nested_loop_join,
                            vec![join_params.left, join_params.right],
                        )),
                    ))
                }
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

                let mut sort_exec: Arc<dyn ExecutionPlan> = Arc::new(
                    SortExec::new(
                        LexOrdering::new(exprs?).unwrap(),
                        Arc::clone(&child.native_plan),
                    )
                    .with_fetch(fetch),
                );

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

                let data_schema =
                    convert_spark_types_to_arrow_schema(common.data_schema.as_slice());
                let required_schema: SchemaRef =
                    convert_spark_types_to_arrow_schema(common.required_schema.as_slice());
                let partition_schema: SchemaRef =
                    convert_spark_types_to_arrow_schema(common.partition_schema.as_slice());
                let projection_vector: Vec<usize> = common
                    .projection_vector
                    .iter()
                    .map(|offset| *offset as usize)
                    .collect();

                let partition_files = scan
                    .file_partition
                    .as_ref()
                    .ok_or_else(|| GeneralError("NativeScan missing file_partition".into()))?;

                // Check if this partition has any files (bucketed scan with bucket pruning may have empty partitions)
                if partition_files.partitioned_file.is_empty() {
                    let empty_exec = Arc::new(EmptyExec::new(required_schema));
                    return Ok((
                        vec![],
                        vec![],
                        Arc::new(SparkPlan::new(spark_plan.plan_id, empty_exec, vec![])),
                    ));
                }

                // Convert the Spark expressions to Physical expressions
                let data_filters: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = common
                    .data_filters
                    .iter()
                    .map(|expr| self.create_expr(expr, Arc::clone(&required_schema)))
                    .collect();

                let default_values: Option<HashMap<Column, ScalarValue>> = if !common
                    .default_values
                    .is_empty()
                {
                    // We have default values. Extract the two lists (same length) of values and
                    // indexes in the schema, and then create a HashMap to use in the SchemaMapper.
                    let default_values: Result<Vec<ScalarValue>, DataFusionError> = common
                        .default_values
                        .iter()
                        .map(|expr| {
                            let literal = self.create_expr(expr, Arc::clone(&required_schema))?;
                            let df_literal =
                                literal.downcast_ref::<DataFusionLiteral>().ok_or_else(|| {
                                    GeneralError("Expected literal of default value.".to_string())
                                })?;
                            Ok(df_literal.value().clone())
                        })
                        .collect();
                    let default_values = default_values?;
                    let default_values_indexes: Vec<usize> = common
                        .default_values_indexes
                        .iter()
                        .map(|offset| *offset as usize)
                        .collect();
                    Some(
                        default_values_indexes
                            .into_iter()
                            .zip(default_values)
                            .map(|(idx, scalar_value)| {
                                let field = required_schema.field(idx);
                                let column = Column::new(field.name().as_str(), idx);
                                (column, scalar_value)
                            })
                            .collect(),
                    )
                } else {
                    None
                };

                // Get one file from this partition (we know it's not empty due to early return above)
                let one_file = partition_files
                    .partitioned_file
                    .first()
                    .map(|f| f.file_path.clone())
                    .expect("partition should have files after empty check");

                let object_store_options: HashMap<String, String> = common
                    .object_store_options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let (object_store_url, _) = prepare_object_store_with_configs(
                    self.session_ctx.runtime_env(),
                    one_file,
                    &object_store_options,
                )?;

                // Get files for this partition
                let files = self.get_partitioned_files(partition_files)?;
                let file_groups: Vec<Vec<PartitionedFile>> = vec![files];

                let scan = init_datasource_exec(
                    required_schema,
                    Some(data_schema),
                    Some(partition_schema),
                    object_store_url,
                    file_groups,
                    Some(projection_vector),
                    Some(data_filters?),
                    default_values,
                    common.session_timezone.as_str(),
                    common.case_sensitive,
                    common.return_null_struct_if_all_fields_missing,
                    common.allow_type_promotion,
                    common.allow_timestamp_ltz_to_ntz,
                    self.session_ctx(),
                    common.encryption_enabled,
                    common.use_field_id,
                    common.ignore_missing_field_id,
                )?;
                Ok((
                    vec![],
                    vec![],
                    Arc::new(SparkPlan::new(spark_plan.plan_id, scan, vec![])),
                ))
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
                let one_file = scan
                    .file_partitions
                    .first()
                    .and_then(|f| f.partitioned_file.first())
                    .map(|f| f.file_path.clone())
                    .ok_or(GeneralError("Failed to locate file".to_string()))?;
                let (object_store_url, _) = prepare_object_store_with_configs(
                    self.session_ctx.runtime_env(),
                    one_file,
                    &object_store_options,
                )?;
                let files =
                    self.get_partitioned_files(&scan.file_partitions[self.partition as usize])?;
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
                // struct; native takes ownership via `AlignedArrowStreamReader::from_raw`.
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
                        AlignedArrowStreamReader::from_raw(address as *mut FFI_ArrowArrayStream)
                    }
                    .map_err(|e| {
                        GeneralError(format!("Failed to import ArrowArrayStream from JVM: {e}"))
                    })?;
                    Some(Arc::new(std::sync::Mutex::new(reader)))
                };

                // The `ScanExec` operator will take actual arrays from Spark during execution
                let scan =
                    ScanExec::new(self.exec_context_id, input_source, &scan.source, data_types)?;

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
            OpStruct::ShuffleWriter(writer) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;

                let writer_input = align_shuffle_writer_input(
                    Arc::clone(&child.native_plan),
                    &writer.expected_output_schema,
                )?;

                let partitioning = self.create_partitioning(
                    writer.partitioning.as_ref().unwrap(),
                    writer_input.schema(),
                )?;

                let codec = match writer.codec.try_into() {
                    Ok(SparkCompressionCodec::None) => Ok(CompressionCodec::None),
                    Ok(SparkCompressionCodec::Snappy) => Ok(CompressionCodec::Snappy),
                    Ok(SparkCompressionCodec::Zstd) => {
                        Ok(CompressionCodec::Zstd(writer.compression_level))
                    }
                    Ok(SparkCompressionCodec::Lz4) => Ok(CompressionCodec::Lz4Frame),
                    _ => Err(GeneralError(format!(
                        "Unsupported shuffle compression codec: {:?}",
                        writer.codec
                    ))),
                }?;

                let write_buffer_size = writer.write_buffer_size as usize;
                // Zero on the wire means the limit is disabled; normalize it here so the writer
                // only ever sees a real limit or none at all.
                let max_buffer_bytes =
                    (writer.max_buffer_bytes > 0).then_some(writer.max_buffer_bytes as usize);
                let shuffle_writer = Arc::new(ShuffleWriterExec::try_new(
                    writer_input,
                    partitioning,
                    codec,
                    writer.output_data_file.clone(),
                    writer.output_index_file.clone(),
                    writer.tracing_enabled,
                    write_buffer_size,
                    max_buffer_bytes,
                )?);

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        shuffle_writer,
                        vec![Arc::clone(&child)],
                    )),
                ))
            }
            OpStruct::ParquetWriter(writer) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;

                let codec = match writer.compression.try_into() {
                    Ok(SparkCompressionCodec::None) => Ok(ParquetCompression::None),
                    Ok(SparkCompressionCodec::Snappy) => Ok(ParquetCompression::Snappy),
                    Ok(SparkCompressionCodec::Zstd) => Ok(ParquetCompression::Zstd(3)),
                    Ok(SparkCompressionCodec::Lz4) => Ok(ParquetCompression::Lz4),
                    Ok(SparkCompressionCodec::Gzip) => Ok(ParquetCompression::Gzip),
                    _ => Err(GeneralError(format!(
                        "Unsupported parquet compression codec: {:?}",
                        writer.compression
                    ))),
                }?;

                let object_store_options: HashMap<String, String> = writer
                    .object_store_options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                let parquet_writer = Arc::new(ParquetWriterExec::try_new(
                    Arc::clone(&child.native_plan),
                    writer.output_path.clone(),
                    writer
                        .work_dir
                        .as_ref()
                        .expect("work_dir is provided")
                        .clone(),
                    writer.job_id.clone(),
                    writer.task_attempt_id,
                    codec,
                    self.partition,
                    writer.column_names.clone(),
                    object_store_options,
                )?);

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        parquet_writer,
                        vec![Arc::clone(&child)],
                    )),
                ))
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

                assert!(
                    !projections.is_empty(),
                    "Expand should have at least one projection"
                );

                let datatypes = projections[0]
                    .iter()
                    .map(|expr| expr.data_type(&child.schema()))
                    .collect::<Result<Vec<DataType>, _>>()?;
                let fields: Vec<Field> = datatypes
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| Field::new(format!("col_{idx}"), dt.clone(), true))
                    .collect();
                let schema = Arc::new(Schema::new(fields));

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
            OpStruct::Explode(explode) => {
                assert_eq!(children.len(), 1);
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;

                // Create the expression for the array to explode
                let raw_child_expr = if let Some(child_expr) = &explode.child {
                    self.create_expr(child_expr, child.schema())?
                } else {
                    return Err(ExecutionError::GeneralError(
                        "Explode operator requires a child expression".to_string(),
                    ));
                };

                let child_schema = child.schema();
                let child_field_name = raw_child_expr
                    .return_field(&child_schema)
                    .expect("Failed to get field from child expression")
                    .name()
                    .to_string();

                // Bridge Spark's outer semantics: DataFusion's `UnnestExec` with
                // `preserve_nulls = true` emits one null row for a NULL list but drops rows
                // whose list is empty. Spark's `explode_outer`/`posexplode_outer` must emit
                // exactly one null row in both cases, so we mark empty rows as null before
                // unnesting. See https://github.com/apache/datafusion/issues/19053. Once
                // Comet moves to a DataFusion release carrying
                // https://github.com/apache/datafusion/pull/22100, `ListEmptyToNullExpr`
                // and the pre-projection below can be removed in favor of
                // `NullHandling::PreserveAndExpandEmpty`. See
                // https://github.com/apache/datafusion-comet/issues/5210.
                //
                // For `posexplode_outer` the wrapped array is materialized in a
                // pre-projection so `ListPositionsExpr` and the array passthrough share
                // a single evaluation of `ListEmptyToNullExpr` instead of re-running it
                // per branch. Plain `explode_outer` references the wrapped array exactly
                // once, so no pre-projection is needed there.
                let (child_expr, child_native_plan): (Arc<dyn PhysicalExpr>, _) =
                    match (explode.outer, explode.position) {
                        (true, true) => {
                            let wrapped: Arc<dyn PhysicalExpr> =
                                Arc::new(ListEmptyToNullExpr::new(raw_child_expr));
                            let reserved_name =
                                format!("__comet_explode_outer_{}", child_field_name);

                            let mut pre_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = child_schema
                                .fields()
                                .iter()
                                .enumerate()
                                .map(|(i, f)| {
                                    (
                                        Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
                                        f.name().to_string(),
                                    )
                                })
                                .collect();
                            let wrapped_idx = pre_exprs.len();
                            pre_exprs.push((wrapped, reserved_name.clone()));

                            let pre_exec = Arc::new(ProjectionExec::try_new(
                                pre_exprs,
                                Arc::clone(&child.native_plan),
                            )?);
                            (
                                Arc::new(Column::new(&reserved_name, wrapped_idx))
                                    as Arc<dyn PhysicalExpr>,
                                pre_exec as Arc<dyn ExecutionPlan>,
                            )
                        }
                        (true, false) => (
                            Arc::new(ListEmptyToNullExpr::new(raw_child_expr))
                                as Arc<dyn PhysicalExpr>,
                            Arc::clone(&child.native_plan),
                        ),
                        (false, _) => (raw_child_expr, Arc::clone(&child.native_plan)),
                    };

                // Create projection expressions for other columns
                let projections: Vec<Arc<dyn PhysicalExpr>> = explode
                    .project_list
                    .iter()
                    .map(|expr| self.create_expr(expr, child.schema()))
                    .collect::<Result<Vec<_>, _>>()?;

                // For posexplode, a parallel List<Int32> positions column is added before the
                // array column so UnnestExec can unnest both in parallel.
                let mut project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = projections
                    .iter()
                    .map(|expr| {
                        let field = expr
                            .return_field(&child_schema)
                            .expect("Failed to get field from expression");
                        let name = field.name().to_string();
                        (Arc::clone(expr), name)
                    })
                    .collect();

                if explode.position {
                    let positions_expr: Arc<dyn PhysicalExpr> =
                        Arc::new(ListPositionsExpr::new(Arc::clone(&child_expr)));
                    project_exprs.push((positions_expr, "pos".to_string()));
                }
                project_exprs.push((Arc::clone(&child_expr), child_field_name.clone()));

                let project_exec =
                    Arc::new(ProjectionExec::try_new(project_exprs, child_native_plan)?);

                let project_schema = project_exec.schema();

                // Build the output schema for UnnestExec
                let mut output_fields: Vec<Field> = Vec::new();

                // Add all projection columns (non-array columns)
                for i in 0..projections.len() {
                    output_fields.push(project_schema.field(i).clone());
                }

                let array_input_index = if explode.position {
                    // With outer=true, UnnestExec preserves rows whose array is empty or NULL
                    // and emits a NULL position for them, so pos must be nullable in that case.
                    output_fields.push(Field::new("pos", DataType::Int32, explode.outer));
                    projections.len() + 1
                } else {
                    projections.len()
                };

                // Extract the element type from the list/array type
                let array_field = project_schema.field(array_input_index);
                let element_type = match array_field.data_type() {
                    DataType::List(field) => field.data_type().clone(),
                    dt => {
                        return Err(ExecutionError::GeneralError(format!(
                            "Expected List type for explode, got {:?}",
                            dt
                        )))
                    }
                };

                output_fields.push(Field::new(
                    array_field.name(),
                    element_type,
                    true, // Element is nullable after unnesting
                ));

                let output_schema = Arc::new(Schema::new(output_fields));

                let mut list_unnests = Vec::with_capacity(2);
                if explode.position {
                    list_unnests.push(ListUnnest {
                        index_in_input_schema: projections.len(),
                        depth: 1,
                    });
                }
                list_unnests.push(ListUnnest {
                    index_in_input_schema: array_input_index,
                    depth: 1,
                });

                let unnest_options = UnnestOptions {
                    preserve_nulls: explode.outer,
                    recursions: vec![],
                };

                let unnest_exec = Arc::new(UnnestExec::new(
                    project_exec,
                    list_unnests,
                    vec![], // No struct columns to unnest
                    output_schema,
                    unnest_options,
                )?);

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, unnest_exec, vec![child])),
                ))
            }
            OpStruct::SortMergeJoin(join) => {
                let (join_params, scans, shuffle_scans) = self.parse_join_parameters(
                    inputs,
                    children,
                    &join.left_join_keys,
                    &join.right_join_keys,
                    join.join_type,
                    &join.condition,
                    partition_count,
                )?;

                let sort_options = join
                    .sort_options
                    .iter()
                    .map(|sort_option| {
                        let sort_expr = self
                            .create_sort_expr(sort_option, join_params.left.schema())
                            .unwrap();
                        SortOptions {
                            descending: sort_expr.options.descending,
                            nulls_first: sort_expr.options.nulls_first,
                        }
                    })
                    .collect();

                let left = Arc::clone(&join_params.left.native_plan);
                let right = Arc::clone(&join_params.right.native_plan);

                let left_schema = left.schema();
                let right_schema = right.schema();
                let join_on = join_params
                    .join_on
                    .into_iter()
                    .map(|(l, r)| {
                        Ok((
                            strip_timestamp_tz(l, left_schema.as_ref())?,
                            strip_timestamp_tz(r, right_schema.as_ref())?,
                        ))
                    })
                    .collect::<Result<Vec<_>, ExecutionError>>()?;

                let join = Arc::new(SortMergeJoinExec::try_new(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    join_on,
                    join_params.join_filter,
                    join_params.join_type,
                    sort_options,
                    // null doesn't equal to null in Spark join key. If the join key is
                    // `EqualNullSafe`, Spark will rewrite it during planning.
                    NullEquality::NullEqualsNothing,
                )?);

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        join,
                        vec![
                            Arc::clone(&join_params.left),
                            Arc::clone(&join_params.right),
                        ],
                    )),
                ))
            }
            OpStruct::HashJoin(join) => {
                let (join_params, scans, shuffle_scans) = self.parse_join_parameters(
                    inputs,
                    children,
                    &join.left_join_keys,
                    &join.right_join_keys,
                    join.join_type,
                    &join.condition,
                    partition_count,
                )?;

                let left = Arc::clone(&join_params.left.native_plan);
                let right = Arc::clone(&join_params.right.native_plan);

                // Null-aware anti-join must run in CollectLeft mode. In Partitioned mode
                // each partition only sees per-partition null/emptiness state, which can
                // produce wrong NOT IN results across partitions. DataFusion's JoinSelection
                // rewrites null-aware joins to CollectLeft for this reason, but Comet
                // executes the physical plan directly so we must pick the mode here.
                let partition_mode = if join.null_aware_anti_join {
                    PartitionMode::CollectLeft
                } else {
                    PartitionMode::Partitioned
                };

                let hash_join = Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    join_params.join_on,
                    join_params.join_filter,
                    &join_params.join_type,
                    None,
                    partition_mode,
                    // null doesn't equal to null in Spark join key. If the join key is
                    // `EqualNullSafe`, Spark will rewrite it during planning.
                    NullEquality::NullEqualsNothing,
                    join.null_aware_anti_join,
                )?);

                // If the hash join is build right, we need to swap the left and right.
                // Exception: null-aware anti-join requires LeftAnti + build-right semantics
                // (which matches DataFusion's default), and swap_inputs would turn LeftAnti
                // into RightAnti, which DataFusion rejects with null_aware=true.
                if join.build_side == BuildSide::BuildLeft as i32 || join.null_aware_anti_join {
                    Ok((
                        scans,
                        shuffle_scans,
                        Arc::new(SparkPlan::new(
                            spark_plan.plan_id,
                            hash_join,
                            vec![join_params.left, join_params.right],
                        )),
                    ))
                } else {
                    let swapped_hash_join =
                        hash_join.as_ref().swap_inputs(PartitionMode::Partitioned)?;

                    let mut additional_native_plans = vec![];
                    if swapped_hash_join.is::<ProjectionExec>() {
                        // a projection was added to the hash join
                        additional_native_plans.push(Arc::clone(swapped_hash_join.children()[0]));
                    }

                    Ok((
                        scans,
                        shuffle_scans,
                        Arc::new(SparkPlan::new_with_additional(
                            spark_plan.plan_id,
                            swapped_hash_join,
                            vec![join_params.left, join_params.right],
                            additional_native_plans,
                        )),
                    ))
                }
            }
            OpStruct::Window(wnd) => {
                let (scans, shuffle_scans, child) =
                    self.create_plan(&children[0], inputs, partition_count)?;
                let input_schema = child.schema();
                let sort_exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = wnd
                    .order_by_list
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, Arc::clone(&input_schema)))
                    .collect();

                let partition_exprs: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = wnd
                    .partition_by_list
                    .iter()
                    .map(|expr| self.create_expr(expr, Arc::clone(&input_schema)))
                    .collect();

                let sort_exprs = &sort_exprs?;
                let partition_exprs = &partition_exprs?;

                let window_expr: Result<Vec<Arc<dyn WindowExpr>>, ExecutionError> = wnd
                    .window_expr
                    .iter()
                    .map(|expr| {
                        self.create_window_expr(
                            expr,
                            Arc::clone(&input_schema),
                            partition_exprs,
                            sort_exprs,
                        )
                    })
                    .collect();

                // Route to `BoundedWindowAggExec` when every window expression can
                // run with bounded memory. This uses DataFusion's
                // `evaluate_stateful` / row-by-row `evaluate` path, which is the
                // correct implementation for `LEAD` / `LAG` with `IGNORE NULLS`
                // (`WindowAggExec` calls `evaluate_all`, whose
                // `evaluate_all_with_ignore_null` has a sign-wrap bug for `LEAD`
                // that produces all-NULL output).
                //
                // Fall back to `WindowAggExec` otherwise. That covers
                // `PERCENT_RANK` / `CUME_DIST` / `NTILE`
                // (`!uses_bounded_memory()` — "Can not execute X in a streaming
                // fashion") and keeps the Spark-compatible Comet UDAFs
                // (`SumDecimal` / `SumInteger` / `AvgDecimal` / `Avg`) on the
                // non-streaming path since they don't implement `retract_batch`.
                // Because `process_agg_func` already picks DataFusion's
                // retract-capable built-ins for sliding aggregate frames,
                // ever-expanding aggregate frames (all that route to
                // `BoundedWindowAggExec` as `PlainAggregateWindowExpr`) never
                // trigger a retract call.
                let window_expr = window_expr?;
                let all_bounded = window_expr.iter().all(|e| e.uses_bounded_memory());
                let window_agg: Arc<dyn ExecutionPlan> = if all_bounded {
                    Arc::new(BoundedWindowAggExec::try_new(
                        window_expr,
                        Arc::clone(&child.native_plan),
                        InputOrderMode::Sorted,
                        !partition_exprs.is_empty(),
                    )?)
                } else {
                    Arc::new(WindowAggExec::try_new(
                        window_expr,
                        Arc::clone(&child.native_plan),
                        !partition_exprs.is_empty(),
                    )?)
                };

                // DataFusion's window functions don't always return the same Arrow
                // type that Spark expects (e.g. `row_number` returns UInt64 while
                // Spark expects Int32). If any window expression carries a
                // `result_type` that differs from the actual output type, wrap the
                // aggregate in a projection that casts the mismatched columns.
                let final_plan: Arc<dyn ExecutionPlan> = {
                    let agg_schema = window_agg.schema();
                    let input_field_count = input_schema.fields().len();
                    let mut needs_cast = false;
                    let mut proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
                        Vec::with_capacity(agg_schema.fields().len());
                    for (idx, field) in agg_schema.fields().iter().enumerate() {
                        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
                        let expr: Arc<dyn PhysicalExpr> = if idx >= input_field_count {
                            let w = &wnd.window_expr[idx - input_field_count];
                            match &w.result_type {
                                Some(t) => {
                                    let expected = to_arrow_datatype(t);
                                    if &expected != field.data_type() {
                                        needs_cast = true;
                                        Arc::new(CastExpr::new(col, expected, None))
                                    } else {
                                        col
                                    }
                                }
                                None => col,
                            }
                        } else {
                            col
                        };
                        proj_exprs.push((expr, field.name().to_string()));
                    }
                    if needs_cast {
                        Arc::new(ProjectionExec::try_new(proj_exprs, window_agg)?)
                    } else {
                        window_agg
                    }
                };

                Ok((
                    scans,
                    shuffle_scans,
                    Arc::new(SparkPlan::new(spark_plan.plan_id, final_plan, vec![child])),
                ))
            }
            OpStruct::ShuffleScan(scan) => {
                let data_types = scan.fields.iter().map(to_arrow_datatype).collect_vec();

                if self.exec_context_id != TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
                    return Err(GeneralError("No input for shuffle scan".to_string()));
                }

                let input_source =
                    if self.exec_context_id == TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
                        None
                    } else {
                        Some(inputs.remove(0))
                    };

                let shuffle_scan =
                    ShuffleScanExec::new(self.exec_context_id, input_source, data_types)?;

                Ok((
                    vec![],
                    vec![shuffle_scan.clone()],
                    Arc::new(SparkPlan::new(
                        spark_plan.plan_id,
                        Arc::new(shuffle_scan),
                        vec![],
                    )),
                ))
            }
            _ => Err(GeneralError(format!(
                "Unsupported or unregistered operator type: {:?}",
                spark_plan.op_struct
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_join_parameters(
        &self,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        children: &[Operator],
        left_join_keys: &[Expr],
        right_join_keys: &[Expr],
        join_type: i32,
        condition: &Option<Expr>,
        partition_count: usize,
    ) -> Result<(JoinParameters, Vec<ScanExec>, Vec<ShuffleScanExec>), ExecutionError> {
        assert_eq!(children.len(), 2);
        let (mut left_scans, mut left_shuffle_scans, left) =
            self.create_plan(&children[0], inputs, partition_count)?;
        let (mut right_scans, mut right_shuffle_scans, right) =
            self.create_plan(&children[1], inputs, partition_count)?;

        left_scans.append(&mut right_scans);
        left_shuffle_scans.append(&mut right_shuffle_scans);

        let left_join_exprs: Vec<_> = left_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, left.schema()))
            .collect::<Result<Vec<_>, _>>()?;
        let right_join_exprs: Vec<_> = right_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, right.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        let join_on = left_join_exprs
            .into_iter()
            .zip(right_join_exprs)
            .collect::<Vec<_>>();

        let join_type = match join_type.try_into() {
            Ok(JoinType::Inner) => DFJoinType::Inner,
            Ok(JoinType::LeftOuter) => DFJoinType::Left,
            Ok(JoinType::RightOuter) => DFJoinType::Right,
            Ok(JoinType::FullOuter) => DFJoinType::Full,
            Ok(JoinType::LeftSemi) => DFJoinType::LeftSemi,
            Ok(JoinType::LeftAnti) => DFJoinType::LeftAnti,
            Err(_) => {
                return Err(GeneralError(format!(
                    "Unsupported join type: {join_type:?}"
                )));
            }
        };

        // Handle join filter as DataFusion `JoinFilter` struct
        let join_filter = if let Some(expr) = condition {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_fields = left_schema.fields();
            let right_fields = right_schema.fields();
            let all_fields: Vec<_> = left_fields
                .into_iter()
                .chain(right_fields)
                .cloned()
                .collect();
            let full_schema = Arc::new(Schema::new(all_fields));

            // Because we cast dictionary array to array in scan operator,
            // we need to change dictionary type to data type for join filter expression.
            let fields: Vec<_> = full_schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => Arc::new(Field::new(
                        f.name(),
                        val_type.as_ref().clone(),
                        f.is_nullable(),
                    )),
                    _ => Arc::clone(f),
                })
                .collect();

            let full_schema = Arc::new(Schema::new(fields));

            let physical_expr = self.create_expr(expr, full_schema)?;
            let (left_field_indices, right_field_indices) =
                expr_to_columns(&physical_expr, left_fields.len(), right_fields.len())?;
            let column_indices = JoinFilter::build_column_indices(
                left_field_indices.clone(),
                right_field_indices.clone(),
            );

            let filter_fields: Vec<Field> = left_field_indices
                .clone()
                .into_iter()
                .map(|i| left.schema().field(i).clone())
                .chain(
                    right_field_indices
                        .clone()
                        .into_iter()
                        .map(|i| right.schema().field(i).clone()),
                )
                // Because we cast dictionary array to array in scan operator,
                // we need to change dictionary type to data type for join filter expression.
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => {
                        Field::new(f.name(), val_type.as_ref().clone(), f.is_nullable())
                    }
                    _ => f.clone(),
                })
                .collect_vec();

            let filter_schema = Schema::new_with_metadata(filter_fields, HashMap::new());

            // Rewrite the physical expression to use the new column indices.
            // DataFusion's join filter is bound to intermediate schema which contains
            // only the fields used in the filter expression. But the Spark's join filter
            // expression is bound to the full schema. We need to rewrite the physical
            // expression to use the new column indices.
            let rewritten_physical_expr = rewrite_physical_expr(
                physical_expr,
                left_schema.fields.len(),
                right_schema.fields.len(),
                &left_field_indices,
                &right_field_indices,
            )?;

            Some(JoinFilter::new(
                rewritten_physical_expr,
                column_indices,
                filter_schema.into(),
            ))
        } else {
            None
        };

        Ok((
            JoinParameters {
                left: Arc::clone(&left),
                right: Arc::clone(&right),
                join_on,
                join_type,
                join_filter,
            },
            left_scans,
            left_shuffle_scans,
        ))
    }

    /// Create a DataFusion physical aggregate expression from Spark physical aggregate expression
    fn create_agg_expr(
        &self,
        spark_expr: &AggExpr,
        schema: SchemaRef,
    ) -> Result<AggregateFunctionExpr, ExecutionError> {
        self.register_query_context(spark_expr.expr_id, spark_expr.query_context.as_ref());

        match spark_expr.expr_struct.as_ref().unwrap() {
            AggExprStruct::Count(expr) => {
                assert!(!expr.children.is_empty());
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&schema)))
                    .collect::<Result<Vec<_>, _>>()?;

                AggregateExprBuilder::new(count_udaf(), children)
                    .schema(schema)
                    .alias("count")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Min(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let child = Arc::new(CastExpr::new(child, datatype.clone(), None));

                AggregateExprBuilder::new(min_udaf(), vec![child])
                    .schema(schema)
                    .alias("min")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Max(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let child = Arc::new(CastExpr::new(child, datatype.clone(), None));

                AggregateExprBuilder::new(max_udaf(), vec![child])
                    .schema(schema)
                    .alias("max")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
            }
            AggExprStruct::Sum(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());

                let builder = match datatype {
                    DataType::Decimal128(_, _) => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = AggregateUDF::new_from_impl(SumDecimal::try_new(
                            datatype,
                            eval_mode,
                            spark_expr.expr_id,
                            Arc::clone(&self.query_context_registry),
                        )?);
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func =
                            AggregateUDF::new_from_impl(SumInteger::try_new(datatype, eval_mode)?);
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    _ => {
                        // cast to the result data type of SUM if necessary, we should not expect
                        // a cast failure since it should have already been checked at Spark side
                        let child =
                            Arc::new(CastExpr::new(Arc::clone(&child), datatype.clone(), None));
                        AggregateExprBuilder::new(sum_udaf(), vec![child])
                    }
                };
                builder
                    .schema(schema)
                    .alias("sum")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Avg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let input_datatype = to_arrow_datatype(expr.sum_datatype.as_ref().unwrap());

                let builder = match datatype {
                    DataType::Decimal128(_, _) => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = AggregateUDF::new_from_impl(AvgDecimal::new(
                            datatype,
                            input_datatype,
                            eval_mode,
                            spark_expr.expr_id,
                            Arc::clone(&self.query_context_registry),
                        ));
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                    _ => {
                        // For all other numeric types (Int8/16/32/64, Float32/64):
                        // Cast to Float64 for accumulation
                        let child: Arc<dyn PhysicalExpr> =
                            Arc::new(CastExpr::new(Arc::clone(&child), DataType::Float64, None));
                        let func = AggregateUDF::new_from_impl(Avg::new("avg", DataType::Float64));
                        AggregateExprBuilder::new(Arc::new(func), vec![child])
                    }
                };
                builder
                    .schema(schema)
                    .alias("avg")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::First(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(FirstValue::new());

                AggregateExprBuilder::new(Arc::new(func), vec![child])
                    .schema(schema)
                    .alias("first")
                    .with_ignore_nulls(expr.ignore_nulls)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Last(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(LastValue::new());

                AggregateExprBuilder::new(Arc::new(func), vec![child])
                    .schema(schema)
                    .alias("last")
                    .with_ignore_nulls(expr.ignore_nulls)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitAndAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_and_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_and")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitOrAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_or_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_or")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitXorAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;

                AggregateExprBuilder::new(bit_xor_udaf(), vec![child])
                    .schema(schema)
                    .alias("bit_xor")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::Covariance(expr) => {
                let child1 =
                    self.create_expr(expr.child1.as_ref().unwrap(), Arc::clone(&schema))?;
                let child2 =
                    self.create_expr(expr.child2.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Covariance::new(
                            "covariance",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr(
                            "covariance",
                            schema,
                            vec![child1, child2],
                            func,
                        )
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Covariance::new(
                            "covariance_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr(
                            "covariance_pop",
                            schema,
                            vec![child1, child2],
                            func,
                        )
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for Variance"
                    ))),
                }
            }
            AggExprStruct::Variance(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Variance::new(
                            "variance",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("variance", schema, vec![child], func)
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Variance::new(
                            "variance_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("variance_pop", schema, vec![child], func)
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for Variance"
                    ))),
                }
            }
            AggExprStruct::Stddev(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => {
                        let func = AggregateUDF::new_from_impl(Stddev::new(
                            "stddev",
                            datatype,
                            StatsType::Sample,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("stddev", schema, vec![child], func)
                    }
                    1 => {
                        let func = AggregateUDF::new_from_impl(Stddev::new(
                            "stddev_pop",
                            datatype,
                            StatsType::Population,
                            expr.null_on_divide_by_zero,
                        ));

                        Self::create_aggr_func_expr("stddev_pop", schema, vec![child], func)
                    }
                    stats_type => Err(GeneralError(format!(
                        "Unknown StatisticsType {stats_type:?} for stddev"
                    ))),
                }
            }
            AggExprStruct::Correlation(expr) => {
                let child1 =
                    self.create_expr(expr.child1.as_ref().unwrap(), Arc::clone(&schema))?;
                let child2 =
                    self.create_expr(expr.child2.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let func = AggregateUDF::new_from_impl(Correlation::new(
                    "correlation",
                    datatype,
                    expr.null_on_divide_by_zero,
                ));
                Self::create_aggr_func_expr("correlation", schema, vec![child1, child2], func)
            }
            AggExprStruct::Percentile(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let percentile =
                    self.create_expr(expr.percentage.as_ref().unwrap(), Arc::clone(&schema))?;
                // Spark's exact Percentile uses full-precision linear interpolation. Comet uses
                // its own UDAF rather than DataFusion's percentile_cont because DataFusion
                // quantizes the interpolation weight.
                let percentile_value = percentile_value(expr.percentage.as_ref().unwrap())?;
                let func = AggregateUDF::new_from_impl(SparkPercentile::try_new(percentile_value)?);
                AggregateExprBuilder::new(func.into(), vec![child, percentile])
                    .schema(schema)
                    .alias("percentile")
                    .with_ignore_nulls(false)
                    .with_distinct(false)
                    .build()
                    .map_err(|e| e.into())
            }
            AggExprStruct::ApproxPercentile(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let input_type = to_arrow_datatype(expr.input_type.as_ref().unwrap());
                let func = AggregateUDF::new_from_impl(ApproxPercentile::new(
                    expr.percentiles.clone(),
                    expr.accuracy,
                    input_type,
                    expr.return_array,
                ));
                Self::create_aggr_func_expr("approx_percentile", schema, vec![child], func)
            }
            AggExprStruct::BloomFilterAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let num_items =
                    self.create_expr(expr.num_items.as_ref().unwrap(), Arc::clone(&schema))?;
                let num_bits =
                    self.create_expr(expr.num_bits.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let version = match expr.version() {
                    spark_expression::BloomFilterVersion::V2 => SparkBloomFilterVersion::V2,
                    // Default (Unspecified or V1) preserves the pre-Spark-4.1 format that
                    // Comet has always emitted, keeping older Spark versions byte-equivalent.
                    _ => SparkBloomFilterVersion::V1,
                };
                let func = AggregateUDF::new_from_impl(BloomFilterAgg::new(
                    Arc::clone(&num_items),
                    Arc::clone(&num_bits),
                    datatype,
                    version,
                ));
                Self::create_aggr_func_expr("bloom_filter_agg", schema, vec![child], func)
            }
            AggExprStruct::CollectSet(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let child = Self::coerce_collect_child_nullability(child, &schema)?;
                let func = AggregateUDF::new_from_impl(SparkCollectSet::new());
                Self::create_aggr_func_expr("collect_set", schema, vec![child], func)
            }
            AggExprStruct::CollectList(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let child = Self::coerce_collect_child_nullability(child, &schema)?;
                let func = AggregateUDF::new_from_impl(SparkCollectList::new());
                Self::create_aggr_func_expr("collect_list", schema, vec![child], func)
            }
            AggExprStruct::Hllpp(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = AggregateUDF::new_from_impl(HllPlusPlus::new(expr.precision));
                Self::create_aggr_func_expr("approx_count_distinct", schema, vec![child], func)
            }
        }
    }

    /// Create a DataFusion windows physical expression from Spark physical expression
    fn create_window_expr<'a>(
        &'a self,
        spark_expr: &'a spark_operator::WindowExpr,
        input_schema: SchemaRef,
        partition_by: &[Arc<dyn PhysicalExpr>],
        sort_exprs: &[PhysicalSortExpr],
    ) -> Result<Arc<dyn WindowExpr>, ExecutionError> {
        let window_func: WindowFunctionDefinition;
        let window_func_name: String;
        let window_args: Vec<Arc<dyn PhysicalExpr>>;
        if let Some(func) = &spark_expr.built_in_window_function {
            match &func.expr_struct {
                Some(ExprStruct::ScalarFunc(f)) => {
                    window_func_name = f.func.clone();
                    window_args = f
                        .args
                        .iter()
                        .map(|expr| self.create_expr(expr, Arc::clone(&input_schema)))
                        .collect::<Result<Vec<_>, ExecutionError>>()?;
                    window_func =
                        self.find_df_window_function(&window_func_name)
                            .ok_or_else(|| {
                                GeneralError(format!(
                                    "{window_func_name} not supported for window function"
                                ))
                            })?;
                }
                other => {
                    return Err(GeneralError(format!(
                        "{other:?} not supported for window function"
                    )))
                }
            };
        } else if let Some(agg_func) = &spark_expr.agg_func {
            // Is the frame ever-expanding (start = UnboundedPreceding)? When it is,
            // DataFusion uses `PlainAggregateWindowExpr` which does not call
            // `retract_batch`, so we can safely use Comet's Spark-compatible
            // UDAFs (SumDecimal/SumInteger/AvgDecimal/Avg). Otherwise it uses
            // `SlidingAggregateWindowExpr` which requires retract — Comet's UDAFs
            // don't implement it, so the caller must fall back to DataFusion's
            // built-ins (which do).
            let is_ever_expanding = spark_expr
                .spec
                .as_ref()
                .and_then(|s| s.frame_specification.as_ref())
                .and_then(|f| f.lower_bound.as_ref())
                .and_then(|lb| lb.lower_frame_bound_struct.as_ref())
                .map(|inner| matches!(inner, LowerFrameBoundStruct::UnboundedPreceding(_)))
                .unwrap_or(true);
            let (func, args) =
                self.process_agg_func(agg_func, Arc::clone(&input_schema), is_ever_expanding)?;
            window_func_name = func.name().to_string();
            window_args = args;
            window_func = func;
        } else {
            return Err(GeneralError(
                "Both func and agg_func are not set".to_string(),
            ));
        }

        let spark_window_frame = match spark_expr
            .spec
            .as_ref()
            .and_then(|inner| inner.frame_specification.as_ref())
        {
            Some(frame) => frame,
            _ => {
                return Err(ExecutionError::DeserializeError(
                    "Cannot deserialize window frame".to_string(),
                ))
            }
        };

        let units = match spark_window_frame.frame_type() {
            WindowFrameType::Rows => WindowFrameUnits::Rows,
            WindowFrameType::Range => WindowFrameUnits::Range,
        };

        let lower_bound: WindowFrameBound = match spark_window_frame
            .lower_bound
            .as_ref()
            .and_then(|inner| inner.lower_frame_bound_struct.as_ref())
        {
            Some(l) => match l {
                LowerFrameBoundStruct::UnboundedPreceding(_) => match units {
                    WindowFrameUnits::Rows => {
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None))
                    }
                    WindowFrameUnits::Range => {
                        WindowFrameBound::Preceding(ScalarValue::Int64(None))
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                LowerFrameBoundStruct::Preceding(offset) => {
                    // Spark encodes ROWS bound direction via the sign of the offset:
                    // negative => PRECEDING, positive => FOLLOWING. The proto
                    // `LowerWindowFrameBound` only carries a `Preceding` variant, so
                    // Comet overloads it for both cases. Route to the matching
                    // DataFusion `WindowFrameBound` based on the sign.
                    match units {
                        WindowFrameUnits::Rows => {
                            let abs = offset.offset.unsigned_abs();
                            if offset.offset < 0 {
                                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(abs)))
                            } else {
                                WindowFrameBound::Following(ScalarValue::UInt64(Some(abs)))
                            }
                        }
                        WindowFrameUnits::Range => {
                            let scalar = match offset.range_offset.as_ref() {
                                Some(lit) => numeric_literal_to_scalar(lit)?,
                                None => ScalarValue::Int64(Some(offset.offset.abs())),
                            };
                            WindowFrameBound::Preceding(scalar)
                        }
                        WindowFrameUnits::Groups => {
                            return Err(GeneralError(
                                "WindowFrameUnits::Groups is not supported.".to_string(),
                            ));
                        }
                    }
                }
                LowerFrameBoundStruct::CurrentRow(_) => WindowFrameBound::CurrentRow,
            },
            None => match units {
                WindowFrameUnits::Rows => WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameUnits::Range => WindowFrameBound::Preceding(ScalarValue::Int64(None)),
                WindowFrameUnits::Groups => {
                    return Err(GeneralError(
                        "WindowFrameUnits::Groups is not supported.".to_string(),
                    ));
                }
            },
        };

        let upper_bound: WindowFrameBound = match spark_window_frame
            .upper_bound
            .as_ref()
            .and_then(|inner| inner.upper_frame_bound_struct.as_ref())
        {
            Some(u) => match u {
                UpperFrameBoundStruct::UnboundedFollowing(_) => match units {
                    WindowFrameUnits::Rows => {
                        WindowFrameBound::Following(ScalarValue::UInt64(None))
                    }
                    WindowFrameUnits::Range => {
                        WindowFrameBound::Following(ScalarValue::Int64(None))
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                UpperFrameBoundStruct::Following(offset) => match units {
                    WindowFrameUnits::Rows => {
                        // Mirror the lower-bound sign handling: the upper proto
                        // variant is `Following`, but Spark encodes the bound
                        // direction via sign. Negative => PRECEDING, positive =>
                        // FOLLOWING.
                        let abs = offset.offset.unsigned_abs();
                        if offset.offset < 0 {
                            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(abs)))
                        } else {
                            WindowFrameBound::Following(ScalarValue::UInt64(Some(abs)))
                        }
                    }
                    WindowFrameUnits::Range => {
                        let scalar = match offset.range_offset.as_ref() {
                            Some(lit) => numeric_literal_to_scalar(lit)?,
                            None => ScalarValue::Int64(Some(offset.offset)),
                        };
                        WindowFrameBound::Following(scalar)
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                UpperFrameBoundStruct::CurrentRow(_) => WindowFrameBound::CurrentRow,
            },
            None => match units {
                WindowFrameUnits::Rows => WindowFrameBound::Following(ScalarValue::UInt64(None)),
                WindowFrameUnits::Range => WindowFrameBound::Following(ScalarValue::Int64(None)),
                WindowFrameUnits::Groups => {
                    return Err(GeneralError(
                        "WindowFrameUnits::Groups is not supported.".to_string(),
                    ));
                }
            },
        };

        let window_frame = WindowFrame::new_bounds(units, lower_bound, upper_bound);
        let lex_orderings = LexOrdering::new(sort_exprs.to_vec());
        let sort_phy_exprs = lex_orderings.as_deref().unwrap_or(&[]);

        datafusion::physical_plan::windows::create_window_expr(
            &window_func,
            window_func_name,
            &window_args,
            partition_by,
            sort_phy_exprs,
            window_frame.into(),
            input_schema,
            spark_expr.ignore_nulls,
            false, // TODO: Spark does not support DISTINCT ... OVER
            None,
        )
        .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
    }

    fn process_agg_func(
        &self,
        agg_func: &AggExpr,
        schema: SchemaRef,
        is_ever_expanding: bool,
    ) -> Result<(WindowFunctionDefinition, Vec<Arc<dyn PhysicalExpr>>), ExecutionError> {
        // Wrap a freshly-constructed AggregateUDF impl as a WindowFunctionDefinition.
        fn udaf<U: datafusion::logical_expr::AggregateUDFImpl + 'static>(
            udaf: U,
        ) -> WindowFunctionDefinition {
            WindowFunctionDefinition::AggregateUDF(Arc::new(AggregateUDF::new_from_impl(udaf)))
        }

        // Resolve a window-capable function by name via the session registry, returning
        // a clean "X not supported for window function" error if missing.
        let by_name = |name: &str| -> Result<WindowFunctionDefinition, ExecutionError> {
            self.find_df_window_function(name)
                .ok_or_else(|| GeneralError(format!("{name} not supported for window function")))
        };

        match &agg_func.expr_struct {
            Some(AggExprStruct::Count(expr)) => {
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&schema)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((by_name("count")?, children))
            }
            Some(AggExprStruct::Min(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok((by_name("min")?, vec![child]))
            }
            Some(AggExprStruct::Max(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok((by_name("max")?, vec![child]))
            }
            Some(AggExprStruct::Sum(expr)) => {
                // For ever-expanding frames, use Comet's Spark-compatible Sum UDAFs
                // (SumDecimal / SumInteger) which enforce Spark overflow semantics.
                // For sliding frames, those UDAFs can't be used (no retract_batch),
                // so delegate to DataFusion's built-in `sum`, which supports retract
                // but doesn't enforce Spark's decimal precision overflow-to-NULL.
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let arrow_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match arrow_type {
                    DataType::Decimal128(_, _) if is_ever_expanding => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = SumDecimal::try_new(
                            arrow_type,
                            eval_mode,
                            agg_func.expr_id,
                            Arc::clone(&self.query_context_registry),
                        )?;
                        Ok((udaf(func), vec![child]))
                    }
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                        if is_ever_expanding =>
                    {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = SumInteger::try_new(arrow_type, eval_mode)?;
                        Ok((udaf(func), vec![child]))
                    }
                    _ => {
                        let actual = child.data_type(&schema)?;
                        let child: Arc<dyn PhysicalExpr> = if actual != arrow_type {
                            Arc::new(CastExpr::new(child, arrow_type, None))
                        } else {
                            child
                        };
                        Ok((by_name("sum")?, vec![child]))
                    }
                }
            }
            Some(AggExprStruct::Avg(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let input_datatype = to_arrow_datatype(expr.sum_datatype.as_ref().unwrap());
                match datatype {
                    DataType::Decimal128(_, _) if is_ever_expanding => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = AvgDecimal::new(
                            datatype,
                            input_datatype,
                            eval_mode,
                            agg_func.expr_id,
                            Arc::clone(&self.query_context_registry),
                        );
                        Ok((udaf(func), vec![child]))
                    }
                    _ if is_ever_expanding => {
                        let child: Arc<dyn PhysicalExpr> =
                            Arc::new(CastExpr::new(child, DataType::Float64, None));
                        let func = Avg::new("avg", DataType::Float64);
                        Ok((udaf(func), vec![child]))
                    }
                    _ => {
                        // Sliding frame — DataFusion's built-in `avg` handles retract.
                        // Cast non-decimal input to Float64 to match Spark's Avg result type.
                        let child: Arc<dyn PhysicalExpr> = match datatype {
                            DataType::Decimal128(_, _) => child,
                            _ => Arc::new(CastExpr::new(child, DataType::Float64, None)),
                        };
                        Ok((by_name("avg")?, vec![child]))
                    }
                }
            }
            Some(AggExprStruct::First(expr)) => {
                // Spark's FIRST_VALUE → DataFusion's `first_value` UDAF. The UDAF honors
                // ignore-nulls via the WindowExpr-level `ignore_nulls` flag.
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok((by_name("first_value")?, vec![child]))
            }
            Some(AggExprStruct::Last(expr)) => {
                // Spark's LAST_VALUE → DataFusion's `last_value` UDAF.
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok((by_name("last_value")?, vec![child]))
            }
            other => Err(GeneralError(format!(
                "{other:?} not supported for window function"
            ))),
        }
    }

    /// Find DataFusion's built-in window function by name.
    fn find_df_window_function(&self, name: &str) -> Option<WindowFunctionDefinition> {
        let registry = &self.session_ctx.state();
        // Prefer the WindowUDF (frame-aware) over the AggregateUDF when both exist.
        // Functions like `nth_value` are registered twice in DataFusion: once as an
        // aggregate and once as a window function. The window variant has the
        // correct IGNORE NULLS / frame semantics for an OVER clause; the aggregate
        // variant wrapped into PlainAggregateWindowExpr does not. Aggregate-only
        // functions (count/sum/min/max/avg/...) have no UDWF and fall through.
        registry
            .udwf(name)
            .map(WindowFunctionDefinition::WindowUDF)
            .ok()
            .or_else(|| {
                registry
                    .udaf(name)
                    .map(WindowFunctionDefinition::AggregateUDF)
                    .ok()
            })
    }

    /// Create a DataFusion physical partitioning from Spark physical partitioning
    fn create_partitioning(
        &self,
        spark_partitioning: &SparkPartitioning,
        input_schema: SchemaRef,
    ) -> Result<CometPartitioning, ExecutionError> {
        match spark_partitioning.partitioning_struct.as_ref().unwrap() {
            PartitioningStruct::HashPartition(hash_partition) => {
                let exprs: PartitionPhyExprResult = hash_partition
                    .hash_expression
                    .iter()
                    .map(|x| self.create_expr(x, Arc::clone(&input_schema)))
                    .collect();
                Ok(CometPartitioning::Hash(
                    exprs?,
                    hash_partition.num_partitions as usize,
                ))
            }
            PartitioningStruct::RangePartition(range_partition) => {
                // Generate the lexical ordering for comparisons
                let exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = range_partition
                    .sort_orders
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, Arc::clone(&input_schema)))
                    .collect();
                let lex_ordering = LexOrdering::new(exprs?).unwrap();

                // Generate the row converter for comparing incoming batches to boundary rows
                let sort_fields: Vec<SortField> = lex_ordering
                    .iter()
                    .map(|sort_expr| {
                        sort_expr
                            .expr
                            .data_type(input_schema.as_ref())
                            .map(|dt| SortField::new_with_options(dt, sort_expr.options))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Deserialize the literals to columnar collections of ScalarValues
                let mut scalar_values: Vec<Vec<ScalarValue>> = vec![vec![]; lex_ordering.len()];
                for boundary_row in &range_partition.boundary_rows {
                    // For each serialized expr in a boundary row, convert to a Literal
                    // expression, then extract the ScalarValue from the Literal and push it
                    // into the collection of ScalarValues
                    for (col_idx, col_values) in scalar_values
                        .iter_mut()
                        .enumerate()
                        .take(lex_ordering.len())
                    {
                        let expr = self.create_expr(
                            &boundary_row.partition_bounds[col_idx],
                            Arc::clone(&input_schema),
                        )?;
                        let literal_expr = expr.downcast_ref::<Literal>().expect("Literal");
                        col_values.push(literal_expr.value().clone());
                    }
                }

                // Convert the collection of ScalarValues to collection of Arrow Arrays
                let arrays: Vec<ArrayRef> = scalar_values
                    .iter()
                    .map(|scalar_vec| ScalarValue::iter_to_array(scalar_vec.iter().cloned()))
                    .collect::<Result<Vec<_>, _>>()?;

                // Create a RowConverter and use to create OwnedRows from the Arrays
                let converter = RowConverter::new(sort_fields)?;
                let boundary_rows = converter.convert_columns(&arrays)?;
                // Rows are only a view into Arrow Arrays. We need to create OwnedRows with their
                // own internal memory ownership to pass as our boundary values to the partitioner.
                let boundary_owned_rows: Vec<OwnedRow> =
                    boundary_rows.iter().map(|row| row.owned()).collect();

                Ok(CometPartitioning::RangePartitioning(
                    lex_ordering,
                    range_partition.num_partitions as usize,
                    Arc::new(converter),
                    boundary_owned_rows,
                ))
            }
            PartitioningStruct::SinglePartition(_) => Ok(CometPartitioning::SinglePartition),
            PartitioningStruct::RoundRobinPartition(rr_partition) => {
                // Treat negative max_hash_columns as 0 (no limit)
                let max_hash_columns = if rr_partition.max_hash_columns <= 0 {
                    0
                } else {
                    rr_partition.max_hash_columns as usize
                };
                Ok(CometPartitioning::RoundRobin(
                    rr_partition.num_partitions as usize,
                    max_hash_columns,
                ))
            }
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

    /// `collect_list` / `collect_set` build their result list with all element fields marked
    /// nullable, regardless of the input's nullability. However `SparkCollectList::return_type`
    /// (and `SparkCollectSet`) derive the element type directly from the child, preserving any
    /// non-nullable nested field. When the child is a nested type with a non-nullable inner field
    /// (e.g. a struct field), the declared aggregate output disagrees with the array the
    /// accumulator actually produces, and DataFusion's grouped `AggregateExec` fails validating
    /// its output batch ("column types must match schema types"). Cast the child to the
    /// all-nullable variant of its type so the declared and produced types stay consistent.
    fn coerce_collect_child_nullability(
        child: Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let child_type = child.data_type(schema.as_ref())?;
        let nullable_type = make_all_fields_nullable(&child_type);
        if child_type.equals_datatype(&nullable_type) {
            Ok(child)
        } else {
            Ok(Arc::new(CastExpr::new(child, nullable_type, None)))
        }
    }

    fn create_aggr_func_expr(
        name: &str,
        schema: SchemaRef,
        children: Vec<Arc<dyn PhysicalExpr>>,
        func: AggregateUDF,
    ) -> Result<AggregateFunctionExpr, ExecutionError> {
        AggregateExprBuilder::new(Arc::new(func), children)
            .schema(schema)
            .alias(name)
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()
            .map_err(|e| e.into())
    }
}

fn percentile_value(expr: &spark_expression::Expr) -> Result<f64, ExecutionError> {
    match &expr.expr_struct {
        Some(ExprStruct::Literal(literal)) if !literal.is_null => match &literal.value {
            Some(Value::DoubleVal(value)) => Ok(*value),
            Some(Value::FloatVal(value)) => Ok(*value as f64),
            _ => Err(GeneralError(
                "Percentile value must be a floating-point literal".to_string(),
            )),
        },
        _ => Err(GeneralError(
            "Percentile value must be a non-null literal".to_string(),
        )),
    }
}

/// Collects the indices of the columns in the input schema that are used in the expression
/// and returns them as a pair of vectors, one for the left side and one for the right side.
fn expr_to_columns(
    expr: &Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
) -> Result<(Vec<usize>, Vec<usize>), ExecutionError> {
    let mut left_field_indices: Vec<usize> = vec![];
    let mut right_field_indices: Vec<usize> = vec![];

    expr.apply(&mut |expr: &Arc<dyn PhysicalExpr>| {
        Ok({
            if let Some(column) = expr.downcast_ref::<Column>() {
                if column.index() > left_field_len + right_field_len {
                    return Err(DataFusionError::Internal(format!(
                        "Column index {} out of range",
                        column.index()
                    )));
                } else if column.index() < left_field_len {
                    left_field_indices.push(column.index());
                } else {
                    right_field_indices.push(column.index() - left_field_len);
                }
            }
            TreeNodeRecursion::Continue
        })
    })?;

    left_field_indices.sort();
    right_field_indices.sort();

    Ok((left_field_indices, right_field_indices))
}

/// Convert a Spark numeric Literal proto into a `ScalarValue` whose data type
/// matches the literal's declared type. Used for RANGE window frame offsets,
/// where the offset's type must match the ORDER BY column's type. Only numeric
/// types are supported; the Scala side rejects non-numeric RANGE offsets before
/// reaching here.
fn numeric_literal_to_scalar(
    lit: &spark_expression::Literal,
) -> Result<ScalarValue, ExecutionError> {
    let data_type = to_arrow_datatype(lit.datatype.as_ref().ok_or_else(|| {
        GeneralError("RANGE frame offset literal is missing datatype".to_string())
    })?);

    if lit.is_null {
        return Err(GeneralError(
            "RANGE frame offset must not be null".to_string(),
        ));
    }

    let value = lit
        .value
        .as_ref()
        .ok_or_else(|| GeneralError("RANGE frame offset literal has no value".to_string()))?;

    let scalar = match value {
        Value::ByteVal(v) => ScalarValue::Int8(Some(*v as i8)),
        Value::ShortVal(v) => ScalarValue::Int16(Some(*v as i16)),
        Value::IntVal(v) => ScalarValue::Int32(Some(*v)),
        Value::LongVal(v) => ScalarValue::Int64(Some(*v)),
        Value::FloatVal(v) => ScalarValue::Float32(Some(*v)),
        Value::DoubleVal(v) => ScalarValue::Float64(Some(*v)),
        Value::DecimalVal(bytes) => {
            let big_integer = BigInt::from_signed_bytes_be(bytes);
            let integer = big_integer.to_i128().ok_or_else(|| {
                GeneralError(format!(
                    "Cannot parse {big_integer:?} as i128 for Decimal RANGE frame offset"
                ))
            })?;
            match data_type {
                DataType::Decimal128(p, s) => ScalarValue::Decimal128(Some(integer), p, s),
                ref dt => {
                    return Err(GeneralError(format!(
                        "Decimal RANGE frame offset has non-Decimal128 datatype: {dt:?}"
                    )))
                }
            }
        }
        other => {
            return Err(GeneralError(format!(
                "Unsupported value variant for RANGE frame offset: {other:?}"
            )))
        }
    };

    Ok(scalar)
}

/// A physical join filter rewritter which rewrites the column indices in the expression
/// to use the new column indices. See `rewrite_physical_expr`.
struct JoinFilterRewriter<'a> {
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &'a [usize],
    right_field_indices: &'a [usize],
}

impl JoinFilterRewriter<'_> {
    fn new<'a>(
        left_field_len: usize,
        right_field_len: usize,
        left_field_indices: &'a [usize],
        right_field_indices: &'a [usize],
    ) -> JoinFilterRewriter<'a> {
        JoinFilterRewriter {
            left_field_len,
            right_field_len,
            left_field_indices,
            right_field_indices,
        }
    }
}

impl TreeNodeRewriter for JoinFilterRewriter<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: Self::Node) -> datafusion::common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.downcast_ref::<Column>() {
            if column.index() < self.left_field_len {
                // left side
                let new_index = self
                    .left_field_indices
                    .iter()
                    .position(|&x| x == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in left field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index,
                ))))
            } else if column.index() < self.left_field_len + self.right_field_len {
                // right side
                let new_index = self
                    .right_field_indices
                    .iter()
                    .position(|&x| x + self.left_field_len == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in right field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index + self.left_field_indices.len(),
                ))))
            } else {
                Err(DataFusionError::Internal(format!(
                    "Column index {} out of range",
                    column.index()
                )))
            }
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Rewrites the physical expression to use the new column indices.
/// This is necessary when the physical expression is used in a join filter, as the column
/// indices are different from the original schema.
fn rewrite_physical_expr(
    expr: Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &[usize],
    right_field_indices: &[usize],
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    let mut rewriter = JoinFilterRewriter::new(
        left_field_len,
        right_field_len,
        left_field_indices,
        right_field_indices,
    );

    Ok(expr.rewrite(&mut rewriter).data()?)
}

pub fn from_protobuf_eval_mode(value: i32) -> Result<EvalMode, prost::UnknownEnumValue> {
    match spark_expression::EvalMode::try_from(value)? {
        spark_expression::EvalMode::Legacy => Ok(EvalMode::Legacy),
        spark_expression::EvalMode::Try => Ok(EvalMode::Try),
        spark_expression::EvalMode::Ansi => Ok(EvalMode::Ansi),
    }
}

fn convert_spark_types_to_arrow_schema(
    spark_types: &[spark_operator::SparkStructField],
) -> SchemaRef {
    let arrow_fields = spark_types
        .iter()
        .map(|spark_type| {
            let field = Field::new(
                String::clone(&spark_type.name),
                to_arrow_datatype(spark_type.data_type.as_ref().unwrap()),
                spark_type.nullable,
            );
            if spark_type.metadata.is_empty() {
                field
            } else {
                field.with_metadata(spark_type.metadata.clone())
            }
        })
        .collect_vec();
    let arrow_schema: SchemaRef = Arc::new(Schema::new(arrow_fields));
    arrow_schema
}

/// Wrap `child` in a `SchemaAlignExec` when its output drifts from what Spark catalyst
/// declared. See <https://github.com/apache/datafusion-comet/issues/4515>.
fn align_shuffle_writer_input(
    child: Arc<dyn ExecutionPlan>,
    expected_proto: &[spark_operator::SparkStructField],
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    if expected_proto.is_empty() {
        return Ok(child);
    }
    let expected = convert_spark_types_to_arrow_schema(expected_proto);
    SchemaAlignExec::try_new_or_passthrough(child, &expected)
        .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
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

            Ok(iceberg::scan::FileScanTaskDeleteFile {
                file_path: del.file_path.clone(),
                file_type,
                // Not serialized; filled in by IcebergScanExec::fill_delete_file_sizes.
                file_size_in_bytes: 0,
                partition_spec_id: del.partition_spec_id,
                equality_ids: if del.equality_ids.is_empty() {
                    None
                } else {
                    Some(del.equality_ids.clone())
                },
                // Plaintext StandardKeyMetadata forwarded verbatim from the JVM; decoded by
                // iceberg-rust with no KMS unwrap. None for unencrypted delete files.
                key_metadata: del.key_metadata.clone().map(Vec::into_boxed_slice),
            })
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
                // rather than from partition_spec_cache: a spec that uses a transform iceberg-rust
                // doesn't recognize (e.g. forward-compatibility tests) fails to deserialize into a
                // PartitionSpec, but its "spec-id" is still present in the JSON and its partition
                // type entry has no usable fields anyway. Falling back to idx keeps ordering
                // deterministic if the field is somehow absent.
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

            Ok(iceberg::scan::FileScanTask {
                file_size_in_bytes: proto_task.file_size_in_bytes,
                data_file_path: proto_task.data_file_path.clone(),
                start: proto_task.start,
                length: proto_task.length,
                record_count: proto_task.record_count,
                data_file_format,
                schema: schema_ref,
                project_field_ids,
                predicate: bound_predicate,
                deletes,
                partition,
                partition_spec,
                name_mapping,
                unified_partition_type: unified_partition_type_for_task,
                case_sensitive: false,
                // Plaintext StandardKeyMetadata forwarded verbatim from the JVM; decoded by
                // iceberg-rust with no KMS unwrap. None for unencrypted data files.
                key_metadata: proto_task.key_metadata.clone().map(Vec::into_boxed_slice),
            })
        })
        .collect();

    results
}

/// Create CASE WHEN expression and add casting as needed
fn create_case_expr(
    when_then_pairs: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    else_expr: Option<Arc<dyn PhysicalExpr>>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    let then_types: Vec<DataType> = when_then_pairs
        .iter()
        .map(|x| x.1.data_type(input_schema))
        .collect::<Result<Vec<_>, _>>()?;

    let else_type: Option<DataType> = else_expr
        .as_ref()
        .map(|x| Arc::clone(x).data_type(input_schema))
        .transpose()?
        .or(Some(DataType::Null));

    if let Some(coerce_type) = get_coerce_type_for_case_expression(&then_types, else_type.as_ref())
    {
        let cast_options = SparkCastOptions::new_without_timezone(EvalMode::Legacy, false);

        let when_then_pairs = when_then_pairs
            .iter()
            .map(|x| {
                let t: Arc<dyn PhysicalExpr> = Arc::new(Cast::new(
                    Arc::clone(&x.1),
                    coerce_type.clone(),
                    cast_options.clone(),
                    None,
                    None,
                ));
                (Arc::clone(&x.0), t)
            })
            .collect::<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>>();

        let else_phy_expr: Option<Arc<dyn PhysicalExpr>> = else_expr.clone().map(|x| {
            Arc::new(Cast::new(
                x,
                coerce_type.clone(),
                cast_options.clone(),
                None,
                None,
            )) as Arc<dyn PhysicalExpr>
        });
        Ok(Arc::new(CaseExpr::try_new(
            None,
            when_then_pairs,
            else_phy_expr,
        )?))
    } else {
        Ok(Arc::new(CaseExpr::try_new(
            None,
            when_then_pairs,
            else_expr.clone(),
        )?))
    }
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
                        big_integer
                            .to_i128()
                            .ok_or_else(|| {
                                GeneralError(format!(
                                    "Cannot parse {big_integer:?} as i128 for Decimal literal"
                                ))
                            })
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
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
                // All entries are null or the list is empty
                new_empty_array(&dt)
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
    use futures::{poll, StreamExt};
    use std::{sync::Arc, task::Poll};

    use arrow::array::{
        Array, DictionaryArray, Int32Array, Int8Array, ListArray, RecordBatch, StringArray,
    };
    use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::config::TableParquetOptions;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
    };
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::{assert_batches_eq, physical_plan::common::collect, prelude::SessionContext};
    use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use crate::execution::{operators::InputBatch, planner::PhysicalPlanner};

    use crate::execution::operators::ExecutionError;
    use crate::execution::planner::literal_to_array_ref;
    use crate::execution::planner::parse_file_scan_tasks_from_common;
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
    use datafusion_comet_proto::spark_expression::expr::ExprStruct;
    use datafusion_comet_proto::spark_expression::ListLiteral;
    use datafusion_comet_proto::{
        spark_expression::expr::ExprStruct::*,
        spark_expression::Expr,
        spark_expression::{self, literal},
        spark_operator,
        spark_operator::{operator::OpStruct, Operator},
    };
    use datafusion_comet_spark_expr::EvalMode;

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

        let spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);

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
        let test_data = "select make_array(named_struct('a', 1, 'b', 'n', 'c', 'x')) c0";

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

        // Spec 0 (older): field 1000 named "region_old".
        let spec0 = PartitionSpec::builder(Arc::clone(&schema_arc))
            .with_spec_id(0)
            .add_unbound_field(iceberg::spec::UnboundPartitionField {
                source_id: 2,
                field_id: Some(1000),
                name: "region_old".to_string(),
                transform: iceberg::spec::Transform::Identity,
            })
            .expect("add field")
            .build()
            .expect("build spec0");

        // Spec 1 (newer): same field id 1000 renamed to "region_new", plus a new field 2000.
        let spec1 = PartitionSpec::builder(Arc::clone(&schema_arc))
            .with_spec_id(1)
            .add_unbound_field(iceberg::spec::UnboundPartitionField {
                source_id: 2,
                field_id: Some(1000),
                name: "region_new".to_string(),
                transform: iceberg::spec::Transform::Identity,
            })
            .expect("add field")
            .add_unbound_field(iceberg::spec::UnboundPartitionField {
                source_id: 3,
                field_id: Some(2000),
                name: "category".to_string(),
                transform: iceberg::spec::Transform::Identity,
            })
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

        let proto_task = spark_operator::IcebergFileScanTask {
            data_file_path: "file:///tmp/data.parquet".to_string(),
            file_size_in_bytes: 100,
            schema_idx: 0,
            partition_spec_idx: Some(0),
            project_field_ids_idx: 0,
            ..Default::default()
        };

        let tasks =
            parse_file_scan_tasks_from_common(&proto_common, &[proto_task]).expect("parse tasks");
        assert_eq!(tasks.len(), 1);

        let unified = tasks[0]
            .unified_partition_type
            .as_ref()
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

    #[test]
    fn test_unified_partition_type_tolerates_unparseable_spec() {
        // Regression for TestForwardCompatibility.testSparkCanReadUnknownTransform: a spec that
        // uses a transform iceberg-rust doesn't recognize fails to deserialize into a
        // PartitionSpec, but its partition field is filtered out on the Scala side (unknown type),
        // so its partition_type_pool entry is an empty struct. Recovering spec_id must not require
        // the full spec to parse -- the merge must succeed (with no fields) rather than erroring.
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
        // entry (as Scala produces once the unknown-type field is filtered).
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
                .unified_partition_type
                .as_ref()
                .map(|t| t.fields().is_empty())
                .unwrap_or(true),
            "unified partition type should have no fields for an all-unknown-transform spec"
        );
    }
}
