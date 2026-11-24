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

//! Reads the file slices of a native Hudi scan that hold only a base file
//! through Comet's native Parquet scan.
//!
//! A base file is a complete snapshot of its file group as of its commit, so a
//! slice with no log files needs no merge, and the JVM scan reads it with
//! Spark's Parquet reader. Comet's Parquet scan reads it the same way, with
//! the scan's data filters pruning row groups and pages, one footer request
//! shared through the metadata cache, and the next file's footer loaded ahead.
//!
//! A base file written before a column changed type is the exception: the JVM
//! scan reads that column as stored and casts it with Hudi's own rules, which
//! the Parquet scan does not have. Each base file's footer is therefore read
//! first, a few files ahead of the read, through the cache the Parquet scan
//! reads it from again. A file that stores a required column as another type,
//! or lacks a required column that is not a table data column, goes to the
//! hudi-rs reader along with the slices that have log files.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::PageIndexPolicy;

use super::hudi_scan::HudiFileSliceSpec;

/// Bytes read from the end of a base file to load its footer in one request.
/// A Hudi base file keeps its bloom filter in the footer, which takes the
/// footer of a file written with the default bloom settings past the 512 KiB
/// Comet's Parquet scan reads for other files.
pub const HUDI_BASE_FILE_METADATA_SIZE_HINT: usize = 1024 * 1024;

/// Base files whose footers a partition checks together, just before it reads
/// them, so a footer is fetched close to its read and stays in the cache.
const FOOTER_WINDOW: usize = 4;

/// Slices this partition read through the Parquet scan, and through hudi-rs.
const PARQUET_SLICES_METRIC: &str = "hudi_slices_read_as_parquet";
const HUDI_READER_SLICES_METRIC: &str = "hudi_slices_read_by_hudi_reader";

/// Builds the hudi-rs scan of the given file slices.
pub type HudiScanFactory =
    Arc<dyn Fn(Vec<HudiFileSliceSpec>) -> Arc<dyn ExecutionPlan> + Send + Sync>;

/// The slices of a partition that have no log files, and what reads their
/// base files.
#[derive(Clone)]
pub struct HudiBaseFileSlices {
    /// Each slice with its base file as the Parquet scan reads it.
    pub slices: Vec<(HudiFileSliceSpec, PartitionedFile)>,
    /// Comet's Parquet scan over every base file in `slices`.
    pub parquet_scan: Arc<dyn ExecutionPlan>,
    /// The store and the metadata cache the Parquet scan reads through.
    pub object_store: Arc<dyn ObjectStore>,
    pub metadata_cache: Arc<FileMetadataCache>,
    pub case_sensitive: bool,
    /// The table's data columns, without its partition columns, lowercased
    /// unless `case_sensitive`. A base file may lack only these.
    pub table_data_columns: Arc<HashSet<String>>,
}

pub struct HudiBaseFileRouteExec {
    output_schema: SchemaRef,
    /// None when every slice of the partition has log files.
    base_file_slices: Option<HudiBaseFileSlices>,
    /// Slices only the hudi-rs reader reads.
    merged_slices: Vec<HudiFileSliceSpec>,
    hudi_scan: HudiScanFactory,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl HudiBaseFileRouteExec {
    pub fn new(
        output_schema: SchemaRef,
        base_file_slices: Option<HudiBaseFileSlices>,
        merged_slices: Vec<HudiFileSliceSpec>,
        hudi_scan: HudiScanFactory,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            output_schema,
            base_file_slices,
            merged_slices,
            hudi_scan,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn base_file_slice_count(&self) -> usize {
        self.base_file_slices
            .as_ref()
            .map_or(0, |base_files| base_files.slices.len())
    }
}

impl fmt::Debug for HudiBaseFileRouteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HudiBaseFileRouteExec")
            .field("output_schema", &self.output_schema)
            .field("base_file_slices", &self.base_file_slice_count())
            .field("merged_slices", &self.merged_slices.len())
            .finish()
    }
}

impl DisplayAs for HudiBaseFileRouteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HudiBaseFileRouteExec: base_file_slices={}, merged_slices={}",
            self.base_file_slice_count(),
            self.merged_slices.len()
        )
    }
}

/// Whether a Parquet column that arrow reads as `stored` is what a Hudi writer
/// stores for the Spark type that arrow reads as `required`, so that reading
/// it converts nothing. Timestamps qualify only as UTC microseconds: Hudi
/// repairs the unit of some older files against the table schema, and reads
/// INT96 with the calendar the file was written in.
fn stored_as_required(stored: &DataType, required: &DataType) -> bool {
    match (stored, required) {
        (
            DataType::Decimal32(stored_precision, stored_scale)
            | DataType::Decimal64(stored_precision, stored_scale)
            | DataType::Decimal128(stored_precision, stored_scale),
            DataType::Decimal128(precision, scale),
        ) => stored_precision == precision && stored_scale == scale,
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
        ) => true,
        (
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::Date32,
            _,
        ) => stored == required,
        _ => false,
    }
}

/// Whether every required column the file holds is stored as the required
/// type, and every one it lacks is a table data column, added to the table
/// after the file was written, which reads as null from either reader.
fn stores_required_types(
    file_schema: &Schema,
    required: &Schema,
    case_sensitive: bool,
    table_data_columns: &HashSet<String>,
) -> bool {
    required.fields().iter().all(|field| {
        let mut stored = file_schema.fields().iter().filter(|candidate| {
            if case_sensitive {
                candidate.name() == field.name()
            } else {
                candidate.name().to_lowercase() == field.name().to_lowercase()
            }
        });
        match (stored.next(), stored.next()) {
            (None, _) => table_data_columns.contains(&if case_sensitive {
                field.name().to_string()
            } else {
                field.name().to_lowercase()
            }),
            (Some(stored), None) => stored_as_required(stored.data_type(), field.data_type()),
            // More than one column answers to the name.
            (Some(_), Some(_)) => false,
        }
    })
}

/// Loads the base file's footer and page index into the cache Comet's Parquet
/// scan reads them from, with the request that scan would issue, and checks
/// the file's schema against the required one.
async fn is_plain_parquet_read(
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<FileMetadataCache>,
    file: &PartitionedFile,
    required: &Schema,
    case_sensitive: bool,
    table_data_columns: &HashSet<String>,
) -> DFResult<bool> {
    let metadata = DFParquetMetadata::new(store.as_ref(), &file.object_meta)
        .with_file_metadata_cache(Some(metadata_cache))
        .with_page_index_policy(Some(PageIndexPolicy::Optional))
        .with_metadata_size_hint(file.metadata_size_hint)
        .fetch_metadata()
        .await?;
    // The schema as the Parquet types give it, without the arrow schema a
    // writer may have embedded in the footer.
    match parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None) {
        Ok(file_schema) => Ok(stores_required_types(
            &file_schema,
            required,
            case_sensitive,
            table_data_columns,
        )),
        Err(_) => Ok(false),
    }
}

/// The Parquet scan over `files` only.
fn with_files(
    parquet_scan: &Arc<dyn ExecutionPlan>,
    files: Vec<PartitionedFile>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let config = parquet_scan
        .downcast_ref::<DataSourceExec>()
        .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
        .ok_or_else(|| {
            DataFusionError::Internal("Hudi base file scan is not a file scan".to_string())
        })?;
    let config = FileScanConfigBuilder::from(config.clone())
        .with_file_groups(vec![FileGroup::new(files)])
        .build();
    Ok(Arc::new(DataSourceExec::new(Arc::new(config))))
}

/// Gives a batch the scan's output schema. Both readers produce the output
/// columns in order with the output types; only schema metadata can differ.
fn with_output_schema(batch: RecordBatch, schema: &SchemaRef) -> DFResult<RecordBatch> {
    if Arc::ptr_eq(batch.schema_ref(), schema) || batch.schema_ref() == schema {
        return Ok(batch);
    }
    let num_rows = batch.num_rows();
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        batch.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .map_err(|e| {
        DataFusionError::Execution(format!(
            "Hudi base file scan produced a batch that does not match the scan schema: {e}"
        ))
    })
}

impl ExecutionPlan for HudiBaseFileRouteExec {
    fn name(&self) -> &str {
        "HudiBaseFileRouteExec"
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
        let schema = Arc::clone(&self.output_schema);
        let required = Arc::clone(&self.output_schema);
        let base_file_slices = self.base_file_slices.clone();
        let hudi_slices = self.merged_slices.clone();
        let hudi_scan = Arc::clone(&self.hudi_scan);
        let context_for_merged = Arc::clone(&context);
        let baseline = BaselineMetrics::new(&self.metrics, 0);
        let parquet_slices = MetricBuilder::new(&self.metrics).global_counter(PARQUET_SLICES_METRIC);
        let hudi_reader_slices =
            MetricBuilder::new(&self.metrics).global_counter(HUDI_READER_SLICES_METRIC);
        let hudi_reader_slices_for_base = hudi_reader_slices.clone();

        let merged = futures::stream::once(async move {
            hudi_reader_slices.add(hudi_slices.len());
            if hudi_slices.is_empty() {
                Ok::<_, DataFusionError>(None)
            } else {
                Ok(Some(hudi_scan(hudi_slices).execute(0, context_for_merged)?))
            }
        })
        .map_ok(|stream| match stream {
            Some(stream) => stream.boxed(),
            None => futures::stream::empty().boxed(),
        })
        .try_flatten();
        let base_files = match base_file_slices {
            None => futures::stream::empty().boxed(),
            Some(base_files) => {
                let HudiBaseFileSlices {
                    slices,
                    parquet_scan,
                    object_store,
                    metadata_cache,
                    case_sensitive,
                    table_data_columns,
                } = base_files;
                let hudi_scan = Arc::clone(&self.hudi_scan);
                let hudi_reader_slices = hudi_reader_slices_for_base.clone();
                // Windows in slice order: a window's footers are checked together when the
                // read reaches it, then its plain files go to the Parquet scan and the rest
                // to the hudi-rs reader.
                let windows: Vec<Vec<(HudiFileSliceSpec, PartitionedFile)>> = slices
                    .chunks(FOOTER_WINDOW)
                    .map(|window| window.to_vec())
                    .collect();
                futures::stream::iter(windows)
                    .then(move |window| {
                        let object_store = Arc::clone(&object_store);
                        let metadata_cache = Arc::clone(&metadata_cache);
                        let required = Arc::clone(&required);
                        let table_data_columns = Arc::clone(&table_data_columns);
                        let parquet_scan = Arc::clone(&parquet_scan);
                        let hudi_scan = Arc::clone(&hudi_scan);
                        let context = Arc::clone(&context);
                        let parquet_slices = parquet_slices.clone();
                        let hudi_reader_slices = hudi_reader_slices.clone();
                        async move {
                            let checked: Vec<(HudiFileSliceSpec, PartitionedFile, bool)> =
                                futures::future::try_join_all(window.into_iter().map(
                                    |(slice, file)| {
                                        let object_store = Arc::clone(&object_store);
                                        let metadata_cache = Arc::clone(&metadata_cache);
                                        let required = Arc::clone(&required);
                                        let table_data_columns = Arc::clone(&table_data_columns);
                                        async move {
                                            let plain = is_plain_parquet_read(
                                                object_store,
                                                metadata_cache,
                                                &file,
                                                required.as_ref(),
                                                case_sensitive,
                                                table_data_columns.as_ref(),
                                            )
                                            .await
                                            .map_err(|e| {
                                                DataFusionError::Execution(format!(
                                                    "failed to read the footer of Hudi base file {}: {e}",
                                                    slice.base_file_path
                                                ))
                                            })?;
                                            Ok::<_, DataFusionError>((slice, file, plain))
                                        }
                                    },
                                ))
                                .await?;
                            let mut parquet_files = Vec::new();
                            let mut other_slices = Vec::new();
                            for (slice, file, plain) in checked {
                                if plain {
                                    parquet_files.push(file);
                                } else {
                                    other_slices.push(slice);
                                }
                            }
                            parquet_slices.add(parquet_files.len());
                            hudi_reader_slices.add(other_slices.len());
                            let mut streams: Vec<SendableRecordBatchStream> = Vec::with_capacity(2);
                            if !parquet_files.is_empty() {
                                streams.push(
                                    with_files(&parquet_scan, parquet_files)?
                                        .execute(0, Arc::clone(&context))?,
                                );
                            }
                            if !other_slices.is_empty() {
                                streams.push(hudi_scan(other_slices).execute(0, context)?);
                            }
                            Ok::<_, DataFusionError>(futures::stream::iter(streams).flatten())
                        }
                    })
                    .try_flatten()
                    .boxed()
            }
        };
        let batches = base_files
            .chain(merged)
            .map(move |batch| {
                let batch = with_output_schema(batch?, &schema)?;
                baseline.record_output(batch.num_rows());
                Ok(batch)
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
