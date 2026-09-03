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
//! requested columns.
//!
//! hudi-rs is on the arrow 57 line while this workspace is on 58. Every batch
//! crosses between the two through the Arrow C data interface, whose layout is
//! fixed by the Arrow specification and therefore identical across arrow crate
//! majors; the transfer is pointer-level, not a copy.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions, StructArray};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use arrow_57::array::{Array as _, StructArray as StructArray57};
use arrow_57::record_batch::RecordBatch as RecordBatch57;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use hudi::file_group::reader::FileGroupReader;
use hudi::table::ReadOptions;

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
    options: HashMap<String, String>,
    output_schema: SchemaRef,
    file_slices: Vec<HudiFileSliceSpec>,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl HudiScanExec {
    pub fn new(
        table_base_uri: String,
        options: HashMap<String, String>,
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
            options,
            output_schema,
            file_slices,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

/// Moves a batch from hudi-rs's arrow 57 into this workspace's arrow 58 over
/// the Arrow C data interface.
fn batch_across_ffi(batch: RecordBatch57) -> DFResult<RecordBatch> {
    let data = StructArray57::from(batch).into_data();
    let (ffi_array, ffi_schema) = arrow_57::ffi::to_ffi(&data)
        .map_err(|e| DataFusionError::Execution(format!("Hudi scan arrow export failed: {e}")))?;
    // SAFETY: FFI_ArrowArray and FFI_ArrowSchema are the #[repr(C)] structs of
    // the Arrow C data interface; their layout is defined by the Arrow
    // specification and is the same in both crate versions. Ownership moves
    // with the structs, so the release callbacks fire exactly once, on the
    // arrow 58 side.
    let (ffi_array, ffi_schema): (arrow::ffi::FFI_ArrowArray, arrow::ffi::FFI_ArrowSchema) =
        unsafe { (std::mem::transmute(ffi_array), std::mem::transmute(ffi_schema)) };
    let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }
        .map_err(|e| DataFusionError::Execution(format!("Hudi scan arrow import failed: {e}")))?;
    Ok(RecordBatch::from(StructArray::from(data)))
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

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let base_uri = self.table_base_uri.clone();
        let options = self.options.clone();
        let schema = Arc::clone(&self.output_schema);
        let slices = self.file_slices.clone();
        let projection: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let baseline = BaselineMetrics::new(&self.metrics, 0);

        let batches = futures::stream::once(async move {
            let reader = FileGroupReader::new_with_options(
                &base_uri,
                options.iter().map(|(k, v)| (k.as_str(), v.as_str())),
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("failed to open Hudi table {base_uri}: {e}"))
            })?;
            let read_options = ReadOptions {
                projection: Some(projection),
                ..Default::default()
            };
            let mut out = Vec::with_capacity(slices.len());
            for slice in &slices {
                let batch = reader
                    .read_file_slice_from_paths(
                        &slice.base_file_path,
                        slice.log_file_paths.iter().map(|s| s.as_str()),
                        &read_options,
                    )
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "failed to read Hudi file slice {}: {e}",
                            slice.base_file_path
                        ))
                    })?;
                let aligned = align_to_schema(batch_across_ffi(batch)?, &schema)?;
                baseline.record_output(aligned.num_rows());
                out.push(aligned);
            }
            Ok::<_, DataFusionError>(futures::stream::iter(out.into_iter().map(Ok)))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            batches.boxed(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
