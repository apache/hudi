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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

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
use futures::{StreamExt, TryStreamExt};
use hudi::table::{ReadOptions, Table};

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
        let base_uri = self.table_base_uri.clone();
        let options = self.options.clone();
        let schema = Arc::clone(&self.output_schema);
        let slices = self.file_slices.clone();
        let projection: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let batch_size = context.session_config().batch_size();
        let baseline = BaselineMetrics::new(&self.metrics, 0);

        let batches = futures::stream::once(async move {
            // The reader comes from an opened table rather than from the base
            // URI alone so that it knows the table's current schema: a base file
            // written before a column was added lacks that column, and only a
            // reader holding the table schema null-fills it (and casts a widened
            // column) as the JVM reader does. Opening the table costs one
            // timeline listing per task. hudi-rs takes the read options (the
            // query instant) per read, not on the table.
            let (read_opts, table_opts): (Vec<_>, Vec<_>) = options
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .partition(|(k, _)| k.starts_with("hoodie.read."));
            let table = Table::new_with_options(&base_uri, table_opts)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("failed to open Hudi table {base_uri}: {e}"))
                })?;
            // The projection is pushed into the base file read, and each slice
            // streams in batches of the session's batch size instead of being
            // read whole.
            let read_options = ReadOptions::new()
                .with_hudi_options(read_opts)
                .with_projection(projection)
                .with_batch_size(batch_size)
                .map_err(|e| DataFusionError::Execution(format!("Hudi scan batch size: {e}")))?;
            let reader = table
                .create_file_group_reader_with_options(
                    Some(&read_options),
                    std::iter::empty::<(&str, String)>(),
                )
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "failed to create a reader for Hudi table {base_uri}: {e}"
                    ))
                })?;
            let reader = Arc::new(reader);
            let slice_streams = futures::stream::iter(slices).then(move |slice| {
                let reader = Arc::clone(&reader);
                let read_options = read_options.clone();
                async move {
                    let base_file_path = slice.base_file_path.clone();
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
                    Ok::<_, DataFusionError>(stream.map_err(move |e| {
                        DataFusionError::Execution(format!(
                            "failed to read Hudi file slice {base_file_path}: {e}"
                        ))
                    }))
                }
            });
            Ok::<_, DataFusionError>(slice_streams.try_flatten())
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
