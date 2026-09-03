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

//! Registry for operator builders using modular pattern

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use datafusion_comet_proto::spark_operator::Operator;
use jni::objects::{Global, JObject};

use super::{PhysicalPlanner, PlanCreationResult};
use crate::execution::operators::ExecutionError;

/// Trait for building physical operators from Spark protobuf operators
pub trait OperatorBuilder: Send + Sync {
    /// Build a Spark plan from a protobuf operator
    fn build(
        &self,
        spark_plan: &datafusion_comet_proto::spark_operator::Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult;
}

/// Enum to identify different operator types for registry dispatch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorType {
    Scan,
    NativeScan,
    IcebergScan,
    Projection,
    Filter,
    HashAgg,
    Limit,
    Sort,
    ShuffleWriter,
    ParquetWriter,
    Expand,
    SortMergeJoin,
    HashJoin,
    Window,
    CsvScan,
}

/// Global registry of operator builders
pub struct OperatorRegistry {
    builders: HashMap<OperatorType, Box<dyn OperatorBuilder>>,
}

impl OperatorRegistry {
    /// Create a new empty registry
    fn new() -> Self {
        Self {
            builders: HashMap::new(),
        }
    }

    /// Get the global singleton instance of the operator registry
    pub fn global() -> &'static OperatorRegistry {
        static REGISTRY: OnceLock<OperatorRegistry> = OnceLock::new();
        REGISTRY.get_or_init(|| {
            let mut registry = OperatorRegistry::new();
            registry.register_all_operators();
            registry
        })
    }

    /// Check if the registry can handle a given operator
    pub fn can_handle(&self, spark_operator: &Operator) -> bool {
        get_operator_type(spark_operator)
            .map(|op_type| self.builders.contains_key(&op_type))
            .unwrap_or(false)
    }

    /// Create a Spark plan using the registered builder for this operator type
    pub fn create_plan(
        &self,
        spark_operator: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let operator_type = get_operator_type(spark_operator).ok_or_else(|| {
            ExecutionError::GeneralError(format!(
                "Unsupported operator type: {:?}",
                spark_operator.op_struct
            ))
        })?;

        let builder = self.builders.get(&operator_type).ok_or_else(|| {
            ExecutionError::GeneralError(format!(
                "No builder registered for operator type: {:?}",
                operator_type
            ))
        })?;

        builder.build(spark_operator, inputs, partition_count, planner)
    }

    /// Register all operator builders
    fn register_all_operators(&mut self) {
        self.register_projection_operators();
    }

    /// Register projection operators
    fn register_projection_operators(&mut self) {
        use crate::execution::operators::projection::ProjectionBuilder;

        self.builders
            .insert(OperatorType::Projection, Box::new(ProjectionBuilder));
    }
}

/// Extract the operator type from a Spark operator
fn get_operator_type(spark_operator: &Operator) -> Option<OperatorType> {
    use datafusion_comet_proto::spark_operator::operator::OpStruct;

    match spark_operator.op_struct.as_ref()? {
        OpStruct::Projection(_) => Some(OperatorType::Projection),
        OpStruct::Filter(_) => Some(OperatorType::Filter),
        OpStruct::HashAgg(_) => Some(OperatorType::HashAgg),
        OpStruct::Limit(_) => Some(OperatorType::Limit),
        OpStruct::Sort(_) => Some(OperatorType::Sort),
        OpStruct::Scan(_) => Some(OperatorType::Scan),
        OpStruct::NativeScan(_) => Some(OperatorType::NativeScan),
        OpStruct::IcebergScan(_) => Some(OperatorType::IcebergScan),
        OpStruct::HudiScan(_) => None, // Not yet in OperatorType enum
        OpStruct::ShuffleWriter(_) => Some(OperatorType::ShuffleWriter),
        OpStruct::ParquetWriter(_) => Some(OperatorType::ParquetWriter),
        OpStruct::Expand(_) => Some(OperatorType::Expand),
        OpStruct::SortMergeJoin(_) => Some(OperatorType::SortMergeJoin),
        OpStruct::HashJoin(_) => Some(OperatorType::HashJoin),
        OpStruct::Window(_) => Some(OperatorType::Window),
        OpStruct::Explode(_) => None, // Not yet in OperatorType enum
        OpStruct::CsvScan(_) => Some(OperatorType::CsvScan),
        OpStruct::ShuffleScan(_) => None, // Not yet in OperatorType enum
        OpStruct::BroadcastNestedLoopJoin(_) => None,
        OpStruct::Sample(_) => None, // Not yet in OperatorType enum
    }
}
