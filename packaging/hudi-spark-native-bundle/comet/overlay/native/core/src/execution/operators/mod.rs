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

//! Operators

pub use crate::errors::ExecutionError;

pub use aligned_stream_reader::*;
pub use copy::*;
pub use hudi_scan::*;
pub use iceberg_scan::*;
pub use scan::*;

mod aligned_stream_reader;
mod copy;
mod dynamic_filter;
pub(crate) use dynamic_filter::DynamicFilterJoinExec;
mod filter;
pub(crate) use filter::CometFilterExec;
mod expand;
pub use expand::ExpandExec;
mod explode;
pub use explode::ExplodeExec;
mod hudi_scan;
pub(crate) mod iceberg_common;
mod iceberg_partition_path;
mod iceberg_scan;
mod iceberg_write;
pub use iceberg_write::IcebergWriteExec;
mod parquet_writer;
pub use parquet_writer::{ParquetCompression, ParquetWriterExec};
mod csv_scan;
pub mod projection;
mod sample;
pub use sample::SampleExec;
mod rank_limit;
pub use rank_limit::{PartitionedRankLimitExec, WindowFnKind};
mod scan;
mod shuffle_scan;
pub use csv_scan::init_csv_datasource_exec;
pub use shuffle_scan::ShuffleScanExec;

/// Fixtures for the nested-nullability drift from
/// <https://github.com/apache/datafusion-comet/issues/5137>, shared by the `expand` and
/// `shuffle_scan` tests so the two boundaries are pinned against the same array.
#[cfg(test)]
pub(crate) mod nested_nullability_fixture {
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, ListArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, FieldRef, Fields};
    use std::sync::Arc;

    pub fn struct_fields(flag_nullable: bool) -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, flag_nullable),
        ])
    }

    /// The element field of an `array<struct<id,flag>>`, with `flag`'s nullability as given.
    pub fn element_field(flag_nullable: bool) -> FieldRef {
        Arc::new(Field::new_list_field(
            DataType::Struct(struct_fields(flag_nullable)),
            true,
        ))
    }

    pub fn list_of_struct_type(flag_nullable: bool) -> DataType {
        DataType::List(element_field(flag_nullable))
    }

    /// `[[{1,true},{2,false}], [{3,true}]]`. Both variants hold identical values; only the `flag`
    /// field's `nullable` flag differs, which is the whole of the drift being absorbed.
    pub fn list_of_struct(flag_nullable: bool) -> ArrayRef {
        let entries = StructArray::new(
            struct_fields(flag_nullable),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
            None,
        );
        Arc::new(ListArray::new(
            element_field(flag_nullable),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(entries),
            None,
        ))
    }
}
