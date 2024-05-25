/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Bulk insert sort mode.
 */
@EnumDescription("Modes for sorting records during bulk insert.")
public enum BulkInsertSortMode {

  @EnumFieldDescription("No sorting. Fastest and matches `spark.write.parquet()` in number of files and overhead.")
  NONE,

  @EnumFieldDescription("This ensures best file sizes, with lowest memory overhead at cost of sorting.")
  GLOBAL_SORT,

  @EnumFieldDescription("Strikes a balance by only sorting within a Spark RDD partition, still "
      + "keeping the memory overhead of writing low. File sizing is not as good as GLOBAL_SORT.")
  PARTITION_SORT,

  @EnumFieldDescription("This ensures that the data for a single physical partition in the table is written by the same Spark executor. "
      + "This should only be used when input data is evenly distributed across different partition paths. If data is skewed "
      + "(most records are intended for a handful of partition paths among all) then this can cause an imbalance among Spark executors.")
  PARTITION_PATH_REPARTITION,

  @EnumFieldDescription("This ensures that the data for a single physical partition in the table is written by the same Spark executor. "
      + "This should only be used when input data is evenly distributed across different partition paths. Compared to "
      + "PARTITION_PATH_REPARTITION, this sort mode does an additional step of sorting the records based on the partition path within a "
      + "single Spark partition, given that data for multiple physical partitions can be sent to the same Spark partition and executor. "
      + "If data is skewed (most records are intended for a handful of partition paths among all) then this can cause an imbalance among "
      + "Spark executors.")
  PARTITION_PATH_REPARTITION_AND_SORT
}
