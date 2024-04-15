/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.execution.datasources.Spark3ParquetSchemaEvolutionUtils
import org.apache.spark.sql.types.StructType

import java.time.ZoneId

class Spark32PlusParquetSchemaEvolutionUtils(sharedConf: Configuration,
                                             filePath: Path,
                                             requiredSchema: StructType,
                                             partitionSchema: StructType) extends
  Spark3ParquetSchemaEvolutionUtils(sharedConf, filePath, requiredSchema, partitionSchema) {

  def buildVectorizedReader(convertTz: ZoneId,
                            datetimeRebaseMode: String,
                            datetimeRebaseTz: String,
                            int96RebaseMode: String,
                            int96RebaseTz: String,
                            useOffHeap: Boolean,
                            capacity: Int): VectorizedParquetRecordReader = {
    if (shouldUseInternalSchema) {
      new Spark32PlusHoodieVectorizedParquetRecordReader(
        convertTz,
        datetimeRebaseMode,
        datetimeRebaseTz,
        int96RebaseMode,
        int96RebaseTz,
        useOffHeap,
        capacity,
        typeChangeInfos)
    } else {
      new VectorizedParquetRecordReader(
        convertTz,
        datetimeRebaseMode,
        datetimeRebaseTz,
        int96RebaseMode,
        int96RebaseTz,
        useOffHeap,
        capacity)
    }
  }
}
