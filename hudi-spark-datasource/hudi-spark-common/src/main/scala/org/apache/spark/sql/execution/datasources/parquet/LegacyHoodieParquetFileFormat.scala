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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.{DataSourceReadOptions, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.LegacyHoodieParquetFileFormat.FILE_FORMAT_ID
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}

/**
 * This legacy parquet file format implementation to support Hudi will be replaced by
 * [[NewHoodieParquetFileFormat]] in the future.
 */
class LegacyHoodieParquetFileFormat extends ParquetFileFormat with SparkAdapterSupport {

  override def shortName(): String = FILE_FORMAT_ID

  override def toString: String = "Hoodie-Parquet"

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    sparkAdapter
      .createLegacyHoodieParquetFileFormat(true, null).get.supportBatch(sparkSession, schema)
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val shouldExtractPartitionValuesFromPartitionPath =
      options.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean

    sparkAdapter
      .createLegacyHoodieParquetFileFormat(shouldExtractPartitionValuesFromPartitionPath, null).get
      .buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
  }
}

object LegacyHoodieParquetFileFormat {
  val FILE_FORMAT_ID = "hoodie-parquet"
}
