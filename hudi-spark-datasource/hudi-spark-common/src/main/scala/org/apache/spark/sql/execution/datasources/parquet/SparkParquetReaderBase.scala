/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

abstract class SparkParquetReaderBase(enableVectorizedReader: Boolean,
                                      enableParquetFilterPushDown: Boolean,
                                      pushDownDate: Boolean,
                                      pushDownTimestamp: Boolean,
                                      pushDownDecimal: Boolean,
                                      pushDownInFilterThreshold: Int,
                                      isCaseSensitive: Boolean,
                                      timestampConversion: Boolean,
                                      enableOffHeapColumnVector: Boolean,
                                      capacity: Int,
                                      returningBatch: Boolean,
                                      enableRecordFilter: Boolean,
                                      timeZoneId: Option[String]) extends SparkColumnarFileReader {
  /**
   * Read an individual parquet file
   *
   * @param file               parquet file to read
   * @param requiredSchema     desired output schema of the data
   * @param partitionSchema    schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt  option of internal schema for schema.on.read
   * @param filters            filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf        the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  final def read(file: PartitionedFile,
                 requiredSchema: StructType,
                 partitionSchema: StructType,
                 internalSchemaOpt: util.Option[InternalSchema],
                 filters: Seq[Filter],
                 storageConf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {
    val conf = storageConf.unwrapCopy()
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    conf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)

    conf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, false);
    conf.setBoolean(SQLConf.CASE_SENSITIVE.key, false);
    // Sets flags for `ParquetToSparkSchemaConverter`
    conf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
    conf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, true)
    // Using string value of this conf to preserve compatibility across spark versions.
    conf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, false)
    if (HoodieSparkUtils.gteqSpark3_4) {
      // PARQUET_INFER_TIMESTAMP_NTZ_ENABLED is required from Spark 3.4.0 or above
      conf.setBoolean("spark.sql.parquet.inferTimestampNTZ.enabled", false)
    }

    ParquetWriteSupport.setSchema(requiredSchema, conf)
    doRead(file, requiredSchema, partitionSchema, internalSchemaOpt, filters, conf)
  }

  /**
   * Implemented for each spark version
   *
   * @param file               parquet file to read
   * @param requiredSchema     desired output schema of the data
   * @param partitionSchema    schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt  option of internal schema for schema.on.read
   * @param filters            filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param sharedConf         the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  protected def doRead(file: PartitionedFile,
                       requiredSchema: StructType,
                       partitionSchema: StructType,
                       internalSchemaOpt: util.Option[InternalSchema],
                       filters: Seq[Filter],
                       sharedConf: Configuration): Iterator[InternalRow]
}

trait SparkParquetReaderBuilder {
  /**
   * Get parquet file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return properties needed for reading a parquet file
   */
  def build(vectorized: Boolean,
            sqlConf: SQLConf,
            options: Map[String, String],
            hadoopConf: Configuration): SparkColumnarFileReader
}
