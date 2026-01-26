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

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, SparkAdapterSupport}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.LegacyHoodieParquetFileFormat.{FILE_FORMAT_ID, HOODIE_TABLE_AVRO_SCHEMA}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * This legacy parquet file format implementation to support Hudi will be replaced by
 * [[NewHoodieParquetFileFormat]] in the future.
 */
class LegacyHoodieParquetFileFormat extends ParquetFileFormat with SparkAdapterSupport with Logging {

  override def shortName(): String = FILE_FORMAT_ID

  override def toString: String = "Hoodie-Parquet"

  /**
   * Try to get table Avro schema from hadoopConf.
   * Callers (e.g., IncrementalRelation) should set the schema using
   * LegacyHoodieParquetFileFormat.setTableAvroSchemaInConf() before reading.
   *
   * @return Some(schema) if found in hadoopConf, None otherwise (falls back to StructType conversion)
   */
  private def getTableAvroSchemaFromConf(hadoopConf: Configuration): Option[Schema] = {
    val schemaStr = hadoopConf.get(HOODIE_TABLE_AVRO_SCHEMA)
    if (schemaStr != null && schemaStr.nonEmpty) {
      try {
        val schema = new Schema.Parser().parse(schemaStr)
        logDebug("Using table Avro schema from hadoopConf")
        Some(schema)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to parse table Avro schema from hadoopConf: ${e.getMessage}")
          None
      }
    } else {
      None
    }
  }

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val avroSchema = getTableAvroSchemaFromConf(sparkSession.sessionState.newHadoopConf()).getOrElse {
      AvroConversionUtils.convertStructTypeToAvroSchema(schema, schema.typeName)
    }
    sparkAdapter
      .createLegacyHoodieParquetFileFormat(true, avroSchema).get.supportBatch(sparkSession, schema)
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

    val avroSchema = getTableAvroSchemaFromConf(hadoopConf).getOrElse {
      val fullTableSchema = StructType(dataSchema.fields ++ partitionSchema.fields)
      AvroConversionUtils.convertStructTypeToAvroSchema(fullTableSchema, dataSchema.typeName)
    }

    sparkAdapter
      .createLegacyHoodieParquetFileFormat(shouldExtractPartitionValuesFromPartitionPath, avroSchema).get
      .buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
  }
}

object LegacyHoodieParquetFileFormat {
  val FILE_FORMAT_ID = "hoodie-parquet"

  /**
   * Configuration key for passing table Avro schema through hadoopConf.
   * This preserves the correct logical types (e.g., timestampMillis vs timestampMicros).
   */
  val HOODIE_TABLE_AVRO_SCHEMA = "hoodie.table.avro.schema"

  /**
   * Helper method to set table Avro schema in hadoopConf.
   * This allows callers to pass the schema to preserve correct logical types.
   */
  def setTableAvroSchemaInConf(hadoopConf: Configuration, avroSchema: Schema): Unit = {
    hadoopConf.set(HOODIE_TABLE_AVRO_SCHEMA, avroSchema.toString)
  }
}
