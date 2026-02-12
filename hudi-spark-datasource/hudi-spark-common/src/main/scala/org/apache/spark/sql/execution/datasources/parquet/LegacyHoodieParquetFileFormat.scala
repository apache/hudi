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
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
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
   * LegacyHoodieParquetFileFormat.setTableAvroSchemaInConf() before reading so that
   * supportBatch() can find it (supportBatch does not receive the relation's options).
   * This is also used as a fallback in buildReaderWithPartitionValues when schema is not in options.
   *
   * @return Some(schema) if found in hadoopConf, None otherwise
   */
  private def getTableAvroSchemaFromConf(hadoopConf: Configuration): Option[Schema] = {
    val schemaStr = hadoopConf.get(LegacyHoodieParquetFileFormat.HOODIE_TABLE_AVRO_SCHEMA)
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

  /**
   * Returns whether batch/columnar read is supported.
   *
   * Where the second parameter `schema` comes from: Spark's FileSourceScanExec (or
   * DataSourceScanExec) calls FileFormat.supportBatch(session, schema). The schema is typically
   * the relation's read schema (HadoopFsRelation.dataSchema or the scan's required schema).
   * During plan canonicalization (e.g. makeCopy), Spark can pass a schema derived from the
   * plan's output attributes, which may have anonymized names (e.g. all "none").
   *
   * We must NOT use that schema for Avro conversion: it can be wrong and cause
   * AvroRuntimeException (duplicate field "none"). We only use the table Avro schema from
   * hadoopConf; if not set, we return false (disable batch).
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    getTableAvroSchemaFromConf(sparkSession.sessionState.newHadoopConf()) match {
      case Some(avroSchema) =>
        sparkAdapter
          .createLegacyHoodieParquetFileFormat(true, avroSchema).get.supportBatch(sparkSession, schema)
      case None =>
        // Table Avro schema not in hadoopConf (supportBatch does not receive options).
        // Do not use the passed-in schema for conversion - it can be wrong (canonicalized "none" names).
        false
    }
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

    val avroSchema = options.get(LegacyHoodieParquetFileFormat.HOODIE_TABLE_AVRO_SCHEMA)
      .map(s => {
        try {
          Some(new Schema.Parser().parse(s))
        } catch {
          case e: Exception =>
            logWarning(s"Failed to parse table Avro schema from options: ${e.getMessage}")
            None
        }
      })
      .flatten
      .orElse(getTableAvroSchemaFromConf(hadoopConf))
      .getOrElse {
        val fullTableSchema = StructType(dataSchema.fields ++ partitionSchema.fields)
        AvroConversionUtils.convertStructTypeToAvroSchema(fullTableSchema, dataSchema.typeName)
      }

    val delegateReader = sparkAdapter
      .createLegacyHoodieParquetFileFormat(shouldExtractPartitionValuesFromPartitionPath, avroSchema).get
      .buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)

    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val useBatchPlan = supportBatch(sparkSession, resultSchema)
    if (useBatchPlan) {
      // Spark built a columnar plan and expects Iterator[ColumnarBatch]. Pass through as-is.
      (file: PartitionedFile) => delegateReader(file)
    } else {
      // Spark built a row-based plan. The delegate may still return ColumnarBatch when it
      // enables vectorized read. Convert any batch to rows so we always yield InternalRow.
      (file: PartitionedFile) => {
        val iter = delegateReader(file).asInstanceOf[Iterator[Any]]
        iter.flatMap {
          case r: InternalRow => Seq(r)
          case b: ColumnarBatch => b.rowIterator().asScala
        }
      }
    }
  }
}

object LegacyHoodieParquetFileFormat {
  val FILE_FORMAT_ID = "hoodie-parquet"

  /**
   * Configuration key for passing table Avro schema.
   * Schema can be passed through options map (buildReader) or hadoopConf (supportBatch + fallback).
   * This preserves the correct logical types (e.g., timestampMillis vs timestampMicros).
   */
  val HOODIE_TABLE_AVRO_SCHEMA = "hoodie.table.avro.schema"

  /**
   * Helper method to set table Avro schema in hadoopConf.
   * Required for supportBatch() which only sees hadoopConf, not the relation's options.
   */
  def setTableAvroSchemaInConf(hadoopConf: Configuration, avroSchema: Schema): Unit = {
    hadoopConf.set(HOODIE_TABLE_AVRO_SCHEMA, avroSchema.toString)
  }
}
