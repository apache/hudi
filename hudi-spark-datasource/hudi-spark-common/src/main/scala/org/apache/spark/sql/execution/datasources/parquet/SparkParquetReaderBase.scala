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
import org.apache.hudi.common.schema.internal.InternalSchema
import org.apache.hudi.common.util
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.spark.internal.Logging
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
                                      enableLogicalTimestampRepair: Boolean,
                                      timeZoneId: Option[String]) extends SparkColumnarFileReader with Logging {
  /**
   * Read an individual parquet file
   *
   * @param file               parquet file to read
   * @param requiredSchema     desired output schema of the data
   * @param partitionSchema    schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt  option of internal schema for schema.on.read
   * @param filters            filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf        the hadoop conf
   * @param tableSchemaOpt     option of table schema for timestamp precision conversion
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  final def read(file: PartitionedFile,
                 requiredSchema: StructType,
                 partitionSchema: StructType,
                 internalSchemaOpt: util.Option[InternalSchema],
                 filters: Seq[Filter],
                 storageConf: StorageConfiguration[Configuration],
                 tableSchemaOpt: util.Option[org.apache.parquet.schema.MessageType] = util.Option.empty()): Iterator[InternalRow] = {
    val readConf = storageConf match {
      case shared: SharedScanStorageConfiguration => sharedReadConf(shared.unwrap(), requiredSchema)
      case _ => ParquetReadConf(storageConf.unwrap(), requiredSchema)
    }
    doRead(file, requiredSchema, partitionSchema, internalSchemaOpt, filters, readConf, tableSchemaOpt)
  }

  /**
   * Read confs prepared from [[SharedScanStorageConfiguration]]s. The tasks of an executor share this reader, so the
   * list is replaced rather than modified. Transient, so it starts empty (null) on each executor.
   */
  @transient @volatile private var sharedReadConfs: List[ParquetReadConf] = Nil

  @transient private var loggedSharedReadConfsFull = false

  private def sharedReadConf(source: Configuration, requiredSchema: StructType): ParquetReadConf = {
    findReadConf(source, requiredSchema).getOrElse {
      synchronized {
        findReadConf(source, requiredSchema).getOrElse {
          val readConf = ParquetReadConf(source, requiredSchema)
          val cached = Option(sharedReadConfs).getOrElse(Nil)
          if (cached.size < SparkParquetReaderBase.MAX_SHARED_READ_CONFS) {
            sharedReadConfs = readConf :: cached
          } else if (!loggedSharedReadConfsFull) {
            loggedSharedReadConfsFull = true
            logWarning(s"The parquet reader holds ${SparkParquetReaderBase.MAX_SHARED_READ_CONFS} shared read confs, so "
              + "it now prepares one per file. The source conf or requested schema of a scan is expected to stay the same.")
          }
          readConf
        }
      }
    }
  }

  private def findReadConf(source: Configuration, requiredSchema: StructType): Option[ParquetReadConf] =
    Option(sharedReadConfs).getOrElse(Nil).find(readConf => (readConf.source eq source)
      && ((readConf.requiredSchema eq requiredSchema) || readConf.requiredSchema == requiredSchema))

  /**
   * Implemented for each spark version
   *
   * @param file               parquet file to read
   * @param requiredSchema     desired output schema of the data
   * @param partitionSchema    schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt  option of internal schema for schema.on.read
   * @param filters            filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param sharedConf         the hadoop conf with the requested schema set; other files may share it, so it is
   *                           copied, not modified, for keys of this file only
   * @param tableSchemaOpt     option of table schema for timestamp precision conversion
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  protected def doRead(file: PartitionedFile,
                       requiredSchema: StructType,
                       partitionSchema: StructType,
                       internalSchemaOpt: util.Option[InternalSchema],
                       filters: Seq[Filter],
                       sharedConf: ParquetReadConf,
                       tableSchemaOpt: util.Option[org.apache.parquet.schema.MessageType]): Iterator[InternalRow]
}

object SparkParquetReaderBase {
  /** Bounds the read confs a reader keeps; a scan uses one or a few requested schemas. */
  private val MAX_SHARED_READ_CONFS = 16

  /**
   * The vectorized batch capacity for a file: the configured capacity, or the row count of the footer's row groups
   * when that is smaller, so a small file does not allocate full-size column vectors. A footer read without row
   * groups keeps the configured capacity.
   */
  def vectorizedBatchCapacity(capacity: Int, footer: ParquetMetadata): Int = {
    val blocks = footer.getBlocks
    if (blocks.isEmpty) {
      capacity
    } else {
      var rows = 0L
      val iterator = blocks.iterator()
      while (iterator.hasNext) {
        rows += iterator.next().getRowCount
      }
      Math.max(1L, Math.min(capacity.toLong, rows)).toInt
    }
  }
}

/**
 * The Hadoop conf of a parquet read: a copy of the caller's conf with the read keys that depend only on the requested
 * schema. Files of a scan may share one instance, so readers never modify it and copy it for keys of a single file. It
 * is a [[JobConf]], so a task attempt context built on it uses it without another copy.
 */
class ParquetReadConf private(val source: Configuration, val requiredSchema: StructType) extends JobConf(source) {
  /** Whether any requested struct has the unshredded variant shape, which the Spark 3.x readers check per file. */
  lazy val requestsUnshreddedVariantStruct: Boolean =
    ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(requiredSchema)
}

object ParquetReadConf {
  def apply(source: Configuration, requiredSchema: StructType): ParquetReadConf = {
    val conf = new ParquetReadConf(source, requiredSchema)
    val requiredSchemaJson = requiredSchema.json
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchemaJson)
    conf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchemaJson)

    conf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, false)
    conf.setBoolean(SQLConf.CASE_SENSITIVE.key, false)
    // Sets flags for `ParquetToSparkSchemaConverter`
    conf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
    conf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, true)
    // Using string value of this conf to preserve compatibility across spark versions.
    conf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, false)
    if (HoodieSparkUtils.gteqSpark3_4) {
      // PARQUET_INFER_TIMESTAMP_NTZ_ENABLED is required from Spark 3.4.0 or above
      conf.setBooleanIfUnset("spark.sql.parquet.inferTimestampNTZ.enabled", true)
    }

    ParquetWriteSupport.setSchema(requiredSchema, conf)
    conf
  }
}

/**
 * A Hadoop conf that all files of a scan share, such as the scan's broadcast conf. [[SparkParquetReaderBase]] prepares
 * its [[ParquetReadConf]] from it once per requested schema instead of once per file, so later changes to this conf do
 * not reach parquet reads. The ORC reader of a multi-format table writes its search argument and read schema into it;
 * parquet reads do not use those keys.
 */
class SharedScanStorageConfiguration(configuration: Configuration) extends HadoopStorageConfiguration(configuration, false)

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
