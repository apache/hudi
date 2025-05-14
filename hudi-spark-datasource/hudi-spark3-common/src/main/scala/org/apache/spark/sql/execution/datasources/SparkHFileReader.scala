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

package org.apache.spark.sql.execution.datasources

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.storage.HoodieSparkHFileReader
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hudi
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.expression.{Literal, Predicate, Predicates}
import org.apache.orc.OrcConf
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{In, StartsWith}
import org.apache.spark.sql.execution.datasources.orc.OrcOptions
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, SparkFileReader, SparkFileReaderBase, SparkHFileReaderBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.StructType

import scala.:+
import scala.jdk.CollectionConverters.seqAsJavaListConverter

class SparkHFileReader(enableVectorizedReader: Boolean,
                       datetimeRebaseModeInRead: String,
                       int96RebaseModeInRead: String,
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
                       timeZoneId: Option[String],
                       ignoreCorruptFiles: Boolean,
                       orcFilterPushDown: Boolean,
                       memoryMode: MemoryMode) extends SparkFileReaderBase(
  enableVectorizedReader = enableVectorizedReader,
  enableParquetFilterPushDown = enableParquetFilterPushDown,
  pushDownDate = pushDownDate,
  pushDownTimestamp = pushDownTimestamp,
  pushDownDecimal = pushDownDecimal,
  pushDownInFilterThreshold = pushDownInFilterThreshold,
  isCaseSensitive = isCaseSensitive,
  timestampConversion = timestampConversion,
  enableOffHeapColumnVector = enableOffHeapColumnVector,
  capacity = capacity,
  returningBatch = returningBatch,
  enableRecordFilter = enableRecordFilter,
  timeZoneId = timeZoneId) {
    /**
     * Implemented for each spark version
     *
     * @param file              parquet file to read
     * @param requiredSchema    desired output schema of the data
     * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
     * @param internalSchemaOpt option of internal schema for schema.on.read
     * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
     * @param sharedConf        the hadoop conf
     * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
     */
    override protected def doRead(file: PartitionedFile,
                                  requiredSchema: StructType,
                                  partitionSchema: StructType,
                                  internalSchemaOpt: org.apache.hudi.common.util.Option[InternalSchema],
                                  filters: Seq[Filter],
                                  sharedConf: Configuration): Iterator[InternalRow] = {
      val path = new StoragePath(file.toPath.toUri)
      val storageConf = new HadoopStorageConfiguration(sharedConf)
      val storage = HoodieStorageUtils.getStorage(path, storageConf)
      val predicates = translateFilters(filters)
      val reader = new HoodieSparkHFileReader(
        path,
        storage,
        storageConf,
        org.apache.hudi.common.util.Option.empty(),
        org.apache.hudi.common.util.Option.empty(),
        predicates.asJava
      )
      val schema = AvroConversionUtils.convertStructTypeToAvroSchema(requiredSchema, "schema")
      transformIterator(reader.getRecordIterator(schema, schema))
    }

  def translateFilters(filters: Seq[Filter]): List[Predicate] = {
    val predicates: List[Predicate] = List()
    for (elem <- filters) {
      elem match {
        case to: EqualTo =>
          predicates :+ Predicates.eq(null, Literal.from(to.value.asInstanceOf[String]))
        case to: In =>
          predicates :+ Predicates.in(null, to.list.map(
            e => Literal.from(e.asInstanceOf[String]).asInstanceOf[hudi.expression.Expression]).toList.asJava)
        case to: StartsWith =>
          predicates :+ Predicates.startsWith(null, Literal.from(to.right.asInstanceOf[String]))
      }
    }
    predicates
  }

  def transformIterator[T](originalIterator: org.apache.hudi.common.util.collection.ClosableIterator[HoodieRecord[T]]): Iterator[T] = {
    new Iterator[T] {
      override def hasNext: Boolean = originalIterator.hasNext

      override def next(): T = originalIterator.next().getData // Extract the payload from HoodieRecord[T]
    }
  }
}

object SparkHFileReader extends SparkHFileReaderBuilder {
  /**
   * Get parquet file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return parquet file reader
   */
  def build(vectorized: Boolean,
            sqlConf: SQLConf,
            options: Map[String, String],
            hadoopConf: Configuration): SparkFileReader = {
    // Set hadoopconf
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sqlConf.sessionLocalTimeZone)
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, sqlConf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis)
    val ignoreCorruptFiles =
      new OrcOptions(options, sqlConf).ignoreCorruptFiles
    val enableVectorizedReader = sqlConf.orcVectorizedReaderEnabled &&
      options.getOrElse(FileFormat.OPTION_RETURNING_BATCH,
          throw new IllegalArgumentException(
            "OPTION_RETURNING_BATCH should always be set for OrcFileFormat. " +
              "To workaround this issue, set spark.sql.orc.enableVectorizedReader=false."))
        .equals("true")
    val memoryMode = if (sqlConf.offHeapColumnVectorEnabled) {
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(hadoopConf, sqlConf.caseSensitiveAnalysis)
    val orcFilterPushDown = sqlConf.orcFilterPushDown

    val parquetOptions = new ParquetOptions(options, sqlConf)
    new SparkHFileReader(
      enableVectorizedReader = enableVectorizedReader,
      datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead,
      int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead,
      enableParquetFilterPushDown = sqlConf.parquetFilterPushDown,
      pushDownDate = sqlConf.parquetFilterPushDownDate,
      pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp,
      pushDownDecimal = sqlConf.parquetFilterPushDownDecimal,
      pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold,
      isCaseSensitive = sqlConf.caseSensitiveAnalysis,
      timestampConversion = sqlConf.isParquetINT96TimestampConversion,
      enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled,
      capacity = sqlConf.parquetVectorizedReaderBatchSize,
      returningBatch = enableVectorizedReader,
      enableRecordFilter = sqlConf.parquetRecordFilterEnabled,
      timeZoneId = Some(sqlConf.sessionLocalTimeZone),
      ignoreCorruptFiles = ignoreCorruptFiles,
      orcFilterPushDown = orcFilterPushDown,
      memoryMode = memoryMode)
  }
}
