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

import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile, TypeDescription}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat
import org.apache.spark.TaskContext
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.datasources.orc._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

class Spark35OrcReader(enableVectorizedReader: Boolean,
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
                                internalSchemaOpt: util.Option[InternalSchema],
                                filters: Seq[Filter],
                                sharedConf: Configuration): Iterator[InternalRow] = {
    val filePath = file.filePath.toPath
    val fs = filePath.getFileSystem(sharedConf)
    val readerOptions = OrcFile.readerOptions(sharedConf).filesystem(fs)
    val orcSchema =
      Utils.tryWithResource(OrcFile.createReader(filePath, readerOptions))(_.getSchema)
    val resultedColPruneInfo = OrcUtils.requestedColumnIds(
      isCaseSensitive, requiredSchema, requiredSchema, orcSchema, sharedConf)

    if (resultedColPruneInfo.isEmpty) {
      Iterator.empty
    } else {
      // ORC predicate pushdown
      if (orcFilterPushDown && filters.nonEmpty) {
        val fileSchema = OrcUtils.toCatalystSchema(orcSchema)
        OrcFilters.createFilter(fileSchema, filters).foreach { f =>
          OrcInputFormat.setSearchArgument(sharedConf, f, fileSchema.fieldNames)
        }
      }

      val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
      val resultSchemaString = OrcUtils.orcResultSchemaString(canPruneCols,
        requiredSchema, requiredSchema, partitionSchema, sharedConf)
      assert(requestedColIds.length == requiredSchema.length,
        "[BUG] requested column IDs do not match required schema")
      val taskConf = new Configuration(sharedConf)

      val includeColumns = requestedColIds.filter(_ != -1).sorted.mkString(",")
      taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, includeColumns)
      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      if (enableVectorizedReader) {
        val batchReader = new OrcColumnarBatchReader(capacity, memoryMode)
        // SPARK-23399 Register a task completion listener first to call `close()` in all cases.
        // There is a possibility that `initialize` and `initBatch` hit some errors (like OOM)
        // after opening a file.
        val iter = new RecordReaderIterator(batchReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        val requestedDataColIds = requestedColIds ++ Array.fill(partitionSchema.length)(-1)
        val requestedPartitionColIds =
          Array.fill(requiredSchema.length)(-1) ++ Range(0, partitionSchema.length)
        batchReader.initialize(fileSplit, taskAttemptContext)
        batchReader.initBatch(
          TypeDescription.fromString(resultSchemaString),
          requiredSchema.fields,
          requestedDataColIds,
          requestedPartitionColIds,
          file.partitionValues)

        iter.asInstanceOf[Iterator[InternalRow]]
      } else {
        val orcRecordReader = new OrcInputFormat[OrcStruct].createRecordReader(fileSplit, taskAttemptContext)
        val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))

        val fullSchema = toAttributes(requiredSchema) ++ toAttributes(partitionSchema)
        val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
        val deserializer = new OrcDeserializer(requiredSchema, requestedColIds)

        if (partitionSchema.length == 0) {
          iter.map(value => unsafeProjection(deserializer.deserialize(value)))
        } else {
          val joinedRow = new JoinedRow()
          iter.map(value =>
            unsafeProjection(joinedRow(deserializer.deserialize(value), file.partitionValues)))
        }
      }
    }
  }
}

object Spark35OrcReader extends SparkOrcReaderBuilder {
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
    new Spark35OrcReader(
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
