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

import org.apache.hudi.internal.schema.InternalSchema

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FileFormat, PartitionedFile, RecordReaderIterator, SparkColumnarFileReader}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class Spark35ParquetReader(enableVectorizedReader: Boolean,
                           datetimeRebaseModeInRead: String,
                           int96RebaseModeInRead: String,
                           enableParquetFilterPushDown: Boolean,
                           pushDownDate: Boolean,
                           pushDownTimestamp: Boolean,
                           pushDownDecimal: Boolean,
                           pushDownInFilterThreshold: Int,
                           pushDownStringPredicate: Boolean,
                           isCaseSensitive: Boolean,
                           timestampConversion: Boolean,
                           enableOffHeapColumnVector: Boolean,
                           capacity: Int,
                           returningBatch: Boolean,
                           enableRecordFilter: Boolean,
                           timeZoneId: Option[String]) extends SparkParquetReaderBase(
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
   * Read an individual parquet file
   * Code from ParquetFileFormat#buildReaderWithPartitionValues from Spark v3.5.1 adapted here
   *
   * @param file               parquet file to read
   * @param requiredSchema     desired output schema of the data
   * @param partitionSchema    schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt  option of internal schema for schema.on.read
   * @param filters            filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param sharedConf         the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override protected def doRead(file: PartitionedFile,
                                requiredSchema: StructType,
                                partitionSchema: StructType,
                                internalSchemaOpt: org.apache.hudi.common.util.Option[InternalSchema],
                                filters: scala.Seq[Filter],
                                sharedConf: Configuration,
                                tableSchemaOpt: org.apache.hudi.common.util.Option[org.apache.parquet.schema.MessageType]): Iterator[InternalRow] = {
    assert(file.partitionValues.numFields == partitionSchema.size)

    val filePath = file.toPath
    val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

    val schemaEvolutionUtils = new ParquetSchemaEvolutionUtils(sharedConf, filePath, requiredSchema,
      partitionSchema, internalSchemaOpt, tableSchemaOpt)

    val fileFooter = if (enableVectorizedReader) {
      // When there are vectorized reads, we can avoid reading the footer twice by reading
      // all row groups in advance and filter row groups according to filters that require
      // push down (no need to read the footer metadata again).
      ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.WITH_ROW_GROUPS)
    } else {
      ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.SKIP_ROW_GROUPS)
    }

    val footerFileMetaData = fileFooter.getFileMetaData
    val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)

    // Try to push down filters when filter push-down is enabled.
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringPredicate,
        pushDownInFilterThreshold,
        isCaseSensitive,
        datetimeRebaseSpec)
      filters.map(schemaEvolutionUtils.rebuildFilterFromParquet)
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        .flatMap(parquetFilters.createFilter(_))
        .reduceOption(FilterApi.and)
    } else {
      None
    }

    // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
    // *only* if the file was created by something other than "parquet-mr", so check the actual
    // writer here for this file.  We have to do this per-file, as each file in the table may
    // have different writers.
    // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
    def isCreatedByParquetMr: Boolean =
      footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

    val convertTz =
      if (timestampConversion && !isCreatedByParquetMr) {
        Some(DateTimeUtils.getZoneId(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      }


    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext =
      new TaskAttemptContextImpl(schemaEvolutionUtils.getHadoopConfClone(footerFileMetaData, enableVectorizedReader), attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val taskContext = Option(TaskContext.get())
    if (enableVectorizedReader) {
      val vectorizedReader = schemaEvolutionUtils.buildVectorizedReader(
        convertTz.orNull,
        datetimeRebaseSpec.mode.toString,
        datetimeRebaseSpec.timeZone,
        int96RebaseSpec.mode.toString,
        int96RebaseSpec.timeZone,
        enableOffHeapColumnVector && taskContext.isDefined,
        capacity)
      // SPARK-37089: We cannot register a task completion listener to close this iterator here
      // because downstream exec nodes have already registered their listeners. Since listeners
      // are executed in reverse order of registration, a listener registered here would close the
      // iterator while downstream exec nodes are still running. When off-heap column vectors are
      // enabled, this can cause a use-after-free bug leading to a segfault.
      //
      // Instead, we use FileScanRDD's task completion listener to close this iterator.
      val iter = new RecordReaderIterator(vectorizedReader)
      try {
        vectorizedReader.initialize(split, hadoopAttemptContext, Option.apply(fileFooter))
        vectorizedReader.initBatch(partitionSchema, file.partitionValues)
        if (returningBatch) {
          vectorizedReader.enableReturningBatches()
        }

        // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
        iter.asInstanceOf[Iterator[InternalRow]]
      } catch {
        case e: Throwable =>
          // SPARK-23457: In case there is an exception in initialization, close the iterator to
          // avoid leaking resources.
          iter.close()
          throw e
      }
    } else {
      // ParquetRecordReader returns InternalRow
      val readSupport = new HoodieParquetReadSupport(
        convertTz,
        enableVectorizedReader = false,
        datetimeRebaseSpec,
        int96RebaseSpec)
      val reader = if (pushed.isDefined && enableRecordFilter) {
        val parquetFilter = FilterCompat.get(pushed.get, null)
        new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
      } else {
        new ParquetRecordReader[InternalRow](readSupport)
      }
      val readerWithRowIndexes = ParquetRowIndexUtil.addRowIndexToRecordReaderIfNeeded(reader,
        requiredSchema)
      val iter = new RecordReaderIterator[InternalRow](readerWithRowIndexes)
      try {
        readerWithRowIndexes.initialize(split, hadoopAttemptContext)

        val fullSchema = toAttributes(requiredSchema) ++ toAttributes(partitionSchema)
        val unsafeProjection = schemaEvolutionUtils.generateUnsafeProjection(fullSchema, timeZoneId, footerFileMetaData.getSchema)

        if (partitionSchema.length == 0) {
          // There is no partition columns
          iter.map(unsafeProjection)
        } else {
          val joinedRow = new JoinedRow()
          iter.map(d => unsafeProjection(joinedRow(d, file.partitionValues)))
        }
      } catch {
        case e: Throwable =>
          // SPARK-23457: In case there is an exception in initialization, close the iterator to
          // avoid leaking resources.
          iter.close()
          throw e
      }
    }
  }
}

object Spark35ParquetReader extends SparkParquetReaderBuilder {
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
            hadoopConf: Configuration): SparkColumnarFileReader = {
    //set hadoopconf
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sqlConf.sessionLocalTimeZone)
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, sqlConf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis)
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, sqlConf.isParquetBinaryAsString)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, sqlConf.isParquetINT96AsTimestamp)
    // Using string value of this conf to preserve compatibility across spark versions. See [HUDI-5868]
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sqlConf.getConfString(
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.defaultValueString).toBoolean
    )
    hadoopConf.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, sqlConf.parquetInferTimestampNTZEnabled)

    val returningBatch = sqlConf.parquetVectorizedReaderEnabled &&
      options.getOrElse(FileFormat.OPTION_RETURNING_BATCH,
        throw new IllegalArgumentException(
          "OPTION_RETURNING_BATCH should always be set for ParquetFileFormat. " +
            "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false."))
        .equals("true")

    val parquetOptions = new ParquetOptions(options, sqlConf)
    new Spark35ParquetReader(
      enableVectorizedReader = vectorized,
      datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead,
      int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead,
      enableParquetFilterPushDown = sqlConf.parquetFilterPushDown,
      pushDownDate = sqlConf.parquetFilterPushDownDate,
      pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp,
      pushDownDecimal = sqlConf.parquetFilterPushDownDecimal,
      pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold,
      pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate,
      isCaseSensitive = sqlConf.caseSensitiveAnalysis,
      timestampConversion = sqlConf.isParquetINT96TimestampConversion,
      enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled,
      capacity = sqlConf.parquetVectorizedReaderBatchSize,
      returningBatch = returningBatch,
      enableRecordFilter = sqlConf.parquetRecordFilterEnabled,
      timeZoneId = Some(sqlConf.sessionLocalTimeZone))
  }
}
