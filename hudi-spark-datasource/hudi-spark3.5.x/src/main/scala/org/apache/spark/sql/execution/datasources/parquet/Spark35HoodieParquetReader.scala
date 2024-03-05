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
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FileFormat, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

object Spark35HoodieParquetReader {

  def getExtraProps(vectorized: Boolean,
                    sqlConf: SQLConf,
                    options: Map[String, String],
                    hadoopConf: Configuration): Map[String,String] = {
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
    Map(
      "enableVectorizedReader" -> vectorized.toString,
      "datetimeRebaseModeInRead" -> parquetOptions.datetimeRebaseModeInRead,
      "int96RebaseModeInRead" -> parquetOptions.int96RebaseModeInRead,
      "enableParquetFilterPushDown" -> sqlConf.parquetFilterPushDown.toString,
      "pushDownDate" -> sqlConf.parquetFilterPushDownDate.toString,
      "pushDownTimestamp" -> sqlConf.parquetFilterPushDownTimestamp.toString,
      "pushDownDecimal" -> sqlConf.parquetFilterPushDownDecimal.toString,
      "pushDownStringPredicate" -> sqlConf.parquetFilterPushDownStringPredicate.toString,
      "pushDownInFilterThreshold" -> sqlConf.parquetFilterPushDownInFilterThreshold.toString,
      "isCaseSensitive" -> sqlConf.caseSensitiveAnalysis.toString,
      "timestampConversion" -> sqlConf.isParquetINT96TimestampConversion.toString,
      "enableOffHeapColumnVector" -> sqlConf.offHeapColumnVectorEnabled.toString,
      "capacity" -> sqlConf.parquetVectorizedReaderBatchSize.toString,
      "returningBatch" -> returningBatch.toString,
      "enableRecordFilter" -> sqlConf.parquetRecordFilterEnabled.toString,
      "timeZoneId" -> sqlConf.sessionLocalTimeZone
    )
  }

  def getReader(file: PartitionedFile,
                requiredSchema: StructType,
                partitionSchema: StructType,
                filters: Seq[Filter],
                sharedConf: Configuration,
                extraProps: Map[String,String]): Iterator[InternalRow] = {
    sharedConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    sharedConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)
    ParquetWriteSupport.setSchema(requiredSchema, sharedConf)
    val enableVectorizedReader = extraProps("enableVectorizedReader").toBoolean
    val datetimeRebaseModeInRead = extraProps("datetimeRebaseModeInRead")
    val int96RebaseModeInRead = extraProps("int96RebaseModeInRead")
    val enableParquetFilterPushDown = extraProps("enableParquetFilterPushDown").toBoolean
    val pushDownDate = extraProps("pushDownDate").toBoolean
    val pushDownTimestamp = extraProps("pushDownTimestamp").toBoolean
    val pushDownDecimal = extraProps("pushDownDecimal").toBoolean
    val pushDownStringPredicate = extraProps("pushDownStringPredicate").toBoolean
    val pushDownInFilterThreshold = extraProps("pushDownInFilterThreshold").toInt
    val isCaseSensitive = extraProps("isCaseSensitive").toBoolean
    val timestampConversion = extraProps("timestampConversion").toBoolean
    val enableOffHeapColumnVector = extraProps("enableOffHeapColumnVector").toBoolean
    val capacity = extraProps("capacity").toInt
    val returningBatch = extraProps("returningBatch").toBoolean
    val enableRecordFilter = extraProps("enableRecordFilter").toBoolean
    val timeZoneId = Option(extraProps("timeZoneId"))

    assert(file.partitionValues.numFields == partitionSchema.size)

    val filePath = file.toPath
    val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

    val schemaEvolutionUtils = new Spark35ParquetSchemaEvolutionUtils(sharedConf, filePath, requiredSchema, partitionSchema)

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
      val vectorizedReader = if (schemaEvolutionUtils.shouldUseInternalSchema) {
        schemaEvolutionUtils.buildVectorizedReader(convertTz,
          datetimeRebaseSpec,
          int96RebaseSpec,
          enableOffHeapColumnVector,
          taskContext,
          capacity)
      } else {
        new VectorizedParquetRecordReader(
          convertTz.orNull,
          datetimeRebaseSpec.mode.toString,
          datetimeRebaseSpec.timeZone,
          int96RebaseSpec.mode.toString,
          int96RebaseSpec.timeZone,
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity)
      }
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
        //logDebug(s"Appending $partitionSchema ${file.partitionValues}")
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
      //logDebug(s"Falling back to parquet-mr")
      // ParquetRecordReader returns InternalRow
      val readSupport = new ParquetReadSupport(
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
        val unsafeProjection = schemaEvolutionUtils.generateUnsafeProjection(fullSchema, timeZoneId)

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
