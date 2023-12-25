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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hudi.HoodieSparkUtils
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop._
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.net.URI

object Spark32HoodieParquetReader {

  def getExtraProps(vectorized: Boolean, sqlConf: SQLConf, options: Map[String, String]): Map[String, String] = {
    val returningBatch = sqlConf.parquetVectorizedReaderEnabled &&
      options.getOrElse("returning_batch",
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
      "pushDownInFilterThreshold" -> sqlConf.parquetFilterPushDownInFilterThreshold.toString,
      "pushDownStringStartWith" -> sqlConf.parquetFilterPushDownStringStartWith.toString,
      "isCaseSensitive" -> sqlConf.caseSensitiveAnalysis.toString,
      "timestampConversion" -> sqlConf.isParquetINT96TimestampConversion.toString,
      "enableOffHeapColumnVector" -> sqlConf.offHeapColumnVectorEnabled.toString,
      "capacity" -> sqlConf.parquetVectorizedReaderBatchSize.toString,
      "returningBatch" -> returningBatch.toString,
      "enableRecordFilter" -> sqlConf.parquetRecordFilterEnabled.toString
    )
  }

  def getReader(file: PartitionedFile,
                requiredSchema: StructType,
                filters: Seq[Filter],
                sharedConf: Configuration,
                extraProps: Map[String, String]): Iterator[InternalRow] = {
    val enableVectorizedReader = extraProps("enableVectorizedReader").toBoolean
    val datetimeRebaseModeInRead = extraProps("datetimeRebaseModeInRead")
    val int96RebaseModeInRead = extraProps("int96RebaseModeInRead")
    val enableParquetFilterPushDown = extraProps("enableParquetFilterPushDown").toBoolean
    val pushDownDate = extraProps("pushDownDate").toBoolean
    val pushDownTimestamp = extraProps("pushDownTimestamp").toBoolean
    val pushDownDecimal = extraProps("pushDownDecimal").toBoolean
    val pushDownInFilterThreshold = extraProps("pushDownInFilterThreshold").toInt
    val pushDownStringStartWith = extraProps("pushDownStringStartWith").toBoolean
    val isCaseSensitive = extraProps("isCaseSensitive").toBoolean
    val timestampConversion = extraProps("timestampConversion").toBoolean
    val enableOffHeapColumnVector = extraProps("enableOffHeapColumnVector").toBoolean
    val capacity = extraProps("capacity").toInt
    val returningBatch = extraProps("returningBatch").toBoolean
    val enableRecordFilter = extraProps("enableRecordFilter").toBoolean
    assert(file.partitionValues.numFields == 0)

    val filePath = new Path(new URI(file.filePath))
    val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

    lazy val footerFileMetaData =
      ParquetFooterReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    // Try to push down filters when filter push-down is enabled.
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = if (HoodieSparkUtils.gteqSpark3_2_1) {
        // NOTE: Below code could only be compiled against >= Spark 3.2.1,
        //       and unfortunately won't compile against Spark 3.2.0
        //       However this code is runtime-compatible w/ both Spark 3.2.0 and >= Spark 3.2.1
        val datetimeRebaseSpec =
        DataSourceUtils.datetimeRebaseSpec(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
        new ParquetFilters(
          parquetSchema,
          pushDownDate,
          pushDownTimestamp,
          pushDownDecimal,
          pushDownStringStartWith,
          pushDownInFilterThreshold,
          isCaseSensitive,
          datetimeRebaseSpec)
      } else {
        // Spark 3.2.0
        val datetimeRebaseMode =
          Spark32DataSourceUtils.datetimeRebaseMode(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
        createParquetFilters(
          parquetSchema,
          pushDownDate,
          pushDownTimestamp,
          pushDownDecimal,
          pushDownStringStartWith,
          pushDownInFilterThreshold,
          isCaseSensitive,
          datetimeRebaseMode)
      }
      filters
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

    val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      int96RebaseModeInRead)

    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext =
      new TaskAttemptContextImpl(sharedConf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val taskContext = Option(TaskContext.get())
    if (enableVectorizedReader) {
      val vectorizedReader = new VectorizedParquetRecordReader(
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
        vectorizedReader.initialize(split, hadoopAttemptContext)
        vectorizedReader.initBatch(StructType(Seq.empty), file.partitionValues)
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
      val readSupport = if (HoodieSparkUtils.gteqSpark3_2_1) {
        // ParquetRecordReader returns InternalRow
        // NOTE: Below code could only be compiled against >= Spark 3.2.1,
        //       and unfortunately won't compile against Spark 3.2.0
        //       However this code is runtime-compatible w/ both Spark 3.2.0 and >= Spark 3.2.1
        val int96RebaseSpec =
        DataSourceUtils.int96RebaseSpec(footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)
        val datetimeRebaseSpec =
          DataSourceUtils.datetimeRebaseSpec(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
        new ParquetReadSupport(
          convertTz,
          enableVectorizedReader = false,
          datetimeRebaseSpec,
          int96RebaseSpec)
      } else {
        val datetimeRebaseMode =
          Spark32DataSourceUtils.datetimeRebaseMode(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
        val int96RebaseMode =
          Spark32DataSourceUtils.int96RebaseMode(footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)
        createParquetReadSupport(
          convertTz,
          /* enableVectorizedReader = */ false,
          datetimeRebaseMode,
          int96RebaseMode)
      }
      val reader = if (pushed.isDefined && enableRecordFilter) {
        val parquetFilter = FilterCompat.get(pushed.get, null)
        new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
      } else {
        new ParquetRecordReader[InternalRow](readSupport)
      }
      val iter = new RecordReaderIterator[InternalRow](reader)
      try {
        reader.initialize(split, hadoopAttemptContext)

        val fullSchema = requiredSchema.toAttributes
        val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

        // There is no partition columns
        iter.map(unsafeProjection)
      } catch {
        case e: Throwable =>
          // SPARK-23457: In case there is an exception in initialization, close the iterator to
          // avoid leaking resources.
          iter.close()
          throw e
      }
    }
  }

  /**
   * NOTE: This method is specific to Spark 3.2.0
   */
  private def createParquetFilters(args: Any*): ParquetFilters = {
    // NOTE: ParquetFilters ctor args contain Scala enum, therefore we can't look it
    //       up by arg types, and have to instead rely on the number of args based on individual class;
    //       the ctor order is not guaranteed
    val ctor = classOf[ParquetFilters].getConstructors.maxBy(_.getParameterCount)
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[ParquetFilters]
  }

  /**
   * NOTE: This method is specific to Spark 3.2.0
   */
  private def createParquetReadSupport(args: Any*): ParquetReadSupport = {
    // NOTE: ParquetReadSupport ctor args contain Scala enum, therefore we can't look it
    //       up by arg types, and have to instead rely on the number of args based on individual class;
    //       the ctor order is not guaranteed
    val ctor = classOf[ParquetReadSupport].getConstructors.maxBy(_.getParameterCount)
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[ParquetReadSupport]
  }
}
