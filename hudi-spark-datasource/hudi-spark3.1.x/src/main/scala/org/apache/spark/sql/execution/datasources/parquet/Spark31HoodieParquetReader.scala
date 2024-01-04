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
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hudi.HoodieSparkUtils
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop._
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.net.URI

object Spark31HoodieParquetReader {

  def getExtraProps(vectorized: Boolean,
                    sqlConf: SQLConf,
                    options: Map[String, String],
                    hadoopConf: Configuration): Map[String, String] = {
    //set hadoopconf
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sqlConf.sessionLocalTimeZone)
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, sqlConf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis)
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, sqlConf.isParquetBinaryAsString)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, sqlConf.isParquetINT96AsTimestamp)

    Map(
      "enableVectorizedReader" -> vectorized.toString,
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
      "returningBatch" -> sqlConf.parquetVectorizedReaderEnabled.toString,
      "enableRecordFilter" -> sqlConf.parquetRecordFilterEnabled.toString,
      "timeZoneId" -> sqlConf.sessionLocalTimeZone
    )
  }

  def getReader(file: PartitionedFile,
                requiredSchema: StructType,
                filters: Seq[Filter],
                sharedConf: Configuration,
                extraProps: Map[String, String]): Iterator[InternalRow] = {
    sharedConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    sharedConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)
    ParquetWriteSupport.setSchema(requiredSchema, sharedConf)
    val enableVectorizedReader = extraProps("enableVectorizedReader").toBoolean
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
    val timeZoneId = Option(extraProps("timeZoneId"))

    assert(file.partitionValues.numFields == 0)

    val filePath = new Path(new URI(file.filePath))
    val split =
      new org.apache.parquet.hadoop.ParquetInputSplit(
        filePath,
        file.start,
        file.start + file.length,
        file.length,
        Array.empty,
        null)

    val schemaEvolutionUtils = new Spark31ParquetSchemaEvolutionUtils(sharedConf, filePath, requiredSchema)

    lazy val footerFileMetaData =
      ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      SQLConf.get.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ))
    // Try to push down filters when filter push-down is enabled.
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = if (HoodieSparkUtils.gteqSpark3_1_3) {
        createParquetFilters(
          parquetSchema,
          pushDownDate,
          pushDownTimestamp,
          pushDownDecimal,
          pushDownStringStartWith,
          pushDownInFilterThreshold,
          isCaseSensitive,
          datetimeRebaseMode)
      } else {
        createParquetFilters(
          parquetSchema,
          pushDownDate,
          pushDownTimestamp,
          pushDownDecimal,
          pushDownStringStartWith,
          pushDownInFilterThreshold,
          isCaseSensitive)
      }
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

    val int96RebaseMode = DataSourceUtils.int96RebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      SQLConf.get.getConf(SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ))

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
        schemaEvolutionUtils.buildVectorizedReader(
          convertTz,
          datetimeRebaseMode,
          int96RebaseMode,
          enableOffHeapColumnVector,
          taskContext,
          capacity)
      } else {
        new VectorizedParquetRecordReader(
          convertTz.orNull,
          datetimeRebaseMode.toString,
          int96RebaseMode.toString,
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity)
      }

      val iter = new RecordReaderIterator(vectorizedReader)
      // SPARK-23457 Register a task completion listener before `initialization`.
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      vectorizedReader.initialize(split, hadoopAttemptContext)
      vectorizedReader.initBatch(StructType(Seq.empty), file.partitionValues)
      if (returningBatch) {
        vectorizedReader.enableReturningBatches()
      }

      // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
      iter.asInstanceOf[Iterator[InternalRow]]
    } else {
      // ParquetRecordReader returns InternalRow
      val readSupport = new ParquetReadSupport(
        convertTz,
        enableVectorizedReader = false,
        datetimeRebaseMode,
        int96RebaseMode)
      val reader = if (pushed.isDefined && enableRecordFilter) {
        val parquetFilter = FilterCompat.get(pushed.get, null)
        new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
      } else {
        new ParquetRecordReader[InternalRow](readSupport)
      }
      val iter = new RecordReaderIterator[InternalRow](reader)
      // SPARK-23457 Register a task completion listener before `initialization`.
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      reader.initialize(split, hadoopAttemptContext)

      val fullSchema = requiredSchema.toAttributes
      val unsafeProjection = schemaEvolutionUtils.generateUnsafeProjection(fullSchema, timeZoneId)
      // There is no partition columns
      iter.map(unsafeProjection)
    }
  }

  private def createParquetFilters(args: Any*): ParquetFilters = {
    // ParquetFilters bears a single ctor (in Spark 3.1)
    val ctor = classOf[ParquetFilters].getConstructors.head
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[ParquetFilters]
  }

}
