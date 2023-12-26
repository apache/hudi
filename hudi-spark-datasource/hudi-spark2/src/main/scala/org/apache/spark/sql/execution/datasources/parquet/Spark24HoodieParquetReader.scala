/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Cast, JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

object Spark24HoodieParquetReader {

  def getExtraProps(vectorized: Boolean, sqlConf: SQLConf, options: Map[String, String]): Map[String, String] = {
    val returningBatch = sqlConf.parquetVectorizedReaderEnabled &&
      options.getOrElse("returning_batch",
        throw new IllegalArgumentException(
          "OPTION_RETURNING_BATCH should always be set for ParquetFileFormat. " +
            "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false."))
        .equals("true")
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
      "returningBatch" -> returningBatch.toString,
      "enableRecordFilter" -> sqlConf.parquetRecordFilterEnabled.toString,
      "timeZoneId" -> sqlConf.sessionLocalTimeZone
    )
  }

  def getReader(file: PartitionedFile,
                requiredSchema: StructType,
                filters: Seq[Filter],
                sharedConf: Configuration,
                extraProps: Map[String, String]): Iterator[InternalRow] = {
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

    val fileSplit =
      new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
    val filePath = fileSplit.getPath

    val split =
      new org.apache.parquet.hadoop.ParquetInputSplit(
        filePath,
        fileSplit.getStart,
        fileSplit.getStart + fileSplit.getLength,
        fileSplit.getLength,
        fileSplit.getLocations,
        null)

    lazy val footerFileMetaData =
      ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    // Try to push down filters when filter push-down is enabled.
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = new ParquetFilters(pushDownDate, pushDownTimestamp, pushDownDecimal,
        pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
      filters
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        .flatMap(parquetFilters.createFilter(parquetSchema, _))
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
        Some(DateTimeUtils.getTimeZone(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      }

    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)

    // Clone new conf
    val hadoopAttemptConf = new Configuration(sharedConf)
    val (implicitTypeChangeInfos, sparkRequestSchema) = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(hadoopAttemptConf, footerFileMetaData, requiredSchema)

    if (!implicitTypeChangeInfos.isEmpty) {
      hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, sparkRequestSchema.json)
    }

    val hadoopAttemptContext =
      new TaskAttemptContextImpl(hadoopAttemptConf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val taskContext = Option(TaskContext.get())
    if (enableVectorizedReader) {
      val vectorizedReader = if (!implicitTypeChangeInfos.isEmpty) {
        new Spark24HoodieVectorizedParquetRecordReader(
          convertTz.orNull,
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity,
          implicitTypeChangeInfos
        )
      } else {
        new VectorizedParquetRecordReader(
          convertTz.orNull,
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity)
      }

      val iter = new RecordReaderIterator(vectorizedReader)
      // SPARK-23457 Register a task completion lister before `initialization`.
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      vectorizedReader.initialize(split, hadoopAttemptContext)

      vectorizedReader.initBatch(StructType(Nil), InternalRow.empty)

      if (returningBatch) {
        vectorizedReader.enableReturningBatches()
      }

      // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
      iter.asInstanceOf[Iterator[InternalRow]]
    } else {
      // ParquetRecordReader returns UnsafeRow
      val readSupport = new ParquetReadSupport(convertTz)
      val reader = if (pushed.isDefined && enableRecordFilter) {
        val parquetFilter = FilterCompat.get(pushed.get, null)
        new ParquetRecordReader[UnsafeRow](readSupport, parquetFilter)
      } else {
        new ParquetRecordReader[UnsafeRow](readSupport)
      }
      val iter = new RecordReaderIterator(reader)
      // SPARK-23457 Register a task completion lister before `initialization`.
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      reader.initialize(split, hadoopAttemptContext)

      val fullSchema = requiredSchema.toAttributes
      val unsafeProjection = if (implicitTypeChangeInfos.isEmpty) {
        GenerateUnsafeProjection.generate(fullSchema, fullSchema)
      } else {
        val newFullSchema = new StructType(requiredSchema.fields.zipWithIndex.map { case (f, i) =>
          if (implicitTypeChangeInfos.containsKey(i)) {
            StructField(f.name, implicitTypeChangeInfos.get(i).getRight, f.nullable, f.metadata)
          } else f
        }).toAttributes
        val castSchema = newFullSchema.zipWithIndex.map { case (attr, i) =>
          if (implicitTypeChangeInfos.containsKey(i)) {
            val srcType = implicitTypeChangeInfos.get(i).getRight
            val dstType = implicitTypeChangeInfos.get(i).getLeft
            val needTimeZone = Cast.needsTimeZone(srcType, dstType)
            Cast(attr, dstType, if (needTimeZone) timeZoneId else None)
          } else attr
        }
        GenerateUnsafeProjection.generate(castSchema, newFullSchema)
      }

      // This is a horrible erasure hack...  if we type the iterator above, then it actually check
      // the type in next() and we get a class cast exception.  If we make that function return
      // Object, then we can defer the cast until later!
      //
      // NOTE: We're making appending of the partitioned values to the rows read from the
      //       data file configurable
      // There is no partition columns
      iter.asInstanceOf[Iterator[InternalRow]].map(unsafeProjection)
    }
  }
}
