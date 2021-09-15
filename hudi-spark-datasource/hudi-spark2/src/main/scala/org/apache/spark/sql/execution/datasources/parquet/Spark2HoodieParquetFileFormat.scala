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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hudi.client.utils.SparkSchemaUtils
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.TableInternalSchemaUtils
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.{SerDeHelper, InternalSchemaUtils}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.util.SerializableConfiguration

class Spark2HoodieParquetFileFormat extends ParquetFileFormat {
  // reference ParquetFileFormat from spark project
  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    if (hadoopConf.get(SparkSchemaUtils.HOODIE_QUERY_SCHEMA, "").isEmpty) {
      // fallback to origin parquet File read
      super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
    } else {
      hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
      hadoopConf.set(
        ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
        requiredSchema.json)
      hadoopConf.set(
        ParquetWriteSupport.SPARK_ROW_SCHEMA,
        requiredSchema.json)
      hadoopConf.set(
        SQLConf.SESSION_LOCAL_TIMEZONE.key,
        sparkSession.sessionState.conf.sessionLocalTimeZone)
      hadoopConf.setBoolean(
        SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
        sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
      hadoopConf.setBoolean(
        SQLConf.CASE_SENSITIVE.key,
        sparkSession.sessionState.conf.caseSensitiveAnalysis)

      ParquetWriteSupport.setSchema(requiredSchema, hadoopConf)

      // Sets flags for `ParquetToSparkSchemaConverter`
      hadoopConf.setBoolean(
        SQLConf.PARQUET_BINARY_AS_STRING.key,
        sparkSession.sessionState.conf.isParquetBinaryAsString)
      hadoopConf.setBoolean(
        SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
        sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
      // it's safe to do cols project here.
      val internalSchemaString = hadoopConf.get(SparkSchemaUtils.HOODIE_QUERY_SCHEMA, "")
      val querySchemaOption = SerDeHelper.fromJson(internalSchemaString)
      if (querySchemaOption.isPresent) {
        val prunedSchema = SparkSchemaUtils.convertAndPruneStructTypeToInternalSchema(requiredSchema, querySchemaOption.get())
        hadoopConf.set(SparkSchemaUtils.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(prunedSchema))
      }
      val broadcastedHadoopConf =
        sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

      // TODO: if you move this into the closure it reverts to the default values.
      // If true, enable using the custom RecordReader for parquet. This only works for
      // a subset of the types (no complex types).
      val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
      val sqlConf = sparkSession.sessionState.conf
      val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
      val enableVectorizedReader: Boolean =
        sqlConf.parquetVectorizedReaderEnabled &&
          resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
      val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
      val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
      val capacity = sqlConf.parquetVectorizedReaderBatchSize
      val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
      // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
      val returningBatch = supportBatch(sparkSession, resultSchema)
      val pushDownDate = sqlConf.parquetFilterPushDownDate
      val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
      val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
      val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
      val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
      val isCaseSensitive = sqlConf.caseSensitiveAnalysis

      (file: PartitionedFile) => {
        assert(file.partitionValues.numFields == partitionSchema.size)

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

        val sharedConf = broadcastedHadoopConf.value.value

        // do deal with internalSchema
        val internalSchemaString = sharedConf.get(SparkSchemaUtils.HOODIE_QUERY_SCHEMA)
        // querySchema must be a pruned schema.
        val querySchemaOpt = SerDeHelper.fromJson(internalSchemaString)
        val internalSchemaChangeEnabled = if (internalSchemaString.isEmpty || !querySchemaOpt.isPresent) false else true
        val tablePath = sharedConf.get(SparkSchemaUtils.HOODIE_TABLE_PATH)
        val commitTime = FSUtils.getCommitTime(filePath.getName).toLong;
        val fileSchema = if (internalSchemaChangeEnabled) {
          TableInternalSchemaUtils.searchSchemaAndCache(commitTime, tablePath, sharedConf)
        } else {
          null
        }

        lazy val footerFileMetaData =
          ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
        // Try to push down filters when filter push-down is enabled.
        val pushed = if (enableParquetFilterPushDown) {
          val parquetSchema = footerFileMetaData.getSchema
          val parquetFilters = new ParquetFilters(pushDownDate, pushDownTimestamp, pushDownDecimal,
            pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
          filters.map(Spark2HoodieParquetFileFormat.rebuildFilterFromParquet(_, fileSchema, querySchemaOpt.get()))
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
        val hadoopAttempConf = broadcastedHadoopConf.value.value
        // reset request schema
        if (internalSchemaChangeEnabled) {
          val mergedSchema = SparkSchemaUtils.mergeSchema(fileSchema, querySchemaOpt.get())
          hadoopAttempConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, mergedSchema.json)
        }
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(hadoopAttempConf, attemptId)

        // Try to push down filters when filter push-down is enabled.
        // Notice: This push-down is RowGroups level, not individual records.
        if (pushed.isDefined) {
          ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
        }
        val taskContext = Option(TaskContext.get())
        if (enableVectorizedReader) {
          val vectorizedReader = new VectorizedParquetRecordReader(
            convertTz.orNull, enableOffHeapColumnVector && taskContext.isDefined, capacity)
          val iter = new RecordReaderIterator(vectorizedReader)
          // SPARK-23457 Register a task completion lister before `initialization`.
          taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
          vectorizedReader.initialize(split, hadoopAttemptContext)
          logDebug(s"Appending $partitionSchema ${file.partitionValues}")
          vectorizedReader.initBatch(partitionSchema, file.partitionValues)
          if (returningBatch) {
            vectorizedReader.enableReturningBatches()
          }

          // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          logDebug(s"Falling back to parquet-mr")
          // ParquetRecordReader returns UnsafeRow
          val reader = if (pushed.isDefined && enableRecordFilter) {
            val parquetFilter = FilterCompat.get(pushed.get, null)
            new ParquetRecordReader[UnsafeRow](new ParquetReadSupport(convertTz), parquetFilter)
          } else {
            new ParquetRecordReader[UnsafeRow](new ParquetReadSupport(convertTz))
          }
          val iter = new RecordReaderIterator(reader)
          // SPARK-23457 Register a task completion lister before `initialization`.
          taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
          reader.initialize(split, hadoopAttemptContext)

          val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
          val joinedRow = new JoinedRow()
          val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

          // This is a horrible erasure hack...  if we type the iterator above, then it actually check
          // the type in next() and we get a class cast exception.  If we make that function return
          // Object, then we can defer the cast until later!
          if (partitionSchema.length == 0) {
            // There is no partition columns
            iter.asInstanceOf[Iterator[InternalRow]]
          } else {
            iter.asInstanceOf[Iterator[InternalRow]]
              .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
          }
        }
      }
    }
  }
}

object Spark2HoodieParquetFileFormat {

  private def rebuildFilterFromParquet(oldFilter: Filter, fileSchema: InternalSchema, querySchema: InternalSchema): Filter = {
    if (fileSchema == null || querySchema == null) {
      oldFilter
    } else {
      oldFilter match {
        case eq: EqualTo =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eq.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else eq.copy(attribute = newAttribute)
        case eqs: EqualNullSafe =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eqs.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else eqs.copy(attribute = newAttribute)
        case gt: GreaterThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else gt.copy(attribute = newAttribute)
        case gtr: GreaterThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gtr.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else gtr.copy(attribute = newAttribute)
        case lt: LessThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else lt.copy(attribute = newAttribute)
        case lte: LessThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lte.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else lte.copy(attribute = newAttribute)
        case i: In =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(i.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else i.copy(attribute = newAttribute)
        case isn: IsNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else isn.copy(attribute = newAttribute)
        case isnn: IsNotNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isnn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else isnn.copy(attribute = newAttribute)
        case And(left, right) =>
          And(rebuildFilterFromParquet(left, fileSchema, querySchema), rebuildFilterFromParquet(right, fileSchema, querySchema))
        case Or(left, right) =>
          Or(rebuildFilterFromParquet(left, fileSchema, querySchema), rebuildFilterFromParquet(right, fileSchema, querySchema))
        case Not(child) =>
          Not(rebuildFilterFromParquet(child, fileSchema, querySchema))
        case ssw: StringStartsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ssw.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else ssw.copy(attribute = newAttribute)
        case ses: StringEndsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ses.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else ses.copy(attribute = newAttribute)
        case sc: StringContains =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(sc.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) oldFilter else sc.copy(attribute = newAttribute)
        case _ =>
          oldFilter
      }
    }
  }
}
