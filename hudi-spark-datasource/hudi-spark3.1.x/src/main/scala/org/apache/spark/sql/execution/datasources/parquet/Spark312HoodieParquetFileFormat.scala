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
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.util.InternalSchemaCache
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.{InternalSchemaUtils, SerDeHelper}
import org.apache.hudi.internal.schema.action.InternalSchemaMerger
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetRecordReader}

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, DataType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

class Spark312HoodieParquetFileFormat extends ParquetFileFormat {

  // reference ParquetFileFormat from spark project
  override def buildReaderWithPartitionValues(
                                               sparkSession: SparkSession,
                                               dataSchema: StructType,
                                               partitionSchema: StructType,
                                               requiredSchema: StructType,
                                               filters: Seq[Filter],
                                               options: Map[String, String],
                                               hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    if (hadoopConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, "").isEmpty) {
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
      // for dataSource v1, we have no method to do project for spark physical plan.
      // it's safe to do cols project here.
      val internalSchemaString = hadoopConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA)
      val querySchemaOption = SerDeHelper.fromJson(internalSchemaString)
      if (querySchemaOption.isPresent && !requiredSchema.isEmpty) {
        val prunedSchema = SparkInternalSchemaConverter.convertAndPruneStructTypeToInternalSchema(requiredSchema, querySchemaOption.get())
        hadoopConf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(prunedSchema))
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
        val filePath = new Path(new URI(file.filePath))
        val split =
          new org.apache.parquet.hadoop.ParquetInputSplit(
            filePath,
            file.start,
            file.start + file.length,
            file.length,
            Array.empty,
            null)
        val sharedConf = broadcastedHadoopConf.value.value
        // do deal with internalSchema
        val internalSchemaString = sharedConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA)
        // querySchema must be a pruned schema.
        val querySchemaOption = SerDeHelper.fromJson(internalSchemaString)
        val internalSchemaChangeEnabled = if (internalSchemaString.isEmpty || !querySchemaOption.isPresent) false else true
        val tablePath = sharedConf.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH)
        val commitInstantTime = FSUtils.getCommitTime(filePath.getName).toLong;
        val fileSchema = if (internalSchemaChangeEnabled) {
          val validCommits = sharedConf.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST)
          InternalSchemaCache.getInternalSchemaByVersionId(commitInstantTime, tablePath, sharedConf, if (validCommits == null) "" else validCommits)
        } else {
          // this should not happened, searchSchemaAndCache will deal with correctly.
          null
        }

        lazy val footerFileMetaData =
          ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
        val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
          footerFileMetaData.getKeyValueMetaData.get,
          SQLConf.get.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ))
        // Try to push down filters when filter push-down is enabled.
        val pushed = if (enableParquetFilterPushDown) {
          val parquetSchema = footerFileMetaData.getSchema
          val parquetFilters = if (HoodieSparkUtils.gteqSpark3_1_3) {
            Spark312HoodieParquetFileFormat.createParquetFilters(
              parquetSchema,
              pushDownDate,
              pushDownTimestamp,
              pushDownDecimal,
              pushDownStringStartWith,
              pushDownInFilterThreshold,
              isCaseSensitive,
              datetimeRebaseMode)
          } else {
            Spark312HoodieParquetFileFormat.createParquetFilters(
              parquetSchema,
              pushDownDate,
              pushDownTimestamp,
              pushDownDecimal,
              pushDownStringStartWith,
              pushDownInFilterThreshold,
              isCaseSensitive)
          }
          filters.map(Spark312HoodieParquetFileFormat.rebuildFilterFromParquet(_, fileSchema, querySchemaOption.get()))
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
        // use new conf
        val hadoopAttempConf = new Configuration(broadcastedHadoopConf.value.value)
        //
        // reset request schema
        var typeChangeInfos: java.util.Map[Integer, Pair[DataType, DataType]] = new java.util.HashMap()
        if (internalSchemaChangeEnabled) {
          val mergedInternalSchema = new InternalSchemaMerger(fileSchema, querySchemaOption.get(), true, true).mergeSchema()
          val mergedSchema = SparkInternalSchemaConverter.constructSparkSchemaFromInternalSchema(mergedInternalSchema)
          typeChangeInfos = SparkInternalSchemaConverter.collectTypeChangedCols(querySchemaOption.get(), mergedInternalSchema)
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
          val vectorizedReader = new Spark312HoodieVectorizedParquetRecordReader(
            convertTz.orNull,
            datetimeRebaseMode.toString,
            int96RebaseMode.toString,
            enableOffHeapColumnVector && taskContext.isDefined,
            capacity, typeChangeInfos)
          val iter = new RecordReaderIterator(vectorizedReader)
          // SPARK-23457 Register a task completion listener before `initialization`.
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

          val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
          val unsafeProjection = if (typeChangeInfos.isEmpty) {
            GenerateUnsafeProjection.generate(fullSchema, fullSchema)
          } else {
            // find type changed.
            val newFullSchema = new StructType(requiredSchema.fields.zipWithIndex.map { case (f, i) =>
              if (typeChangeInfos.containsKey(i)) {
                StructField(f.name, typeChangeInfos.get(i).getRight, f.nullable, f.metadata)
              } else f
            }).toAttributes ++ partitionSchema.toAttributes
            val castSchema = newFullSchema.zipWithIndex.map { case (attr, i) =>
              if (typeChangeInfos.containsKey(i)) {
                Cast(attr, typeChangeInfos.get(i).getLeft)
              } else attr
            }
            GenerateUnsafeProjection.generate(castSchema, newFullSchema)
          }

          if (partitionSchema.length == 0) {
            // There is no partition columns
            iter.map(unsafeProjection)
          } else {
            val joinedRow = new JoinedRow()
            iter.map(d => unsafeProjection(joinedRow(d, file.partitionValues)))
          }
        }
      }
    }
  }
}

object Spark312HoodieParquetFileFormat {

  val PARQUET_FILTERS_CLASS_NAME = "org.apache.spark.sql.execution.datasources.parquet.ParquetFilters"

  private def createParquetFilters(arg: Any*): ParquetFilters = {
    val clazz = Class.forName(PARQUET_FILTERS_CLASS_NAME, true, Thread.currentThread().getContextClassLoader)
    val ctor = clazz.getConstructors.head
    ctor.newInstance(arg.map(_.asInstanceOf[AnyRef]): _*).asInstanceOf[ParquetFilters]
  }

  private def rebuildFilterFromParquet(oldFilter: Filter, fileSchema: InternalSchema, querySchema: InternalSchema): Filter = {
    if (fileSchema == null || querySchema == null) {
      oldFilter
    } else {
      oldFilter match {
        case eq: EqualTo =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eq.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eq.copy(attribute = newAttribute)
        case eqs: EqualNullSafe =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eqs.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eqs.copy(attribute = newAttribute)
        case gt: GreaterThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gt.copy(attribute = newAttribute)
        case gtr: GreaterThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gtr.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gtr.copy(attribute = newAttribute)
        case lt: LessThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lt.copy(attribute = newAttribute)
        case lte: LessThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lte.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lte.copy(attribute = newAttribute)
        case i: In =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(i.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else i.copy(attribute = newAttribute)
        case isn: IsNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isn.copy(attribute = newAttribute)
        case isnn: IsNotNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isnn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isnn.copy(attribute = newAttribute)
        case And(left, right) =>
          And(rebuildFilterFromParquet(left, fileSchema, querySchema), rebuildFilterFromParquet(right, fileSchema, querySchema))
        case Or(left, right) =>
          Or(rebuildFilterFromParquet(left, fileSchema, querySchema), rebuildFilterFromParquet(right, fileSchema, querySchema))
        case Not(child) =>
          Not(rebuildFilterFromParquet(child, fileSchema, querySchema))
        case ssw: StringStartsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ssw.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ssw.copy(attribute = newAttribute)
        case ses: StringEndsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ses.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ses.copy(attribute = newAttribute)
        case sc: StringContains =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(sc.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else sc.copy(attribute = newAttribute)
        case AlwaysTrue =>
          AlwaysTrue
        case AlwaysFalse =>
          AlwaysFalse
        case _ =>
          AlwaysTrue
      }
    }
  }
}
