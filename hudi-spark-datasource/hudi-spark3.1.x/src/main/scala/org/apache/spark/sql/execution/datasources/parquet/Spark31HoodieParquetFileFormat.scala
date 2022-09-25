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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.common.util.{InternalSchemaCache, ReflectionUtils, StringUtils}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.action.InternalSchemaMerger
import org.apache.hudi.internal.schema.utils.{InternalSchemaUtils, SerDeHelper}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Cast, JoinedRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.Spark31HoodieParquetFileFormat.{createParquetFilters, pruneInternalSchema, rebuildFilterFromParquet}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, DataType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.net.URI


/**
 * This class is an extension of [[ParquetFileFormat]] overriding Spark-specific behavior
 * that's not possible to customize in any other way
 *
 * NOTE: This is a version of [[AvroDeserializer]] impl from Spark 3.1.2 w/ w/ the following changes applied to it:
 * <ol>
 *   <li>Avoiding appending partition values to the rows read from the data file</li>
 *   <li>Schema on-read</li>
 * </ol>
 */
class Spark31HoodieParquetFileFormat(private val shouldAppendPartitionValues: Boolean) extends ParquetFileFormat {

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
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

    val internalSchemaStr = hadoopConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA)
    // For Spark DataSource v1, there's no Physical Plan projection/schema pruning w/in Spark itself,
    // therefore it's safe to do schema projection here
    if (!isNullOrEmpty(internalSchemaStr)) {
      val prunedInternalSchemaStr =
        pruneInternalSchema(internalSchemaStr, requiredSchema)
      hadoopConf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, prunedInternalSchemaStr)
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
      assert(!shouldAppendPartitionValues || file.partitionValues.numFields == partitionSchema.size)

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

      // Fetch internal schema
      val internalSchemaStr = sharedConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA)
      // Internal schema has to be pruned at this point
      val querySchemaOption = SerDeHelper.fromJson(internalSchemaStr)

      val shouldUseInternalSchema = !isNullOrEmpty(internalSchemaStr) && querySchemaOption.isPresent

      val tablePath = sharedConf.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH)
      val fileSchema = if (shouldUseInternalSchema) {
        val commitInstantTime = FSUtils.getCommitTime(filePath.getName).toLong;
        val validCommits = sharedConf.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST)
        InternalSchemaCache.getInternalSchemaByVersionId(commitInstantTime, tablePath, sharedConf, if (validCommits == null) "" else validCommits)
      } else {
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
        filters.map(rebuildFilterFromParquet(_, fileSchema, querySchemaOption.orElse(null)))
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter)
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

      // Clone new conf
      val hadoopAttemptConf = new Configuration(broadcastedHadoopConf.value.value)
      var typeChangeInfos: java.util.Map[Integer, Pair[DataType, DataType]] = if (shouldUseInternalSchema) {
        val mergedInternalSchema = new InternalSchemaMerger(fileSchema, querySchemaOption.get(), true, true).mergeSchema()
        val mergedSchema = SparkInternalSchemaConverter.constructSparkSchemaFromInternalSchema(mergedInternalSchema)

        hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, mergedSchema.json)

        SparkInternalSchemaConverter.collectTypeChangedCols(querySchemaOption.get(), mergedInternalSchema)
      } else {
        new java.util.HashMap()
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
        val vectorizedReader =
          if (shouldUseInternalSchema) {
            new Spark31HoodieVectorizedParquetRecordReader(
              convertTz.orNull,
              datetimeRebaseMode.toString,
              int96RebaseMode.toString,
              enableOffHeapColumnVector && taskContext.isDefined,
              capacity,
              typeChangeInfos)
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

        // NOTE: We're making appending of the partitioned values to the rows read from the
        //       data file configurable
        if (shouldAppendPartitionValues) {
          logDebug(s"Appending $partitionSchema ${file.partitionValues}")
          vectorizedReader.initBatch(partitionSchema, file.partitionValues)
        } else {
          vectorizedReader.initBatch(StructType(Nil), InternalRow.empty)
        }

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

        // NOTE: We're making appending of the partitioned values to the rows read from the
        //       data file configurable
        if (!shouldAppendPartitionValues || partitionSchema.length == 0) {
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

object Spark31HoodieParquetFileFormat {

  def pruneInternalSchema(internalSchemaStr: String, requiredSchema: StructType): String = {
    val querySchemaOption = SerDeHelper.fromJson(internalSchemaStr)
    if (querySchemaOption.isPresent && requiredSchema.nonEmpty) {
      val prunedSchema = SparkInternalSchemaConverter.convertAndPruneStructTypeToInternalSchema(requiredSchema, querySchemaOption.get())
      SerDeHelper.toJson(prunedSchema)
    } else {
      internalSchemaStr
    }
  }

  private def createParquetFilters(args: Any*): ParquetFilters = {
    // ParquetFilters bears a single ctor (in Spark 3.1)
    val ctor = classOf[ParquetFilters].getConstructors.head
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[ParquetFilters]
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
