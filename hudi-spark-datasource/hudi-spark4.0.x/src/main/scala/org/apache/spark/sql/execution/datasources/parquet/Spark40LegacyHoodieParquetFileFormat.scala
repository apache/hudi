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

import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineLayout
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.util.InternalSchemaCache
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.action.InternalSchemaMerger
import org.apache.hudi.internal.schema.utils.{InternalSchemaUtils, SerDeHelper}
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.parquet.Spark40LegacyHoodieParquetFileFormat._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, DataType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * This class is an extension of [[ParquetFileFormat]] overriding Spark-specific behavior
 * that's not possible to customize in any other way
 *
 * NOTE: This is a version of [[AvroDeserializer]] impl from Spark 3.2.1 w/ w/ the following changes applied to it:
 * <ol>
 *   <li>Avoiding appending partition values to the rows read from the data file</li>
 *   <li>Schema on-read</li>
 * </ol>
 */
class Spark40LegacyHoodieParquetFileFormat(private val shouldAppendPartitionValues: Boolean) extends ParquetFileFormat {

  def supportsColumnar(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    // Only output columnar if there is WSCG to read it.
    val requiredWholeStageCodegenSettings =
      conf.wholeStageEnabled && !WholeStageCodegenExec.isTooManyFields(conf, schema)
    requiredWholeStageCodegenSettings &&
      supportBatch(sparkSession, schema)
  }

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
    // Using string value of this conf to preserve compatibility across spark versions.
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sparkSession.sessionState.conf.getConfString(
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.defaultValueString).toBoolean
    )
    hadoopConf.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, sparkSession.sessionState.conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, sparkSession.sessionState.conf.legacyParquetNanosAsLong)
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
    val enableVectorizedReader: Boolean = supportBatch(sparkSession, resultSchema)
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringPredicate
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead
    val timeZoneId = Option(sqlConf.sessionLocalTimeZone)
    // Should always be set by FileSourceScanExec creating this.
    // Check conf before checking option, to allow working around an issue by changing conf.
    val returningBatch = sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
      supportsColumnar(sparkSession, resultSchema).toString.equals("true")


    (file: PartitionedFile) => {
      assert(!shouldAppendPartitionValues || file.partitionValues.numFields == partitionSchema.size)

      val filePath = file.filePath.toPath
      val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

      val sharedConf = broadcastedHadoopConf.value.value

      // Fetch internal schema
      val internalSchemaStr = sharedConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA)
      // Internal schema has to be pruned at this point
      val querySchemaOption = SerDeHelper.fromJson(internalSchemaStr)

      var shouldUseInternalSchema = !isNullOrEmpty(internalSchemaStr) && querySchemaOption.isPresent

      val tablePath = sharedConf.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH)
      val fileSchema = if (shouldUseInternalSchema) {
        val commitInstantTime = FSUtils.getCommitTime(filePath.getName).toLong;
        val validCommits = sharedConf.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST)
        val storage = new HoodieHadoopStorage(tablePath, sharedConf)
        //TODO: HARDCODED TIMELINE OBJECT
        val layout = TimelineLayout.fromVersion(TimelineLayoutVersion.CURR_LAYOUT_VERSION)
        InternalSchemaCache.getInternalSchemaByVersionId(
          commitInstantTime, tablePath, storage, if (validCommits == null) "" else validCommits, layout)
      } else {
        null
      }

      val fileFooter = if (enableVectorizedReader) {
        // When there are vectorized reads, we can avoid reading the footer twice by reading
        // all row groups in advance and filter row groups according to filters that require
        // push down (no need to read the footer metadata again).
        ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.WITH_ROW_GROUPS)
      } else {
        ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.SKIP_ROW_GROUPS)
      }

      val footerFileMetaData = fileFooter.getFileMetaData
      val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
      val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)
      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        val parquetSchema = footerFileMetaData.getSchema
        val parquetFilters = new ParquetFilters(
          parquetSchema,
          pushDownDate,
          pushDownTimestamp,
          pushDownDecimal,
          pushDownStringStartWith,
          pushDownInFilterThreshold,
          isCaseSensitive,
          datetimeRebaseSpec)
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

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)

      // Clone new conf
      val hadoopAttemptConf = new Configuration(broadcastedHadoopConf.value.value)
      val typeChangeInfos: java.util.Map[Integer, Pair[DataType, DataType]] = if (shouldUseInternalSchema) {
        val mergedInternalSchema = new InternalSchemaMerger(fileSchema, querySchemaOption.get(), true, true).mergeSchema()
        val mergedSchema = SparkInternalSchemaConverter.constructSparkSchemaFromInternalSchema(mergedInternalSchema)

        hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, mergedSchema.json)

        SparkInternalSchemaConverter.collectTypeChangedCols(querySchemaOption.get(), mergedInternalSchema)
      } else {
        val (implicitTypeChangeInfo, sparkRequestSchema) = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(hadoopAttemptConf, footerFileMetaData, requiredSchema)
        if (!implicitTypeChangeInfo.isEmpty) {
          shouldUseInternalSchema = true
          hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, sparkRequestSchema.json)
        }
        implicitTypeChangeInfo
      }

      if (enableVectorizedReader && shouldUseInternalSchema &&
        !typeChangeInfos.values().forall(_.getLeft.isInstanceOf[AtomicType])) {
        throw new IllegalArgumentException(
          "Nested types with type changes(implicit or explicit) cannot be read in vectorized mode. " +
            "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false.")
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
            new HoodieVectorizedParquetRecordReader(
              convertTz.orNull,
              datetimeRebaseSpec.mode.toString,
              datetimeRebaseSpec.timeZone,
              int96RebaseSpec.mode.toString,
              int96RebaseSpec.timeZone,
              enableOffHeapColumnVector && taskContext.isDefined,
              capacity,
              typeChangeInfos)
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
        } catch {
          case e: Throwable =>
            // SPARK-23457: In case there is an exception in initialization, close the iterator to
            // avoid leaking resources.
            iter.close()
            throw e
        }
      } else {
        logDebug(s"Falling back to parquet-mr")
        val readSupport = new HoodieParquetReadSupport(
          convertTz,
          enableVectorizedReader = false,
          enableTimestampFieldRepair = true,
          datetimeRebaseSpec,
          int96RebaseSpec)

        val reader = if (pushed.isDefined && enableRecordFilter) {
          val parquetFilter = FilterCompat.get(pushed.get, null)
          new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
        } else {
          new ParquetRecordReader[InternalRow](readSupport)
        }
        val readerWithRowIndexes = ParquetRowIndexUtil.addRowIndexToRecordReaderIfNeeded(reader, requiredSchema)
        val iter = new RecordReaderIterator[InternalRow](readerWithRowIndexes)
        try {
          readerWithRowIndexes.initialize(split, hadoopAttemptContext)

          val fullSchema = DataTypeUtils.toAttributes(requiredSchema) ++ DataTypeUtils.toAttributes(partitionSchema)
          val unsafeProjection = if (typeChangeInfos.isEmpty) {
            GenerateUnsafeProjection.generate(fullSchema, fullSchema)
          } else {
            // find type changed.
            val newSchema = new StructType(requiredSchema.fields.zipWithIndex.map { case (f, i) =>
              if (typeChangeInfos.containsKey(i)) {
                StructField(f.name, typeChangeInfos.get(i).getRight, f.nullable, f.metadata)
              } else f
            })
            val newFullSchema = DataTypeUtils.toAttributes(newSchema) ++ DataTypeUtils.toAttributes(partitionSchema)
            val castSchema = newFullSchema.zipWithIndex.map { case (attr, i) =>
              if (typeChangeInfos.containsKey(i)) {
                val srcType = typeChangeInfos.get(i).getRight
                val dstType = typeChangeInfos.get(i).getLeft
                val needTimeZone = Cast.needsTimeZone(srcType, dstType)
                Cast(attr, dstType, if (needTimeZone) timeZoneId else None)
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
}

object Spark40LegacyHoodieParquetFileFormat {

  def pruneInternalSchema(internalSchemaStr: String, requiredSchema: StructType): String = {
    val querySchemaOption = SerDeHelper.fromJson(internalSchemaStr)
    if (querySchemaOption.isPresent && requiredSchema.nonEmpty) {
      val prunedSchema = SparkInternalSchemaConverter.convertAndPruneStructTypeToInternalSchema(requiredSchema, querySchemaOption.get())
      SerDeHelper.toJson(prunedSchema)
    } else {
      internalSchemaStr
    }
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
