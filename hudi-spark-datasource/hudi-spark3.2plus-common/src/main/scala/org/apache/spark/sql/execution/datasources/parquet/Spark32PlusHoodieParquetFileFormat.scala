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
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.common.util.{BuildUtils, HoodieTimer, InternalSchemaCache}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.Types.Field
import org.apache.hudi.internal.schema.action.InternalSchemaMerger
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.{InternalSchemaUtils, SerDeHelper}
import org.apache.hudi.secondary.index._
import org.apache.hudi.secondary.index.filter._
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Cast, JoinedRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.Spark32PlusHoodieParquetFileFormat._
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, DataType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.roaringbitmap.RoaringBitmap

import java.net.URI

import scala.collection.JavaConverters._

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
class Spark32PlusHoodieParquetFileFormat(private val shouldAppendPartitionValues: Boolean) extends ParquetFileFormat {

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
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

    (file: PartitionedFile) => {
      assert(!shouldAppendPartitionValues || file.partitionValues.numFields == partitionSchema.size)

      val filePath = new Path(new URI(file.filePath))
      val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

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
        ParquetFooterReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData

      val internalSchema = AvroInternalSchemaConverter.convert(footerFileMetaData.getSchema)

      val timer = new HoodieTimer
      timer.startTimer()
      // BaseFilePath -> List[HoodieSecondaryIndex]
      val indexData = BuildUtils.getBaseFileIndexInfo(sharedConf)
      val indexDataOpt = if (indexData != null) {
        Some(indexData.asScala)
      } else {
        None
      }

      // Because of schema evolution, field name in filter may not be consistence with file schema
      val newFilters = filters.map(rebuildFilterFromParquet(_, fileSchema, querySchemaOption.orElse(null)))

      // Try to use index only if some fields have been built index with this file
      val filtersNeedPush = indexDataOpt.map(indexData => {
        val normalizedFilePath = filePath.toString
        val index = normalizedFilePath.indexOf(tablePath)
        val baseFilePath: String = if (index >= 0) {
          normalizedFilePath.substring(tablePath.length + 1)
        } else {
          normalizedFilePath
        }

        indexData.get(baseFilePath).map(validIndex => {
          timer.startTimer()
          val indexMeta = SecondaryIndexUtils.fromJsonString(
            sharedConf.get(HoodieTableConfig.SECONDARY_INDEXES_METADATA.key())).asScala

          // Pair[IndexDir, IndexType] -> IndexReader
          val indexReaders: Map[Pair[String, SecondaryIndexType], ISecondaryIndexReader] =
            Map.empty.withDefault(key => SecondaryIndexFactory.getIndexReader(key.getLeft, key.getRight, sharedConf))
          // Determine and convert filter to index filter
          val indexFolder = BuildUtils.getIndexFolderPath(tablePath)
          val filtersTuple = splitFilters(newFilters, internalSchema.columns().asScala, indexMeta,
            validIndex.asScala, filePath.getName, indexFolder, indexReaders)
          logInfo(s"Split filter cost ${timer.endTimer()} ms")

          // Pass index hit row id set through hadoop configuration
          if (filtersTuple._1.nonEmpty) {
            timer.startTimer()
            val indexFilter = filtersTuple._1.reduce((x, y) => new AndFilter(x, y))
            val specificRowIdSet = indexFilter.getRowIdSet.getContainer.asInstanceOf[RoaringBitmap]
            SecondaryIndexUtils.setSpecificRowIdSet(sharedConf, specificRowIdSet)
            logInfo(s"Exec index filter $indexFilter cost ${timer.endTimer()} ms")
          }

          // Filters which will be pushed down as parquet filters
          filtersTuple._2
        }).getOrElse(newFilters)
      }).getOrElse(newFilters)
      logInfo(s"Convert index total cost ${timer.endTimer()} ms, push down $filtersNeedPush as parquet filter")

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
            Spark32PlusDataSourceUtils.datetimeRebaseMode(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
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
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        filtersNeedPush.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
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
            val int96RebaseSpec =
              DataSourceUtils.int96RebaseSpec(footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)
            val datetimeRebaseSpec =
              DataSourceUtils.datetimeRebaseSpec(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
            new Spark32PlusHoodieVectorizedParquetRecordReader(
              convertTz.orNull,
              datetimeRebaseSpec.mode.toString,
              datetimeRebaseSpec.timeZone,
              int96RebaseSpec.mode.toString,
              int96RebaseSpec.timeZone,
              enableOffHeapColumnVector && taskContext.isDefined,
              capacity,
              typeChangeInfos)
          } else if (HoodieSparkUtils.gteqSpark3_2_1) {
            // NOTE: Below code could only be compiled against >= Spark 3.2.1,
            //       and unfortunately won't compile against Spark 3.2.0
            //       However this code is runtime-compatible w/ both Spark 3.2.0 and >= Spark 3.2.1
            val int96RebaseSpec =
              DataSourceUtils.int96RebaseSpec(footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)
            val datetimeRebaseSpec =
              DataSourceUtils.datetimeRebaseSpec(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
            new HoodieVectorizedParquetRecordReader(
              convertTz.orNull,
              datetimeRebaseSpec.mode.toString,
              datetimeRebaseSpec.timeZone,
              int96RebaseSpec.mode.toString,
              int96RebaseSpec.timeZone,
              enableOffHeapColumnVector && taskContext.isDefined,
              capacity)
          } else {
            // Spark 3.2.0
            val datetimeRebaseMode =
              Spark32PlusDataSourceUtils.datetimeRebaseMode(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
            val int96RebaseMode =
              Spark32PlusDataSourceUtils.int96RebaseMode(footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)
            createVectorizedParquetRecordReader(
              convertTz.orNull,
              datetimeRebaseMode.toString,
              int96RebaseMode.toString,
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
          vectorizedReader.initialize(split, hadoopAttemptContext)

          // NOTE: We're making appending of the partitioned values to the rows read from the
          //       data file configurable
          if (shouldAppendPartitionValues) {
            logDebug(s"Appending $partitionSchema ${file.partitionValues}")
            initBatchAndEnableReturningBatch(vectorizedReader, partitionSchema, file.partitionValues, returningBatch)
          } else {
            initBatchAndEnableReturningBatch(vectorizedReader, StructType(Nil), InternalRow.empty, returningBatch)
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
            Spark32PlusDataSourceUtils.datetimeRebaseMode(footerFileMetaData.getKeyValueMetaData.get, datetimeRebaseModeInRead)
          val int96RebaseMode =
            Spark32PlusDataSourceUtils.int96RebaseMode(footerFileMetaData.getKeyValueMetaData.get, int96RebaseModeInRead)
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

  private def initBatchAndEnableReturningBatch(
      recordReader: RecordReader[Void, AnyRef],
      partitionColumns: StructType,
      partitionValues: InternalRow,
      returningBatch: Boolean): Unit = {
    recordReader match {
      case reader: VectorizedParquetRecordReader =>
        reader.initBatch(partitionColumns, partitionValues)
        if (returningBatch) reader.enableReturningBatches()
      case reader: HoodieVectorizedParquetRecordReader =>
        reader.initBatch(partitionColumns, partitionValues)
        if (returningBatch) reader.enableReturningBatches()
      case _ =>
    }
  }
}

object Spark32PlusHoodieParquetFileFormat {

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

  /**
   * NOTE: This method is specific to Spark 3.2.0
   */
  private def createVectorizedParquetRecordReader(args: Any*): VectorizedParquetRecordReader = {
    // NOTE: ParquetReadSupport ctor args contain Scala enum, therefore we can't look it
    //       up by arg types, and have to instead rely on the number of args based on individual class;
    //       the ctor order is not guaranteed
    val ctor = classOf[VectorizedParquetRecordReader].getConstructors.maxBy(_.getParameterCount)
    ctor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*)
      .asInstanceOf[VectorizedParquetRecordReader]
  }

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

  /**
   * Split passed-in filters into index filters and normal filters which will
   * be pushed down as parquet filters
   *
   * @param filters      Spark sql filter
   * @param fields       Hudi fields which converted from parquet file schema
   * @param indexMeta    Secondary index meta data for this table
   * @param validIndex   Indexes which have been built for this base file
   * @param fileName     File name
   * @param indexFolder  Folder to save index data
   * @param indexReaders Index data reader
   * @return Index filters and normal filters
   */
  private def splitFilters(
      filters: Seq[Filter],
      fields: Seq[Field],
      indexMeta: Seq[HoodieSecondaryIndex],
      validIndex: Seq[HoodieSecondaryIndex],
      fileName: String,
      indexFolder: String,
      indexReaders: Map[Pair[String, SecondaryIndexType], ISecondaryIndexReader]): (Seq[IndexFilter], Seq[Filter]) = {

    var indexFilters: Seq[IndexFilter] = Seq.empty
    var newFilters: Seq[Filter] = Seq.empty
    filters.foreach(filter => {
      val indexFilter = buildIndexFilter(filter, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
      if (indexFilter.isDefined) {
        indexFilters = indexFilters :+ indexFilter.get
      } else {
        newFilters = newFilters :+ filter
      }
    })

    (indexFilters, newFilters)
  }

  /**
   * Build hoodie index filter from {@code org.apache.spark.sql.sources.Filter}
   * Because of the schema evolution, the field name may be changed, so the
   * passed-in field name must be the real field name,
   *
   * @param filter       Spark sql filter
   * @param fields       Hudi fields which converted from parquet file schema
   * @param indexMeta    Secondary index meta data for this table
   * @param validIndex   Indexes which have been built for this base file
   * @param fileName     File name
   * @param indexFolder  Folder to save index data
   * @param indexReaders Index data reader
   * @return Hoodie index filter
   */
  private def buildIndexFilter(
      filter: Filter,
      fields: Seq[Field],
      indexMeta: Seq[HoodieSecondaryIndex],
      validIndex: Seq[HoodieSecondaryIndex],
      fileName: String,
      indexFolder: String,
      indexReaders: Map[Pair[String, SecondaryIndexType], ISecondaryIndexReader]): Option[IndexFilter] = {
    filter match {
      case eq: EqualTo if canUseIndex(eq.attribute, indexMeta, validIndex) =>
        rebuildHudiField(eq.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new TermFilter(indexReader, field, eq.value)
            })
      case eqs: EqualNullSafe if canUseIndex(eqs.attribute, indexMeta, validIndex) =>
        rebuildHudiField(eqs.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new NullFilter(indexReader, field)
            })
      case gt: GreaterThan if canUseIndex(gt.attribute, indexMeta, validIndex) =>
        rebuildHudiField(gt.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, gt.value, null, false, false)
            })
      case gtr: GreaterThanOrEqual if canUseIndex(gtr.attribute, indexMeta, validIndex) =>
        rebuildHudiField(gtr.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, gtr.value, null, true, false)
            })
      case lt: LessThan if canUseIndex(lt.attribute, indexMeta, validIndex) =>
        rebuildHudiField(lt.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, null, lt.value, false, false)
            })
      case lte: LessThanOrEqual if canUseIndex(lte.attribute, indexMeta, validIndex) =>
        rebuildHudiField(lte.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RangeFilter(indexReader, field, null, lte.value, false, true)
            })
      case i: In if canUseIndex(i.attribute, indexMeta, validIndex) =>
        rebuildHudiField(i.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              val values = i.values.toList.map(_.asInstanceOf[AnyRef]).asJava
              new TermListFilter(indexReader, field, values)
            })
      case isn: IsNull if canUseIndex(isn.attribute, indexMeta, validIndex) =>
        rebuildHudiField(isn.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new NullFilter(indexReader, field)
            })
      case isnn: IsNotNull if canUseIndex(isnn.attribute, indexMeta, validIndex) =>
        rebuildHudiField(isnn.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new NotNullFilter(indexReader, field)
            })
      case And(left, right) =>
        buildIndexFilter(left, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
            .flatMap(left =>
              buildIndexFilter(right, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
                  .map(right => new AndFilter(left, right)))
      case Or(left, right) =>
        buildIndexFilter(left, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
            .flatMap(left =>
              buildIndexFilter(right, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
                  .map(right => new OrFilter(left, right)))
      case Not(child) =>
        buildIndexFilter(child, fields, indexMeta, validIndex, fileName, indexFolder, indexReaders)
            .map(x => new NotFilter(x))
      case ssw: StringStartsWith if canUseIndex(ssw.attribute, indexMeta, validIndex) =>
        rebuildHudiField(ssw.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new PrefixFilter(indexReader, field, ssw.value)
            })
      case ses: StringEndsWith if canUseIndex(ses.attribute, indexMeta, validIndex) =>
        rebuildHudiField(ses.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RegexFilter(indexReader, field, s"%${ses.value}")
            })
      case sc: StringContains if canUseIndex(sc.attribute, indexMeta, validIndex) =>
        rebuildHudiField(sc.attribute, fields)
            .map(field => {
              val indexReader = buildIndexReader(field.name(), indexMeta, fileName, indexFolder, indexReaders)
              new RegexFilter(indexReader, field, s"%${sc.value}%")
            })
      case AlwaysTrue =>
        Some(new AllRowFilter(null))
      case AlwaysFalse =>
        Some(new EmptyRowFilter(null))
      case _ =>
        None
    }
  }

  /**
   * Check whether the given field can use secondary index with this file
   *
   * @param fieldName  Field name  to be checked
   * @param indexMeta  Secondary index meta data for this table
   * @param validIndex All HoodieSecondaryIndex that have been built index in this file
   * @return true if can use secondary index for this field
   */
  private def canUseIndex(
      fieldName: String,
      indexMeta: Seq[HoodieSecondaryIndex],
      validIndex: Seq[HoodieSecondaryIndex]): Boolean = {
    indexMeta.exists(index =>
      index.getColumns.size() == 1 &&
          index.getColumns.keySet().contains(fieldName) &&
          validIndex.contains(index))
  }

  private def buildIndexReader(
      fieldName: String,
      indexMeta: Seq[HoodieSecondaryIndex],
      fileName: String,
      indexFolder: String,
      indexReaders: Map[Pair[String, SecondaryIndexType], ISecondaryIndexReader]): ISecondaryIndexReader = {
    indexMeta.find(index => index.getColumns.keySet().contains(fieldName))
        .map(index => {
          val indexSavePath = BuildUtils.getIndexSaveDir(indexFolder, index.getIndexType.name(), fileName)
          indexReaders(Pair.of(indexSavePath.toString, index.getIndexType))
        }).get
  }

  /**
   * Get hudi field from given field name
   *
   * @param fieldName Field name in filter
   * @param fields    Hudi fields which converted from parquet file schema
   * @return Hudi field
   */
  private def rebuildHudiField(
      fieldName: String, fields: Seq[Field]): Option[Field] = {
    fields.find(field => field.name().equals(fieldName))
  }
}

