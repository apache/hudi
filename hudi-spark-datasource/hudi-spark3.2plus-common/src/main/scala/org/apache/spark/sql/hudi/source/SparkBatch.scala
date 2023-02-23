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

package org.apache.spark.sql.hudi.source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, RecordReader, TaskAttemptID, TaskID, TaskType}
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.InternalSchemaCache
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.internal.schema.action.InternalSchemaMerger
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetFooterReader, ParquetOptions, ParquetReadSupport, ParquetWriteSupport, Spark32PlusDataSourceUtils, Spark32PlusHoodieVectorizedParquetRecordReader, VectorizedParquetRecordReader}
import org.apache.spark.sql.execution.datasources.parquet.Spark32PlusHoodieParquetFileFormat.{createParquetFilters, createParquetReadSupport, createVectorizedParquetRecordReader, pruneInternalSchema, rebuildFilterFromParquet}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, DataType, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

class SparkBatch(spark: SparkSession,
                 selectedPartitions: Seq[PartitionDirectory],
                 partitionSchema: StructType,
                 requiredSchema: StructType,
                 filters: Seq[Filter],
                 options: Map[String, String],
                 @transient hadoopConf: Configuration) extends Batch with Serializable {

  override def planInputPartitions(): Array[InputPartition] = {
    // TODO support more accurate task planning.
    val maxSplitBytes = FilePartition.maxSplitBytes(spark, selectedPartitions)
    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          sparkSession = spark,
          file = file,
          filePath = filePath,
          isSplitable = true,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    FilePartition.getFilePartitions(spark, splitFiles, maxSplitBytes).toArray

  }

  override def createReaderFactory(): PartitionReaderFactory = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      spark.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      spark.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      spark.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(requiredSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      spark.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      spark.sessionState.conf.isParquetINT96AsTimestamp)
    val broadcastedConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val sqlConf = spark.sessionState.conf
    ReaderFactory(sqlConf, broadcastedConf)
  }

  case class ReaderFactory(sqlConf: SQLConf,
                           broadcastedConf: Broadcast[SerializableConfiguration]) extends FilePartitionReaderFactory {
    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema: StructType = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val enableOffHeapColumnVector: Boolean = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean =
      sqlConf.parquetVectorizedReaderEnabled &&
        resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity: Int = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
    val pushDownDate: Boolean = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetOptions = new ParquetOptions(options, spark.sessionState.conf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

    override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
      val reader = createReader(partitionedFile)

      val fileReader = new PartitionReader[InternalRow] {
        override def next(): Boolean = reader.nextKeyValue()

        override def get(): InternalRow = reader.getCurrentValue.asInstanceOf[InternalRow]

        override def close(): Unit = reader.close()
      }

      new PartitionReaderWithPartitionValues(fileReader, requiredSchema,
        partitionSchema, partitionedFile.partitionValues)
    }

    def createReader(partitionedFile: PartitionedFile): RecordReader[Void, _ >: InternalRow <: AnyRef] = {

      val sharedConf = broadcastedConf.value.value
      val internalSchemaStr = sharedConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA)
      // For Spark DataSource v1, there's no Physical Plan projection/schema pruning w/in Spark itself,
      // therefore it's safe to do schema projection here
      if (!isNullOrEmpty(internalSchemaStr)) {
        val prunedInternalSchemaStr =
          pruneInternalSchema(internalSchemaStr, requiredSchema)
        sharedConf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, prunedInternalSchemaStr)
      }

      val filePath = new Path(new URI(partitionedFile.filePath))
      val split = new FileSplit(filePath, partitionedFile.start, partitionedFile.length, Array.empty[String])

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
      val hadoopAttemptConf = new Configuration(sharedConf)
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
            new VectorizedParquetRecordReader(
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
        vectorizedReader.initialize(split, hadoopAttemptContext)
        vectorizedReader
      } else {
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
        val baseReader = if (pushed.isDefined && enableRecordFilter) {
          val parquetFilter = FilterCompat.get(pushed.get, null)
          new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
        } else {
          new ParquetRecordReader[InternalRow](readSupport)
        }
        baseReader
      }
    }
  }
}
