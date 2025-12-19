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

import org.apache.hudi.{HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieSchemaConversionUtils, HoodieSparkUtils, HoodieTableSchema, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.cdc.{CDCFileGroupIterator, HoodieCDCFileGroupSplit, HoodieCDCFileIndex}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.{HoodieMemoryConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaUtils}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, ParquetTableSchemaResolver}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.exception.HoodieNotSupportedException
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.IOUtils
import org.apache.hudi.io.storage.HoodieSparkParquetReader.ENABLE_LOGICAL_TIMESTAMP_REPAIR
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.schema.{AvroSchemaRepair, MessageType}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.hudi.MultipleColumnarFileFormatReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchUtils}
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable

import scala.collection.JavaConverters.mapAsJavaMapConverter

trait HoodieFormatTrait {

  // Used so that the planner only projects once and does not stack overflow
  var isProjected: Boolean = false
  def getRequiredFilters: Seq[Filter]
}

/**
 * This class utilizes {@link HoodieFileGroupReader} and its related classes to support reading
 * from Parquet or ORC formatted base files and their log files.
 */
class HoodieFileGroupReaderBasedFileFormat(tablePath: String,
                                           tableSchema: HoodieTableSchema,
                                           tableName: String,
                                           queryTimestamp: String,
                                           mandatoryFields: Seq[String],
                                           isMOR: Boolean,
                                           isBootstrap: Boolean,
                                           isIncremental: Boolean,
                                           validCommits: String,
                                           shouldUseRecordPosition: Boolean,
                                           requiredFilters: Seq[Filter],
                                           isMultipleBaseFileFormatsEnabled: Boolean,
                                           hoodieFileFormat: HoodieFileFormat)
  extends ParquetFileFormat with SparkAdapterSupport with HoodieFormatTrait with Logging with Serializable {

  private lazy val schema = tableSchema.schema

  private lazy val tableSchemaAsMessageType: HOption[MessageType] = {
    HOption.ofNullable(
      ParquetTableSchemaResolver.convertAvroSchemaToParquet(schema.getAvroSchema, new Configuration())
    )
  }

  private lazy val hasTimestampMillisFieldInTableSchema = AvroSchemaRepair.hasTimestampMillisField(schema.getAvroSchema)
  private lazy val supportBatchWithTableSchema = HoodieSparkUtils.gteqSpark3_5 || !hasTimestampMillisFieldInTableSchema
  override def shortName(): String = "HudiFileGroup"

  override def toString: String = "HoodieFileGroupReaderBasedFileFormat"

  def getRequiredFilters: Seq[Filter] = requiredFilters

  private val sanitizedTableName = AvroSchemaUtils.getAvroRecordQualifiedName(tableName)

  /**
   * Flag saying whether vectorized reading is supported.
   */
  private var supportVectorizedRead = false

  /**
   * Flag saying whether batch output is supported.
   */
  private var supportReturningBatch = false

  /**
   * Checks if the file format supports vectorized reading, please refer to SPARK-40918.
   *
   * NOTE: for mor read, even for file-slice with only base file, we can read parquet file with vectorized read,
   * but the return result of the whole data-source-scan phase cannot be batch,
   * because when there are any log file in a file slice, it needs to be read by the file group reader.
   * Since we are currently performing merges based on rows, the result returned by merging should be based on rows,
   * we cannot assume that all file slices have only base files.
   * So we need to set the batch result back to false.
   *
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    val parquetBatchSupported = ParquetUtils.isBatchReadSupportedForSchema(conf, schema) && supportBatchWithTableSchema
    val orcBatchSupported = conf.orcVectorizedReaderEnabled &&
      schema.forall(s => OrcUtils.supportColumnarReads(
        s.dataType, sparkSession.sessionState.conf.orcVectorizedReaderNestedColumnEnabled))

    val supportBatch = if (isMultipleBaseFileFormatsEnabled) {
      parquetBatchSupported && orcBatchSupported
    } else if (hoodieFileFormat == HoodieFileFormat.PARQUET) {
      parquetBatchSupported
    } else if (hoodieFileFormat == HoodieFileFormat.ORC) {
      orcBatchSupported
    } else {
      throw new HoodieNotSupportedException("Unsupported file format: " + hoodieFileFormat)
    }
    supportVectorizedRead = !isIncremental && !isBootstrap && supportBatch
    supportReturningBatch = !isMOR && supportVectorizedRead
    logInfo(s"supportReturningBatch: $supportReturningBatch, supportVectorizedRead: $supportVectorizedRead, isIncremental: $isIncremental, " +
      s"isBootstrap: $isBootstrap, superSupportBatch: $supportBatch")
    supportReturningBatch
  }

  //for partition columns that we read from the file, we don't want them to be constant column vectors so we
  //modify the vector types in this scenario
  override def vectorTypes(requiredSchema: StructType,
                           partitionSchema: StructType,
                           sqlConf: SQLConf): Option[Seq[String]] = {
    val originalVectorTypes = super.vectorTypes(requiredSchema, partitionSchema, sqlConf)
    if (mandatoryFields.isEmpty) {
      originalVectorTypes
    } else {
      val regularVectorType = if (!sqlConf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
      originalVectorTypes.map {
        o: Seq[String] => o.zipWithIndex.map(a => {
          if (a._2 >= requiredSchema.length && mandatoryFields.contains(partitionSchema.fields(a._2 - requiredSchema.length).name)) {
            regularVectorType
          } else {
            a._1
          }
        })
      }
    }
  }

  private lazy val internalSchemaOpt: HOption[InternalSchema] = if (tableSchema.internalSchema.isEmpty) {
    HOption.empty()
  } else {
    HOption.of(tableSchema.internalSchema.get)
  }

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = false

  override def buildReaderWithPartitionValues(spark: SparkSession,
                                              dataStructType: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val isCount = requiredSchema.isEmpty && !isMOR && !isIncremental
    val augmentedStorageConf = new HadoopStorageConfiguration(hadoopConf).getInline
    setSchemaEvolutionConfigs(augmentedStorageConf)
    augmentedStorageConf.set(ENABLE_LOGICAL_TIMESTAMP_REPAIR, hasTimestampMillisFieldInTableSchema.toString)
    val (remainingPartitionSchemaArr, fixedPartitionIndexesArr) = partitionSchema.fields.toSeq.zipWithIndex.filter(p => !mandatoryFields.contains(p._1.name)).unzip

    // The schema of the partition cols we want to append the value instead of reading from the file
    val remainingPartitionSchema = StructType(remainingPartitionSchemaArr)

    // index positions of the remainingPartitionSchema fields in partitionSchema
    val fixedPartitionIndexes = fixedPartitionIndexesArr.toSet

    // schema that we want fg reader to output to us
    val exclusionFields = new java.util.HashSet[String]()
    exclusionFields.add("op")
    partitionSchema.fields.foreach(f => exclusionFields.add(f.name))
    val requestedStructType = StructType(requiredSchema.fields ++ partitionSchema.fields.filter(f => mandatoryFields.contains(f.name)))
    val requestedSchema = HoodieSchemaUtils.pruneDataSchema(schema, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(requestedStructType, sanitizedTableName), exclusionFields)
    val dataSchema = HoodieSchemaUtils.pruneDataSchema(schema, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(dataStructType, sanitizedTableName), exclusionFields)

    spark.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", supportVectorizedRead.toString)

    val baseFileReader = spark.sparkContext.broadcast(buildBaseFileReader(spark, options, augmentedStorageConf.unwrap(), dataStructType, supportVectorizedRead))
    val fileGroupBaseFileReader = if (isMOR && supportVectorizedRead) {
      // for file group reader to perform read, we always need to read the record without vectorized reader because our merging is based on row level.
      // TODO: please consider to support vectorized reader in file group reader
      spark.sparkContext.broadcast(buildBaseFileReader(spark, options, augmentedStorageConf.unwrap(), dataStructType, enableVectorizedRead = false))
    } else {
      baseFileReader
    }

    val broadcastedStorageConf = spark.sparkContext.broadcast(new SerializableConfiguration(augmentedStorageConf.unwrap()))
    val fileIndexProps: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options, null)

    val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(engineContext.getTaskContextSupplier, options.asJava)

    // Create metaclient on driver to avoid expensive operations on executors
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
      .builder().setConf(augmentedStorageConf).setBasePath(tablePath).build

    (file: PartitionedFile) => {
      // executor
      val storageConf = new HadoopStorageConfiguration(broadcastedStorageConf.value.value)
      val iter = file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val fileGroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
            .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
          fileSliceMapping.getSlice(fileGroupName) match {
            case Some(fileSlice) if !isCount && (requiredSchema.nonEmpty || fileSlice.getLogFiles.findAny().isPresent) =>
              val readerContext = new SparkFileFormatInternalRowReaderContext(fileGroupBaseFileReader.value, filters, requiredFilters, storageConf, metaClient.getTableConfig)
              readerContext.setEnableLogicalTimestampFieldRepair(storageConf.getBoolean(ENABLE_LOGICAL_TIMESTAMP_REPAIR, true))
              val props = metaClient.getTableConfig.getProps
              options.foreach(kv => props.setProperty(kv._1, kv._2))
              props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction))
              val baseFileLength = if (fileSlice.getBaseFile.isPresent) {
                fileSlice.getBaseFile.get.getFileSize
              } else {
                0
              }
              val reader = HoodieFileGroupReader.newBuilder()
                .withReaderContext(readerContext)
                .withHoodieTableMetaClient(metaClient)
                .withLatestCommitTime(queryTimestamp)
                .withFileSlice(fileSlice)
                .withDataSchema(dataSchema)
                .withRequestedSchema(requestedSchema)
                .withInternalSchema(internalSchemaOpt)
                .withProps(props)
                .withStart(file.start)
                .withLength(baseFileLength)
                .withShouldUseRecordPosition(shouldUseRecordPosition)
                .build()
              // Append partition values to rows and project to output schema
              appendPartitionAndProject(
                reader.getClosableIterator,
                requestedStructType,
                remainingPartitionSchema,
                outputSchema,
                fileSliceMapping.getPartitionValues,
                fixedPartitionIndexes)

            case _ =>
              readBaseFile(file, baseFileReader.value, requestedStructType, remainingPartitionSchema, fixedPartitionIndexes,
                requiredSchema, partitionSchema, outputSchema, filters ++ requiredFilters, storageConf)
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          buildCDCRecordIterator(hoodiePartitionCDCFileGroupSliceMapping, fileGroupBaseFileReader.value, storageConf, fileIndexProps, requiredSchema, metaClient)

        case _ =>
          readBaseFile(file, baseFileReader.value, requestedStructType, remainingPartitionSchema, fixedPartitionIndexes,
            requiredSchema, partitionSchema, outputSchema, filters ++ requiredFilters, storageConf)
      }
      CloseableIteratorListener.addListener(iter)
    }
  }

  private def buildBaseFileReader(spark: SparkSession,
                                  options: Map[String, String],
                                  configuration: Configuration,
                                  dataSchema: StructType,
                                  enableVectorizedRead: Boolean): SparkColumnarFileReader = {
    if (isMultipleBaseFileFormatsEnabled) {
      new MultipleColumnarFileFormatReader(
        sparkAdapter.createParquetFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration),
        sparkAdapter.createOrcFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration, dataSchema))
    } else if (hoodieFileFormat == HoodieFileFormat.PARQUET) {
      sparkAdapter.createParquetFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration)
    } else if (hoodieFileFormat == HoodieFileFormat.ORC) {
      sparkAdapter.createOrcFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration, dataSchema)
    } else {
      throw new HoodieNotSupportedException("Unsupported file format: " + hoodieFileFormat)
    }
  }

  private def setSchemaEvolutionConfigs(conf: StorageConfiguration[Configuration]): Unit = {
    if (internalSchemaOpt.isPresent) {
      conf.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, tablePath)
      conf.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
    }
  }

  private def buildCDCRecordIterator(hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping,
                                     baseFileReader: SparkColumnarFileReader,
                                     storageConf: StorageConfiguration[Configuration],
                                     props: TypedProperties,
                                     requiredSchema: StructType,
                                     metaClient: HoodieTableMetaClient): Iterator[InternalRow] = {
    val fileSplits = hoodiePartitionCDCFileGroupSliceMapping.getFileSplits().toArray
    val cdcFileGroupSplit: HoodieCDCFileGroupSplit = HoodieCDCFileGroupSplit(fileSplits)
    props.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, tableName)
    val cdcSchema = HoodieCDCFileIndex.FULL_CDC_SPARK_SCHEMA
    new CDCFileGroupIterator(
      cdcFileGroupSplit,
      metaClient,
      storageConf,
      baseFileReader,
      tableSchema,
      cdcSchema,
      requiredSchema,
      props)
  }

  private def appendPartitionAndProject(iter: ClosableIterator[InternalRow],
                                        inputSchema: StructType,
                                        partitionSchema: StructType,
                                        to: StructType,
                                        partitionValues: InternalRow,
                                        fixedPartitionIndexes: Set[Int]): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      //'inputSchema' and 'to' should be the same so the projection will just be an identity func
      projectSchema(iter, inputSchema, to)
    } else {
      val fixedPartitionValues = if (partitionSchema.length == partitionValues.numFields) {
        //need to append all of the partition fields
        partitionValues
      } else {
        //some partition fields read from file, some were not
        getFixedPartitionValues(partitionValues, partitionSchema, fixedPartitionIndexes)
      }
      val unsafeProjection = generateUnsafeProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(joinedRow(d, fixedPartitionValues)))
    }
  }

  private def projectSchema(iter: ClosableIterator[InternalRow],
                            from: StructType,
                            to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(d))
  }

  private def makeCloseableFileGroupMappingRecordIterator(closeableFileGroupRecordIterator: ClosableIterator[InternalRow],
                                                          mappingFunction: Function[InternalRow, InternalRow]): Iterator[InternalRow] = {
    CloseableIteratorListener.addListener(closeableFileGroupRecordIterator)
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = mappingFunction(closeableFileGroupRecordIterator.next())

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }

  // executor
  private def readBaseFile(file: PartitionedFile, parquetFileReader: SparkColumnarFileReader, requestedSchema: StructType,
                           remainingPartitionSchema: StructType, fixedPartitionIndexes: Set[Int], requiredSchema: StructType,
                           partitionSchema: StructType, outputSchema: StructType, filters: Seq[Filter],
                           storageConf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {
    if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
      //none of partition fields are read from the file, so the reader will do the appending for us
      parquetFileReader.read(file, requiredSchema, partitionSchema, internalSchemaOpt, filters, storageConf, tableSchemaAsMessageType)
    } else if (remainingPartitionSchema.fields.length == 0) {
      //we read all of the partition fields from the file
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      //we need to modify the partitioned file so that the partition values are empty
      val modifiedFile = pfileUtils.createPartitionedFile(InternalRow.empty, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      //and we pass an empty schema for the partition schema
      parquetFileReader.read(modifiedFile, outputSchema, new StructType(), internalSchemaOpt, filters, storageConf, tableSchemaAsMessageType)
    } else {
      //need to do an additional projection here. The case in mind is that partition schema is "a,b,c" mandatoryFields is "a,c",
      //then we will read (dataSchema + a + c) and append b. So the final schema will be (data schema + a + c +b)
      //but expected output is (data schema + a + b + c)
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      val partitionValues = getFixedPartitionValues(file.partitionValues, partitionSchema, fixedPartitionIndexes)
      val modifiedFile = pfileUtils.createPartitionedFile(partitionValues, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      val iter = parquetFileReader.read(modifiedFile, requestedSchema, remainingPartitionSchema, internalSchemaOpt, filters, storageConf, tableSchemaAsMessageType)
      projectIter(iter, StructType(requestedSchema.fields ++ remainingPartitionSchema.fields), outputSchema)
    }
  }

  private def projectIter(iter: Iterator[Any], from: StructType, to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    val batchProjection = ColumnarBatchUtils.generateProjection(from, to)
    iter.map {
      case ir: InternalRow => unsafeProjection(ir)
      case cb: ColumnarBatch => batchProjection(cb)
    }.asInstanceOf[Iterator[InternalRow]]
  }

  private def getFixedPartitionValues(allPartitionValues: InternalRow, partitionSchema: StructType, fixedPartitionIndexes: Set[Int]): InternalRow = {
    InternalRow.fromSeq(allPartitionValues.toSeq(partitionSchema).zipWithIndex.filter(p => fixedPartitionIndexes.contains(p._2)).map(p => p._1))
  }

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    if (isMultipleBaseFileFormatsEnabled || hoodieFileFormat == HoodieFileFormat.PARQUET) {
      ParquetUtils.inferSchema(sparkSession, options, files)
    } else {
      OrcUtils.inferSchema(sparkSession, files, options)
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    throw new HoodieNotSupportedException("HoodieFileGroupReaderBasedFileFormat does not support writing")
  }
}
