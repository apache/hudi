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
import org.apache.hudi.cdc.{CDCFileGroupIterator, HoodieCDCFileGroupSplit, HoodieCDCFileIndex}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.{HoodieMemoryConfig, HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.HoodieSchemaUtils
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, ParquetTableSchemaResolver}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.exception.HoodieNotSupportedException
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.IOUtils
import org.apache.hudi.io.storage.HoodieSparkParquetReader.ENABLE_LOGICAL_TIMESTAMP_REPAIR
import org.apache.hudi.io.storage.VectorConversionUtils
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.schema.{HoodieSchemaRepair, MessageType}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeProjection}
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

import scala.collection.JavaConverters.{mapAsJavaMapConverter, setAsJavaSetConverter}

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
                                           val hoodieFileFormat: HoodieFileFormat,
                                           val isBlobDescriptorMode: Boolean = false)
  extends ParquetFileFormat with SparkAdapterSupport with HoodieFormatTrait with Logging with Serializable {

  private lazy val schema = tableSchema.schema

  private lazy val tableSchemaAsMessageType: HOption[MessageType] = {
    HOption.ofNullable(
      ParquetTableSchemaResolver.convertAvroSchemaToParquet(schema, new Configuration())
    )
  }

  private lazy val hasTimestampMillisFieldInTableSchema = HoodieSchemaRepair.hasTimestampMillisField(schema)
  private lazy val supportBatchWithTableSchema = HoodieSparkUtils.gteqSpark3_5 || !hasTimestampMillisFieldInTableSchema
  override def shortName(): String = "HudiFileGroup"

  override def toString: String = "HoodieFileGroupReaderBasedFileFormat"

  def getRequiredFilters: Seq[Filter] = requiredFilters

  private val sanitizedTableName = HoodieSchemaUtils.getRecordQualifiedName(tableName)

  /**
   * Flag saying whether vectorized reading is supported.
   */
  private var supportVectorizedRead = false

  /**
   * Flag saying whether batch output is supported.
   */
  private var supportReturningBatch = false

  /**
   * Cached result of vector column detection keyed by schema identity.
   * Avoids re-parsing metadata on repeated supportBatch / readBaseFile calls with the same schema.
   */
  @transient private var cachedVectorDetection: (StructType, Map[Int, HoodieSchema.Vector]) = _

  private def detectVectorColumnsCached(schema: StructType): Map[Int, HoodieSchema.Vector] = {
    if (cachedVectorDetection != null && (cachedVectorDetection._1 eq schema)) {
      cachedVectorDetection._2
    } else {
      val result = detectVectorColumns(schema)
      cachedVectorDetection = (schema, result)
      result
    }
  }

  @transient private var cachedBlobDetection: (StructType, Set[Int]) = _

  private def detectBlobColumnsCached(schema: StructType): Set[Int] = {
    if (cachedBlobDetection != null && (cachedBlobDetection._1 eq schema)) {
      cachedBlobDetection._2
    } else {
      import scala.collection.JavaConverters._
      val result = VectorConversionUtils.detectBlobColumnsFromMetadata(schema).asScala.map(_.intValue()).toSet
      cachedBlobDetection = (schema, result)
      result
    }
  }

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
    // Vector columns are stored as FIXED_LEN_BYTE_ARRAY in Parquet but read as ArrayType in Spark.
    // The binary→array conversion requires row-level access, so disable vectorized batch reading.
    if (detectVectorColumnsCached(schema).nonEmpty) {
      supportVectorizedRead = false
      supportReturningBatch = false
      false
    } else if (isBlobDescriptorMode && detectBlobColumnsCached(schema).nonEmpty) {
      // Blob DESCRIPTOR mode strips the data sub-field from blob structs and null-pads
      // post-read, which requires row-level access.
      supportVectorizedRead = false
      supportReturningBatch = false
      false
    } else {
      val conf = sparkSession.sessionState.conf
      val parquetBatchSupported = ParquetUtils.isBatchReadSupportedForSchema(conf, schema) && supportBatchWithTableSchema
      val orcBatchSupported = conf.orcVectorizedReaderEnabled &&
        schema.forall(s => OrcUtils.supportColumnarReads(
          s.dataType, sparkSession.sessionState.conf.orcVectorizedReaderNestedColumnEnabled))
      // TODO: Implement columnar batch reading https://github.com/apache/hudi/issues/17736
      val lanceBatchSupported = false

      val supportBatch = if (isMultipleBaseFileFormatsEnabled) {
        parquetBatchSupported && orcBatchSupported
      } else if (hoodieFileFormat == HoodieFileFormat.PARQUET) {
        parquetBatchSupported
      } else if (hoodieFileFormat == HoodieFileFormat.ORC) {
        orcBatchSupported
      } else if (hoodieFileFormat == HoodieFileFormat.LANCE) {
        lanceBatchSupported
      } else {
        throw new HoodieNotSupportedException("Unsupported file format: " + hoodieFileFormat)
      }
      supportVectorizedRead = !isIncremental && !isBootstrap && supportBatch
      supportReturningBatch = !isMOR && supportVectorizedRead
      logInfo(s"supportReturningBatch: $supportReturningBatch, supportVectorizedRead: $supportVectorizedRead, isIncremental: $isIncremental, " +
        s"isBootstrap: $isBootstrap, superSupportBatch: $supportBatch")
      supportReturningBatch
    }
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
                           path: Path): Boolean = {
    // NOTE: When we have and only the base file that needs to be read with normal reading mode,
    // we can consider the current format to be equivalent to `org.apache.spark.sql.execution.datasources.parquet.ParquetFormat`.
    // Naturally, we can maintain the same `isSplitable` logic as the upper-level format.
    // This will enable us to take advantage of spark's file splitting capability.
    // For overly large single files, we can use multiple concurrent tasks to read them, thereby reducing the overall job reading time consumption
    val superSplitable = super.isSplitable(sparkSession, options, path)
    val isLance = hoodieFileFormat == HoodieFileFormat.LANCE
    val splitable = !isMOR && !isIncremental && !isBootstrap && !isLance && superSplitable
    logInfo(s"isSplitable: $splitable, super.isSplitable: $superSplitable, isMOR: $isMOR, isIncremental: $isIncremental, isBootstrap: $isBootstrap")
    splitable
  }

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

    // Per-query set of blob columns that read_blob() materializes in the current query, written
    // into HadoopFsRelation.options by ReadBlobRule. Under DESCRIPTOR mode these columns keep their
    // `data` sub-field so the bytes flow through to BatchedBlobReader. The conf entry below makes
    // the same set visible to the MOR path (SparkFileFormatInternalRowReaderContext) at task time.
    val forceContentCols: Set[String] = options
      .get(HoodieReaderConfig.BLOB_INLINE_READ_FORCE_CONTENT_COLUMNS)
      .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)
    if (forceContentCols.nonEmpty) {
      augmentedStorageConf.set(
        HoodieReaderConfig.BLOB_INLINE_READ_FORCE_CONTENT_COLUMNS,
        forceContentCols.mkString(","))
    }
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
    val dataStructTypeWithMandatoryPartitionFields = StructType(dataStructType.fields ++ partitionSchema.fields.filter(f => mandatoryFields.contains(f.name)))
    val dataSchema = HoodieSchemaUtils.pruneDataSchema(schema, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(dataStructTypeWithMandatoryPartitionFields, sanitizedTableName), exclusionFields)

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
                requiredSchema, partitionSchema, outputSchema, filters ++ requiredFilters, storageConf, forceContentCols)
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          buildCDCRecordIterator(hoodiePartitionCDCFileGroupSliceMapping, fileGroupBaseFileReader.value, storageConf, fileIndexProps, requiredSchema, metaClient)

        case _ =>
          readBaseFile(file, baseFileReader.value, requestedStructType, remainingPartitionSchema, fixedPartitionIndexes,
            requiredSchema, partitionSchema, outputSchema, filters ++ requiredFilters, storageConf, forceContentCols)
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
      val parquetReader = sparkAdapter.createParquetFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration)
      val orcReader = sparkAdapter.createOrcFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration, dataSchema)
      val lanceReader = sparkAdapter.createLanceFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration).orNull
      new MultipleColumnarFileFormatReader(parquetReader, orcReader, lanceReader)
    } else if (hoodieFileFormat == HoodieFileFormat.PARQUET) {
      sparkAdapter.createParquetFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration)
    } else if (hoodieFileFormat == HoodieFileFormat.ORC) {
      sparkAdapter.createOrcFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration, dataSchema)
    } else if (hoodieFileFormat == HoodieFileFormat.LANCE) {
      sparkAdapter.createLanceFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration).orNull
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

  private def detectVectorColumns(schema: StructType): Map[Int, HoodieSchema.Vector] =
    SparkFileFormatInternalRowReaderContext.detectVectorColumnsFromMetadata(schema)

  private def replaceVectorFieldsWithBinary(schema: StructType, vectorCols: Map[Int, HoodieSchema.Vector]): StructType =
    SparkFileFormatInternalRowReaderContext.replaceVectorColumnsWithBinary(schema, vectorCols)

  /**
   * Detects vector columns and replaces them with BinaryType in one step.
   *
   * <p>The BinaryType rewrite is Parquet-specific: Hudi stores VECTOR columns as
   * FIXED_LEN_BYTE_ARRAY in Parquet, so the reader must see BinaryType and the raw
   * bytes are post-converted back to ArrayType. Other formats (e.g. Lance) encode
   * vectors natively as Arrow FixedSizeList and return ArrayType directly, so the
   * rewrite would introduce a spurious ArrayType→BinaryType cast during schema
   * evolution and break the read. Skip the rewrite for those formats.
   *
   * @return (modified schema with BinaryType for vectors, vector column ordinal map)
   */
  private def withVectorRewrite(schema: StructType): (StructType, Map[Int, HoodieSchema.Vector]) = {
    // Only Parquet needs the BinaryType rewrite; other formats (Lance) return ArrayType natively.
    if (hoodieFileFormat != HoodieFileFormat.PARQUET) {
      (schema, Map.empty[Int, HoodieSchema.Vector])
    } else {
      val vecs = detectVectorColumns(schema)
      if (vecs.isEmpty) {
        (schema, vecs)
      } else {
        (replaceVectorFieldsWithBinary(schema, vecs), vecs)
      }
    }
  }

  /**
   * Detects BLOB columns and strips the {@code data} sub-field when DESCRIPTOR mode is active.
   * Only applies to Parquet format; other formats handle DESCRIPTOR mode natively.
   *
   * @param forceContentCols Top-level column names that must keep their {@code data} sub-field
   *                         (i.e. the columns the current query reads via {@code read_blob()}).
   *                         These are excluded from the strip set so the bytes are materialized.
   */
  private def withBlobDescriptorRewrite(schema: StructType,
                                        forceContentCols: Set[String]): (StructType, Set[Int]) = {
    if (hoodieFileFormat != HoodieFileFormat.PARQUET) {
      (schema, Set.empty[Int])
    } else {
      import scala.collection.JavaConverters._
      val detected = VectorConversionUtils.detectBlobColumnsFromMetadata(schema).asScala.map(_.intValue()).toSet
      val toStrip = if (forceContentCols.isEmpty) detected
        else detected.filterNot(idx => forceContentCols.contains(schema.fields(idx).name))
      if (toStrip.isEmpty) {
        (schema, Set.empty[Int])
      } else {
        val javaBlobCols: java.util.Set[Integer] = toStrip.map(Integer.valueOf).asJava
        (VectorConversionUtils.stripBlobDataField(schema, javaBlobCols), toStrip)
      }
    }
  }

  /**
   * Wraps an iterator to re-insert null {@code data} fields into blob structs
   * after Parquet DESCRIPTOR mode read (expanding 2-field → 3-field structs).
   */
  private def wrapWithBlobNullPadding(iter: Iterator[InternalRow],
                                       readSchema: StructType,
                                       targetSchema: StructType,
                                       blobCols: Set[Int]): Iterator[InternalRow] = {
    val blobProjection = UnsafeProjection.create(targetSchema)
    val javaBlobCols: java.util.Set[Integer] = blobCols.map(Integer.valueOf).asJava
    val mapper = VectorConversionUtils.buildBlobNullPadRowMapper(readSchema, javaBlobCols, blobProjection.apply(_))
    iter.map(mapper.apply(_))
  }

  /**
   * Wraps an iterator to convert binary VECTOR columns back to typed arrays.
   * The read schema has BinaryType for vector columns; the target schema has ArrayType.
   */
  private def wrapWithVectorConversion(iter: Iterator[InternalRow],
                                        readSchema: StructType,
                                        targetSchema: StructType,
                                        vectorCols: Map[Int, HoodieSchema.Vector]): Iterator[InternalRow] = {
    val vectorProjection = UnsafeProjection.create(targetSchema)
    val javaVectorCols: java.util.Map[Integer, HoodieSchema.Vector] =
      vectorCols.map { case (k, v) => (Integer.valueOf(k), v) }.asJava
    val mapper = VectorConversionUtils.buildRowMapper(readSchema, javaVectorCols, vectorProjection.apply(_))
    iter.map(mapper.apply(_))
  }

  // executor
  private def readBaseFile(file: PartitionedFile, parquetFileReader: SparkColumnarFileReader, requestedSchema: StructType,
                           remainingPartitionSchema: StructType, fixedPartitionIndexes: Set[Int], requiredSchema: StructType,
                           partitionSchema: StructType, outputSchema: StructType, filters: Seq[Filter],
                           storageConf: StorageConfiguration[Configuration],
                           forceContentCols: Set[String]): Iterator[InternalRow] = {
    // Detect vector columns and create modified schemas with BinaryType.
    // Each schema is detected independently because ordinals are relative to the schema being
    // modified — outputSchema and requestedSchema may have vector columns at different positions
    // than requiredSchema (e.g. when partition columns are interleaved).
    val (modifiedRequiredSchema, vectorCols) = withVectorRewrite(requiredSchema)
    val hasVectors = vectorCols.nonEmpty
    val (modifiedOutputSchema, outputVectorCols) = if (hasVectors) withVectorRewrite(outputSchema) else (outputSchema, Map.empty[Int, HoodieSchema.Vector])
    val (modifiedRequestedSchema, _) = if (hasVectors) withVectorRewrite(requestedSchema) else (requestedSchema, Map.empty[Int, HoodieSchema.Vector])

    // Blob DESCRIPTOR mode: strip `data` sub-field from blob structs so Parquet skips
    // those column chunks entirely (real I/O savings). Applied after vector rewrite.
    // `forceContentCols` carries the columns referenced by read_blob() in the current query
    // (set by ReadBlobRule via HadoopFsRelation.options); those columns retain `data` so bytes
    // are materialized for read_blob().
    val (blobRequiredSchema, blobCols) = if (isBlobDescriptorMode) withBlobDescriptorRewrite(modifiedRequiredSchema, forceContentCols) else (modifiedRequiredSchema, Set.empty[Int])
    val hasBlobs = blobCols.nonEmpty
    val (blobOutputSchema, outputBlobCols) = if (hasBlobs) withBlobDescriptorRewrite(modifiedOutputSchema, forceContentCols) else (modifiedOutputSchema, Set.empty[Int])
    val (blobRequestedSchema, _) = if (hasBlobs) withBlobDescriptorRewrite(modifiedRequestedSchema, forceContentCols) else (modifiedRequestedSchema, Set.empty[Int])

    val rawIter = if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
      //none of partition fields are read from the file, so the reader will do the appending for us
      parquetFileReader.read(file, blobRequiredSchema, partitionSchema, internalSchemaOpt, filters, storageConf, tableSchemaAsMessageType)
    } else if (remainingPartitionSchema.fields.length == 0) {
      //we read all of the partition fields from the file
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      //we need to modify the partitioned file so that the partition values are empty
      val modifiedFile = pfileUtils.createPartitionedFile(InternalRow.empty, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      //and we pass an empty schema for the partition schema
      parquetFileReader.read(modifiedFile, blobOutputSchema, new StructType(), internalSchemaOpt, filters, storageConf, tableSchemaAsMessageType)
    } else {
      //need to do an additional projection here. The case in mind is that partition schema is "a,b,c" mandatoryFields is "a,c",
      //then we will read (dataSchema + a + c) and append b. So the final schema will be (data schema + a + c +b)
      //but expected output is (data schema + a + b + c)
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      val partitionValues = getFixedPartitionValues(file.partitionValues, partitionSchema, fixedPartitionIndexes)
      val modifiedFile = pfileUtils.createPartitionedFile(partitionValues, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      val iter = parquetFileReader.read(modifiedFile, blobRequestedSchema, remainingPartitionSchema, internalSchemaOpt, filters, storageConf, tableSchemaAsMessageType)
      projectIter(iter, StructType(blobRequestedSchema.fields ++ remainingPartitionSchema.fields), blobOutputSchema)
    }

    // Post-read: re-insert null `data` field into blob structs (expanding 2-field → 3-field)
    val blobPaddedIter = if (hasBlobs) {
      val readSchema = if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
        StructType(blobRequiredSchema.fields ++ partitionSchema.fields)
      } else {
        blobOutputSchema
      }
      val targetSchema = if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
        StructType(modifiedRequiredSchema.fields ++ partitionSchema.fields)
      } else {
        modifiedOutputSchema
      }
      wrapWithBlobNullPadding(rawIter, readSchema, targetSchema, outputBlobCols)
    } else {
      rawIter
    }

    if (hasVectors) {
      // The raw iterator has BinaryType for vector columns; convert back to ArrayType
      val readSchema = if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
        StructType(modifiedRequiredSchema.fields ++ partitionSchema.fields)
      } else {
        modifiedOutputSchema
      }
      wrapWithVectorConversion(blobPaddedIter, readSchema, outputSchema, outputVectorCols)
    } else {
      blobPaddedIter
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
