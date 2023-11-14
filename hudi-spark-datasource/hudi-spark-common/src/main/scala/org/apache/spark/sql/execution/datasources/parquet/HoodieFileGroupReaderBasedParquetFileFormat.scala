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

import kotlin.NotImplementedError
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.MergeOnReadSnapshotRelation.createPartitionedFile
import org.apache.hudi.cdc.{CDCFileGroupIterator, CDCRelation, HoodieCDCFileGroupSplit}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieFileGroupId, HoodieLogFile, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.{HoodieBaseRelation, HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieSparkUtils, HoodieTableSchema, HoodieTableState, MergeOnReadSnapshotRelation, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/**
 * This class utilizes {@link HoodieFileGroupReader} and its related classes to support reading
 * from Parquet formatted base files and their log files.
 */
class HoodieFileGroupReaderBasedParquetFileFormat(tableState: HoodieTableState,
                                                  tableSchema: HoodieTableSchema,
                                                  tableName: String,
                                                  mergeType: String,
                                                  mandatoryFields: Seq[String],
                                                  isMOR: Boolean,
                                                  isBootstrap: Boolean,
                                                  isIncremental: Boolean,
                                                  shouldUseRecordPosition: Boolean,
                                                  requiredFilters: Seq[Filter]
                                           ) extends ParquetFileFormat with SparkAdapterSupport {
  var isProjected = false

  /**
   * Support batch needs to remain consistent, even if one side of a bootstrap merge can support
   * while the other side can't
   */
  private var supportBatchCalled = false
  private var supportBatchResult = false

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    if (!supportBatchCalled) {
      supportBatchCalled = true
      supportBatchResult = !isMOR && !isIncremental && super.supportBatch(sparkSession, schema)
    }
    supportBatchResult
  }

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = false

  override def buildReaderWithPartitionValues(spark: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", supportBatchResult)
    val requiredSchemaWithMandatory = generateRequiredSchemaWithMandatory(requiredSchema, dataSchema, partitionSchema)
    val requiredSchemaSplits = requiredSchemaWithMandatory.fields.partition(f => HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name))
    val requiredMeta = StructType(requiredSchemaSplits._1)
    val requiredWithoutMeta = StructType(requiredSchemaSplits._2)
    val augmentedHadoopConf = FSUtils.buildInlineConf(hadoopConf)
    val (baseFileReader, preMergeBaseFileReader, _, _) = buildFileReaders(
      spark, dataSchema, partitionSchema, if (isIncremental) requiredSchemaWithMandatory else requiredSchema,
      filters, options, augmentedHadoopConf, requiredSchemaWithMandatory, requiredWithoutMeta, requiredMeta)
    val broadcastedHadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(augmentedHadoopConf))
    val props: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options)

    (file: PartitionedFile) => {
      file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val filePath = sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file)
          if (FSUtils.isLogFile(filePath)) {
            val partitionValues = fileSliceMapping.getPartitionValues
            val fileSlice = fileSliceMapping.getSlice(FSUtils.getFileId(filePath.getName).substring(1)).get
            buildFileGroupIterator(
              Option.empty[PartitionedFile => Iterator[InternalRow]],
              partitionValues,
              Option.empty[HoodieBaseFile],
              getLogFilesFromSlice(fileSlice),
              requiredSchemaWithMandatory,
              outputSchema,
              partitionSchema,
              broadcastedHadoopConf.value.value,
              -1,
              -1,
              shouldUseRecordPosition
            )
          } else {
            fileSliceMapping.getSlice(FSUtils.getFileId(filePath.getName)) match {
              case Some(fileSlice) =>
                val hoodieBaseFile = fileSlice.getBaseFile.get()
                val bootstrapFileOpt = hoodieBaseFile.getBootstrapBaseFile
                val partitionValues = fileSliceMapping.getPartitionValues
                val logFiles = getLogFilesFromSlice(fileSlice)
                if (requiredSchemaWithMandatory.isEmpty) {
                  val baseFile = createPartitionedFile(partitionValues, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen)
                  baseFileReader(baseFile)
                } else if (bootstrapFileOpt.isPresent) {
                  // TODO: Use FileGroupReader here: HUDI-6942.
                  throw new NotImplementedError("Not support reading bootstrap file")
                } else {
                  if (logFiles.isEmpty) {
                    throw new IllegalStateException(
                      "should not be here since file slice should not have been broadcasted "
                        + "since it has no log or data files")
                  }
                  buildFileGroupIterator(
                    Option(preMergeBaseFileReader),
                    partitionValues,
                    Option(hoodieBaseFile),
                    logFiles,
                    requiredSchemaWithMandatory,
                    outputSchema,
                    partitionSchema,
                    broadcastedHadoopConf.value.value,
                    0,
                    hoodieBaseFile.getFileLen,
                    shouldUseRecordPosition
                  )
                }
              // TODO: Use FileGroupReader here: HUDI-6942.
              case _ => baseFileReader(file)
            }
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          val filePath: Path = sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file)
          val fileGroupId: HoodieFileGroupId = new HoodieFileGroupId(filePath.getParent.toString, filePath.getName)
          val fileSplits = hoodiePartitionCDCFileGroupSliceMapping.getFileSplitsFor(fileGroupId).get.toArray
          val fileGroupSplit: HoodieCDCFileGroupSplit = HoodieCDCFileGroupSplit(fileSplits)
          buildCDCRecordIterator(fileGroupSplit, preMergeBaseFileReader, broadcastedHadoopConf.value.value, requiredSchema, props)
        // TODO: Use FileGroupReader here: HUDI-6942.
        case _ => baseFileReader(file)
      }
    }
  }

  protected def buildCDCRecordIterator(cdcFileGroupSplit: HoodieCDCFileGroupSplit,
                                       preMergeBaseFileReader: PartitionedFile => Iterator[InternalRow],
                                       hadoopConf: Configuration,
                                       requiredSchema: StructType,
                                       props: TypedProperties): Iterator[InternalRow] = {
    val metaClient = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, tableState.tablePath, props)
    val cdcSchema = CDCRelation.FULL_CDC_SPARK_SCHEMA
    new CDCFileGroupIterator(
      cdcFileGroupSplit,
      metaClient,
      hadoopConf,
      preMergeBaseFileReader,
      tableSchema,
      cdcSchema,
      requiredSchema,
      props)
  }

  protected def buildFileGroupIterator(preMergeBaseFileReader: Option[PartitionedFile => Iterator[InternalRow]],
                                       partitionValues: InternalRow,
                                       baseFile: Option[HoodieBaseFile],
                                       logFiles: List[HoodieLogFile],
                                       requiredSchemaWithMandatory: StructType,
                                       outputSchema: StructType,
                                       partitionSchema: StructType,
                                       hadoopConf: Configuration,
                                       start: Long,
                                       length: Long,
                                       shouldUseRecordPosition: Boolean): Iterator[InternalRow] = {
    val readerContext: HoodieReaderContext[InternalRow] = new SparkFileFormatInternalRowReaderContext(
      preMergeBaseFileReader, partitionValues)
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
      .builder().setConf(hadoopConf).setBasePath(tableState.tablePath).build
    val reader = new HoodieFileGroupReader[InternalRow](
      readerContext,
      hadoopConf,
      tableState.tablePath,
      tableState.latestCommitTimestamp.get,
      if (baseFile.nonEmpty) HOption.of(baseFile.get) else HOption.empty(),
      HOption.of(logFiles.map(f => f.getPath.toString).asJava),
      HoodieBaseRelation.convertToAvroSchema(requiredSchemaWithMandatory, tableName),
      metaClient.getTableConfig.getProps,
      start,
      length,
      shouldUseRecordPosition)
    reader.initRecordIterators()
    // Append partition values to rows and project to output schema
    appendPartitionAndProject(
      reader.getClosableIterator.asInstanceOf[java.util.Iterator[InternalRow]].asScala,
      requiredSchemaWithMandatory,
      partitionSchema,
      outputSchema,
      partitionValues)
  }

  private def appendPartitionAndProject(iter: Iterator[InternalRow],
                                        inputSchema: StructType,
                                        partitionSchema: StructType,
                                        to: StructType,
                                        partitionValues: InternalRow): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      projectSchema(iter, inputSchema, to)
    } else {
      val unsafeProjection = generateUnsafeProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      iter.map(d => unsafeProjection(joinedRow(d, partitionValues)))
    }
  }

  private def projectSchema(iter: Iterator[InternalRow],
                            from: StructType,
                            to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    iter.map(d => unsafeProjection(d))
  }

  private def generateRequiredSchemaWithMandatory(requiredSchema: StructType,
                                                  dataSchema: StructType,
                                                  partitionSchema: StructType): StructType = {
    // Helper method to get the StructField for nested fields
    @tailrec
    def findNestedField(schema: StructType, fieldParts: Array[String]): Option[StructField] = {
      fieldParts.toList match {
        case head :: Nil => schema.fields.find(_.name == head) // If it's the last part, find and return the field
        case head :: tail => // If there are more parts, find the field and its nested fields
          schema.fields.find(_.name == head) match {
            case Some(StructField(_, nested: StructType, _, _)) => findNestedField(nested, tail.toArray)
            case _ => None // The path is not valid
          }
        case _ => None // Empty path, should not happen if the input is correct
      }
    }

    // If not MergeOnRead or if projection is compatible
    if (isIncremental) {
      StructType(dataSchema.toArray ++ partitionSchema.fields)
    } else if (!isMOR || MergeOnReadSnapshotRelation.isProjectionCompatible(tableState)) {
      val added: mutable.Buffer[StructField] = mutable.Buffer[StructField]()
      for (field <- mandatoryFields) {
        if (requiredSchema.getFieldIndex(field).isEmpty) {
          // Support for nested fields
          val fieldParts = field.split("\\.")
          val fieldToAdd = findNestedField(dataSchema, fieldParts)
            .orElse(findNestedField(partitionSchema, fieldParts))
            .getOrElse(throw new IllegalArgumentException(s"Field $field does not exist in the table schema"))
          added.append(fieldToAdd)
        }
      }
      val addedFields = StructType(added.toArray)
      StructType(requiredSchema.toArray ++ addedFields.fields)
    } else {
      dataSchema
    }
  }

  protected def buildFileReaders(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType,
                                 requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String],
                                 hadoopConf: Configuration, requiredSchemaWithMandatory: StructType,
                                 requiredWithoutMeta: StructType, requiredMeta: StructType):
  (PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow]) = {

    val recordKeyRelatedFilters = getRecordKeyRelatedFilters(filters, tableState.recordKeyField)
    val baseFileReader = super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema,
      filters ++ requiredFilters, options, new Configuration(hadoopConf))

    //file reader for reading a hudi base file that needs to be merged with log files
    val preMergeBaseFileReader = if (isMOR) {
      // Add support for reading files using inline file system.
      super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchemaWithMandatory,
        if (shouldUseRecordPosition) requiredFilters else recordKeyRelatedFilters ++ requiredFilters,
        options, new Configuration(hadoopConf))
    } else {
      _: PartitionedFile => Iterator.empty
    }

    //Rules for appending partitions and filtering in the bootstrap readers:
    // 1. if it is mor, we don't want to filter data or append partitions
    // 2. if we need to merge the bootstrap base and skeleton files then we cannot filter
    // 3. if we need to merge the bootstrap base and skeleton files then we should never append partitions to the
    //    skeleton reader
    val needMetaCols = requiredMeta.nonEmpty
    val needDataCols = requiredWithoutMeta.nonEmpty

    //file reader for bootstrap skeleton files
    val skeletonReader = if (needMetaCols && isBootstrap) {
      if (needDataCols || isMOR) {
        // no filter and no append
        super.buildReaderWithPartitionValues(sparkSession, HoodieSparkUtils.getMetaSchema, StructType(Seq.empty),
          requiredMeta, Seq.empty, options, new Configuration(hadoopConf))
      } else {
        // filter and append
        super.buildReaderWithPartitionValues(sparkSession, HoodieSparkUtils.getMetaSchema, partitionSchema,
          requiredMeta, filters ++ requiredFilters, options, new Configuration(hadoopConf))
      }
    } else {
      _: PartitionedFile => Iterator.empty
    }

    //file reader for bootstrap base files
    val bootstrapBaseReader = if (needDataCols && isBootstrap) {
      val dataSchemaWithoutMeta = StructType(dataSchema.fields.filterNot(sf => isMetaField(sf.name)))
      if (isMOR) {
        // no filter and no append
        super.buildReaderWithPartitionValues(sparkSession, dataSchemaWithoutMeta, StructType(Seq.empty), requiredWithoutMeta,
          Seq.empty, options, new Configuration(hadoopConf))
      } else if (needMetaCols) {
        // no filter but append
        super.buildReaderWithPartitionValues(sparkSession, dataSchemaWithoutMeta, partitionSchema, requiredWithoutMeta,
          Seq.empty, options, new Configuration(hadoopConf))
      } else {
        // filter and append
        super.buildReaderWithPartitionValues(sparkSession, dataSchemaWithoutMeta, partitionSchema, requiredWithoutMeta,
          filters ++ requiredFilters, options, new Configuration(hadoopConf))
      }
    } else {
      _: PartitionedFile => Iterator.empty
    }

    (baseFileReader, preMergeBaseFileReader, skeletonReader, bootstrapBaseReader)
  }

  protected def getRecordKeyRelatedFilters(filters: Seq[Filter], recordKeyColumn: String): Seq[Filter] = {
    filters.filter(f => f.references.exists(c => c.equalsIgnoreCase(recordKeyColumn)))
  }

  protected def getLogFilesFromSlice(fileSlice: FileSlice): List[HoodieLogFile] = {
    fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList
  }
}
