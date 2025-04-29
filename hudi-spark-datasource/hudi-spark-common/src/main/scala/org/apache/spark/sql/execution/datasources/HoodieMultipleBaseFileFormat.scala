/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources

import org.apache.hudi.{HoodieBaseRelation, HoodiePartitionFileSliceMapping, HoodieTableSchema, HoodieTableState, LogFileIterator, MergeOnReadSnapshotRelation, RecordMergingFileIterator, SparkAdapterSupport}
import org.apache.hudi.DataSourceReadOptions.{REALTIME_PAYLOAD_COMBINE_OPT_VAL, REALTIME_SKIP_MERGE_OPT_VAL}
import org.apache.hudi.MergeOnReadSnapshotRelation.createPartitionedFile
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * File format that supports reading multiple base file formats in a table.
 */
class HoodieMultipleBaseFileFormat(tableState: Broadcast[HoodieTableState],
                                   tableSchema: Broadcast[HoodieTableSchema],
                                   tableName: String,
                                   mergeType: String,
                                   mandatoryFields: Seq[String],
                                   isMOR: Boolean,
                                   isIncremental: Boolean,
                                   requiredFilters: Seq[Filter]
                                  ) extends FileFormat with SparkAdapterSupport with Serializable {
  private val parquetFormat = new ParquetFileFormat()
  private val orcFormat = new OrcFileFormat()

  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    // This is a simple heuristic assuming all files have the same extension.
    val fileFormat = detectFileFormat(files.head.getPath.toString)

    fileFormat match {
      case "parquet" => parquetFormat.inferSchema(sparkSession, options, files)
      case "orc" => orcFormat.inferSchema(sparkSession, options, files)
      case _ => throw new UnsupportedOperationException(s"File format $fileFormat is not supported.")
    }
  }

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    false
  }

  // Used so that the planner only projects once and does not stack overflow
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
      supportBatchResult =
        !isMOR && !isIncremental && parquetFormat.supportBatch(sparkSession, schema) && orcFormat.supportBatch(sparkSession, schema)
    }
    supportBatchResult
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write operations are not supported in this example.")
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val requiredSchemaWithMandatory = if (isIncremental) {
      StructType(dataSchema.toArray ++ partitionSchema.fields)
    } else if (!isMOR || MergeOnReadSnapshotRelation.isProjectionCompatible(tableState.value)) {
      // add mandatory fields to required schema
      val added: mutable.Buffer[StructField] = mutable.Buffer[StructField]()
      for (field <- mandatoryFields) {
        if (requiredSchema.getFieldIndex(field).isEmpty) {
          val fieldToAdd = dataSchema.fields(dataSchema.getFieldIndex(field).get)
          added.append(fieldToAdd)
        }
      }
      val addedFields = StructType(added.toArray)
      StructType(requiredSchema.toArray ++ addedFields.fields)
    } else {
      dataSchema
    }

    val (parquetBaseFileReader, orcBaseFileReader, preMergeParquetBaseFileReader, preMergeOrcBaseFileReader) = buildFileReaders(
      sparkSession, dataSchema, partitionSchema, if (isIncremental) requiredSchemaWithMandatory else requiredSchema,
      filters, options, hadoopConf, requiredSchemaWithMandatory)

    val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (file: PartitionedFile) => {
      val filePath = sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file)
      val fileFormat = detectFileFormat(filePath.toString)
      val iter = file.partitionValues match {
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          if (FSUtils.isLogFile(filePath)) {
            // no base file
            val fileSlice = fileSliceMapping.getSlice(FSUtils.getFileId(filePath.getName).substring(1)).get
            val logFiles = getLogFilesFromSlice(fileSlice)
            val outputAvroSchema = HoodieBaseRelation.convertToAvroSchema(outputSchema, tableName)
            new LogFileIterator(logFiles, filePath.getParent, tableSchema.value, outputSchema, outputAvroSchema,
              tableState.value, broadcastedHadoopConf.value.value)
          } else {
            // We do not broadcast the slice if it has no log files
            fileSliceMapping.getSlice(FSUtils.getFileId(filePath.getName)) match {
              case Some(fileSlice) =>
                val hoodieBaseFile = fileSlice.getBaseFile.get()
                val baseFileFormat = detectFileFormat(hoodieBaseFile.getFileName)
                val partitionValues = fileSliceMapping.getPartitionValues
                val logFiles = getLogFilesFromSlice(fileSlice)
                if (requiredSchemaWithMandatory.isEmpty) {
                  val baseFile = createPartitionedFile(partitionValues, hoodieBaseFile.getStoragePath, 0, hoodieBaseFile.getFileLen)
                  baseFileFormat match {
                    case "parquet" => parquetBaseFileReader(baseFile)
                    case "orc" => orcBaseFileReader(baseFile)
                    case _ => throw new UnsupportedOperationException(s"Base file format $baseFileFormat is not supported.")
                  }
                } else {
                  if (logFiles.nonEmpty) {
                    val baseFile = createPartitionedFile(InternalRow.empty, hoodieBaseFile.getStoragePath, 0, hoodieBaseFile.getFileLen)
                    buildMergeOnReadIterator(
                      baseFileFormat match {
                        case "parquet" => preMergeParquetBaseFileReader(baseFile)
                        case "orc" => preMergeOrcBaseFileReader(baseFile)
                        case _ => throw new UnsupportedOperationException(s"Base file format $baseFileFormat is not supported.")
                      },
                      logFiles,
                      filePath.getParent,
                      requiredSchemaWithMandatory,
                      requiredSchemaWithMandatory,
                      outputSchema,
                      partitionSchema,
                      partitionValues,
                      broadcastedHadoopConf.value.value)
                  } else {
                    throw new IllegalStateException("should not be here since file slice should not have been broadcasted since it has no log or base files")
                  }
                }
              case _ => fileFormat match {
                case "parquet" => parquetBaseFileReader(file)
                case "orc" => orcBaseFileReader(file)
                case _ => throw new UnsupportedOperationException(s"Base file format $fileFormat is not supported.")
              }
            }
          }
        case _ => fileFormat match {
          case "parquet" => parquetBaseFileReader(file)
          case "orc" => orcBaseFileReader(file)
          case _ => throw new UnsupportedOperationException(s"Base file format $fileFormat is not supported.")
        }
      }
      CloseableIteratorListener.addListener(iter)
    }
  }

  /**
   * Build file readers to read individual physical files
   */
  protected def buildFileReaders(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType,
                                 requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String],
                                 hadoopConf: Configuration, requiredSchemaWithMandatory: StructType):
  (PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow]) = {
    val parquetBaseFileReader = parquetFormat.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema,
      filters ++ requiredFilters, options, new Configuration(hadoopConf))
    val orcBaseFileReader = orcFormat.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema,
      filters ++ requiredFilters, options, new Configuration(hadoopConf))

    val preMergeParquetBaseFileReader = if (isMOR) {
      parquetFormat.buildReaderWithPartitionValues(sparkSession, dataSchema, StructType(Seq.empty),
        requiredSchemaWithMandatory, requiredFilters, options, new Configuration(hadoopConf))
    } else {
      _: PartitionedFile => Iterator.empty
    }

    val preMergeOrcBaseFileReader = if (isMOR) {
      orcFormat.buildReaderWithPartitionValues(sparkSession, dataSchema, StructType(Seq.empty),
        requiredSchemaWithMandatory, requiredFilters, options, new Configuration(hadoopConf))
    } else {
      _: PartitionedFile => Iterator.empty
    }

    (parquetBaseFileReader, orcBaseFileReader, preMergeParquetBaseFileReader, preMergeOrcBaseFileReader)
  }

  /**
   * Create iterator for a file slice that has log files
   */
  protected def buildMergeOnReadIterator(iter: Iterator[InternalRow], logFiles: List[HoodieLogFile],
                                         partitionPath: StoragePath, inputSchema: StructType, requiredSchemaWithMandatory: StructType,
                                         outputSchema: StructType, partitionSchema: StructType, partitionValues: InternalRow,
                                         hadoopConf: Configuration): Iterator[InternalRow] = {

    val requiredAvroSchema = HoodieBaseRelation.convertToAvroSchema(requiredSchemaWithMandatory, tableName)
    val morIterator = mergeType match {
      case REALTIME_SKIP_MERGE_OPT_VAL => throw new UnsupportedOperationException("Skip merge is not currently " +
        "implemented for the New Hudi Parquet File format")
      //new SkipMergeIterator(logFiles, partitionPath, iter, inputSchema, tableSchema.value,
      //  requiredSchemaWithMandatory, requiredAvroSchema, tableState.value, hadoopConf)
      case REALTIME_PAYLOAD_COMBINE_OPT_VAL =>
        new RecordMergingFileIterator(logFiles, partitionPath, iter, inputSchema, tableSchema.value,
          requiredSchemaWithMandatory, requiredAvroSchema, tableState.value, hadoopConf)
    }
    appendPartitionAndProject(morIterator, requiredSchemaWithMandatory, partitionSchema,
      outputSchema, partitionValues)
  }

  /**
   * Append partition values to rows and project to output schema
   */
  protected def appendPartitionAndProject(iter: Iterator[InternalRow],
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

  protected def projectSchema(iter: Iterator[InternalRow],
                              from: StructType,
                              to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    iter.map(d => unsafeProjection(d))
  }

  protected def getLogFilesFromSlice(fileSlice: FileSlice): List[HoodieLogFile] = {
    fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList
  }

  private def detectFileFormat(filePath: String): String = {
    // Logic to detect file format based on the filePath or its content.
    if (filePath.endsWith(".parquet")) "parquet"
    else if (filePath.endsWith(".orc")) "orc"
    else ""
  }
}
