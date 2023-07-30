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

package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceReadOptions.{REALTIME_PAYLOAD_COMBINE_OPT_VAL, REALTIME_SKIP_MERGE_OPT_VAL}
import org.apache.hudi.MergeOnReadSnapshotRelation.createPartitionedFile
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{BaseFile, FileSlice, HoodieLogFile, HoodieRecord}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.{HoodieBaseRelation, HoodieSparkUtils, HoodieTableSchema, HoodieTableState, InternalRowBroadcast, LogFileIterator, MergeOnReadSnapshotRelation, RecordMergingFileIterator, SkipMergeIterator, SparkAdapterSupport}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.BootstrapMORIteratorFactory.{BuildReaderWithPartitionValuesFunc, SupportBatchFunc, getLogFilesFromSlice}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaIteratorConverter



class BootstrapMORIteratorFactory(tableState: Broadcast[HoodieTableState],
                                  tableSchema: Broadcast[HoodieTableSchema],
                                  tableName: String,
                                  mergeType: String,
                                  mandatoryFields: Seq[String],
                                  isMOR: Boolean,
                                  isBootstrap: Boolean,
                                  supportBatchFunc: SupportBatchFunc,
                                  buildReaderWithPartitionValuesFunc: BuildReaderWithPartitionValuesFunc) extends SparkAdapterSupport with Serializable {

  protected val isPayloadMerge: Boolean = isMOR && mergeType.equalsIgnoreCase(REALTIME_PAYLOAD_COMBINE_OPT_VAL)

  def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)

    val requiredSchemaWithMandatory = if (isMOR && !isPayloadMerge) {
        requiredSchema
    } else if (!isMOR || MergeOnReadSnapshotRelation.isProjectionCompatible(tableState.value)) {
      //add mandatory fields to required schema
      val added: mutable.Buffer[StructField] = mutable.Buffer[StructField]()
      for (field <- mandatoryFields) {
        requiredSchema.indexOf(field)
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

    val requiredSchemaSplits = requiredSchemaWithMandatory.fields.partition(f => HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name))
    val requiredMeta = StructType(requiredSchemaSplits._1)
    val requiredWithoutMeta = StructType(requiredSchemaSplits._2)
    val needMetaCols = requiredMeta.nonEmpty
    val needDataCols = requiredWithoutMeta.nonEmpty
    val bootstrapReaderOutput = StructType(requiredMeta.fields ++ requiredWithoutMeta.fields)

    val skeletonReaderAppend = needMetaCols && isBootstrap && !(needDataCols || isPayloadMerge) && partitionSchema.nonEmpty
    val bootstrapBaseAppend = needDataCols && isBootstrap && !isPayloadMerge && partitionSchema.nonEmpty

    val (baseFileReader, preMergeBaseFileReader, skeletonReader, bootstrapBaseReader) = buildFileReaders(sparkSession,
      dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf, requiredSchemaWithMandatory,
      requiredWithoutMeta, requiredMeta)

    val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (file: PartitionedFile) => {
      file.partitionValues match {
        case broadcast: InternalRowBroadcast =>
          val filePath = sparkAdapter.getFilePath(file)
          if (FSUtils.isLogFile(filePath)) {
            //no base file
            val fileSlice = broadcast.getSlice(FSUtils.getFileId(filePath.getName).substring(1)).get
            val logFiles = getLogFilesFromSlice(fileSlice)
            val outputAvroSchema = HoodieBaseRelation.convertToAvroSchema(outputSchema, tableName)
            new LogFileIterator(logFiles, filePath.getParent,  tableSchema.value, outputSchema, outputAvroSchema,
              tableState.value, broadcastedHadoopConf.value.value)
          } else {
            //We do not broadcast the slice if it has no log files or bootstrap base
            broadcast.getSlice(FSUtils.getFileId(filePath.getName)) match {
              case Some(fileSlice) =>
                val hoodieBaseFile = fileSlice.getBaseFile.get()
                val bootstrapFileOpt = hoodieBaseFile.getBootstrapBaseFile
                val partitionValues = broadcast.getInternalRow
                val logFiles = getLogFilesFromSlice(fileSlice)
                if (requiredSchema.isEmpty) {
                  val baseFile = createPartitionedFile(partitionValues, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen)
                  baseFileReader(baseFile)
                } else if (isBootstrap && bootstrapFileOpt.isPresent) {
                  val bootstrapIterator = buildBootstrapIterator(skeletonReader, bootstrapBaseReader,
                    skeletonReaderAppend, bootstrapBaseAppend, bootstrapFileOpt.get(), hoodieBaseFile, partitionValues,
                    needMetaCols, needDataCols)
                  (isMOR, isPayloadMerge, logFiles.nonEmpty) match {
                    case (true, _, true) =>
                      buildMergeOnReadIterator(bootstrapIterator, logFiles, filePath.getParent, bootstrapReaderOutput,
                        requiredSchemaWithMandatory, outputSchema, partitionSchema, partitionValues, broadcastedHadoopConf.value.value)
                    case (true, true, false) =>
                      appendPartitionAndProject(bootstrapIterator, bootstrapReaderOutput, partitionSchema, outputSchema, partitionValues)
                    case (false, _, false) => bootstrapIterator
                    case (false, _, true) => throw new IllegalStateException("should not be log files if not mor table")
                  }
                } else {
                  val baseFile = createPartitionedFile(InternalRow.empty, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen)
                  if (isMOR && logFiles.nonEmpty) {
                    val iterForMerge = if (isPayloadMerge) {
                      preMergeBaseFileReader(baseFile)
                    } else {
                      baseFileReader(baseFile)
                    }
                    buildMergeOnReadIterator(iterForMerge, logFiles, filePath.getParent, requiredSchemaWithMandatory,
                      requiredSchemaWithMandatory, outputSchema, partitionSchema, partitionValues, broadcastedHadoopConf.value.value)
                  } else {
                    baseFileReader(baseFile)
                  }
                }
              case _ => baseFileReader(file)
            }
          }
        case _ => baseFileReader(file)
      }
    }
  }

  /**
   * Build file readers to read individual physical files
   */
  def buildFileReaders(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType,
                       requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String],
                       hadoopConf: Configuration, requiredSchemaWithMandatory: StructType,
                       requiredWithoutMeta: StructType, requiredMeta: StructType):
  (PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow]) = {

    //file reader when you just read a hudi parquet file and don't do any merging

    val baseFileReader = buildReaderWithPartitionValuesFunc(sparkSession, dataSchema, partitionSchema, requiredSchema,
      filters, options, hadoopConf, partitionSchema.nonEmpty, !isMOR, "")

    //file reader for reading a hudi base file that needs to be merged with log files
    val preMergeBaseFileReader = if (isPayloadMerge) {
      buildReaderWithPartitionValuesFunc(sparkSession, dataSchema, StructType(Seq.empty),
      requiredSchemaWithMandatory, Seq.empty, options, hadoopConf, false, false, "mor")
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

    val supportBatchOverride = supportBatchFunc(sparkSession, requiredMeta) && supportBatchFunc(sparkSession, requiredWithoutMeta)

    //file reader for bootstrap skeleton files
    val skeletonReader = if (needMetaCols && isBootstrap) {
      if (needDataCols || isMOR) {
        // no filter and no append
        buildReaderWithPartitionValuesFunc(sparkSession, HoodieSparkUtils.getMetaSchema, StructType(Seq.empty),
          requiredMeta, Seq.empty, options, hadoopConf, false, supportBatchOverride, "skeleton")
      } else {
        // filter and append
        buildReaderWithPartitionValuesFunc(sparkSession, HoodieSparkUtils.getMetaSchema, partitionSchema,
          requiredMeta, filters, options, hadoopConf, partitionSchema.nonEmpty, supportBatchOverride, "skeleton")
      }
    } else {
      _: PartitionedFile => Iterator.empty
    }

    //file reader for bootstrap base files
    val bootstrapBaseReader = if (needDataCols && isBootstrap) {
      val dataSchemaWithoutMeta = StructType(dataSchema.fields.filterNot(sf => isMetaField(sf.name)))
      if (isMOR) {
        // no filter and no append
        buildReaderWithPartitionValuesFunc(sparkSession, dataSchemaWithoutMeta, StructType(Seq.empty), requiredWithoutMeta,
          Seq.empty, options, hadoopConf, false, supportBatchOverride, "bootstrap")
      } else if (needMetaCols) {
        // no filter but append
        buildReaderWithPartitionValuesFunc(sparkSession, dataSchemaWithoutMeta, partitionSchema, requiredWithoutMeta,
          Seq.empty, options, hadoopConf, partitionSchema.nonEmpty, supportBatchOverride, "bootstrap")
      } else {
        // filter and append
        buildReaderWithPartitionValuesFunc(sparkSession, dataSchemaWithoutMeta, partitionSchema, requiredWithoutMeta,
          filters, options, hadoopConf, partitionSchema.nonEmpty, supportBatchOverride, "bootstrap")
      }
    } else {
      _: PartitionedFile => Iterator.empty
    }

    (baseFileReader, preMergeBaseFileReader, skeletonReader, bootstrapBaseReader)
  }

  /**
   * Create iterator for a file slice that has bootstrap base and skeleton file
   */
  def buildBootstrapIterator(skeletonReader: PartitionedFile => Iterator[InternalRow],
                             bootstrapBaseReader: PartitionedFile => Iterator[InternalRow],
                             skeletonReaderAppend: Boolean, bootstrapBaseAppend: Boolean,
                             bootstrapBaseFile: BaseFile, hoodieBaseFile: BaseFile,
                             partitionValues: InternalRow, needMetaCols: Boolean,
                             needDataCols: Boolean): Iterator[InternalRow] = {
    lazy val skeletonFile = if (skeletonReaderAppend) {
      createPartitionedFile(partitionValues, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen)
    } else {
      createPartitionedFile(InternalRow.empty, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen)
    }

    lazy val dataFile = if (bootstrapBaseAppend) {
      createPartitionedFile(partitionValues, bootstrapBaseFile.getHadoopPath, 0, bootstrapBaseFile.getFileLen)
    } else {
      createPartitionedFile(InternalRow.empty, bootstrapBaseFile.getHadoopPath, 0, bootstrapBaseFile.getFileLen)
    }

    lazy val skeletonIterator = skeletonReader(skeletonFile)
    lazy val dataFileIterator = bootstrapBaseReader(dataFile)

    (needMetaCols, needDataCols) match {
      case (true, true) => doBootstrapMerge(skeletonIterator, dataFileIterator)
      case (true, false) => skeletonIterator
      case (false, true) => dataFileIterator
      case (false, false) => throw new IllegalStateException("should not be here if only partition cols are required")
    }
  }

  /**
   * Merge skeleton and data file iterators
   */
  def doBootstrapMerge(skeletonFileIterator: Iterator[Any], dataFileIterator: Iterator[Any]): Iterator[InternalRow] = {
    new Iterator[Any] {
      val combinedRow = new JoinedRow()
      override def hasNext: Boolean = {
        checkState(dataFileIterator.hasNext == skeletonFileIterator.hasNext,
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!")
        dataFileIterator.hasNext && skeletonFileIterator.hasNext
      }

      override def next(): Any = {
        (skeletonFileIterator.next(), dataFileIterator.next()) match {
          case (s: ColumnarBatch, d: ColumnarBatch) =>
            val numCols = s.numCols() + d.numCols()
            val vecs: Array[ColumnVector] = new Array[ColumnVector](numCols)
            for (i <- 0 until numCols) {
              if (i < s.numCols()) {
                vecs(i) = s.column(i)
              } else {
                vecs(i) = d.column(i - s.numCols())
              }
            }
            assert(s.numRows() == d.numRows())
            sparkAdapter.makeColumnarBatch(vecs, s.numRows())
          case(_: ColumnarBatch, _:InternalRow) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
          case(_: InternalRow, _:ColumnarBatch) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
          case(s: InternalRow, d: InternalRow) => combinedRow(s, d)
        }
      }
    }.asInstanceOf[Iterator[InternalRow]]
  }

  /**
   * Create iterator for a file slice that has log files
   */
  def buildMergeOnReadIterator(iter: Iterator[InternalRow], logFiles: List[HoodieLogFile],
                               partitionPath: Path, inputSchema: StructType, requiredSchemaWithMandatory: StructType,
                               outputSchema: StructType, partitionSchema: StructType, partitionValues: InternalRow,
                               hadoopConf: Configuration): Iterator[InternalRow] = {

    val requiredAvroSchema = HoodieBaseRelation.convertToAvroSchema(requiredSchemaWithMandatory, tableName)
    mergeType match {
      case REALTIME_SKIP_MERGE_OPT_VAL =>
        new SkipMergeIterator(logFiles, partitionPath, iter, inputSchema, tableSchema.value,
          requiredSchemaWithMandatory, requiredAvroSchema, tableState.value, hadoopConf)
      case REALTIME_PAYLOAD_COMBINE_OPT_VAL =>
        val morIterator =  new RecordMergingFileIterator(logFiles, partitionPath, iter, inputSchema, tableSchema.value,
          requiredSchemaWithMandatory, requiredAvroSchema, tableState.value, hadoopConf)
        appendPartitionAndProject(morIterator, requiredSchemaWithMandatory, partitionSchema,
          outputSchema, partitionValues)
    }
  }

  /**
   * Append partition values to rows and project to output schema
   */
  def appendPartitionAndProject(iter: Iterator[InternalRow],
                                inputSchema: StructType,
                                partitionSchema: StructType,
                                to: StructType,
                                partitionValues: InternalRow): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      projectSchema(iter, inputSchema, to)
    } else {
      val unsafeProjection = HoodieCatalystExpressionUtils.
        generateUnsafeProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      iter.map(d => unsafeProjection(joinedRow(d, partitionValues)))
    }
  }

  def projectSchema(iter: Iterator[InternalRow],
                    from: StructType,
                    to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = HoodieCatalystExpressionUtils.generateUnsafeProjection(from, to)
    iter.map(d => unsafeProjection(d))
  }
}

object BootstrapMORIteratorFactory {

  def getLogFilesFromSlice(fileSlice: FileSlice): List[HoodieLogFile] = {
    fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList
  }

  type SupportBatchFunc = (SparkSession, StructType) => Boolean

  type BuildReaderWithPartitionValuesFunc = (SparkSession,
    StructType, StructType, StructType, Seq[Filter],
    Map[String, String], Configuration, Boolean, Boolean,
    String) => PartitionedFile => Iterator[InternalRow]

  trait MORBootstrapFileFormat {
    var isProjected: Boolean
  }
}
