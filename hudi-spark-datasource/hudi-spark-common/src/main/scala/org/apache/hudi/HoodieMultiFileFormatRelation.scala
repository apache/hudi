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

package org.apache.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieBaseRelation.projectReader
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieMultiFileFormatRelation.{createPartitionedFile, inferFileFormat}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieFileFormat, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.asScalaIteratorConverter

/**
 * Base split for all Hoodie multi-file format relations.
 */
case class HoodieMultiFileFormatSplit(baseFile: Option[PartitionedFile],
                                      logFiles: List[HoodieLogFile]) extends HoodieFileSplit

/**
 * Base relation to handle table with multiple base file formats.
 * [[BaseFileOnlyRelation]] converts to [[HadoopFsRelation]], which cannot be done for multiple file formats
 * because the file format is not known at the time creating the relation. It is only known when we the
 * relation is collecting file splits. Hence, a separate relation to handle multiple file formats.
 */
abstract class BaseHoodieMultiFileFormatRelation(override val sqlContext: SQLContext,
                                                 override val metaClient: HoodieTableMetaClient,
                                                 override val optParams: Map[String, String],
                                                 private val userSchema: Option[StructType],
                                                 private val globPaths: Seq[Path],
                                                 private val prunedDataSchema: Option[StructType] = None)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema, prunedDataSchema) with SparkAdapterSupport {

  def buildSplits(fileSlices: Seq[FileSlice]): Seq[HoodieMultiFileFormatSplit]

  /**
   * Provided with partition and data filters, collects target file splits to read records from,
   * while performing pruning if necessary.
   */
  override protected def collectFileSplits(partitionFilters: Seq[Expression],
                                           dataFilters: Seq[Expression]): Seq[FileSplit] = {
    val convertedPartitionFilters =
      HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)

    if (globPaths.isEmpty) {
      val fileSlices = fileIndex.filterFileSlices(dataFilters, convertedPartitionFilters).flatMap(s => s._2)
      buildSplits(fileSlices)
    } else {
      val fileSlices = listLatestFileSlices(globPaths, partitionFilters, dataFilters)
      buildSplits(fileSlices)
    }
  }

  override type FileSplit = HoodieMultiFileFormatSplit
}

case class HoodieMultiFileFormatCOWRelation(override val sqlContext: SQLContext,
                                            override val metaClient: HoodieTableMetaClient,
                                            override val optParams: Map[String, String],
                                            private val userSchema: Option[StructType],
                                            private val globPaths: Seq[Path],
                                            private val prunedDataSchema: Option[StructType] = None)
  extends BaseHoodieMultiFileFormatRelation(sqlContext, metaClient, optParams, userSchema, globPaths, prunedDataSchema) {

  override type Relation = HoodieMultiFileFormatCOWRelation

  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  override lazy val mandatoryFields: Seq[String] = Seq.empty

  override def buildSplits(fileSlices: Seq[FileSlice]): Seq[FileSplit] = {
    fileSlices.map { fileSlice =>
      val baseFile = toScalaOption(fileSlice.getBaseFile)
      val partitionedBaseFile = baseFile.map { file =>
        createPartitionedFile(
          getPartitionColumnsAsInternalRow(file.getFileStatus), file.getFileStatus.getPath, 0, file.getFileLen)
      }

      HoodieMultiFileFormatSplit(partitionedBaseFile, List[HoodieLogFile]())
    }.toList
  }

  /**
   * Composes RDD provided file splits to read from, table and partition schemas, data filters to be applied.
   */
  override protected def composeRDD(fileSplits: Seq[HoodieMultiFileFormatSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    val (partitionSchema, dataSchema, requiredDataSchema) = tryPrunePartitionColumns(tableSchema, requiredSchema)

    // Group splits by their file format. The collection of [[PartitionedFile]] is used to create [[FilePartition]]
    // for [[HoodieFileScanRDD]]. Each [[FilePartition]] is processed by single Spark task.
    val groupedSplits: Map[HoodieFileFormat, Array[PartitionedFile]] = fileSplits.flatMap { split =>
      split.baseFile.map(file => (inferFileFormat(file.filePath.toString), file))
    }.groupBy(_._1).mapValues(_.map(_._2).toArray)

    // Build RDD for each file format
    val allRDDs: Seq[HoodieUnsafeRDD] = groupedSplits.map { case (fileFormat, partitionedFiles) =>
      val filePartition: FilePartition = new FilePartition(partitionedFiles.length, partitionedFiles)
      val baseFileReader = createBaseFileReader(
        spark = sparkSession,
        partitionSchema = partitionSchema,
        dataSchema = dataSchema,
        requiredDataSchema = requiredDataSchema,
        filters = filters,
        options = optParams,
        hadoopConf = embedInternalSchema(new Configuration(conf), requiredSchema.internalSchema),
        baseFileFormat = fileFormat
      )
      val projectedReader = projectReader(baseFileReader, requiredSchema.structTypeSchema)

      sparkAdapter.createHoodieFileScanRDD(sparkSession, projectedReader.apply, Seq(filePartition), requiredSchema.structTypeSchema)
        .asInstanceOf[HoodieUnsafeRDD]
    }.toSeq

    // Union all RDDs together
    sparkSession.sparkContext.union(allRDDs)
  }
}

case class HoodieMultiFileFormatMORRelation(override val sqlContext: SQLContext,
                                            override val metaClient: HoodieTableMetaClient,
                                            override val optParams: Map[String, String],
                                            private val userSchema: Option[StructType],
                                            private val globPaths: Seq[Path],
                                            private val prunedDataSchema: Option[StructType] = None)
  extends BaseHoodieMultiFileFormatRelation(sqlContext, metaClient, optParams, userSchema, globPaths, prunedDataSchema) {

  override type Relation = HoodieMultiFileFormatMORRelation

  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  override def buildSplits(fileSlices: Seq[FileSlice]): Seq[FileSplit] = {
    fileSlices.map { fileSlice =>
      val baseFile = toScalaOption(fileSlice.getBaseFile)
      val logFiles = fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList
      val partitionedBaseFile = baseFile.map { file =>
        createPartitionedFile(
          getPartitionColumnsAsInternalRow(file.getFileStatus), file.getFileStatus.getPath, 0, file.getFileLen)
      }

      HoodieMultiFileFormatSplit(partitionedBaseFile, logFiles)
    }.toList
  }

  protected lazy val mandatoryFieldsForMerging: Seq[String] =
    Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())

  override lazy val mandatoryFields: Seq[String] = mandatoryFieldsForMerging

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  /**
   * Composes RDD provided file splits to read from, table and partition schemas, data filters to be applied.
   */
  override protected def composeRDD(fileSplits: Seq[HoodieMultiFileFormatSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    val requiredFilters = Seq.empty
    val optionalFilters = filters

    val allRDDs: Seq[HoodieMergeOnReadRDD] = fileSplits.map(split => {
      val baseFile: Option[PartitionedFile] = split.baseFile
      // get the base file readers based on file format
      val readers = baseFile match {
        case Some(file) =>
          val baseFileFormat = inferFileFormat(file.filePath.toString)
          createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters, baseFileFormat)
        case None =>
          createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)
      }
      val convertedSplits = Seq(HoodieMergeOnReadFileSplit(split.baseFile, split.logFiles))
      new HoodieMergeOnReadRDD(
        sqlContext.sparkContext,
        config = jobConf,
        fileReaders = readers,
        tableSchema = tableSchema,
        requiredSchema = requiredSchema,
        tableState = tableState,
        mergeType = mergeType,
        fileSplits = convertedSplits)
    })
    sparkSession.sparkContext.union(allRDDs)
  }
}

object HoodieMultiFileFormatRelation extends SparkAdapterSupport {

  def createPartitionedFile(partitionValues: InternalRow,
                            filePath: Path,
                            start: Long,
                            length: Long): PartitionedFile = {
    sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
      partitionValues, filePath, start, length)
  }

  // Utility to infer file format based on file extension
  def inferFileFormat(filePath: String): HoodieFileFormat = {
    val fileExtension = FSUtils.getFileExtension(filePath)
    HoodieFileFormat.fromFileExtension(fileExtension)
  }
}
