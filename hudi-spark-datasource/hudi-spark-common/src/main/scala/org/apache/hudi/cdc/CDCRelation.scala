/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cdc

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, HoodieDataSourceHelper, HoodieTableSchema, SparkAdapterSupport}
import org.apache.hudi.HoodieConversionUtils._
import org.apache.hudi.common.table.cdc.CDCFileTypeEnum._
import org.apache.hudi.common.table.cdc.CDCUtils._
import org.apache.hudi.common.table.cdc.CDCOperationEnum._
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieCommitMetadata, HoodieFileFormat, HoodieFileGroupId, HoodieLogFile, HoodieReplaceCommitMetadata, HoodieWriteStat, WriteOperationType}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.HoodieTimeline._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.{HoodieException, HoodieNotSupportedException}
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class CDCRelation(
    override val sqlContext: SQLContext,
    metaClient: HoodieTableMetaClient,
    cdcSupplementalLogging: Boolean,
    startInstant: String,
    endInstant: String,
    options: Map[String, String]
) extends BaseRelation with PrunedFilteredScan with Logging {

  val spark: SparkSession = sqlContext.sparkSession

  val fs: FileSystem = metaClient.getFs.getFileSystem

  val basePath: Path = metaClient.getBasePathV2

  val (tableAvroSchema, _) = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    val avroSchema = Try(schemaUtil.getTableAvroSchema) match {
      case Success(schema) => schema
      case Failure(e) =>
        throw new IllegalArgumentException("Failed to fetch schema from the table", e)
    }
    // try to find internalSchema
    val internalSchemaFromMeta = try {
      schemaUtil.getTableInternalSchemaFromCommitMetadata.orElse(InternalSchema.getEmptyInternalSchema)
    } catch {
      case _: Exception => InternalSchema.getEmptyInternalSchema
    }
    (avroSchema, internalSchemaFromMeta)
  }

  val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  val commits: Map[HoodieInstant, HoodieCommitMetadata] =
    CDCRelation.getCompletedCommitInstantInSpecifiedRange(metaClient, startInstant, endInstant)

  /**
   * Parse the commit metadata between (startInstant, endInstant], and extract the touched partitions
   * and files to build the filesystem view.
   */
  lazy val fsView: HoodieTableFileSystemView = {
    val touchedPartition = commits.flatMap { case (_, commitMetadata) =>
      val partitionSet = commitMetadata.getPartitionToWriteStats.keySet()
      val replacedPartitionSet = commitMetadata match {
        case replaceCommitMetadata: HoodieReplaceCommitMetadata =>
          replaceCommitMetadata.getPartitionToReplaceFileIds.keySet().asScala
        case _ => Set.empty[String]
      }
      partitionSet.asScala ++ replacedPartitionSet
    }.toSet
    val touchedFiles = touchedPartition.flatMap { partition =>
      val partitionPath = FSUtils.getPartitionPath(basePath, partition)
      fs.listStatus(partitionPath)
    }.toArray
    new HoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline.filterCompletedInstants, touchedFiles)
  }

  /**
   * At the granularity of a file group, trace the mapping between each commit/instant and changes to this file group.
   */
  val changeFilesForPerFileGroupAndCommit: Map[HoodieFileGroupId, HoodieCDCFileGroupSplit] = {
    val fgToCommitChanges = mutable.Map.empty[HoodieFileGroupId,
      mutable.Map[HoodieInstant, ChangeFileForSingleFileGroupAndCommit]]

    commits.foreach {
      case (instant, commitMetadata) =>
        // parse `partitionToWriteStats` in the metadata of commit
        commitMetadata.getPartitionToWriteStats.asScala.foreach {
          case (partition, hoodieWriteStats) =>
            hoodieWriteStats.asScala.foreach { writeStat =>
              val fileGroupId = new HoodieFileGroupId(partition, writeStat.getFileId)
              // Identify the CDC source involved in this commit and
              // determine its type for subsequent loading using different methods.
              val changeFile = parseWriteStat(fileGroupId, instant, writeStat,
                commitMetadata.getOperationType == WriteOperationType.DELETE)
              if (fgToCommitChanges.contains(fileGroupId)) {
                fgToCommitChanges(fileGroupId)(instant) = changeFile
              } else {
                fgToCommitChanges.put(fileGroupId, mutable.Map(instant -> changeFile))
              }
            }
        }

        // parse `partitionToReplaceFileIds` in the metadata of commit
        commitMetadata match {
          case replaceCommitMetadata: HoodieReplaceCommitMetadata =>
            replaceCommitMetadata.getPartitionToReplaceFileIds.asScala.foreach {
              case (partition, fileIds) =>
                fileIds.asScala.foreach { fileId =>
                  toScalaOption(fsView.fetchLatestFileSlice(partition, fileId)).foreach {
                    fileSlice =>
                      val fileGroupId = new HoodieFileGroupId(partition, fileId)
                      val changeFile =
                        ChangeFileForSingleFileGroupAndCommit(REPLACED_FILE_GROUP, null, Some(fileSlice))
                      if (fgToCommitChanges.contains(fileGroupId)) {
                        fgToCommitChanges(fileGroupId)(instant) = changeFile
                      } else {
                        fgToCommitChanges.put(fileGroupId, mutable.Map(instant -> changeFile))
                      }
                  }
                }
            }
          case _ =>
        }
      case _ =>
    }
    fgToCommitChanges.map { case (fgId, instantToChanges) =>
      (fgId, HoodieCDCFileGroupSplit(instantToChanges.toArray.sortBy(_._1)))
    }.toMap
  }

  override final def needConversion: Boolean = false

  override def schema: StructType = CDCRelation.cdcSchema()

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val internalRows = buildScan0(requiredColumns, filters)
    internalRows.asInstanceOf[RDD[Row]]
  }

  def buildScan0(requiredColumns: Array[String], filters: Array[Filter]): RDD[InternalRow] = {
    val nameToField = schema.fields.map(f => f.name -> f).toMap
    val requiredSchema = StructType(requiredColumns.map(nameToField))
    val originTableSchema = HoodieTableSchema(tableStructSchema, tableAvroSchema.toString)
    val parquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = spark,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = tableStructSchema,
      filters = Nil,
      options = options,
      hadoopConf = spark.sessionState.newHadoopConf()
    )
    val cdcRdd = new HoodieCDCRDD(
      spark,
      metaClient,
      cdcSupplementalLogging,
      parquetReader,
      originTableSchema,
      schema,
      requiredSchema,
      changeFilesForPerFileGroupAndCommit.values.toArray
    )
    cdcRdd.asInstanceOf[RDD[InternalRow]]
  }

  /**
   * Parse HoodieWriteStat, judge which type the file is, and what strategy should be used to parse CDC data.
   * Then build a [[ChangeFileForSingleFileGroupAndCommit]] object.
   */
  private def parseWriteStat(
      fileGroupId: HoodieFileGroupId,
      instant: HoodieInstant,
      writeStat: HoodieWriteStat,
      isDeleteOperation: Boolean): ChangeFileForSingleFileGroupAndCommit = {
    if (StringUtils.isNullOrEmpty(writeStat.getCdcPath)) {
      // no cdc log files can be used directly. we reuse the existing data file to retrieve the change data.
      val path = writeStat.getPath
      if (path.endsWith(HoodieFileFormat.PARQUET.getFileExtension)) {
        // this is a base file
        if (isDeleteOperation && writeStat.getNumWrites == 0L && writeStat.getNumDeletes != 0) {
          // This is a delete operation wherein all the records in this file group are deleted
          // and no records have been writen out a new file.
          // So, we find the previous file that this operation delete from, and treat each of
          // records as a deleted one.
          val absPartitionPath = FSUtils.getPartitionPath(basePath, fileGroupId.getPartitionPath)
          val deletedFile = FSUtils.getBaseFile(fs, absPartitionPath, fileGroupId.getFileId, writeStat.getPrevCommit)
            .getPath.toUri.toString
          ChangeFileForSingleFileGroupAndCommit(REMOVE_BASE_File, deletedFile)
        } else if (writeStat.getNumUpdateWrites == 0L && writeStat.getNumDeletes == 0
            && writeStat.getNumWrites == writeStat.getNumInserts) {
          // all the records in this file are new.
          ChangeFileForSingleFileGroupAndCommit(ADD_BASE_File, path)
        } else {
          throw new HoodieException("There should be a cdc log file.")
        }
      } else {
        // this is a log file
        val dependentFileSlice =
          getDependentFileSliceForLogFile(fileGroupId, instant, path, writeStat)
        ChangeFileForSingleFileGroupAndCommit(MOR_LOG_FILE, path, dependentFileSlice)
      }
    } else {
      // this is a cdc log
      ChangeFileForSingleFileGroupAndCommit(CDC_LOG_FILE, writeStat.getCdcPath)
    }
  }

  /**
   * For a mor log file, get the completed previous file slice from the related commit metadata.
   * This file slice will be used when we extract the change data from this mor log file.
   */
  private def getDependentFileSliceForLogFile(
      fgId: HoodieFileGroupId,
      instant: HoodieInstant,
      currentLogFile: String,
      writeStat: HoodieWriteStat): Option[FileSlice] = {
    val baseCommitTime = FSUtils.getCommitTime(writeStat.getPath)
    val partitionPath = FSUtils.getPartitionPath(basePath, fgId.getPartitionPath)

    instant.getAction match {
      case HoodieTimeline.DELTA_COMMIT_ACTION =>
        val currentLogFileName = new Path(currentLogFile).getName
        val detail = metaClient.getActiveTimeline.getInstantDetails(instant).get()
        val pair = HoodieCommitMetadata.getFileSliceForFileGroupFromDeltaCommit(detail, fgId)
        if (pair == null) {
          None;
        } else {
          val baseFile = new HoodieBaseFile(fs.getFileStatus(new Path(partitionPath, pair.getLeft)))
          val logFilePaths = pair.getRight.asScala
            .filter(_ != currentLogFileName).map(new Path(partitionPath, _)).toArray
          val logFiles = fs.listStatus(logFilePaths).map(new HoodieLogFile(_))
          Some(new FileSlice(fgId, baseCommitTime, baseFile, logFiles))
        }
      case _ => None
    }
  }
}

object CDCRelation {

  val CDC_OPERATION_DELETE: UTF8String = UTF8String.fromString(DELETE.getValue)
  val CDC_OPERATION_INSERT: UTF8String = UTF8String.fromString(INSERT.getValue)
  val CDC_OPERATION_UPDATE: UTF8String = UTF8String.fromString(UPDATE.getValue)

  /**
   * CDC Schema.
   * Here we use the debezium format.
   */
  def cdcSchema(): StructType = {
    StructType(
      Seq(
        StructField(CDC_OPERATION_TYPE, StringType),
        StructField(CDC_COMMIT_TIMESTAMP, StringType),
        StructField(CDC_BEFORE_IMAGE, StringType),
        StructField(CDC_AFTER_IMAGE, StringType)
      )
    )
  }

  def isCDCTable(metaClient: HoodieTableMetaClient): Boolean = {
    metaClient.getTableConfig.isCDCEnabled
  }

  /**
   * The only approach to create the CDC relation.
   */
  def getCDCRelation(
      sqlContext: SQLContext,
      metaClient: HoodieTableMetaClient,
      options: Map[String, String]): CDCRelation = {

    if (!isCDCTable(metaClient)) {
      throw new IllegalArgumentException(s"It isn't a CDC hudi table on ${metaClient.getBasePathV2.toString}")
    }

    val startingInstant = options.getOrElse(DataSourceReadOptions.BEGIN_INSTANTTIME.key(),
      throw new HoodieException("CDC Query should provide the valid start version or timestamp")
    )
    val endingInstant = options.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(),
      getTimestampOfLatestInstant(metaClient)
    )
    if (startingInstant > endingInstant) {
      throw new HoodieException(s"This is not a valid range between $startingInstant and $endingInstant")
    }

    val supplementalLogging = metaClient.getTableConfig.isCDCSupplementalLoggingEnabled
    if (!supplementalLogging) {
      //TODO: When this case is supported, remove the judgment.
      throw new HoodieNotSupportedException(
        "Hudi don't support to disable 'hoodie.table.cdc.supplemental.logging' for now.");
    }

    new CDCRelation(sqlContext, metaClient, supplementalLogging, startingInstant, endingInstant, options)
  }

  def getTimestampOfLatestInstant(metaClient: HoodieTableMetaClient): String = {
    val latestInstant = metaClient.getActiveTimeline.lastInstant()
    if (latestInstant.isPresent) {
      latestInstant.get().getTimestamp
    } else {
      throw new HoodieException("No valid instant in Active Timeline.")
    }
  }

  /**
   * Extract the required instants from all the instants between (startTs, endTs].
   *
   * There are some conditions:
   * 1) the instant should be completed;
   * 2) the instant should be in (startTs, endTs];
   * 3) the action of the instant is one of 'commit', 'deltacommit', 'replacecommit';
   * 4) the write type of the commit should have the ability to change the data.
   *
   *  And, we need to recognize which is a 'replacecommit', that help to find the list of file group replaced.
   */
  def getCompletedCommitInstantInSpecifiedRange(
      metaClient: HoodieTableMetaClient,
      startTs: String,
      endTs: String): Map[HoodieInstant, HoodieCommitMetadata] = {
    val requiredActions = Set(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION)
    val activeTimeLine = metaClient.getActiveTimeline
    val instantAndCommitMetadataList = activeTimeLine.getInstantsAsList.asScala
      .filter { instant =>
        instant.isCompleted &&
          isInRange(instant.getTimestamp, startTs, endTs) &&
          requiredActions.contains(instant.getAction.toLowerCase)}
      .map { instant =>
        val commitMetadata = if (instant.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION) {
          HoodieReplaceCommitMetadata.fromBytes(
            activeTimeLine.getInstantDetails(instant).get(),
            classOf[HoodieReplaceCommitMetadata]
          )
        } else {
          HoodieCommitMetadata.fromBytes(
            activeTimeLine.getInstantDetails(instant).get(),
            classOf[HoodieCommitMetadata]
          )
        }
        (instant, commitMetadata)
      }
      .filter { case (_, commitMetadata) =>
        maybeChangeData(commitMetadata.getOperationType)
      }
    instantAndCommitMetadataList.toMap
  }

  def maybeChangeData(operation: WriteOperationType): Boolean = {
    operation == WriteOperationType.INSERT ||
      operation == WriteOperationType.UPSERT ||
      operation == WriteOperationType.DELETE ||
      operation == WriteOperationType.BULK_INSERT ||
      operation == WriteOperationType.DELETE_PARTITION ||
      operation == WriteOperationType.INSERT_OVERWRITE ||
      operation == WriteOperationType.INSERT_OVERWRITE_TABLE ||
      operation == WriteOperationType.BOOTSTRAP
  }
}
