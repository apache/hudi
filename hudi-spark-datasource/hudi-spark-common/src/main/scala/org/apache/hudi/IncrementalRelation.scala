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

package org.apache.hudi

import org.apache.hudi.DataSourceReadOptions.INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME
import org.apache.hudi.HoodieBaseRelation.isSchemaEvolutionEnabledOnRead
import org.apache.hudi.HoodieSparkConfUtils.getHollowCommitHandling
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieFileFormat, HoodieRecord, HoodieReplaceCommitMetadata}
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.USE_TRANSITION_TIME
import org.apache.hudi.common.table.timeline.TimelineUtils.{HollowCommitHandling, handleHollowCommitIfNeeded}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{HoodieTimer, InternalSchemaCache}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.{HoodieException, HoodieIncrementalPathNotFoundException}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.hudi.table.HoodieSparkTable

import org.apache.avro.Schema
import org.apache.hadoop.fs.GlobPattern
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.parquet.LegacyHoodieParquetFileFormat
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Relation, that implements the Hoodie incremental view.
 *
 * Implemented for Copy_on_write storage.
 * TODO: rebase w/ HoodieBaseRelation HUDI-5362
 *
 */
class IncrementalRelation(val sqlContext: SQLContext,
                          val optParams: Map[String, String],
                          val userSchema: Option[StructType],
                          val metaClient: HoodieTableMetaClient) extends BaseRelation with TableScan {

  private val log = LoggerFactory.getLogger(classOf[IncrementalRelation])

  val skeletonSchema: StructType = HoodieSparkUtils.getMetaSchema
  private val basePath = metaClient.getBasePath
  // TODO : Figure out a valid HoodieWriteConfig
  private val hoodieTable = HoodieSparkTable.create(HoodieWriteConfig.newBuilder().withPath(basePath.toString).build(),
    new HoodieSparkEngineContext(new JavaSparkContext(sqlContext.sparkContext)),
    metaClient)

  private val hollowCommitHandling: HollowCommitHandling = getHollowCommitHandling(optParams)

  private val commitTimeline = handleHollowCommitIfNeeded(
    hoodieTable.getMetaClient.getCommitTimeline.filterCompletedInstants,
    hoodieTable.getMetaClient,
    hollowCommitHandling)

  if (commitTimeline.empty()) {
    throw new HoodieException("No instants to incrementally pull")
  }
  if (!optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME.key)) {
    throw new HoodieException(s"Specify the begin instant time to pull from using " +
      s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME.key}")
  }

  if (!metaClient.getTableConfig.populateMetaFields()) {
    throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
  }

  private val useEndInstantSchema = optParams.getOrElse(INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.key,
    INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.defaultValue).toBoolean

  private val lastInstant = commitTimeline.lastInstant().get()

  private val commitsTimelineToReturn = {
    if (hollowCommitHandling == USE_TRANSITION_TIME) {
      commitTimeline.findInstantsInRangeByStateTransitionTime(
        optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key),
        optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(), lastInstant.getStateTransitionTime))
    } else {
      commitTimeline.findInstantsInRange(
        optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key),
        optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(), lastInstant.getTimestamp))
    }
  }
  private val commitsToReturn = commitsTimelineToReturn.getInstantsAsStream.iterator().asScala.toList

  // use schema from a file produced in the end/latest instant

  val (usedSchema, internalSchema) = {
    log.info("Inferring schema..")
    val schemaResolver = new TableSchemaResolver(metaClient)
    val iSchema : InternalSchema = if (!isSchemaEvolutionEnabledOnRead(optParams, sqlContext.sparkSession)) {
      InternalSchema.getEmptyInternalSchema
    } else if (useEndInstantSchema && !commitsToReturn.isEmpty) {
      InternalSchemaCache.searchSchemaAndCache(commitsToReturn.last.getTimestamp.toLong, metaClient, hoodieTable.getConfig.getInternalSchemaCacheEnable)
    } else {
      schemaResolver.getTableInternalSchemaFromCommitMetadata.orElse(null)
    }

    val tableSchema = if (useEndInstantSchema && iSchema.isEmptySchema) {
      if (commitsToReturn.isEmpty) schemaResolver.getTableAvroSchema(false) else
        schemaResolver.getTableAvroSchema(commitsToReturn.last, false)
    } else {
      schemaResolver.getTableAvroSchema(false)
    }
    if (tableSchema.getType == Schema.Type.NULL) {
      // if there is only one commit in the table and is an empty commit without schema, return empty RDD here
      (StructType(Nil), InternalSchema.getEmptyInternalSchema)
    } else {
      val dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)
      if (iSchema != null && !iSchema.isEmptySchema) {
        // if internalSchema is ready, dataSchema will contains skeletonSchema
        (dataSchema, iSchema)
      } else {
        (StructType(skeletonSchema.fields ++ dataSchema.fields), InternalSchema.getEmptyInternalSchema)
      }
    }
  }

  private val filters = optParams.getOrElse(DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS.key,
    DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS.defaultValue).split(",").filter(!_.isEmpty)

  override def schema: StructType = usedSchema

  override def buildScan(): RDD[Row] = {
    if (usedSchema == StructType(Nil)) {
      // if first commit in a table is an empty commit without schema, return empty RDD here
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      val regularFileIdToFullPath = mutable.HashMap[String, String]()
      var metaBootstrapFileIdToFullPath = mutable.HashMap[String, String]()

      // create Replaced file group
      val replacedTimeline = commitsTimelineToReturn.getCompletedReplaceTimeline
      val replacedFile = replacedTimeline.getInstants.asScala.flatMap { instant =>
        val replaceMetadata = HoodieReplaceCommitMetadata.
          fromBytes(metaClient.getActiveTimeline.getInstantDetails(instant).get, classOf[HoodieReplaceCommitMetadata])
        replaceMetadata.getPartitionToReplaceFileIds.entrySet().asScala.flatMap { entry =>
          entry.getValue.asScala.map { e =>
            val fullPath = FSUtils.constructAbsolutePath(basePath, entry.getKey).toString
            (e, fullPath)
          }
        }
      }.toMap

      for (commit <- commitsToReturn) {
        val metadata: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit)
          .get, classOf[HoodieCommitMetadata])

        if (HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS == commit.getTimestamp) {
          metaBootstrapFileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).asScala.filterNot { case (k, v) =>
            replacedFile.contains(k) && v.startsWith(replacedFile(k))
          }
        } else {
          regularFileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).asScala.filterNot { case (k, v) =>
            replacedFile.contains(k) && v.startsWith(replacedFile(k))
          }
        }
      }

      if (metaBootstrapFileIdToFullPath.nonEmpty) {
        // filer out meta bootstrap files that have had more commits since metadata bootstrap
        metaBootstrapFileIdToFullPath = metaBootstrapFileIdToFullPath
          .filterNot(fileIdFullPath => regularFileIdToFullPath.contains(fileIdFullPath._1))
      }

      val pathGlobPattern = optParams.getOrElse(
        DataSourceReadOptions.INCR_PATH_GLOB.key,
        DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)
      val (filteredRegularFullPaths, filteredMetaBootstrapFullPaths) = {
        if (!pathGlobPattern.equals(DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)) {
          val globMatcher = new GlobPattern("*" + pathGlobPattern)
          (regularFileIdToFullPath.filter(p => globMatcher.matches(p._2)).values,
            metaBootstrapFileIdToFullPath.filter(p => globMatcher.matches(p._2)).values)
        } else {
          (regularFileIdToFullPath.values, metaBootstrapFileIdToFullPath.values)
        }
      }
      // pass internalSchema to hadoopConf, so it can be used in executors.
      val validCommits = metaClient
        .getCommitsAndCompactionTimeline.filterCompletedInstants.getInstantsAsStream.toArray().map(_.asInstanceOf[HoodieInstant].getFileName).mkString(",")
      sqlContext.sparkContext.hadoopConfiguration.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(internalSchema))
      sqlContext.sparkContext.hadoopConfiguration.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, metaClient.getBasePath.toString)
      sqlContext.sparkContext.hadoopConfiguration.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
      val formatClassName = metaClient.getTableConfig.getBaseFileFormat match {
        case HoodieFileFormat.PARQUET => LegacyHoodieParquetFileFormat.FILE_FORMAT_ID
        case HoodieFileFormat.ORC => "orc"
      }

      // Fallback to full table scan if any of the following conditions matches:
      //   1. the start commit is archived
      //   2. the end commit is archived
      //   3. there are files in metadata be deleted
      val fallbackToFullTableScan = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key,
        DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.defaultValue).toBoolean

      val sOpts = optParams.filter(p => !p._1.equalsIgnoreCase("path"))

      val startInstantTime = optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key)
      val startInstantArchived = commitTimeline.isBeforeTimelineStarts(startInstantTime)
      val endInstantTime = optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(), lastInstant.getTimestamp)
      val endInstantArchived = commitTimeline.isBeforeTimelineStarts(endInstantTime)

      val scanDf = if (fallbackToFullTableScan && (startInstantArchived || endInstantArchived)) {
        if (hollowCommitHandling == USE_TRANSITION_TIME) {
          throw new HoodieException("Cannot use stateTransitionTime while enables full table scan")
        }
        log.info(s"Falling back to full table scan as startInstantArchived: $startInstantArchived, endInstantArchived: $endInstantArchived")
        fullTableScanDataFrame(startInstantTime, endInstantTime)
      } else {
        if (filteredRegularFullPaths.isEmpty && filteredMetaBootstrapFullPaths.isEmpty) {
          sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], usedSchema)
        } else {
          log.info("Additional Filters to be applied to incremental source are :" + filters.mkString("Array(", ", ", ")"))

          var df: DataFrame = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], usedSchema)

          var doFullTableScan = false

          if (fallbackToFullTableScan) {
            val timer = HoodieTimer.start

            val allFilesToCheck = filteredMetaBootstrapFullPaths ++ filteredRegularFullPaths
            val storageConf = HadoopFSUtils.getStorageConfWithCopy(sqlContext.sparkContext.hadoopConfiguration)
            val localBasePathStr = basePath.toString
            val firstNotFoundPath = sqlContext.sparkContext.parallelize(allFilesToCheck.toSeq, allFilesToCheck.size)
              .map(path => {
                val storage = HoodieStorageUtils.getStorage(localBasePathStr, storageConf)
                storage.exists(new StoragePath(path))
              }).collect().find(v => !v)
            val timeTaken = timer.endTimer()
            log.info("Checking if paths exists took " + timeTaken + "ms")

            if (firstNotFoundPath.isDefined) {
              doFullTableScan = true
              log.info("Falling back to full table scan as some files cannot be found.")
            }
          }

          if (doFullTableScan) {
            fullTableScanDataFrame(startInstantTime, endInstantTime)
          } else {
            if (metaBootstrapFileIdToFullPath.nonEmpty) {
              df = sqlContext.sparkSession.read
                .format("hudi_v1")
                .schema(usedSchema)
                .option(DataSourceReadOptions.READ_PATHS.key, filteredMetaBootstrapFullPaths.mkString(","))
                // Setting time to the END_INSTANT_TIME, to avoid pathFilter filter out files incorrectly.
                .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), endInstantTime)
                .load()
            }

            if (regularFileIdToFullPath.nonEmpty) {
              try {
                df = df.union(sqlContext.read.options(sOpts)
                  .schema(usedSchema).format(formatClassName)
                  // Setting time to the END_INSTANT_TIME, to avoid pathFilter filter out files incorrectly.
                  .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), endInstantTime)
                  .load(filteredRegularFullPaths.toList: _*)
                  .filter(String.format("%s >= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                    commitsToReturn.head.getTimestamp))
                  .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                    commitsToReturn.last.getTimestamp)))
              } catch {
                case e : AnalysisException =>
                  if (e.getMessage.contains("Path does not exist")) {
                    throw new HoodieIncrementalPathNotFoundException(e)
                  } else {
                    throw e
                  }
              }

            }
            df
          }
        }
      }

      filters.foldLeft(scanDf)((e, f) => e.filter(f)).rdd
    }
  }

  private def fullTableScanDataFrame(startInstantTime: String, endInstantTime: String): DataFrame = {
    val hudiDF = sqlContext.read
      .format("hudi_v1")
      .schema(usedSchema)
      .load(basePath.toString)
      .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, //Notice the > in place of >= because we are working with optParam instead of first commit > optParam
        startInstantTime))
      .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        endInstantTime))

    // schema enforcement does not happen in above spark.read with hudi. hence selecting explicitly w/ right column order
    val fieldNames = usedSchema.fieldNames
    hudiDF.select(fieldNames.head, fieldNames.tail: _*)
  }
}
