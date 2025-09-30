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
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{HoodieTimer, InternalSchemaCache}
import org.apache.hudi.exception.{HoodieException, HoodieIncrementalPathNotFoundException}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}

import org.apache.avro.Schema
import org.apache.hadoop.fs.GlobPattern
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.datasources.parquet.LegacyHoodieParquetFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
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
class IncrementalRelationV2(val sqlContext: SQLContext,
                            val optParams: Map[String, String],
                            val userSchema: Option[StructType],
                            val metaClient: HoodieTableMetaClient,
                            val rangeType: RangeType) extends BaseRelation with TableScan {

  private val log = LoggerFactory.getLogger(classOf[IncrementalRelationV2])

  val skeletonSchema: StructType = HoodieSparkUtils.getMetaSchema
  private val basePath = metaClient.getBasePath

  private val commitTimeline =
    metaClient.getCommitTimeline.filterCompletedInstants

  if (commitTimeline.empty()) {
    throw new HoodieException("No instants to incrementally pull")
  }
  if (!optParams.contains(DataSourceReadOptions.START_COMMIT.key)) {
    throw new HoodieException(s"Specify the start completion time to pull from using " +
      s"option ${DataSourceReadOptions.START_COMMIT.key}")
  }

  if (!metaClient.getTableConfig.populateMetaFields()) {
    throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
  }

  private val queryContext: IncrementalQueryAnalyzer.QueryContext =
    IncrementalQueryAnalyzer.builder()
      .metaClient(metaClient)
      .startCompletionTime(optParams(DataSourceReadOptions.START_COMMIT.key))
      .endCompletionTime(optParams.getOrElse(DataSourceReadOptions.END_COMMIT.key, null))
      .rangeType(rangeType)
      .build()
      .analyze()

  private val commitsToReturn = queryContext.getInstants.asScala.toList

  private val useEndInstantSchema = optParams.getOrElse(INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.key,
    INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.defaultValue).toBoolean

  // use schema from a file produced in the end/latest instant

  val (usedSchema, internalSchema) = {
    log.info("Inferring schema..")
    val schemaResolver = new TableSchemaResolver(metaClient)
    val iSchema : InternalSchema = if (!isSchemaEvolutionEnabledOnRead(optParams, sqlContext.sparkSession)) {
      InternalSchema.getEmptyInternalSchema
    } else if (useEndInstantSchema && !commitsToReturn.isEmpty) {
      InternalSchemaCache.searchSchemaAndCache(commitsToReturn.last.requestedTime.toLong, metaClient)
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

  override def schema: StructType = {
    addCompletionTimeColumn(usedSchema)
  }

  override def buildScan(): RDD[Row] = {
    if (schema == StructType(Nil)) {
      // if first commit in a table is an empty commit without schema, return empty RDD here
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      val requestedToCompletionTimeMap = buildCompletionTimeMapping()
      val broadcastTimeMap = sqlContext.sparkContext.broadcast(requestedToCompletionTimeMap)
      val regularFileIdToFullPath = mutable.HashMap[String, String]()
      var metaBootstrapFileIdToFullPath = mutable.HashMap[String, String]()

      // create Replaced file group
      val replacedInstants = commitsToReturn.filter(_.getAction.equals(HoodieTimeline.REPLACE_COMMIT_ACTION))
      val replacedFile = replacedInstants.flatMap { instant =>
        val replaceMetadata = metaClient.getActiveTimeline.readReplaceCommitMetadata(instant)
        replaceMetadata.getPartitionToReplaceFileIds.entrySet().asScala.flatMap { entry =>
          entry.getValue.asScala.map { e =>
            val fullPath = FSUtils.constructAbsolutePath(basePath, entry.getKey).toString
            (e, fullPath)
          }
        }
      }.toMap

      for (commit <- commitsToReturn) {
        val metadata: HoodieCommitMetadata = commitTimeline.readCommitMetadata(commit)

        if (HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS == commit.requestedTime) {
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
      val instantFileNameGenerator = metaClient.getTimelineLayout.getInstantFileNameGenerator;
      val validCommits = metaClient
        .getCommitsAndCompactionTimeline.filterCompletedInstants.getInstantsAsStream.toArray().map(a => instantFileNameGenerator.getFileName(a.asInstanceOf[HoodieInstant])).mkString(",")
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

      val startInstantArchived = !queryContext.getArchivedInstants.isEmpty
      if (queryContext.isEmpty) {
        // no commits to read
        // scalastyle:off return
        return sqlContext.sparkContext.emptyRDD[Row]
        // scalastyle:on return
      }
      val endInstantTime = queryContext.getLastInstant

      val scanDf = if (fallbackToFullTableScan && startInstantArchived) {
        log.info(s"Falling back to full table scan as startInstantArchived: $startInstantArchived")
        fullTableScanDataFrame(commitsToReturn, broadcastTimeMap)
      } else {
        if (filteredRegularFullPaths.isEmpty && filteredMetaBootstrapFullPaths.isEmpty) {
          sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], schema)
        } else {
          log.info("Additional Filters to be applied to incremental source are :" + filters.mkString("Array(", ", ", ")"))

          var df: DataFrame = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], schema)

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
            fullTableScanDataFrame(commitsToReturn, broadcastTimeMap)
          } else {
            if (metaBootstrapFileIdToFullPath.nonEmpty) {
              val bootstrapDF = sqlContext.sparkSession.read
                .format("hudi_v1")
                .schema(schema)
                .option(DataSourceReadOptions.READ_PATHS.key, filteredMetaBootstrapFullPaths.mkString(","))
                // Setting time to the END_INSTANT_TIME, to avoid pathFilter filter out files incorrectly.
                .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), endInstantTime)
                .load()
              df = transformDataFrameWithCompletionTime(bootstrapDF, broadcastTimeMap)
            }

            if (regularFileIdToFullPath.nonEmpty) {
              try {
                val commitTimesToReturn = commitsToReturn.map(_.requestedTime)
                val baseDf = sqlContext.read.options(sOpts)
                  .schema(schema).format(formatClassName)
                  // Setting time to the END_INSTANT_TIME, to avoid pathFilter filter out files incorrectly.
                  .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), endInstantTime)
                  .load(filteredRegularFullPaths.toList: _*)
                  .filter(col(HoodieRecord.COMMIT_TIME_METADATA_FIELD).isin(commitTimesToReturn: _*))
                val df_with_completion_time = transformDataFrameWithCompletionTime(baseDf, broadcastTimeMap)
                df = df.union(df_with_completion_time)
              } catch {
                case e: AnalysisException =>
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

  private def fullTableScanDataFrame(commitsToFilter: List[HoodieInstant],
                                     broadcastTimeMap: org.apache.spark.broadcast.Broadcast[Map[String, String]]): DataFrame = {
    val commitTimesToFilter = commitsToFilter.map(_.requestedTime)
    val hudiDF = sqlContext.read
      .format("hudi_v1")
      .schema(schema)
      .load(basePath.toString)
      .filter(col(HoodieRecord.COMMIT_TIME_METADATA_FIELD).isin(commitTimesToFilter: _*))

    val fieldNames = schema.fieldNames
    val selectedDf = hudiDF.select(fieldNames.head, fieldNames.tail: _*)
    val transformedRDD = addCompletionTimeColumn(selectedDf.rdd, broadcastTimeMap)
    sqlContext.createDataFrame(transformedRDD, schema)
  }

  private def buildCompletionTimeMapping(): Map[String, String] = {
    commitsToReturn.map { instant =>
      val requestedTime = instant.requestedTime()
      val completionTime = Option(instant.getCompletionTime).orNull
      requestedTime -> completionTime
    }.toMap
  }

  private def transformDataFrameWithCompletionTime(df: DataFrame,
                                                  broadcastMap: org.apache.spark.broadcast.Broadcast[Map[String, String]]): DataFrame = {
    val transformedRDD = addCompletionTimeColumn(df.rdd, broadcastMap)
    sqlContext.createDataFrame(transformedRDD, schema)
  }

  private def addCompletionTimeColumn(rdd: RDD[Row],
                                    broadcastMap: org.apache.spark.broadcast.Broadcast[Map[String, String]]): RDD[Row] = {
    val commitTimeFieldIndex = schema.fieldIndex(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
    val completionTimeFieldIndex = schema.fieldIndex(HoodieRecord.COMMIT_COMPLETION_TIME_METADATA_FIELD)
    
    rdd.map { row =>
      val currentRequestedTime = row.getString(commitTimeFieldIndex)
      val completionTime = broadcastMap.value.getOrElse(currentRequestedTime, currentRequestedTime)
      val newValues = row.toSeq.zipWithIndex.map { case (value, index) =>
        if (index == completionTimeFieldIndex) {
          completionTime
        } else {
          value
        }
      }
      Row.fromSeq(newValues)
    }
  }

  private def addCompletionTimeColumn(baseSchema: StructType): StructType = {
    import org.apache.spark.sql.types.{StringType, StructField}
    val completionTimeField = StructField(HoodieRecord.COMMIT_COMPLETION_TIME_METADATA_FIELD, StringType, nullable = true)
    StructType(baseSchema.fields :+ completionTimeField)
  }
}
