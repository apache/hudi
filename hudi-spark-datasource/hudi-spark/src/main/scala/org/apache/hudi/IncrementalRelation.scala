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

import org.apache.avro.Schema

import java.util.stream.Collectors
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieRecord, HoodieReplaceCommitMetadata, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hadoop.fs.GlobPattern
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.table.HoodieSparkTable
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Relation, that implements the Hoodie incremental view.
  *
  * Implemented for Copy_on_write storage.
  *
  */
class IncrementalRelation(val sqlContext: SQLContext,
                          val optParams: Map[String, String],
                          val userSchema: StructType,
                          val metaClient: HoodieTableMetaClient) extends BaseRelation with TableScan {

  private val log = LogManager.getLogger(classOf[IncrementalRelation])

  val skeletonSchema: StructType = HoodieSparkUtils.getMetaSchema
  private val basePath = metaClient.getBasePath
  // TODO : Figure out a valid HoodieWriteConfig
  private val hoodieTable = HoodieSparkTable.create(HoodieWriteConfig.newBuilder().withPath(basePath).build(),
    new HoodieSparkEngineContext(new JavaSparkContext(sqlContext.sparkContext)),
    metaClient)
  private val commitTimeline = hoodieTable.getMetaClient.getCommitTimeline.filterCompletedInstants()
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

  val useEndInstantSchema = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.key,
    DataSourceReadOptions.INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.defaultValue).toBoolean

  private val lastInstant = commitTimeline.lastInstant().get()

  private val commitsTimelineToReturn = commitTimeline.findInstantsInRange(
    optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key),
    optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(), lastInstant.getTimestamp))
  private val commitsToReturn = commitsTimelineToReturn.getInstants.iterator().toList

  // use schema from a file produced in the end/latest instant
  val usedSchema: StructType = {
    log.info("Inferring schema..")
    val schemaResolver = new TableSchemaResolver(metaClient)
    val tableSchema = if (useEndInstantSchema) {
      if (commitsToReturn.isEmpty)  schemaResolver.getTableAvroSchemaWithoutMetadataFields() else
        schemaResolver.getTableAvroSchemaWithoutMetadataFields(commitsToReturn.last)
    } else {
      schemaResolver.getTableAvroSchemaWithoutMetadataFields()
    }
    if (tableSchema.getType == Schema.Type.NULL) {
      // if there is only one commit in the table and is an empty commit without schema, return empty RDD here
      StructType(Nil)
    } else {
      val dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)
      StructType(skeletonSchema.fields ++ dataSchema.fields)
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
      val replacedFile = replacedTimeline.getInstants.collect(Collectors.toList[HoodieInstant]).flatMap { instant =>
        val replaceMetadata = HoodieReplaceCommitMetadata.
          fromBytes(metaClient.getActiveTimeline.getInstantDetails(instant).get, classOf[HoodieReplaceCommitMetadata])
        replaceMetadata.getPartitionToReplaceFileIds.entrySet().flatMap { entry =>
          entry.getValue.map { e =>
            val fullPath = FSUtils.getPartitionPath(basePath, entry.getKey).toString
            (e, fullPath)
          }
        }
      }.toMap

      for (commit <- commitsToReturn) {
        val metadata: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit)
          .get, classOf[HoodieCommitMetadata])

        if (HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS == commit.getTimestamp) {
          metaBootstrapFileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).toMap.filterNot { case (k, v) =>
            replacedFile.contains(k) && v.startsWith(replacedFile(k))
          }
        } else {
          regularFileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).toMap.filterNot { case (k, v) =>
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
      // unset the path filter, otherwise if end_instant_time is not the latest instant, path filter set for RO view
      // will filter out all the files incorrectly.
      sqlContext.sparkContext.hadoopConfiguration.unset("mapreduce.input.pathFilter.class")
      val sOpts = optParams.filter(p => !p._1.equalsIgnoreCase("path"))
      if (filteredRegularFullPaths.isEmpty && filteredMetaBootstrapFullPaths.isEmpty) {
        sqlContext.sparkContext.emptyRDD[Row]
      } else {
        log.info("Additional Filters to be applied to incremental source are :" + filters)

        var df: DataFrame = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], usedSchema)

        if (metaBootstrapFileIdToFullPath.nonEmpty) {
          df = sqlContext.sparkSession.read
            .format("hudi")
            .schema(usedSchema)
            .option(DataSourceReadOptions.READ_PATHS.key, filteredMetaBootstrapFullPaths.mkString(","))
            .load()
        }

        if (regularFileIdToFullPath.nonEmpty) {
          df = df.union(sqlContext.read.options(sOpts)
            .schema(usedSchema)
            .parquet(filteredRegularFullPaths.toList: _*)
            .filter(String.format("%s >= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              commitsToReturn.head.getTimestamp))
            .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              commitsToReturn.last.getTimestamp)))
        }

        filters.foldLeft(df)((e, f) => e.filter(f)).rdd
      }
    }
  }
}
