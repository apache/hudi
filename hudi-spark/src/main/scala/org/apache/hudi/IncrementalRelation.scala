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


import com.google.common.collect.Lists
import org.apache.avro.Schema
import org.apache.hadoop.fs.GlobPattern
import org.apache.hadoop.fs.Path
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.bootstrap.index.BootstrapIndex
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.table.HoodieTable

import org.apache.hadoop.fs.GlobPattern
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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
                          val basePath: String,
                          val optParams: Map[String, String],
                          val userSchema: StructType) extends BaseRelation with TableScan {

  private val log = LogManager.getLogger(classOf[IncrementalRelation])


  val skeletonSchema: StructType = HoodieSparkUtils.getMetaSchema
  private val metaClient = new HoodieTableMetaClient(sqlContext.sparkContext.hadoopConfiguration, basePath, true)

  // MOR tables not supported yet
  if (metaClient.getTableType.equals(HoodieTableType.MERGE_ON_READ)) {
    throw new HoodieException("Incremental view not implemented yet, for merge-on-read tables")
  }
  // TODO : Figure out a valid HoodieWriteConfig
  private val hoodieTable = HoodieTable.create(metaClient, HoodieWriteConfig.newBuilder().withPath(basePath).build(),
    sqlContext.sparkContext.hadoopConfiguration)
  private val commitTimeline = hoodieTable.getMetaClient.getCommitTimeline.filterCompletedInstants()
  if (commitTimeline.empty()) {
    throw new HoodieException("No instants to incrementally pull")
  }
  if (!optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY)) {
    throw new HoodieException(s"Specify the begin instant time to pull from using " +
      s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY}")
  }

  private val lastInstant = commitTimeline.lastInstant().get()

  private val commitsToReturn = commitTimeline.findInstantsInRange(
    optParams(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY),
    optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, lastInstant.getTimestamp))
    .getInstants.iterator().toList

  // use schema from a file produced in the latest instant
  val latestSchema: StructType = {
    log.info("Inferring schema..")
    val schemaResolver = new TableSchemaResolver(metaClient)
    val tableSchema = schemaResolver.getTableAvroSchemaWithoutMetadataFields
    val dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)
    StructType(skeletonSchema.fields ++ dataSchema.fields)
  }


  private val filters = {
    if (optParams.contains(DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS_OPT_KEY)) {
      val filterStr = optParams.getOrElse(
        DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS_OPT_KEY,
        DataSourceReadOptions.DEFAULT_PUSH_DOWN_FILTERS_OPT_VAL)
      filterStr.split(",").filter(!_.isEmpty)
    } else {
      Array[String]()
    }
  }

  override def schema: StructType = latestSchema

  override def buildScan(): RDD[Row] = {
    val regularFileIdToFullPath = mutable.HashMap[String, String]()
    var metaBootstrapFileIdToFullPath = mutable.HashMap[String, String]()

    for (commit <- commitsToReturn) {
      val metadata: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit)
        .get, classOf[HoodieCommitMetadata])

      if (HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS == commit.getTimestamp) {
        metaBootstrapFileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).toMap
      } else {
        regularFileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).toMap
      }
    }

    if (metaBootstrapFileIdToFullPath.nonEmpty) {
      // filer out meta bootstrap files that have had more commits since metadata bootstrap
      metaBootstrapFileIdToFullPath = metaBootstrapFileIdToFullPath
        .filterNot(fileIdFullPath => regularFileIdToFullPath.contains(fileIdFullPath._1))
    }

    val pathGlobPattern = optParams.getOrElse(
      DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY,
      DataSourceReadOptions.DEFAULT_INCR_PATH_GLOB_OPT_VAL)
    val (filteredRegularFullPaths, filteredMetaBootstrapFullPaths) = {
      if(!pathGlobPattern.equals(DataSourceReadOptions.DEFAULT_INCR_PATH_GLOB_OPT_VAL)) {
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

      var df: DataFrame = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], latestSchema)

      if (metaBootstrapFileIdToFullPath.nonEmpty) {
        df = sqlContext.sparkSession.read
               .format("hudi")
               .schema(latestSchema)
               .option(DataSourceReadOptions.READ_PATHS_OPT_KEY, filteredMetaBootstrapFullPaths.mkString(","))
               .load()
      }

      if (regularFileIdToFullPath.nonEmpty)
      {
        df = df.union(sqlContext.read.options(sOpts)
                        .schema(latestSchema)
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
