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

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.table.HoodieTable
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

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

  val fs = new Path(basePath).getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
  val metaClient = new HoodieTableMetaClient(sqlContext.sparkContext.hadoopConfiguration, basePath, true)
  // MOR datasets not supported yet
  if (metaClient.getTableType.equals(HoodieTableType.MERGE_ON_READ)) {
    throw new HoodieException("Incremental view not implemented yet, for merge-on-read datasets")
  }
  // TODO : Figure out a valid HoodieWriteConfig
  val hoodieTable = HoodieTable.getHoodieTable(metaClient, HoodieWriteConfig.newBuilder().withPath(basePath).build(),
    sqlContext.sparkContext)
  val commitTimeline = hoodieTable.getMetaClient.getCommitTimeline.filterCompletedInstants()
  if (commitTimeline.empty()) {
    throw new HoodieException("No instants to incrementally pull")
  }
  if (!optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY)) {
    throw new HoodieException(s"Specify the begin instant time to pull from using " +
      s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY}")
  }

  val lastInstant = commitTimeline.lastInstant().get()

  val commitsToReturn = commitTimeline.findInstantsInRange(
    optParams(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY),
    optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, lastInstant.getTimestamp))
    .getInstants.iterator().toList

  // use schema from a file produced in the latest instant
  val latestSchema = {
    // use last instant if instant range is empty
    val instant = commitsToReturn.lastOption.getOrElse(lastInstant)
    val latestMeta = HoodieCommitMetadata
          .fromBytes(commitTimeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])
    val metaFilePath = latestMeta.getFileIdAndFullPaths(basePath).values().iterator().next()
    AvroConversionUtils.convertAvroSchemaToStructType(ParquetUtils.readAvroSchema(
      sqlContext.sparkContext.hadoopConfiguration, new Path(metaFilePath)))
  }

  val filters = {
    if (optParams.contains(DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS_OPT_KEY)) {
      val filterStr = optParams.get(DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS_OPT_KEY).getOrElse("")
      filterStr.split(",").filter(!_.isEmpty)
    } else {
      Array[String]()
    }
  }

  override def schema: StructType = latestSchema

  override def buildScan(): RDD[Row] = {
    val fileIdToFullPath = mutable.HashMap[String, String]()
    for (commit <- commitsToReturn) {
      val metadata: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit)
        .get, classOf[HoodieCommitMetadata])
      fileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).toMap
    }
    // unset the path filter, otherwise if end_instant_time is not the latest instant, path filter set for RO view
    // will filter out all the files incorrectly.
    sqlContext.sparkContext.hadoopConfiguration.unset("mapreduce.input.pathFilter.class")
    val sOpts = optParams.filter(p => !p._1.equalsIgnoreCase("path"))
    if (fileIdToFullPath.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      log.info("Additional Filters to be applied to incremental source are :" + filters)
      filters.foldLeft(sqlContext.read.options(sOpts)
        .schema(latestSchema)
        .parquet(fileIdToFullPath.values.toList: _*)
        .filter(String.format("%s >= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.head.getTimestamp))
        .filter(String.format("%s <= '%s'",
          HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitsToReturn.last.getTimestamp)))((e, f) => e.filter(f))
        .toDF().rdd
    }
  }
}
