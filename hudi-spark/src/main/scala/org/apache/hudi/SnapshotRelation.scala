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

package org.apache.hudi

import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.{HoodieParquetInputFormat, HoodieROTablePathFilter}
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.table.HoodieTable

import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters._

/**
 * This is the Spark DataSourceV1 relation to read Hudi MOR table.
 * @param sqlContext
 * @param basePath
 * @param optParams
 * @param userSchema
 */
class SnapshotRelation(val sqlContext: SQLContext,
                       val basePath: String,
                       val optParams: Map[String, String],
                       val userSchema: StructType) extends BaseRelation with PrunedFilteredScan {

  private val log = LogManager.getLogger(classOf[SnapshotRelation])
  private val conf = sqlContext.sparkContext.hadoopConfiguration

  // Load Hudi metadata
  val metaClient = new HoodieTableMetaClient(conf, basePath, true)
  private val hoodieTable = HoodieTable.create(metaClient, HoodieWriteConfig.newBuilder().withPath(basePath).build(), conf)
  private val commitTimeline = hoodieTable.getMetaClient.getCommitsAndCompactionTimeline
  if (commitTimeline.empty()) {
    throw new HoodieException("No Valid Hudi timeline exists")
  }
  private val completedCommitTimeline = hoodieTable.getMetaClient.getCommitsTimeline.filterCompletedInstants()
  private val lastInstant = completedCommitTimeline.lastInstant().get()

  // Set config for listStatus() in HoodieParquetInputFormat
  conf.setClass(
    "mapreduce.input.pathFilter.class",
    classOf[HoodieROTablePathFilter],
    classOf[org.apache.hadoop.fs.PathFilter])
  conf.setStrings("mapreduce.input.fileinputformat.inputdir", basePath)
  conf.setStrings("mapreduce.input.fileinputformat.input.dir.recursive", "true")
  conf.setStrings("hoodie.realtime.last.commit", lastInstant.getTimestamp)

  private val hoodieInputFormat = new HoodieParquetInputFormat
  hoodieInputFormat.setConf(conf)

  // List all parquet files
  private val fileStatus = hoodieInputFormat.listStatus(new JobConf(conf))

  val (parquetPaths, parquetWithLogPaths) = if (lastInstant.getAction.equals(HoodieTimeline.COMMIT_ACTION)
    || lastInstant.getAction.equals(HoodieTimeline.COMPACTION_ACTION)) {
    (fileStatus.map(f => f.getPath.toString).toList, Map.empty[String, String])
  } else {
    val fileGroups = HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(conf, util.Arrays.stream(fileStatus)).asScala
    // Split the file group to: parquet file without a matching log file, parquet file need to merge with log files
    val parquetPaths: List[String] = fileGroups.filter(p => p._2.size() == 0).keys.toList
    val parquetWithLogPaths: Map[String, String] = fileGroups
      .filter(p => p._2.size() > 0)
      .map{ case(k, v) => (k, v.asScala.toList.mkString(","))}
      .toMap
    (parquetPaths, parquetWithLogPaths)
  }

  if (log.isDebugEnabled) {
    log.debug("Stand alone parquet files: \n" + parquetPaths.mkString("\n"))
    log.debug("Parquet files that have matching log files: \n" + parquetWithLogPaths.map(m => s"${m._1}:${m._2}").mkString("\n"))
  }

  // Add log file map to options
  private val finalOps = optParams ++ parquetWithLogPaths

  // use schema from latest metadata, if not present, read schema from the data file
  private val latestSchema = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    val tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchemaWithoutMetadataFields);
    AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)
  }

  override def schema: StructType = latestSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (parquetWithLogPaths.isEmpty) {
      sqlContext
        .read
        .options(finalOps)
        .schema(schema)
        .format("parquet")
        .load(parquetPaths:_*)
        .selectExpr(requiredColumns:_*)
        .rdd
    } else {
      val regularParquet = sqlContext
        .read
        .options(finalOps)
        .schema(schema)
        .format("parquet")
        .load(parquetPaths:_*)
      // Hudi parquet files needed to merge with log file
      sqlContext
        .read
        .options(finalOps)
        .schema(schema)
        .format("org.apache.spark.sql.execution.datasources.parquet.HoodieParquetRealtimeFileFormat")
        .load(parquetWithLogPaths.keys.toList: _*)
        .union(regularParquet)
        .rdd
    }
  }
}
