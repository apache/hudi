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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.joda.time.DateTime

import java.io.IOException
import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ValidateHoodieSyncProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "src_table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "dst_table", DataTypes.StringType, None),
    ProcedureParameter.required(2, "mode", DataTypes.StringType, "complete"),
    ProcedureParameter.required(3, "hive_server_url", DataTypes.StringType, None),
    ProcedureParameter.required(4, "hive_pass", DataTypes.StringType, None),
    ProcedureParameter.optional(5, "src_db", DataTypes.StringType, "rawdata"),
    ProcedureParameter.optional(6, "target_db", DataTypes.StringType, "dwh_hoodie"),
    ProcedureParameter.optional(7, "partition_cnt", DataTypes.IntegerType, 5),
    ProcedureParameter.optional(8, "hive_user", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def build: Procedure = new ValidateHoodieSyncProcedure()

  /**
   * Returns the input parameters of this procedure.
   */
  override def parameters: Array[ProcedureParameter] = PARAMETERS

  /**
   * Returns the type of rows produced by this procedure.
   */
  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val srcTable = getArgValueOrDefault(args, PARAMETERS(0))
    val dstTable = getArgValueOrDefault(args, PARAMETERS(1))
    val mode = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val hiveServerUrl = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val hivePass = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]

    val srcDb = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val tgtDb = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val partitionCount = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[Integer]
    val hiveUser = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[String]

    val srcBasePath = getBasePath(srcTable, Option.empty)
    val dstBasePath = getBasePath(dstTable, Option.empty)

    val srcMetaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(srcBasePath).build
    val targetMetaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(dstBasePath).build

    val targetTimeline = targetMetaClient.getActiveTimeline.getCommitsTimeline
    val sourceTimeline = srcMetaClient.getActiveTimeline.getCommitsTimeline

    var sourceCount: Long = 0
    var targetCount: Long = 0

    if ("complete".equals(mode)) {
      sourceCount = countRecords(hiveServerUrl, srcMetaClient, srcDb, hiveUser, hivePass)
      targetCount = countRecords(hiveServerUrl, targetMetaClient, tgtDb, hiveUser, hivePass);
    } else if ("latestPartitions".equals(mode)) {
      sourceCount = countRecords(hiveServerUrl, srcMetaClient, srcDb, partitionCount, hiveUser, hivePass)
      targetCount = countRecords(hiveServerUrl, targetMetaClient, tgtDb, partitionCount, hiveUser, hivePass)
    } else {
      logError(s"Unsupport mode $mode")
    }

    val targetLatestCommit =
      if (targetTimeline.getInstants.iterator().hasNext) targetTimeline.lastInstant().get().getTimestamp else "0"
    val sourceLatestCommit =
      if (sourceTimeline.getInstants.iterator().hasNext) sourceTimeline.lastInstant().get().getTimestamp else "0"

    if (sourceLatestCommit != null
      && HoodieTimeline.compareTimestamps(targetLatestCommit, HoodieTimeline.GREATER_THAN, sourceLatestCommit))
      Seq(Row(getString(targetMetaClient, targetTimeline, srcMetaClient, sourceCount, targetCount, sourceLatestCommit)))
    else
      Seq(Row(getString(srcMetaClient, sourceTimeline, targetMetaClient, targetCount, sourceCount, targetLatestCommit)))
  }

  def getString(target: HoodieTableMetaClient, targetTimeline: HoodieTimeline, source: HoodieTableMetaClient,
                sourceCount: Long, targetCount: Long, sourceLatestCommit: String): String = {

    val commitsToCatchup: List[HoodieInstant] =
      targetTimeline.findInstantsAfter(sourceLatestCommit, Integer.MAX_VALUE).getInstants.iterator().asScala.toList
    if (commitsToCatchup.isEmpty) {
      s"Count difference now is count(${target.getTableConfig.getTableName}) - count(${source.getTableConfig.getTableName}) == ${targetCount - sourceCount}"
    } else {
      val newInserts = countNewRecords(target, commitsToCatchup.map(elem => elem.getTimestamp))
      s"Count difference now is count(${target.getTableConfig.getTableName}) - count(${source.getTableConfig.getTableName}) == ${targetCount - sourceCount}" +
        s". Catach up count is $newInserts"
    }
  }

  def countRecords(jdbcUrl: String, source: HoodieTableMetaClient, dbName: String, user: String, pass: String): Long = {
    var conn: Connection = null
    var rs: ResultSet = null
    var count: Long = -1
    try {
      conn = DriverManager.getConnection(jdbcUrl, user, pass)
      val stmt = conn.createStatement()

      stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
      stmt.execute("set hive.stats.autogather=false");

      rs = stmt.executeQuery(
        s"select count(`_hoodie_commit_time`) as cnt from $dbName.${source.getTableConfig.getTableName}")
      if (rs.next()) {
        count = rs.getLong("cnt")
      }

      println(s"Total records in ${source.getTableConfig.getTableName} is $count")
    } finally {
      conn.close()
      if (rs != null) {
        rs.close()
      }
    }
    count
  }

  @throws[SQLException]
  def countRecords(jdbcUrl: String, source: HoodieTableMetaClient, srcDb: String, partitions: Int, user: String, pass: String): Long = {
    def getDate(dateTime: DateTime): String = {
      s"${dateTime.getYear}-${dateTime.getMonthOfYear}%02d-${dateTime.getDayOfMonth}%02d"
    }
    var dateTime = DateTime.now
    val endDateStr = getDate(dateTime)
    dateTime = dateTime.minusDays(partitions)
    val startDateStr = getDate(dateTime)
    println("Start date " + startDateStr + " and end date " + endDateStr)
    countRecords(jdbcUrl, source, srcDb, startDateStr, endDateStr, user, pass)

  }

  @throws[SQLException]
  private def countRecords(jdbcUrl: String, source: HoodieTableMetaClient, srcDb: String, startDateStr: String, endDateStr: String, user: String, pass: String): Long = {
    var rs: ResultSet = null
    try {
      val conn = DriverManager.getConnection(jdbcUrl, user, pass)
      val stmt = conn.createStatement
      try { // stmt.execute("set mapred.job.queue.name=<queue_name>");
        stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat")
        stmt.execute("set hive.stats.autogather=false")
        rs = stmt.executeQuery(s"select count(`_hoodie_commit_time`) as cnt from $srcDb.${source.getTableConfig.getTableName} where datestr>'$startDateStr' and datestr<='$endDateStr'")
        if (rs.next)
          rs.getLong("cnt")
        else
          -1
      } finally {
        if (rs != null) rs.close()
        if (conn != null) conn.close()
        if (stmt != null) stmt.close()
      }
    }
  }

  @throws[IOException]
  def countNewRecords(target: HoodieTableMetaClient, commitsToCatchup: List[String]): Long = {
    var totalNew: Long = 0
    val timeline: HoodieTimeline = target.reloadActiveTimeline.getCommitTimeline.filterCompletedInstants
    for (commit <- commitsToCatchup) {
      val c: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commit)).get, classOf[HoodieCommitMetadata])
      totalNew += c.fetchTotalRecordsWritten - c.fetchTotalUpdateRecordsWritten
    }
    totalNew
  }
}

object ValidateHoodieSyncProcedure {
  val NAME = "sync_validate"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ValidateHoodieSyncProcedure()
  }
}
