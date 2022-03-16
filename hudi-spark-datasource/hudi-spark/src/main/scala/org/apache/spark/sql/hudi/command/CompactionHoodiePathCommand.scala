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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieTimeline}
import org.apache.hudi.common.util.{HoodieTimer, Option => HOption}
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.CompactionOperation
import org.apache.spark.sql.catalyst.plans.logical.CompactionOperation.{CompactionOperation, RUN, SCHEDULE}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

case class CompactionHoodiePathCommand(path: String,
  operation: CompactionOperation, instantTimestamp: Option[Long] = None)
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(sparkSession.sessionState.newHadoopConf()).build()

    assert(metaClient.getTableType == HoodieTableType.MERGE_ON_READ,
      s"Must compaction on a Merge On Read table.")
    val client = HoodieCLIUtils.createHoodieClientFromPath(sparkSession, path, Map.empty)

    operation match {
      case SCHEDULE =>
        val instantTime = instantTimestamp.map(_.toString).getOrElse(HoodieActiveTimeline.createNewInstantTime)
        if (client.scheduleCompactionAtInstant(instantTime, HOption.empty[java.util.Map[String, String]])) {
          Seq(Row(instantTime))
        } else {
          Seq.empty[Row]
        }
      case RUN =>
        // Do compaction
        val timeLine = metaClient.getActiveTimeline
         val pendingCompactionInstants = timeLine.getWriteTimeline.getInstants.iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.COMPACTION_ACTION)
           .map(_.getTimestamp)
          .toSeq.sortBy(f => f)
        val willCompactionInstants = if (instantTimestamp.isEmpty) {
           if (pendingCompactionInstants.nonEmpty) {
             pendingCompactionInstants
           } else { // If there are no pending compaction, schedule to generate one.
             // CompactionHoodiePathCommand will return instanceTime for SCHEDULE.
             val scheduleSeq = CompactionHoodiePathCommand(path, CompactionOperation.SCHEDULE).run(sparkSession)
             if (scheduleSeq.isEmpty) {
               Seq.empty
             } else {
               Seq(scheduleSeq.take(1).get(0).getString(0)).filter(_ != null)
             }
           }
        } else {
          // Check if the compaction timestamp has exists in the pending compaction
          if (pendingCompactionInstants.contains(instantTimestamp.get.toString)) {
            Seq(instantTimestamp.get.toString)
          } else {
            throw new IllegalArgumentException(s"Compaction instant: ${instantTimestamp.get} is not found in $path," +
              s" Available pending compaction instants are: ${pendingCompactionInstants.mkString(",")} ")
          }
        }
        if (willCompactionInstants.isEmpty) {
          logInfo(s"No need to compaction on $path")
          Seq.empty[Row]
        } else {
          logInfo(s"Run compaction at instants: [${willCompactionInstants.mkString(",")}] on $path")
          val timer = new HoodieTimer
          timer.startTimer()
          willCompactionInstants.foreach {compactionInstant =>
            val writeResponse = client.compact(compactionInstant)
            handleResponse(writeResponse.getCommitMetadata.get())
            client.commitCompaction(compactionInstant, writeResponse.getCommitMetadata.get(), HOption.empty())
          }
          logInfo(s"Finish Run compaction at instants: [${willCompactionInstants.mkString(",")}]," +
            s" spend: ${timer.endTimer()}ms")
          Seq.empty[Row]
        }
      case _=> throw new UnsupportedOperationException(s"Unsupported compaction operation: $operation")
    }
  }

  private def handleResponse(metadata: HoodieCommitMetadata): Unit = {

    // Handle error
    val writeStats = metadata.getPartitionToWriteStats.entrySet().flatMap(e => e.getValue).toList
    val errorsCount = writeStats.map(state => state.getTotalWriteErrors).sum
    if (errorsCount > 0) {
      throw new HoodieException(s" Found $errorsCount when writing record")
    }
  }

  override val output: Seq[Attribute] = {
    operation match {
      case RUN => Seq.empty
      case SCHEDULE => Seq(AttributeReference("instant", StringType, nullable = false)())
    }
  }
}
