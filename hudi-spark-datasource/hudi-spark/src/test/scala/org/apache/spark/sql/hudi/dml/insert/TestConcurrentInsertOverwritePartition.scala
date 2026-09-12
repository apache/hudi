/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.common.model.{HoodieReplaceCommitMetadata, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.HoodieWriteConflictException
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Two SQL sessions run INSERT OVERWRITE of the same, still empty, partition of a MOR table at the same time,
 * the way a retried daily load does when the previous attempt is still running.
 */
class TestConcurrentInsertOverwritePartition extends HoodieSparkSqlTestBase {

  private val targetPartition = "2026-08-04"
  private val seededPartition = "2026-08-03"
  private val rowsPerWriter = 200

  test("Concurrent INSERT OVERWRITE of the same empty partition under OCC") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           | create table $tableName (
           |   rank_source_id int,
           |   locode string,
           |   device string,
           |   normalized_query string,
           |   serp_item_position int,
           |   url string,
           |   modeled_ingested_at long,
           |   day string
           | ) using hudi
           | partitioned by (day)
           | tblproperties (
           |   type = 'mor',
           |   primaryKey = 'rank_source_id,locode,device,normalized_query,serp_item_position',
           |   preCombineField = 'modeled_ingested_at',
           |   hoodie.datasource.write.hive_style_partitioning = 'true',
           |   hoodie.metadata.enable = 'true'
           | )
           | location '$basePath'
       """.stripMargin)
      // Seed an unrelated partition so the table has a completed commit older than both overwrites. On a
      // brand-new table the loser's instant can sort below the first completed commit and its uncleaned
      // files would then be read as if they belonged to an archived commit, hiding the outcome under test.
      spark.sql(
        s"""
           | insert into $tableName partition (day = '$seededPartition')
           | values (0, 'US', 'desktop', 'seed', 1, 'https://example.com/seed', 0L)
       """.stripMargin)
      // Source data lives in one file so each INSERT OVERWRITE runs the gate UDF in exactly one task.
      val sourceTable = generateTableName
      spark.sql(s"create table $sourceTable using parquet as select id from range(0, $rowsPerWriter, 1, 1)")

      val pool = Executors.newFixedThreadPool(2)
      val writers = try {
        Seq("writer_1", "writer_2").map(name => pool.submit(() => Try {
          runInsertOverwrite(name, tableName, sourceTable)
        })).map(_.get(5, TimeUnit.MINUTES))
      } finally {
        pool.shutdownNow()
      }
      assertWriterOutcome(writers, basePath, tableName)
    }
  }

  private def runInsertOverwrite(name: String, tableName: String, sourceTable: String): String = {
    val session = spark.newSession()
    session.udf.register("wait_for_other_writer", (id: Long) => {
      TestConcurrentInsertOverwritePartition.bothWritersPlanned.countDown()
      // Release only once both statements have started their write stage, i.e. both have
      // initialised their table view and planned the overwrite against an empty partition.
      assert(TestConcurrentInsertOverwritePartition.bothWritersPlanned.await(2, TimeUnit.MINUTES),
        "the other writer never reached its write stage")
      id
    })
    occSqlConf.foreach { case (k, v) => session.sql(s"set $k=$v") }
    session.sql(
      s"""
         | insert overwrite table $tableName partition (day = '$targetPartition')
         | select cast(id as int), 'US', 'desktop', concat('q', id), 1, concat('https://example.com/', id),
         |        wait_for_other_writer(id) as modeled_ingested_at
         | from $sourceTable
     """.stripMargin)
    name
  }

  private def assertWriterOutcome(writers: Seq[Try[String]], basePath: String, tableName: String): Unit = {
    val metaClient = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())).setBasePath(basePath).build()
    val completedOverwrites = metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.asScala
      .map(i => (i, metaClient.getActiveTimeline.readReplaceCommitMetadata(i)))
      .filter(_._2.getOperationType == WriteOperationType.INSERT_OVERWRITE)

    val rowCount = spark.sql(s"select count(*) from $tableName where day = '$targetPartition'").head().getLong(0)
    val keyCount = spark.sql(
      s"select count(distinct rank_source_id, locode, device, normalized_query, serp_item_position) from $tableName where day = '$targetPartition'")
      .head().getLong(0)

    println(s"overwrites committed=${completedOverwrites.map { case (i, md) => s"$i replaced=${md.getPartitionToReplaceFileIds}" }} " +
      s"rows=$rowCount distinctKeys=$keyCount writers=${writers.map(_.toString.take(200))}")
    assertResult(rowsPerWriter)(keyCount)
    // Exactly one overwrite wins the partition and it planned against the empty partition; the other is
    // rejected by OCC. Whether the loser planned before or after the winner committed is timing dependent
    // here, the write-client level test pins the empty-partition ordering deterministically.
    assertResult(1, s"overwrites that committed: ${completedOverwrites.map(_._1)}")(completedOverwrites.size)
    assert(completedOverwrites.head._2.getPartitionToReplaceFileIds.get(s"day=$targetPartition").isEmpty,
      "the winning overwrite replaced nothing: the partition was empty when it planned")
    assert(writers.count(_.isFailure) == 1 && writers.exists(_.failed.toOption.exists(isWriteConflict)),
      s"one writer must fail with a write conflict, got: ${writers.map(_.failed.toOption.map(_.toString))}")
    assertResult(rowsPerWriter)(rowCount)
  }

  private def isWriteConflict(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[HoodieWriteConflictException])

  private def occSqlConf: Seq[(String, String)] = Seq(
    "hoodie.write.concurrency.mode" -> "optimistic_concurrency_control",
    "hoodie.cleaner.policy.failed.writes" -> "LAZY",
    "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    "hoodie.write.lock.wait_time_ms" -> "60000",
    "hoodie.write.lock.num_retries" -> "3",
    "hoodie.timestamp.ordering.validate.enable" -> "true",
    "hoodie.table.services.enabled" -> "false"
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestConcurrentInsertOverwritePartition.bothWritersPlanned = new CountDownLatch(2)
  }
}

object TestConcurrentInsertOverwritePartition {
  @volatile var bothWritersPlanned: CountDownLatch = new CountDownLatch(2)
}
