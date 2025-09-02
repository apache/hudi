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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row

import java.util.concurrent.{CountDownLatch, Executors, ExecutorService, Future, TimeUnit}

import scala.util.Try

class TestConcurrencyControlProcedures extends HoodieSparkProcedureTestBase {

  test("Test show_clustering Procedure Concurrency Control") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  partition_field string
           |) using hudi
           | options (
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  type = 'cow',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.index.column.stats.enable = 'true',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.write.concurrency.mode = 'optimistic_concurrency_control',
           |  hoodie.write.lock.provider = '${classOf[InProcessLockProvider].getName}',
           |  hoodie.write.lock.acquire.wait.timeout.ms = '5000',
           |  hoodie.write.lock.acquire.client.num.retries = '10',
           |  hoodie.clean.failed.writes.policy = 'LAZY',
           |  hoodie.datasource.write.partitionpath.field = 'partition_field'
           | )
           | location '$basePath'
           | partitioned by (partition_field)
     """.stripMargin)

      for (i <- 1 to 5) {
        spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10.0}, ${1000 + i}, 'partition1')")
      }
      for (i <- 6 to 10) {
        spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10.0}, ${1000 + i}, 'partition2')")
      }
      for (i <- 11 to 15) {
        spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10.0}, ${1000 + i}, 'partition3')")
      }

      val metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(new Configuration), basePath)
      assert(metaClient.getActiveTimeline.filterPendingClusteringTimeline().empty())

      val executor: ExecutorService = Executors.newFixedThreadPool(6)
      val latch = new CountDownLatch(6)

      try {
        val schedule1Task: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(3, TimeUnit.SECONDS)
          Try {
            spark.sql(
              s"""CALL run_clustering(
                 |  table => '$tableName',
                 |  predicate => 'partition_field="partition1"',
                 |  op => 'schedule'
                 |)""".stripMargin).collect()
          }
        })

        val schedule2Task: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(3, TimeUnit.SECONDS)
          Thread.sleep(100)
          Try {
            spark.sql(
              s"""CALL run_clustering(
                 |  table => '$tableName',
                 |  predicate => 'partition_field="partition2"',
                 |  op => 'schedule'
                 |)""".stripMargin).collect()
          }
        })

        val schedule3Task: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(3, TimeUnit.SECONDS)
          Thread.sleep(200)
          Try {
            spark.sql(
              s"""CALL run_clustering(
                 |  table => '$tableName',
                 |  predicate => 'partition_field="partition3"',
                 |  op => 'schedule'
                 |)""".stripMargin).collect()
          }
        })

        val showEarlyTask: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(3, TimeUnit.SECONDS)
          Thread.sleep(150)
          Try {
            val early = spark.sql(s"call show_clustering(table => '$tableName')")
            early.show(false)
            early.collect()
          }
        })

        val showMiddleTask: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(3, TimeUnit.SECONDS)
          Thread.sleep(250)
          Try {
            val mid = spark.sql(s"call show_clustering(table => '$tableName')")
            mid.show(false)
            mid.collect()
          }
        })

        val executeTask: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(3, TimeUnit.SECONDS)
          Thread.sleep(300)
          Try {
            spark.sql(s"call run_clustering(table => '$tableName', op => 'execute')").collect()
          }
        })

        val schedule1Result = schedule1Task.get(10, TimeUnit.SECONDS)
        val schedule2Result = schedule2Task.get(10, TimeUnit.SECONDS)
        val schedule3Result = schedule3Task.get(10, TimeUnit.SECONDS)
        val showEarlyResult = showEarlyTask.get(10, TimeUnit.SECONDS)
        val showMiddleResult = showMiddleTask.get(10, TimeUnit.SECONDS)
        val executeResult = executeTask.get(10, TimeUnit.SECONDS)

        assert(schedule1Result.isSuccess, s"Schedule 1 failed: ${schedule1Result.failed.getOrElse("Unknown")}")
        assert(schedule2Result.isSuccess, s"Schedule 2 failed: ${schedule2Result.failed.getOrElse("Unknown")}")
        assert(schedule3Result.isSuccess, s"Schedule 3 failed: ${schedule3Result.failed.getOrElse("Unknown")}")
        assert(showEarlyResult.isSuccess, s"Show early failed: ${showEarlyResult.failed.getOrElse("Unknown")}")
        assert(showMiddleResult.isSuccess, s"Show middle failed: ${showMiddleResult.failed.getOrElse("Unknown")}")
        assert(executeResult.isSuccess, s"Execute failed: ${executeResult.failed.getOrElse("Unknown")}")


        val earlyCount = showEarlyResult.get.length
        val middleCount = showMiddleResult.get.length

        Thread.sleep(400)
        val finalShowResultDf = spark.sql(s"call show_clustering(table => '$tableName')")
        finalShowResultDf.show(false)
        val finalShowResult = finalShowResultDf.collect()
        val finalCount = finalShowResult.length

        assert(middleCount > earlyCount,
          s"Middle count ($middleCount) should be >= early count ($earlyCount)")
        assert(finalCount > middleCount,
          s"Final count ($finalCount) should be >= middle count ($middleCount)")

      } finally {
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow()
        }
      }
    }
  }

  test("Test show_compaction Procedure Concurrency Control") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  partition_field string
             |) using hudi
             | options (
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  type = 'mor',
             |  hoodie.metadata.enable = 'true',
             |  hoodie.write.concurrency.mode = 'optimistic_concurrency_control',
             |  hoodie.write.lock.provider = '${classOf[InProcessLockProvider].getName}',
             |  hoodie.write.lock.acquire.wait.timeout.ms = '5000',
             |  hoodie.write.lock.acquire.client.num.retries = '10',
             |  hoodie.clean.failed.writes.policy = 'LAZY',
             |  hoodie.datasource.write.partitionpath.field = 'partition_field'
             | )
             | location '$basePath'
             | partitioned by (partition_field)
       """.stripMargin)

        for (i <- 1 to 3) {
          spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10.0}, ${1000 + i}, 'partition1')")
          spark.sql(s"update $tableName set name = 'updated$i', ts = ${1001 + i} where id = $i")
        }
        for (i <- 4 to 6) {
          spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10.0}, ${1000 + i}, 'partition2')")
          spark.sql(s"update $tableName set name = 'updated$i', ts = ${1001 + i} where id = $i")
        }
        for (i <- 7 to 10) {
          spark.sql(s"insert into $tableName values($i, 'name$i', ${i * 10.0}, ${1000 + i}, 'partition2')")
          spark.sql(s"update $tableName set name = 'updated$i', ts = ${1001 + i} where id = $i")
        }

        val executor: ExecutorService = Executors.newFixedThreadPool(4)
        val latch = new CountDownLatch(4)

        try {
          val schedule1Task: Future[Try[Array[Row]]] = executor.submit(() => {
            latch.countDown()
            latch.await(3, TimeUnit.SECONDS)
            Try {
              spark.sql(s"call run_compaction(table => '$tableName', op => 'schedule')").collect()
            }
          })

          val showEarlyTask: Future[Try[Array[Row]]] = executor.submit(() => {
            latch.countDown()
            latch.await(3, TimeUnit.SECONDS)
            Try {
              val early = spark.sql(s"call show_compaction(table => '$tableName')")
              early.show(false)
              early.collect()
            }
          })

          val showMiddleTask: Future[Try[Array[Row]]] = executor.submit(() => {
            latch.countDown()
            latch.await(3, TimeUnit.SECONDS)
            Thread.sleep(500)
            Try {
              val mid = spark.sql(s"call show_compaction(table => '$tableName')")
              mid.show(false)
              mid.collect()
            }
          })

          val executeTask: Future[Try[Array[Row]]] = executor.submit(() => {
            latch.countDown()
            latch.await(3, TimeUnit.SECONDS)
            Thread.sleep(1000)
            Try {
              spark.sql(s"call run_compaction(table => '$tableName', op => 'execute')").collect()
            }
          })

          val schedule1Result = schedule1Task.get(10, TimeUnit.SECONDS)
          val showEarlyResult = showEarlyTask.get(10, TimeUnit.SECONDS)
          val showMiddleResult = showMiddleTask.get(10, TimeUnit.SECONDS)
          val executeResult = executeTask.get(10, TimeUnit.SECONDS)

          assert(schedule1Result.isSuccess, s"Schedule 1 failed: ${schedule1Result.failed.getOrElse("Unknown")}")
          assert(showEarlyResult.isSuccess, s"Show early failed: ${showEarlyResult.failed.getOrElse("Unknown")}")
          assert(showMiddleResult.isSuccess, s"Show middle failed: ${showMiddleResult.failed.getOrElse("Unknown")}")
          assert(executeResult.isSuccess, s"Execute failed: ${executeResult.failed.getOrElse("Unknown")}")

          val earlyCount = showEarlyResult.get.length
          val middleCount = showMiddleResult.get.length

          Thread.sleep(1500)
          val finalShowResultDf = spark.sql(s"call show_compaction(table => '$tableName')")
          finalShowResultDf.show(false)
          val finalShowResult = finalShowResultDf.collect()

          assert(middleCount > earlyCount,
            s"Middle count ($middleCount) should be > early count ($earlyCount)")
          assert(!finalShowResult(0).getString(2).equals(showMiddleResult.get(0).getString(2)),
            s"Final show state is COMPLETED while middle is still REQUESTED")

        } finally {
          executor.shutdown()
          if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow()
          }
        }
      }
    }
  }

  test("Test show_commits Procedure Concurrency Control") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  partition_field string
           |) using hudi
           | options (
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  type = 'cow',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.write.concurrency.mode = 'optimistic_concurrency_control',
           |  hoodie.write.lock.provider = '${classOf[InProcessLockProvider].getName}',
           |  hoodie.write.lock.acquire.wait.timeout.ms = '5000',
           |  hoodie.write.lock.acquire.client.num.retries = '10',
           |  hoodie.clean.failed.writes.policy = 'LAZY',
           |  hoodie.datasource.write.partitionpath.field = 'partition_field'
           | )
           | location '$basePath'
           | partitioned by (partition_field)
     """.stripMargin)

      val executor: ExecutorService = Executors.newFixedThreadPool(6)
      val latch = new CountDownLatch(6)

      try {
        val insertTask1: Future[Try[Unit]] = executor.submit(() => {
          latch.countDown()
          latch.await(6, TimeUnit.SECONDS)
          Try {
            spark.sql(s"insert into $tableName values(1, 'name1', 10.0, 1000, 'partition1')")
            spark.sql(s"insert into $tableName values(2, 'name2', 20.0, 1001, 'partition1')")
          }
        })

        val insertTask2: Future[Try[Unit]] = executor.submit(() => {
          latch.countDown()
          latch.await(6, TimeUnit.SECONDS)
          Thread.sleep(2000)
          Try {
            spark.sql(s"insert into $tableName values(3, 'name3', 30.0, 1002, 'partition2')")
          }
        })

        val insertTask3: Future[Try[Unit]] = executor.submit(() => {
          latch.countDown()
          latch.await(6, TimeUnit.SECONDS)
          Thread.sleep(4000)
          Try {
            spark.sql(s"insert into $tableName values(5, 'name5', 50.0, 1004, 'partition3')")
          }
        })

        val showEarlyTask: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(6, TimeUnit.SECONDS)
          Thread.sleep(1000)
          Try {
            val early = spark.sql(s"call show_commits(table => '$tableName')")
            early.show(false)
            early.collect()
          }
        })

        val showMiddleTask: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(6, TimeUnit.SECONDS)
          Thread.sleep(3000)
          Try {
            val mid = spark.sql(s"call show_commits(table => '$tableName')")
            mid.show(false)
            mid.collect()
          }
        })

        val showLateTask: Future[Try[Array[Row]]] = executor.submit(() => {
          latch.countDown()
          latch.await(6, TimeUnit.SECONDS)
          Thread.sleep(5000)
          Try {
            val late = spark.sql(s"call show_commits(table => '$tableName')")
            late.show(false)
            late.collect()
          }
        })

        val insert1Result = insertTask1.get(15, TimeUnit.SECONDS)
        val insert2Result = insertTask2.get(15, TimeUnit.SECONDS)
        val insert3Result = insertTask3.get(15, TimeUnit.SECONDS)
        val showEarlyResult = showEarlyTask.get(15, TimeUnit.SECONDS)
        val showMiddleResult = showMiddleTask.get(15, TimeUnit.SECONDS)
        val showLateResult = showLateTask.get(15, TimeUnit.SECONDS)


        assert(showEarlyResult.isSuccess, s"Show early failed: ${showEarlyResult.failed.getOrElse("Unknown")}")
        assert(showMiddleResult.isSuccess, s"Show middle failed: ${showMiddleResult.failed.getOrElse("Unknown")}")
        assert(showLateResult.isSuccess, s"Show late failed: ${showLateResult.failed.getOrElse("Unknown")}")

        val earlyCount = showEarlyResult.get.length
        val middleCount = showMiddleResult.get.length
        val lateCount = showLateResult.get.length

        assert(middleCount > earlyCount,
          s"Middle count ($middleCount) should be >= early count ($earlyCount)")
        assert(lateCount > middleCount,
          s"Late count ($lateCount) should be >= middle count ($middleCount)")

      } finally {
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow()
        }
      }
    }
  }
}
