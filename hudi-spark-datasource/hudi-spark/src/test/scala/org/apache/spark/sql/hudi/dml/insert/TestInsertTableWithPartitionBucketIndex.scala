/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.common.model.{HoodieFailedWritesCleaningPolicy, PartitionBucketIndexHashingConfig}
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class TestInsertTableWithPartitionBucketIndex extends HoodieSparkSqlTestBase {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Write operation including :
   * Case1: Mor + bulk_insert + asRow enable + simple bucket index
   * Case2: Mor + bulk_insert + asRow disable + simple bucket index
   * Case3: Cow + bulk_insert + asRow enable + simple bucket index
   * Case4: Cow + bulk_insert + asRow disable + simple bucket index
   * Case5: Mor + bulk_insert + asRow enable + partition level bucket index
   * Case6: Mor + bulk_insert + asRow disable + partition level bucket index
   * Case7: Cow + bulk_insert + asRow enable + partition level bucket index
   * Case8: Cow + bulk_insert + asRow disable + partition level bucket index
   *
   * Total WorkFlow are as followed:
   * Step1: Create a new table using simple bucket index
   * Step2: Upgrade table to partition level bucket index using `call partition_bucket_index_manager`
   * Step3: Trigger another write operation through partition level bucket index
   */
  test("Test Bulk Insert Into Partition Bucket Index Table With Rescale") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          Seq("true", "false").foreach { bulkInsertAsRow =>
            withTable(generateTableName) { tableName =>
              val tablePath = s"""${tmp.getCanonicalPath}/$tableName"""
              // Create a partitioned table
              spark.sql(
                s"""
                   |create table $tableName (
                   |  id int,
                   |  dt string,
                   |  name string,
                   |  price double,
                   |  ts long
                   |) using hudi
                   | tblproperties (
                   | primaryKey = 'id,name',
                   | type = '$tableType',
                   | preCombineField = 'ts',
                   | hoodie.index.type = 'BUCKET',
                   | hoodie.bucket.index.hash.field = 'id,name',
                   | hoodie.bucket.index.num.buckets = 1,
                   | hoodie.datasource.write.row.writer.enable = '$bulkInsertAsRow')
                   | partitioned by (dt)
                   | location '${tablePath}'
                   | """.stripMargin)

              // Note: Do not write the field alias, the partition field must be placed last.
              spark.sql(
                s"""
                   | insert into $tableName values
                   | (1, 'a1,1', 10, 1000, "2021-01-05"),
                   | (11, 'a1,1', 10, 1000, "2021-01-05"),
                   | (2, 'a2', 20, 2000, "2021-01-06"),
                   | (22, 'a2', 20, 2000, "2021-01-06")
                   | """.stripMargin)

              // upgrade to partition level bucket index and rescale dt=2021-01-05 from 1 to 2
              val expressions = "dt=(2021\\-01\\-05|2021\\-01\\-07),2"
              val rule = "regex"
              val defaultBucketNumber = 1
              spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

              checkAnswer(s"select id, name, price, ts, dt from $tableName")(
                Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
                Seq(11, "a1,1", 10.0, 1000, "2021-01-05"),
                Seq(2, "a2", 20.0, 2000, "2021-01-06"),
                Seq(22, "a2", 20.0, 2000, "2021-01-06")
              )

              // insert into new partition 2021-01-07 with partition level bucket index
              spark.sql(
                s"""
                   | insert into $tableName values
                   | (3, 'a3,3', 30, 3000, "2021-01-07"),
                   | (33, 'a3,3', 30, 3000, "2021-01-07")
                   | """.stripMargin)

              checkAnswer(s"select id, name, price, ts, dt from $tableName")(
                Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
                Seq(11, "a1,1", 10.0, 1000, "2021-01-05"),
                Seq(2, "a2", 20.0, 2000, "2021-01-06"),
                Seq(22, "a2", 20.0, 2000, "2021-01-06"),
                Seq(3, "a3,3", 30.0, 3000, "2021-01-07"),
                Seq(33, "a3,3", 30.0, 3000, "2021-01-07")
              )
              val metaClient = createMetaClient(spark, tablePath)
              val actual: List[String] = PartitionBucketIndexUtils.getAllFileIDWithPartition(metaClient).asScala.toList
              val expected: List[String] = List("dt=2021-01-05" + "00000000",
                "dt=2021-01-05" + "00000000",
                "dt=2021-01-05" + "00000001",
                "dt=2021-01-06" + "00000000",
                "dt=2021-01-07" + "00000000",
                "dt=2021-01-07" + "00000001")
              assert(actual.sorted == expected.sorted)
            }
          }
        }
      }
    }
  }

  test("Test Bucket Rescale Dry Run") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2") {
      withTempDir { tmp =>
        withTable(generateTableName) { tableName =>
          val tablePath = s"""${tmp.getCanonicalPath}/$tableName"""
          // Create a partitioned table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  dt string,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               | primaryKey = 'id,name',
               | type = 'mor',
               | preCombineField = 'ts',
               | hoodie.index.type = 'BUCKET',
               | hoodie.bucket.index.hash.field = 'id,name',
               | hoodie.bucket.index.num.buckets = 1,
               | hoodie.datasource.write.row.writer.enable = 'true')
               | partitioned by (dt)
               | location '${tablePath}'
               | """.stripMargin)

          // Note: Do not write the field alias, the partition field must be placed last.
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 10, 1000, "2021-01-05"),
               | (11, 'a1,1', 10, 1000, "2021-01-05"),
               | (2, 'a2', 20, 2000, "2021-01-06"),
               | (22, 'a2', 20, 2000, "2021-01-06")
               | """.stripMargin)

          // upgrade to partition level bucket index and rescale dt=2021-01-05 from 1 to 2
          val expressions = "dt=(2021\\-01\\-05|2021\\-01\\-08),2"
          val rule = "regex"
          val defaultBucketNumber = 1
          val sql = s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber)"
          val resExpect =
            s"""
               |dt=2021-01-05 => 2
               |""".stripMargin

          checkAnswer(sql)(
            Seq("SUCCESS", "DRY_RUN_OVERWRITE", resExpect)
          )

          val metaClient = createMetaClient(spark, tablePath)
          // check there is no active hashing config
          assert(!metaClient.getStorage.exists(new StoragePath(metaClient.getHashingMetadataConfigPath)))
    }}}}

  test("Test Bulk Insert Into Partition Bucket Index Table Without Rescale") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2") {
      withTempDir { tmp =>
        withTable(generateTableName) { inputTable =>
          Seq("true").foreach { bulkInsertAsRow =>
            val tableName = inputTable
            val tablePath = s"""${tmp.getCanonicalPath}/$inputTable"""
            // Create a partitioned table
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  dt string,
                 |  name string,
                 |  price double,
                 |  ts long
                 |) using hudi
                 | tblproperties (
                 | primaryKey = 'id,name',
                 | preCombineField = 'ts',
                 | hoodie.index.type = 'BUCKET',
                 | hoodie.bucket.index.hash.field = 'id,name',
                 | hoodie.bucket.index.num.buckets = 1,
                 | hoodie.datasource.write.row.writer.enable = '$bulkInsertAsRow')
                 | partitioned by (dt)
                 | location '${tablePath}'
                 | """.stripMargin)

            // Note: Do not write the field alias, the partition field must be placed last.
            spark.sql(
              s"""
                 | insert into $tableName values
                 | (1, 'a1,1', 10, 1000, "2021-01-05"),
                 | (11, 'a1,1', 10, 1000, "2021-01-05"),
                 | (2, 'a2', 20, 2000, "2021-01-06"),
                 | (22, 'a2', 20, 2000, "2021-01-06")
                 | """.stripMargin)

            // upgrade to partition level bucket index without rescale
            val expressions = "dt=2021\\-01\\-07,2"
            val rule = "regex"
            val defaultBucketNumber = 1
            spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

            checkAnswer(s"select id, name, price, ts, dt from $tableName")(
              Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(11, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(2, "a2", 20.0, 2000, "2021-01-06"),
              Seq(22, "a2", 20.0, 2000, "2021-01-06")
            )

            // insert into new partition 2021-01-07 with partition level bucket index
            spark.sql(
              s"""
                 | insert into $tableName values
                 | (3, 'a3,3', 30, 3000, "2021-01-07"),
                 | (33, 'a3,3', 30, 3000, "2021-01-07")
                 | """.stripMargin)

            checkAnswer(s"select id, name, price, ts, dt from $tableName")(
              Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(11, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(2, "a2", 20.0, 2000, "2021-01-06"),
              Seq(22, "a2", 20.0, 2000, "2021-01-06"),
              Seq(3, "a3,3", 30.0, 3000, "2021-01-07"),
              Seq(33, "a3,3", 30.0, 3000, "2021-01-07")
            )
            val metaClient = createMetaClient(spark, tablePath)
            val actual: List[String] = PartitionBucketIndexUtils.getAllFileIDWithPartition(metaClient).asScala.toList
            val expected: List[String] = List("dt=2021-01-05" + "00000000",
              "dt=2021-01-06" + "00000000",
              "dt=2021-01-07" + "00000000",
              "dt=2021-01-07" + "00000001")
            assert(actual.sorted == expected.sorted)
          }
        }
      }
    }
  }

  /**
   * Write operation including :
   * Case1: Mor + upsert + simple bucket index
   * Case2: Cow + upsert + simple bucket index
   * Case3: Mor + upsert + partition level bucket index
   * Case4: Cow + upsert + partition level bucket index
   *
   * Total WorkFlow are as followed:
   * Step1: Create a new table using simple bucket index
   * Step2: Upgrade table to partition level bucket index using `call partition_bucket_index_manager`
   * Step3: Trigger another write operation through partition level bucket index
   *
   *
   */
  test("Test Upsert Into Partition Bucket Index Table") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "upsert") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { tableName =>
            val tablePath = tmp.getCanonicalPath + "/" + tableName
            // Create a partitioned table
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  dt string,
                 |  name string,
                 |  price double,
                 |  ts long
                 |) using hudi
                 | tblproperties (
                 | primaryKey = 'id,name',
                 | type = '$tableType',
                 | preCombineField = 'ts',
                 | hoodie.index.type = 'BUCKET',
                 | hoodie.bucket.index.hash.field = 'id,name',
                 | hoodie.bucket.index.num.buckets = 1)
                 | partitioned by (dt)
                 | location '$tablePath'
                 | """.stripMargin)

            // Note: Do not write the field alias, the partition field must be placed last.
            spark.sql(
              s"""
                 | insert into $tableName values
                 | (1, 'a1,1', 10, 1000, "2021-01-05"),
                 | (11, 'a1,1', 10, 1000, "2021-01-05"),
                 | (2, 'a2', 20, 2000, "2021-01-06"),
                 | (22, 'a2', 20, 2000, "2021-01-06")
                 | """.stripMargin)

            // upgrade to partition level bucket index and rescale dt=2021-01-05 from 1 to 2
            val expressions = "dt=(2021\\-01\\-05|2021\\-01\\-07),2"
            val rule = "regex"
            val defaultBucketNumber = 1
            spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

            checkAnswer(s"select id, name, price, ts, dt from $tableName")(
              Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(11, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(2, "a2", 20.0, 2000, "2021-01-06"),
              Seq(22, "a2", 20.0, 2000, "2021-01-06")
            )

            // insert into new partition 2021-01-07 with partition level bucket index
            // update (1, 'a1,1', 10, 1000, "2021-01-05") to (1, 'a1,1', 100, 1000, "2021-01-05")
            spark.sql(
              s"""
                 | insert into $tableName values
                 | (1, 'a1,1', 100, 1000, "2021-01-05"),
                 | (3, 'a3,3', 30, 3000, "2021-01-07"),
                 | (33, 'a3,3', 30, 3000, "2021-01-07")
                 | """.stripMargin)

            checkAnswer(s"select id, name, price, ts, dt from $tableName")(
              Seq(1, "a1,1", 100.0, 1000, "2021-01-05"),
              Seq(11, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(2, "a2", 20.0, 2000, "2021-01-06"),
              Seq(22, "a2", 20.0, 2000, "2021-01-06"),
              Seq(3, "a3,3", 30.0, 3000, "2021-01-07"),
              Seq(33, "a3,3", 30.0, 3000, "2021-01-07")
            )
            val metaClient = createMetaClient(spark, tablePath)
            val actual: List[String] = PartitionBucketIndexUtils.getAllFileIDWithPartition(metaClient).asScala.toList
            val expected: List[String] = List("dt=2021-01-05" + "00000000",
              "dt=2021-01-05" + "00000000",
              "dt=2021-01-05" + "00000001",
              "dt=2021-01-06" + "00000000",
              "dt=2021-01-07" + "00000000",
              "dt=2021-01-07" + "00000001")
            // compare file group as expected
            assert(actual.sorted == expected.sorted)
          }
        }
      }
    }
  }

  test("Test BucketID Pruning With Partition Bucket Index") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "upsert") {
      withTempDir { tmp =>
        withTable(generateTableName) { tableName =>
          val tablePath = tmp.getCanonicalPath + "/" + tableName
          val defaultBucketNumber = 2
          // Create a partitioned table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  dt string,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               | primaryKey = 'id',
               | type = 'mor',
               | preCombineField = 'ts',
               | hoodie.index.type = 'BUCKET',
               | hoodie.bucket.index.hash.field = 'id',
               | hoodie.bucket.index.num.buckets = $defaultBucketNumber)
               | partitioned by (dt)
               | location '$tablePath'
               | """.stripMargin)

          // Note: Do not write the field alias, the partition field must be placed last.
          // do commit 1
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 1.0, 1, "2021-01-05"),
               | (11, 'a11,11', 11.0, 11, "2021-01-05"),
               | (111, 'a111', 111.0, 111, "2021-01-05"),
               | (1111, 'a1111', 1111.0, 1111, "2021-01-05")
               | """.stripMargin)

          // upgrade to partition level bucket index and rescale dt=2021-01-05 from 1 to 2
          // do bucket rescale commit 2
          val expressions = "dt=(2021\\-01\\-05|2021\\-01\\-07),4"
          val rule = "regex"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          // do commit 3 update id = 1111
          spark.sql(
            s"""
               | insert into $tableName values
               | (1111, 'a2222,2222', 2222.0, 2222, "2021-01-05")
               | """.stripMargin)

          // do bucket rescale commit 4
          val expressions2 = "dt=(2021\\-01\\-05|2021\\-01\\-07),3"
          val rule2 = "regex"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions2', rule => '$rule2', bucket_number => $defaultBucketNumber, dry_run => false)")

          // do commit 5 update id = 1111
          spark.sql(
            s"""
               | insert into $tableName values
               | (1111, 'a3333,3333', 3333.0, 3333, "2021-01-05")
               | """.stripMargin)
          val metaClient = createMetaClient(spark, tablePath)
          val committedInstant = metaClient.getActiveTimeline.filterCompletedInstants().getInstants
          val size = committedInstant.size()

          val writeCommit5 = committedInstant.get(size - 1)
          val bucketRescaleCommit4 = committedInstant.get(size - 2)
          val writeCommit3 = committedInstant.get(size - 3)
          val bucketRescaleCommit2 = committedInstant.get(size - 4)
          val writeCommit1 = committedInstant.get(size - 5)

          // normal query with bucket id pruning at latest writeCommit5
          checkAnswer(s"select id, price, ts, dt from $tableName where id = '1111'")(
            Seq(1111, 3333.0, 3333, "2021-01-05")
          )
          // time travel to writeCommit3
          checkAnswer(s"select id, price, ts, dt from $tableName timestamp as of '${writeCommit3.requestedTime()}' where id = '1111'")(
            Seq(1111, 2222.0, 2222, "2021-01-05")
          )
          // time travel to writeCommit1
          checkAnswer(s"select id, price, ts, dt from $tableName timestamp as of '${writeCommit1.requestedTime()}' where id = '1111'")(
            Seq(1111, 1111.0, 1111, "2021-01-05")
          )

          // incremental query with bucket id pruning
          checkAnswer(s"select id, price, ts, dt from hudi_table_changes('$tableName', 'latest_state', '${bucketRescaleCommit2.getCompletionTime}', '${writeCommit3.getCompletionTime}') where id = '1111'")(
            Seq(1111, 2222.0, 2222, "2021-01-05")
          )
        }
      }
    }
  }

  test("Test Rollback Hashing Config") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.rollback.using.markers" -> "false",
      "hoodie.clean.automatic" -> "false",
      "hoodie.metadata.enable" -> "false") {
      withTempDir { tmp =>
        Seq("EAGER", "LAZY").foreach { lazyClean =>
          withTable(generateTableName) { tableName =>
            val tablePath = tmp.getCanonicalPath + "/" + tableName
            // Create a partitioned table
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  dt string,
                 |  name string,
                 |  price double,
                 |  ts long
                 |) using hudi
                 | tblproperties (
                 | primaryKey = 'id,name',
                 | type = 'cow',
                 | preCombineField = 'ts',
                 | hoodie.index.type = 'BUCKET',
                 | hoodie.bucket.index.hash.field = 'id,name',
                 | hoodie.clean.failed.writes.policy = '$lazyClean',
                 | hoodie.cleaner.policy.failed.writes = '$lazyClean',
                 | hoodie.bucket.index.num.buckets = 1)
                 | partitioned by (dt)
                 | location '$tablePath'
                 | """.stripMargin)

            // Note: Do not write the field alias, the partition field must be placed last.
            spark.sql(
              s"""
                 | insert into $tableName values
                 | (1, 'a1,1', 10, 1000, "2021-01-05"),
                 | (11, 'a1,1', 10, 1000, "2021-01-05"),
                 | (2, 'a2', 20, 2000, "2021-01-06"),
                 | (22, 'a2', 20, 2000, "2021-01-06")
                 | """.stripMargin)
            val metaClient = createMetaClient(spark, tablePath)

            // upgrade to partition level bucket index and rescale dt=2021-01-05 from 1 to 2
            val expressions = "dt=(2021\\-01\\-05|2021\\-01\\-07),2"
            val rule = "regex"
            val defaultBucketNumber = 1
            spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

            val expressions2 = "dt=(2021\\-01\\-05|2021\\-01\\-07),3"
            spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions2', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

            // delete latest replace commit
            val replaceCommit = metaClient.getActiveTimeline.getCompletedReplaceTimeline.lastInstant().get()
            metaClient.getActiveTimeline.deleteInstantFileIfExists(replaceCommit)

            // do another insert
            // if eager clean, hoodie will rollback replace and related hashing_config
            // if lazy clean, hoodie will pick committed hashing_config
            spark.sql(
              s"""
                 | insert into $tableName values
                 | (3, 'a3,3', 30, 3000, "2021-01-07"),
                 | (33, 'a3,3', 30, 3000, "2021-01-07"),
                 | (333, 'a3,3', 30, 3000, "2021-01-07")
                 | """.stripMargin)

            // only take cares of partition dt=2021-01-07
            val actual: List[String] = PartitionBucketIndexUtils.getAllFileIDWithPartition(metaClient).asScala.filter(path => {
              path.contains("2021-01-07")
            }).toList

            // expressions2 is rollback or uncommitted, so that only expressions1 works, which means dt=2021-01-07 has two bucket numbers.
            val expected: List[String] = List(
              "dt=2021-01-07" + "00000000",
              "dt=2021-01-07" + "00000001")
            // compare file group as expected
            assert(actual.sorted == expected.sorted)

            if (lazyClean.equalsIgnoreCase(HoodieFailedWritesCleaningPolicy.EAGER.name())) {
              // deleted replaceCommit relate hashing_config is not existed (rollback)
              assert(!PartitionBucketIndexHashingConfig.isHashingConfigExisted(replaceCommit, metaClient))
            } else {
              // expressions2 is rollback, so that expressions works, that dt=2021-01-07 has two bucket numbers.
              // uncommitted hashing config is existed, but take no works.
              assert(PartitionBucketIndexHashingConfig.isHashingConfigExisted(replaceCommit, metaClient))
            }
          }
        }
      }
    }
  }

  test("Test Hashing Config Archive") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.clean.commits.retained" -> "1",
      "hoodie.keep.min.commits" -> "2",
      "hoodie.keep.max.commits" -> "3") {
      withTempDir { tmp =>
        withTable(generateTableName) { tableName =>
          val tablePath = tmp.getCanonicalPath + "/" + tableName
          // Create a partitioned table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  dt string,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               | primaryKey = 'id,name',
               | type = 'cow',
               | preCombineField = 'ts',
               | hoodie.index.type = 'BUCKET',
               | hoodie.bucket.index.hash.field = 'id,name',
               | hoodie.bucket.index.num.buckets = 1)
               | partitioned by (dt)
               | location '$tablePath'
               | """.stripMargin)

          // Note: Do not write the field alias, the partition field must be placed last.
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 10, 1000, "2021-01-05"),
               | (11, 'a1,1', 10, 1000, "2021-01-05"),
               | (2, 'a2', 20, 2000, "2021-01-06"),
               | (22, 'a2', 20, 2000, "2021-01-06")
               | """.stripMargin)
          val metaClient = createMetaClient(spark, tablePath)

          // upgrade to partition level bucket index and rescale dt=2021-01-05 from 1 to 2
          val expressions = "dt=(2021\\-01\\-05|2021\\-01\\-07),2"
          val rule = "regex"
          val defaultBucketNumber = 1
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          val expressions2 = "dt=(2021\\-01\\-05|2021\\-01\\-07),3"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions2', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          val expressions3 = "dt=(2021\\-01\\-05|2021\\-01\\-07),4"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions3', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          val expressions4 = "dt=(2021\\-01\\-05|2021\\-01\\-07),5"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions4', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          val expressions5 = "dt=(2021\\-01\\-05|2021\\-01\\-07),6"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions5', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          val expressions6 = "dt=(2021\\-01\\-05|2021\\-01\\-07),6"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions6', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          assert(PartitionBucketIndexHashingConfig.getArchiveHashingConfigInstants(metaClient).size() == 2)
          assert(PartitionBucketIndexHashingConfig.getCommittedHashingConfigInstants(metaClient).size() == 4)

          // take care of showConfig command
          val expected = PartitionBucketIndexHashingConfig.getAllHashingConfig(metaClient).asScala.map(config => {
            config.toString
          }).mkString(";")
          checkAnswer(s"call partition_bucket_index_manager(table => '$tableName', show_config => true)")(
            Seq("SUCCESS", "SHOW_CONFIG", expected)
          )
        }
      }
    }
  }

  test("Test Add Expression and Rollback Command") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "upsert") {
      withTempDir { tmp =>
        withTable(generateTableName) { tableName =>
          val tablePath = tmp.getCanonicalPath + "/" + tableName
          // Create a partitioned table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  dt string,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               | primaryKey = 'id,name',
               | type = 'cow',
               | preCombineField = 'ts',
               | hoodie.index.type = 'BUCKET',
               | hoodie.bucket.index.hash.field = 'id,name',
               | hoodie.bucket.index.num.buckets = 1)
               | partitioned by (dt)
               | location '$tablePath'
               | """.stripMargin)

          // Note: Do not write the field alias, the partition field must be placed last.
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 10, 1000, "2021-01-05"),
               | (11, 'a1,1', 10, 1000, "2021-01-05"),
               | (2, 'a2', 20, 2000, "2021-01-06"),
               | (22, 'a2', 20, 2000, "2021-01-06")
               | """.stripMargin)
          val metaClient = createMetaClient(spark, tablePath)

          val expressions = "dt=(2021\\-01\\-07|2021\\-01\\-08),2"
          val rule = "regex"
          val defaultBucketNumber = 1
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucket_number => $defaultBucketNumber, dry_run => false)")

          val expressions2 = "dt=(2021\\-01\\-09|2021\\-01\\-10),3"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', add => '$expressions2', dry_run => false)")

          val actualExpression = PartitionBucketIndexHashingConfig.loadingLatestHashingConfig(metaClient).getExpressions
          val expectedExpression = s"""$expressions2;$expressions"""
          assert(actualExpression.equals(expectedExpression.replace("\\","")))
          val commit = metaClient.reloadActiveTimeline().getCompletedReplaceTimeline.lastInstant()

          // rollback latest committed hashing config
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', rollback => '${commit.get().requestedTime()}')")
          val actualExpression2 = PartitionBucketIndexHashingConfig.loadingLatestHashingConfig(metaClient).getExpressions
          assert(actualExpression2.equals(expressions.replace("\\","")))
        }
      }
    }
  }
}
