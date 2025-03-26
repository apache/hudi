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

package org.apache.spark.sql.hudi.dml

import org.apache.hudi.common.model.{HoodieFailedWritesCleaningPolicy, PartitionBucketIndexHashingConfig}
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexUtils
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
              spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucketNumber => $defaultBucketNumber)")

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
            spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucketNumber => $defaultBucketNumber)")

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
            spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucketNumber => $defaultBucketNumber)")

            val expressions2 = "dt=(2021\\-01\\-05|2021\\-01\\-07),3"
            spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions2', rule => '$rule', bucketNumber => $defaultBucketNumber)")

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
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions', rule => '$rule', bucketNumber => $defaultBucketNumber)")

          val expressions2 = "dt=(2021\\-01\\-05|2021\\-01\\-07),3"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions2', rule => '$rule', bucketNumber => $defaultBucketNumber)")

          val expressions3 = "dt=(2021\\-01\\-05|2021\\-01\\-07),4"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions3', rule => '$rule', bucketNumber => $defaultBucketNumber)")

          val expressions4 = "dt=(2021\\-01\\-05|2021\\-01\\-07),5"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions4', rule => '$rule', bucketNumber => $defaultBucketNumber)")

          val expressions5 = "dt=(2021\\-01\\-05|2021\\-01\\-07),6"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions5', rule => '$rule', bucketNumber => $defaultBucketNumber)")

          val expressions6 = "dt=(2021\\-01\\-05|2021\\-01\\-07),6"
          spark.sql(s"call partition_bucket_index_manager(table => '$tableName', overwrite => '$expressions6', rule => '$rule', bucketNumber => $defaultBucketNumber)")

          assert(PartitionBucketIndexHashingConfig.getArchiveHashingConfig(metaClient).size() == 2)
          assert(PartitionBucketIndexHashingConfig.getCommittedHashingConfig(metaClient).size() == 4)
        }
      }
    }
  }
}
