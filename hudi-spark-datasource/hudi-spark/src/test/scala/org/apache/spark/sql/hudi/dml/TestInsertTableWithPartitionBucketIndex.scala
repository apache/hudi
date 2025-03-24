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

import org.apache.hudi.index.bucket.PartitionBucketIndexUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import scala.collection.JavaConverters._

class TestInsertTableWithPartitionBucketIndex extends HoodieSparkSqlTestBase {

  /**
   * Total WorkFlow are as followed:
   * Step1: Create a new table using simple bucket index
   * Step2: Upgrade table to partition level bucket index using `call partition_bucket_index_manager`
   * Step3: Trigger another write operation through partition level bucket index
   *
   */
  test("Test Bulk Insert Into Partition Bucket Index Table") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | hoodie.bucket.index.num.buckets = 1
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
      Seq("cow", "mor").foreach { tableType =>
        withTempDir { tmp =>
          val tableName = generateTableName
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
