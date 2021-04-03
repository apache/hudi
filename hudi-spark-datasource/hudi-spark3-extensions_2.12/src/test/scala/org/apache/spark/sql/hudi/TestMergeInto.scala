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

package org.apache.spark.sql.hudi

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{lit, col}

class TestMergeInto extends TestHoodieSqlBase {

  // TODO: now spark3.0.0 not support multiple MATCHED/NOT MATCHED clauses in merge. add tests for multiple MATCHED/NOT MATCHED clauses when we move to Spark 3.1
  test("Test MergeInto Basic") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "basicMerge").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table basicMerge (
           |keyid int,
           |name string,
           |price double,
           |col1 long,
           |p string,
           |p1 string,
           |p2 string) using hudi
           |partitioned by (p,p1,p2)
           |options('hoodie.datasource.write.table.type'='MERGE_ON_READ',
           |'hoodie.datasource.write.precombine.field'='col1',
           |'hoodie.datasource.write.recordkey.field'='keyid',
           |'hoodie.datasource.write.payload.class'='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload',
           |'hoodie.datasource.write.keygenerator.class'='org.apache.hudi.keygen.ComplexKeyGenerator')
           |location '${tablePath}' """.stripMargin).show()

      spark.range(0, 10).toDF("keyid")
        .withColumn("name", lit("named"))
        .withColumn("price", lit(2.34))
        .withColumn("col1", lit(2))
        .withColumn("p", lit("0"))
        .withColumn("p1", lit("1"))
        .withColumn("p2", lit("1")).registerTempTable("sourceTable")
      // test insert into static partition
      spark.sql("insert into basicMerge partition(p,p1,p2) select * from sourceTable")

      spark.range(0, 15).toDF("keyid")
        .withColumn("name", lit("merged"))
        .withColumn("price", lit(99.0))
        .withColumn("col1", lit(9))
        .withColumn("p", lit("0"))
        .withColumn("p1", lit("1"))
        .withColumn("p2", lit("1")).registerTempTable("stable")
      // merge with all actions update/delete/insert
      spark.sql("merge into basicMerge as t using stable as s on t.keyid = s.keyid" +
        " when matched and s.keyid=5 then update set t.name=s.name, t.price=s.price*10 " +
        "when matched and s.keyid=3 then delete  " +
        "when not matched and s.keyid <12 then insert * ")
      checkAnswer("select keyid, name, price from basicMerge where keyid=5")(Seq(5, "merged", 990.0))
      checkAnswer("select count(*) from basicMerge where keyid>9")(Seq(2))

      // test insert only, insert keyid = 14
      spark.sql(
        "merge into basicMerge as t0 using (select 14 as keyid, 'merged2' as name, 10 as price, 1000 as col1, 0 as p, 1 as p1, 1 as p2) as s0 " +
          "on t0.keyid = s0.keyid when not matched and s0.price%2=0 then insert *"
      )
      spark.sql("select * from basicMerge").show(false)
      // test update only, update keyid=0  set t0.price = t0.price=s0.price*s0.col1
      spark.sql(
        "merge into basicMerge as t0 using (select 0 as keyid, 'merged2' as name, 10 as price, 1000 as col1, 0 as p, 1 as p1, 1 as p2) as s0 " +
          "on t0.keyid = s0.keyid when matched and s0.price%2=0 then update set t0.price=s0.price*s0.col1"
      )
      // test delete only, delete keyid = 2

      spark.sql(
        "merge into basicMerge as t0 using (select 2 as keyid, 'merged2' as name, 10 as price, 1000 as col1, 0 as p, 1 as p1, 1 as p2) as s0 " +
          "on t0.keyid = s0.keyid when matched and s0.price%2=0 then delete"
      )

      // check delete result
      checkAnswer("select count(*) from basicMerge where keyid=2")(Seq(0))
      // check update result
      checkAnswer("select keyid, price from basicMerge where keyid=0")(Seq(0, 10000.0))
      // check insert result
      checkAnswer("select keyid, name from basicMerge where keyid=14")(Seq(14, "merged2"))
    }
  }

  test("testMergeWithSourceCTE") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "MergeWithSourceCTE").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table MergeWithSourceCTE (
           |keyid int,
           |name string,
           |price double,
           |col1 long,
           |p string,
           |p1 string,
           |p2 string) using hudi
           |partitioned by (p,p1,p2)
           |options('hoodie.datasource.write.table.type'='MERGE_ON_READ',
           |'hoodie.datasource.write.precombine.field'='col1',
           |'hoodie.datasource.write.recordkey.field'='keyid',
           |'hoodie.datasource.write.payload.class'='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload',
           |'hoodie.datasource.write.keygenerator.class'='org.apache.hudi.keygen.ComplexKeyGenerator')
           |location '${tablePath}' """.stripMargin).show()

      spark.range(0, 10).toDF("keyid")
        .withColumn("name", lit("named"))
        .withColumn("price", lit(2.34))
        .withColumn("col1", lit(2))
        .withColumn("p", lit("0"))
        .withColumn("p1", lit("1"))
        .withColumn("p2", lit("1")).registerTempTable("sourceTable")
      // test insert into static partition
      spark.sql("insert into MergeWithSourceCTE partition(p,p1,p2) select * from sourceTable")

      spark.range(0, 15).toDF("keyid")
        .withColumn("name", lit("merged"))
        .withColumn("price", lit(99.0))
        .withColumn("col1", lit(9))
        .withColumn("p", lit("0"))
        .withColumn("p1", lit("1"))
        .withColumn("p2", lit("1")).registerTempTable("source")

      spark.sql("WITH cte1 AS (SELECT keyid + 1 AS keyid, name, price, col1, p, p1, p2 FROM source) " +
        "merge into MergeWithSourceCTE as t using cte1 as s on t.keyid=s.keyid " +
        "when matched and s.keyid=5 then update set t.name=s.name, t.price=s.price*10 " +
        "when matched and s.keyid=3 then delete " +
        "when not matched and s.keyid <12 then insert * ")

      checkAnswer("select keyid, name, price from MergeWithSourceCTE where keyid=5")(Seq(5, "merged", 990.0))
      checkAnswer("select count(*) from MergeWithSourceCTE where keyid>9")(Seq(2))
    }
  }

  test("testArbitraryCondition") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "arbitraryConditionTable").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table arbitraryConditionTable (
           |keyid int,
           |name string,
           |price double,
           |col1 long,
           |p string,
           |p1 string,
           |p2 string) using hudi
           |partitioned by (p,p1,p2)
           |options('hoodie.datasource.write.table.type'='MERGE_ON_READ',
           |'hoodie.datasource.write.precombine.field'='col1',
           |'hoodie.datasource.write.recordkey.field'='keyid',
           |'hoodie.datasource.write.payload.class'='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload',
           |'hoodie.datasource.write.keygenerator.class'='org.apache.hudi.keygen.ComplexKeyGenerator')
           |location '${tablePath}' """.stripMargin).show()

      spark.range(0, 10).toDF("keyid")
        .withColumn("name", lit("named"))
        .withColumn("price", lit(2.34))
        .withColumn("col1", lit(2))
        .withColumn("p", lit("0"))
        .withColumn("p1", lit("1"))
        .withColumn("p2", lit("1")).registerTempTable("sourceTable")
      // test insert into static partition
      spark.sql("insert into arbitraryConditionTable partition(p,p1,p2) select * from sourceTable")

      spark.range(0, 15).toDF("keyid")
        .withColumn("name", lit("merged"))
        .withColumn("price", lit(99.0))
        .withColumn("col1", col("keyid"))
        .withColumn("p", lit("0"))
        .withColumn("p1", lit("1"))
        .withColumn("p2", lit("1")).registerTempTable("source")

      // now cte1 not contains keyid
      spark.sql("WITH cte1 AS (SELECT name as name, price as price, keyid+3 as col1, p as p, p1 as p1, p2 as p2 FROM source) " +
        "merge into arbitraryConditionTable as t using cte1 as s on (t.keyid==s.col1 and s.col1>0) " +
        "when matched and s.col1=5 then update set t.name=s.name, t.price=s.price*10 " +
        "when matched and s.col1=3 then delete ")

      checkAnswer("select keyid, name, price from arbitraryConditionTable where keyid=5")(Seq(5, "merged", 990.0))
      checkAnswer("select count(*) from arbitraryConditionTable where keyid=3")(Seq(0))
    }
  }

  test("test merge empty table") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "test1").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table test1 (
           |keyid int,
           |name string,
           |price double,
           |col1 long,
           |p string,
           |p1 string,
           |p2 string) using hudi
           |partitioned by (p,p1,p2)
           |options('hoodie.datasource.write.table.type'='MERGE_ON_READ',
           |'hoodie.datasource.write.precombine.field'='col1',
           |'hoodie.datasource.write.recordkey.field'='keyid',
           |'hoodie.datasource.write.payload.class'='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload',
           |'hoodie.datasource.write.keygenerator.class'='org.apache.hudi.keygen.ComplexKeyGenerator')
           |location '${tablePath}' """.stripMargin).show()

      spark.range(0, 10).toDF("keyid")
        .withColumn("name", lit("named"))
        .withColumn("price", lit(2.34))
        .withColumn("col1", lit(2))
        .withColumn("p", lit("0"))
        .withColumn("p1", lit("1"))
        .withColumn("p2", lit("1")).registerTempTable("sourceTable")
      // test insert into static partition
      spark.sql(
        "merge into test1 as t0 using sourceTable  as s0 " +
          "on t0.keyid = s0.keyid when not matched then insert *"
      )
      checkAnswer("select count(*) from test1")(Seq(10))
    }
  }
}
