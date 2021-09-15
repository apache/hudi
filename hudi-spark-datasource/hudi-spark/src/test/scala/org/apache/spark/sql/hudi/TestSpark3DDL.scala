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

package org.apache.spark.sql.hudi

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}

class TestSpark3DDL extends TestHoodieSqlBase {

  test("Test Alter Table") {
    withTempDir { tmp =>
      Seq("mor", "cow").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (!HoodieSqlUtils.isSpark3) {

        } else {
          val spark3 = SparkSession
            .builder()
            .appName("Spark SQL data sources example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.session.state.builder", "org.apache.spark.sql.hudi.HoodieSessionStateBuilder")
            .master("local[1]")
            .getOrCreate()
          spark3.sql("set hoodie.schema.evolution.enable=true")
          spark3.sql(
            s"""
               |create table $tableName (
               | id int,
               | name string,
               | price double,
               | ts long
               |) using hudi
               | location '$tablePath'
               | options (
               | type = '$tableType',
               | primaryKey = 'id',
               | preCombineField = 'ts'
               | )
            """.stripMargin)
          def createTestResult: Array[Row] = {
            spark3.sql(s"select * from ${tableName}")
              .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name").collect()
          }
          spark3.sql(s"show create table ${tableName}").show(false)
          spark3.sql(s"insert into ${tableName} values (1, 'jack', 0.9, 1000)")
          spark3.sql(s"update ${tableName} set price = 1.9 where id = 1")

          // change int -> long
          spark3.sql(s"alter table ${tableName} alter column id type long")
          checkAnswer(createTestResult)(
            Seq(1, "jack", 1.9, 1000)
          )
          // test add action, include position change
          spark3.sql(s"alter table ${tableName} add columns(ext1 string comment 'add ext1' after name)")
          spark3.sql(s"insert into ${tableName} values (2, 'jack', 'exx1', 0.9, 1000)")
          checkAnswer(createTestResult)(
            Seq(1, "jack", null, 1.9, 1000), Seq(2, "jack","exx1", 0.9, 1000)
          )
          // test rename
          spark3.sql(s"alter table ${tableName} rename column price to newprice")
          checkAnswer(createTestResult)(
            Seq(1, "jack", null, 1.9, 1000), Seq(2, "jack","exx1", 0.9, 1000)
          )
          spark3.sql(s"update ${tableName} set ext1 = 'haha' where id = 1 ")
          checkAnswer(createTestResult)(
            Seq(1, "jack", "haha", 1.9, 1000), Seq(2, "jack","exx1", 0.9, 1000)
          )
          // drop column newprice
          spark3.sql(s"alter table ${tableName} drop column newprice")
          checkAnswer(createTestResult)(
            Seq(1, "jack", "haha", 1000), Seq(2, "jack","exx1", 1000)
          )
          // add newprice back
          spark3.sql(s"alter table ${tableName} add columns(newprice string comment 'add newprice back' after ext1)")
          checkAnswer(createTestResult)(
            Seq(1, "jack", "haha", null, 1000), Seq(2, "jack","exx1", null, 1000)
          )
          spark3.stop()
        }
      }
    }
  }

  test("Test Alter Table complex") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSqlUtils.isSpark3) {
          val spark3 = SparkSession
            .builder()
            .appName("Spark SQL data sources example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("hoodie.insert.shuffle.parallelism", "4")
            .config("hoodie.upsert.shuffle.parallelism", "4")
            .config("hoodie.delete.shuffle.parallelism", "4")
            .master("local[4]")
            .getOrCreate()
          spark3.sql("set hoodie.schema.evolution.enable=true")
          spark3.sql(
            s"""
               |create table $tableName (
               | id int,
               | name string,
               | members map<String, struct<n:string, a:int>>,
               | user struct<name:string, age:int, score: int>,
               | ts long
               |) using hudi
               | location '$tablePath'
               | options (
               | type = '$tableType',
               | primaryKey = 'id',
               | preCombineField = 'ts'
               | )
          """.stripMargin)

          spark3.sql(s"insert into ${tableName} values(1, 'jack', map('k1', struct('v1', 100), 'k2', struct('v2', 200)), struct('jackStruct', 29, 100), 1000)")

          // rename column
          spark3.sql(s"alter table ${tableName} rename column user to userx")

          checkAnswer(spark3.sql(s"select ts, userx.score, id, userx.age, name from ${tableName}").collect())(
            Seq(1000, 100, 1, 29, "jack")
          )

          // drop column
          spark.sql(s"alter table ${tableName} drop columns(name, userx.name, userx.score)")

          spark3.sql(s"select * from ${tableName}").show(false)

          // add cols back, and adjust cols position
          spark3.sql(s"alter table ${tableName} add columns(name string comment 'add name back' after userx," +
            s" userx.name string comment 'add userx.name back' first, userx.score int comment 'add userx.score back' after age)")

          // query new columns: name, userx.name, userx.score, those field should not be read.
          checkAnswer(spark3.sql(s"select name, userx.name, userx.score from ${tableName}").collect())(
            Seq(null, null, null)
          )

          // insert again
          spark3.sql(s"insert into ${tableName} values(2 , map('k1', struct('v1', 100), 'k2', struct('v2', 200)), struct('jackStructNew', 291 , 101), 'jacknew', 1000)")

          // check again
          checkAnswer(spark3.sql(s"select name, userx.name as uxname, userx.score as uxs from ${tableName} order by id").collect())(
            Seq(null, null, null),
            Seq("jacknew", "jackStructNew", 101))

          // change type
          spark3.sql(s"alter table ${tableName} alter column userx.age type long")

          checkAnswer(spark3.sql(s"select userx.age, id, name from ${tableName} order by id").collect())(
            Seq(29, 1, null),
            Seq(291, 2, "jacknew"))
        }
      }
    }
  }
}

