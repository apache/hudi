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

class TestInsertTable extends TestHoodieSqlBase {

  test("Test Insert Into") {
    withTempDir { tmp =>
      // Create a partitioned table
      val tablePath = new Path(tmp.toString, "p1").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table p1 (
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
           |location '${tablePath}' """.stripMargin)
      // Insert into dynamic partition
      spark.sql("insert into p1 partition(p=1,p1=2,p2) select 1 as keyid, 'sd' as name, 2.45 as price, 9 as col1, '10'")

      checkAnswer("select keyid, name, price, p, p1, p2 from p1")(Seq(1, "sd", 2.45, "1", "2", "10"))

      // Insert into static partition
      spark.sql("insert into p1 partition(p=1,p1=2,p2=3) select 2 as keyid, 'sd' as name, 2.55 as price, 9 as col1")
      checkAnswer("select keyid, name, price, p, p1, p2 from p1 where keyid = 2")(Seq(2, "sd", 2.55, "1", "2", "3"))

    }
  }

  test("Test Insert Into None Partitioned Table") {
    withTempDir { tmp =>
      // Create none partitioned mor table
      val tablePath = new Path(tmp.toString, "p2").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table p2 (
           |keyid int,
           |name string,
           |price double,
           |col1 long) using hudi
           |options('hoodie.datasource.write.table.type'='MERGE_ON_READ',
           |'hoodie.datasource.write.precombine.field'='col1',
           |'hoodie.datasource.write.recordkey.field'='keyid',
           |'hoodie.datasource.write.payload.class'='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload',
           |'hoodie.datasource.write.keygenerator.class'='org.apache.hudi.keygen.ComplexKeyGenerator')
           |location '${tablePath}' """.stripMargin).show()

      // test insert into static partition
      spark.sql("insert into p2  select 1 as keyid, 'sd' as name, 2.45 as price, 9 as col1")

      checkAnswer("select keyid, name, price, col1 from p2")(Seq(1, "sd", 2.45, 9))

      spark.sql("insert into p2 select 2 as keyid, 'sd' as name, 2.45 as price, 109 as col1")

      checkAnswer("select keyid, name, price, col1 from p2")(Seq(1, "sd", 2.45, 9), Seq(2, "sd", 2.45, 109))
    }
  }

  test("Test Insert Overwrite") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "p3").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table p3 (
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
           |location '${tablePath}' """.stripMargin)

      // Insert into dynamic partition
      spark.sql("insert overwrite p3 partition(p=1,p1=2,p2) select 1 as keyid, 'sd' as name, 2.45 as price, 9 as col1, 10")

      checkAnswer("select keyid, name, price, p, p1, p2 from p3")(Seq(1, "sd", 2.45, "1", "2", "10"))


      // Insert into static partition
      spark.sql("insert overwrite p3 partition(p=1,p1=2,p2=10) select 2 as keyid, 'sd' as name, 2.55 as price, 9 as col1")
      checkAnswer("select keyid, name, price, p, p1, p2 from p3")(Seq(2, "sd", 2.55, "1", "2", "10"))

    }
  }

}


