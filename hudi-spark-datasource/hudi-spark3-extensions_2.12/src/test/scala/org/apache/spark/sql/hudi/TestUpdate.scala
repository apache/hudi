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

class TestUpdate extends TestHoodieSqlBase {

  test("Test update Table") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "updateTable").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table updateTable (
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
      spark.sql("insert into updateTable partition(p=1,p1=2,p2) select 1 as keyid, 'sd' as name, 2.45 as price, 9 as col1, '10'")
      spark.sql("insert into updateTable partition(p=1,p1=2,p2=3) select 2 as keyid, 'sd' as name, 2.55 as price, 12 as col1")

      // testUpdateWithAlias
      spark.sql("update updateTable as u set u.name = 'name_updated' where u.col1 > 9")
      checkAnswer("select keyid, name from updateTable where col1 > 9")(Seq(2, "name_updated"))

      // testUpdateNonExistingRecords
      spark.sql("update updateTable as u set u.name = 'name_updated_100' where u.col1 > 100")
      checkAnswer("select count(*) from updateTable")(Seq(2))

      //testUpdateWithInSubquery
      spark.range(1, 12).toDF("keyid").registerTempTable("testCondition")
      spark.sql("update updateTable set name = 'name_updated_1000', col1=keyid*col1 where col1 in (select keyid from testCondition)")
      checkAnswer("select keyid, name from updateTable where col1 < 12")(Seq(1, "name_updated_1000"))
      checkAnswer("select keyid, name from updateTable where col1 > 11")(Seq(2, "name_updated"))
    }
  }

  test("Test nested Table") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "nestedTable").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table nestedTable (
           |keyid int,
           |a1 struct<number:int,score:float>,
           |col3 int,
           |p string,
           |p1 string,
           |p2 string) using hudi
           |partitioned by (p,p1,p2)
           |options('hoodie.datasource.write.table.type'='MERGE_ON_READ',
           |'hoodie.datasource.write.precombine.field'='col3',
           |'hoodie.datasource.write.recordkey.field'='keyid',
           |'hoodie.datasource.write.payload.class'='org.apache.hudi.common.model.OverwriteWithLatestAvroPayload',
           |'hoodie.datasource.write.keygenerator.class'='org.apache.hudi.keygen.ComplexKeyGenerator')
           |location '${tablePath}' """.stripMargin)

      spark.sql("insert into nestedTable select 2 as keyid, named_struct('number',90,'score', 100.5) as a1, 4 as col3, 1,2,3")
      spark.sql("update nestedTable set a1.number=100 where col3=4")
      checkAnswer("select a1.number from nestedTable where col3=4")(Seq(100))
    }
  }
}
