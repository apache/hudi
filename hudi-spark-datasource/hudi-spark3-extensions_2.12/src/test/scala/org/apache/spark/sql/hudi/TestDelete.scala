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

class TestDelete extends TestHoodieSqlBase {

  test("Test Delete Table") {
    withTempDir { tmp =>
      val tablePath = new Path(tmp.toString, "deleteTable").toUri.toString
      spark.sql("set spark.hoodie.shuffle.parallelism=4")
      spark.sql(
        s"""create table deleteTable (
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

      spark.sql("insert into deleteTable partition(p=1,p1=2,p2) select 1 as keyid, 'sd' as name, 2.45 as price, 9 as col1, '10'")
      spark.sql("insert into deleteTable partition(p=1,p1=2,p2=3) select 2 as keyid, 'sd' as name, 2.55 as price, 12 as col1")
      spark.sql("delete from deleteTable where col1 = 9")
      checkAnswer("select col1 from deleteTable")(Seq(12))

      // test DeleteNonExistingRecords
      spark.sql("delete from deleteTable where col1 > 100")
      checkAnswer("select col1 from deleteTable")(Seq(12))

      // testDeleteWithoutCondition
      spark.sql("delete from deleteTable")
      checkAnswer("select count(*) from deleteTable")(Seq(0))

      spark.sql("insert into deleteTable partition(p=1,p1=2,p2) select 1 as keyid, 'sd' as name, 10.45 as price, 9 as col1, '10'")

      // testDeleteWithAlias
      spark.sql("delete from deleteTable as t where t.col1 = 9")
      checkAnswer("select count(*) from deleteTable")(Seq(0))

      // testDeleteWithInSubquery
      spark.sql("insert into deleteTable partition(p=1,p1=2,p2) select 1 as keyid, 'sd' as name, 10.45 as price, 100 as col1, '10'")
      spark.sql("insert into deleteTable partition(p=1,p1=2,p2) select 10 as keyid, 'sd' as named, 99.45 as price, 3 as col1, '10'")

      spark.range(0, 9).toDF("keyid").registerTempTable("testCondition")

      spark.sql("delete from deleteTable where keyid in (select keyid from testCondition)")
      checkAnswer("select keyid from deleteTable")(Seq(10))
    }
  }
}
