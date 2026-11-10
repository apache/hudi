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


package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * TIMESTAMP AS OF with a Hudi-format instant (yyyyMMddHHmmssSSS) in the positions a production query puts it:
 * top level, inside a CTE, inside two CTEs joined, and inside a derived table.
 */
class TestTimeTravelInCte extends HoodieSparkSqlTestBase {

  private def withTwoCommitTable(f: (String, String, String) => Unit): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long,
           |  datestr string
           |) using hudi
           | tblproperties (type = 'cow', primaryKey = 'id', preCombineField = 'ts')
           | partitioned by (datestr)
           | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 1000, '2026-10-19')")
      val metaClient = createMetaClient(spark, s"${tmp.getCanonicalPath}/$tableName")
      val instant1 = metaClient.getActiveTimeline.getAllCommitsTimeline.lastInstant().get().requestedTime
      spark.sql(s"insert into $tableName values(2, 'a2', 2000, '2026-10-19')")
      metaClient.reloadActiveTimeline()
      val instant2 = metaClient.getActiveTimeline.getAllCommitsTimeline.lastInstant().get().requestedTime
      f(tableName, instant1, instant2)
    }
  }

  test("Time travel with a Hudi instant at top level") {
    withTwoCommitTable { (t, i1, _) =>
      checkAnswer(s"select count(*) from $t TIMESTAMP AS OF '$i1' where datestr = '2026-10-19'")(Seq(1))
    }
  }

  test("Time travel with a Hudi instant inside a CTE") {
    withTwoCommitTable { (t, i1, _) =>
      checkAnswer(
        s"""with a as (select * from $t TIMESTAMP AS OF '$i1' where datestr = '2026-10-19')
           |select count(*) from a""".stripMargin)(Seq(1))
    }
  }

  test("Time travel with Hudi instants inside two joined CTEs") {
    withTwoCommitTable { (t, i1, i2) =>
      checkAnswer(
        s"""with old as (select id, name from $t TIMESTAMP AS OF '$i1' where datestr = '2026-10-19'),
           |new as (select id, name from $t TIMESTAMP AS OF '$i2' where datestr = '2026-10-19')
           |select new.id from new left join old on new.id = old.id where old.id is null""".stripMargin)(Seq(2))
    }
  }

  test("Time travel with a Hudi instant inside a derived table") {
    withTwoCommitTable { (t, i1, _) =>
      checkAnswer(
        s"select count(*) from (select * from $t TIMESTAMP AS OF '$i1' where datestr = '2026-10-19') x")(Seq(1))
    }
  }
}
