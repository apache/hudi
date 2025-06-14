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

import org.apache.hudi.testutils.DataSourceTestUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestMergeIntoLogOnlyTable extends HoodieSparkSqlTestBase {

  test("Test Query Log Only MOR Table") {
    withRecordType()(withTempDir { tmp =>
      // Create table with INMEMORY index to generate log only mor table.
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.index.type = 'INMEMORY',
           |  hoodie.compact.inline = 'true'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      // 3 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tmp.getCanonicalPath))
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 10.0, 1000),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000)
      )
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 1 as id, 'a1' as name, 11 as price, 1001L as ts
           | ) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      // 4 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tmp.getCanonicalPath))
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1001),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000)
      )
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 4 as id, 'a4' as name, 11 as price, 1000L as ts
           | ) s0
           | on h0.id = s0.id
           | when not matched then insert *
           |""".stripMargin)

      // 5 commits will trigger compaction.
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tmp.getCanonicalPath))
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1001),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4", 11.0, 1000)
      )
    })
  }
}
