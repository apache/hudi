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

import org.apache.hudi.config.HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME
import org.apache.hudi.io.HoodieWriteMergeHandle

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * MERGE INTO updates of an existing key merged through [[HoodieWriteMergeHandle]], which merges with the incoming
 * record's ExpressionPayload. The merge result is already in the table schema, so it must not be wrapped in the
 * payload again: ExpressionPayload decodes payload bytes with the MERGE source schema, which differs from the
 * table schema when the source columns are NOT NULL and the table columns are nullable.
 */
class TestMergeIntoWriteMergeHandle extends HoodieSparkSqlTestBase {

  private def withWriteMergeHandle(f: => Unit): Unit = {
    withSQLConf(MERGE_HANDLE_CLASS_NAME.key -> classOf[HoodieWriteMergeHandle[_, _, _, _]].getName)(f)
  }

  private def createTable(tableName: String, location: String): Unit = {
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | tblproperties (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         | )
         | location '$location'
       """.stripMargin)
  }

  test("Test MergeInto update with a non-nullable source through HoodieWriteMergeHandle") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTable(tableName, tmp.getCanonicalPath)
      withWriteMergeHandle {
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000)")

        // The source is a subquery of constants, so its columns are NOT NULL while the table's are nullable.
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 1 as id, 'a1_new' as name, 11.0 as price, 2000L as ts
             |  union all
             |  select 3 as id, 'a3' as name, 30.0 as price, 2001L as ts
             | ) as s0
             | on t0.id = s0.id
             | when matched then update set *
             | when not matched then insert *
         """.stripMargin)

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1_new", 11.0, 2000L),
          Seq(2, "a2", 20.0, 1000L),
          Seq(3, "a3", 30.0, 2001L))
      }
    }
  }

  test("Test MergeInto update with a nullable source through HoodieWriteMergeHandle") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTable(tableName, tmp.getCanonicalPath)
      withWriteMergeHandle {
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000)")

        // A scan of a table gives nullable columns, matching the table schema.
        val sourceTable = generateTableName
        spark.sql(s"create table $sourceTable using parquet as " +
          "select 1 as id, 'a1_new' as name, 11.0 as price, 2000L as ts " +
          "union all select 3 as id, 'a3' as name, 30.0 as price, 2001L as ts")
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using $sourceTable as s0
             | on t0.id = s0.id
             | when matched then update set *
             | when not matched then insert *
         """.stripMargin)

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1_new", 11.0, 2000L),
          Seq(2, "a2", 20.0, 1000L),
          Seq(3, "a3", 30.0, 2001L))
      }
    }
  }

  test("Test MergeInto conditional update keeps the stored row through HoodieWriteMergeHandle") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTable(tableName, tmp.getCanonicalPath)
      withWriteMergeHandle {
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")

        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (select 1 as id, 'a1_new' as name, 11.0 as price, 2000L as ts) as s0
             | on t0.id = s0.id
             | when matched and s0.price > 100 then update set *
         """.stripMargin)

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000L))
      }
    }
  }
}
