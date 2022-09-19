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

package org.apache.spark.sql.hudi.command.index

import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase

class TestSecondaryIndex extends HoodieSparkSqlTestBase {
  test("Test Create/Show/Drop Secondary Index") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 3.2d, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 5.5d, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 1.25d, 1002)")

        // Check query result
        val selectResult1 = spark.sql(s"select id, name, price, ts from $tableName where id = 1").collect()
        assertResult(Seq(1, "a1", 3.2, 1000))(selectResult1(0).toSeq)

        val selectResult2 = spark.sql(s"select id, name, price, ts from $tableName order by id").collect()
        assertResult(3)(selectResult2.length)
        assertResult(Seq(1, "a1", 3.2, 1000),
          Seq(2, "a2", 5.5, 1001),
          Seq(3, "a3", 1.25, 1002)
        )(selectResult2(0).toSeq, selectResult2(1).toSeq, selectResult2(2).toSeq)

        val selectResult3 = spark.sql(s"select id, name, price, ts from $tableName where name = 'a2'").collect()
        assertResult(Seq(2, "a2", 5.5, 1001))(selectResult3(0).toSeq)

        val selectResult4 = spark.sql(s"select id, name, price, ts from $tableName where price = 1.25").collect()
        assertResult(Seq(3, "a3", 1.25, 1002))(selectResult4(0).toSeq)

        checkAnswer(s"show indexes from default.$tableName")()

        checkAnswer(s"create index idx_name on $tableName using lucene (name) options(block_size=1024)")()
        checkAnswer(s"create index idx_price on $tableName using lucene (price options(order='desc')) options(block_size=512)")()

        // Create an index with multiple columns
        checkAnswer(s"create index idx_id_ts on $tableName using lucene (id, ts)")()

        // Create an index with the occupied name
        checkException(s"create index idx_price on $tableName using lucene (price)")(
          "Secondary index already exists: idx_price"
        )

        // Create indexes repeatedly on columns(index name is different, but the index type and involved column is same)
        checkException(s"create index idx_price_1 on $tableName using lucene (price)")(
          "Secondary index already exists: idx_price_1"
        )

        spark.sql(s"show indexes from $tableName").show()
        checkAnswer(s"show indexes from $tableName")(
          Seq("idx_id_ts", "id,ts", "lucene", "", ""),
          Seq("idx_name", "name", "lucene", "", "{\"block_size\":\"1024\"}"),
          Seq("idx_price", "price", "lucene", "{\"price\":{\"order\":\"desc\"}}", "{\"block_size\":\"512\"}")
        )

        // Build secondary index for this table
        spark.sql(s"call run_build(table => '$tableName')").show()
        spark.sql(s"call show_build(table => '$tableName')").show()

        // Check query result again
        spark.sql(s"select id, name, price, ts from $tableName order by id").show()
        assertResult(selectResult1)(spark.sql(s"select id, name, price, ts from $tableName where id = 1").collect())
        assertResult(selectResult2)(spark.sql(s"select id, name, price, ts from $tableName order by id").collect())
        assertResult(selectResult3)(spark.sql(s"select id, name, price, ts from $tableName where name = 'a2'").collect())
        assertResult(selectResult4)(spark.sql(s"select id, name, price, ts from $tableName where price = 1.25").collect())

        checkAnswer(s"drop index idx_name on $tableName")()
        checkException(s"drop index idx_name on $tableName")("Secondary index not exists: idx_name")

        spark.sql(s"show indexes from $tableName").show()
        checkAnswer(s"show indexes from $tableName")(
          Seq("idx_id_ts", "id,ts", "lucene", "", ""),
          Seq("idx_price", "price", "lucene", "{\"price\":{\"order\":\"desc\"}}", "{\"block_size\":\"512\"}")
        )

        checkAnswer(s"drop index idx_price on $tableName")()
        checkAnswer(s"show indexes from $tableName")(
          Seq("idx_id_ts", "id,ts", "lucene", "", "")
        )

        checkException(s"drop index idx_price on $tableName")("Secondary index not exists: idx_price")

        checkException(s"create index idx_price_1 on $tableName using lucene (field_not_exist)")(
          "Field not exists: field_not_exist"
        )
      }
    }
  }
}
