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
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        checkAnswer(s"show indexes from default.$tableName")()

        checkAnswer(s"create index idx_name on $tableName using lucene (name) options(block_size=1024)")()
        checkAnswer(s"create index idx_price on $tableName using lucene (price options(order='desc')) options(block_size=512)")()

        // Create an index with multiple columns
        checkException(s"create index idx_id_ts on $tableName using lucene (id, ts)")("Lucene index only support single column")

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
          Seq("idx_name", "name", "lucene", "", "{\"block_size\":\"1024\"}"),
          Seq("idx_price", "price", "lucene", "{\"price\":{\"order\":\"desc\"}}", "{\"block_size\":\"512\"}")
        )

        checkAnswer(s"drop index idx_name on $tableName")()
        checkException(s"drop index idx_name on $tableName")("Secondary index not exists: idx_name")

        spark.sql(s"show indexes from $tableName").show()
        checkAnswer(s"show indexes from $tableName")(
          Seq("idx_price", "price", "lucene", "{\"price\":{\"order\":\"desc\"}}", "{\"block_size\":\"512\"}")
        )

        checkAnswer(s"drop index idx_price on $tableName")()
        checkAnswer(s"show indexes from $tableName")()

        checkException(s"drop index idx_price on $tableName")("Secondary index not exists: idx_price")

        checkException(s"create index idx_price_1 on $tableName using lucene (field_not_exist)")(
          "Field not exists: field_not_exist"
        )
      }
    }
  }
}
