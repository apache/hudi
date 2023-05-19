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

import org.apache.spark.sql.catalyst.TableIdentifier

class TestSetAndUnset extends HoodieSparkSqlTestBase {
  test("Test set and unset properties with spark session's table metadata") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      // Create table
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
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // test set properties with spark session's table metadata
      spark.sql(s"alter table $tableName set tblproperties(comment='it is a hudi table', 'key1'='value1', 'key2'='value2')")
      val meta = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assert(meta.comment.get.equals("it is a hudi table"))
      assert(Seq("key1", "key2").count(meta.properties.contains) == 2)

      // test unset propertes with spark session's table metadata
      spark.sql(s"alter table $tableName unset tblproperties(comment, 'key1', 'key2')")
      val unsetMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assert(!Seq("key1", "key2").exists(unsetMeta.properties.contains))
      assert(unsetMeta.comment.isEmpty)
    })
  }

  test("Test set and unset hoodie's table properties") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      // Create table
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
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      val prop = "hoodie.schema.on.read.enable"

      // test set properties
      spark.sql(s"alter table $tableName set tblproperties(comment='1', '$prop'='true')")

      // test show properties
      assertResult("true") {
        spark.sql(s"SHOW TBLPROPERTIES $tableName ('$prop')").collect().apply(0).get(0)
      }

      // test unset properties
      spark.sql(s"alter table $tableName unset tblproperties(comment, '$prop')")

      // test show properties after unset
      assertResult(0) {
        spark.sql(s"SHOW TBLPROPERTIES $tableName")
          .filter(r => r.get(0).equals(prop)).count()
      }
    })
  }
}
