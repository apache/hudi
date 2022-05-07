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

import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class TestAlterTable extends HoodieSparkSqlTestBase {

  test("Test Alter Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        // change column comment
        spark.sql(s"alter table $tableName change column id id int comment 'primary id'")
        var catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        spark.sql(s"alter table $tableName change column name name string comment 'name column'")
        spark.sessionState.catalog.refreshTable(new TableIdentifier(tableName))
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        assertResult("name column") (
          catalogTable.schema(catalogTable.schema.fieldIndex("name")).getComment().get
        )

        // alter table name.
        val newTableName = s"${tableName}_1"
        spark.sql(s"alter table $tableName rename to $newTableName")
        assertResult(false)(
          spark.sessionState.catalog.tableExists(new TableIdentifier(tableName))
        )
        assertResult(true) (
          spark.sessionState.catalog.tableExists(new TableIdentifier(newTableName))
        )

        val hadoopConf = spark.sessionState.newHadoopConf()
        val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath)
          .setConf(hadoopConf).build()
        assertResult(newTableName) (metaClient.getTableConfig.getTableName)

        // insert some data
        spark.sql(s"insert into $newTableName values(1, 'a1', 10, 1000)")

        // add column
        spark.sql(s"alter table $newTableName add columns(ext0 string)")
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(newTableName))
        assertResult(Seq("id", "name", "price", "ts", "ext0")) {
          HoodieSqlCommonUtils.removeMetaFields(catalogTable.schema).fields.map(_.name)
        }
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1000, null)
        )

        // change column's data type
        checkExceptionContain(s"alter table $newTableName change column id id bigint") (
          "ALTER TABLE CHANGE COLUMN is not supported for changing column 'id'" +
            " with type 'IntegerType' to 'id' with type 'LongType'"
        )

        // Insert data to the new table.
        spark.sql(s"insert into $newTableName values(2, 'a2', 12, 1000, 'e0')")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1000, null),
          Seq(2, "a2", 12.0, 1000, "e0")
        )

        // Merge data to the new table.
        spark.sql(
          s"""
             |merge into $newTableName t0
             |using (
             |  select 1 as id, 'a1' as name, 12 as price, 1001 as ts, 'e0' as ext0
             |) s0
             |on t0.id = s0.id
             |when matched then update set *
             |when not matched then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 12.0, 1001, "e0"),
          Seq(2, "a2", 12.0, 1000, "e0")
        )

        // Update data to the new table.
        spark.sql(s"update $newTableName set price = 10, ext0 = null where id = 1")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1001, null),
          Seq(2, "a2", 12.0, 1000, "e0")
        )
        spark.sql(s"update $newTableName set price = 10, ext0 = null where id = 2")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1001, null),
          Seq(2, "a2", 10.0, 1000, null)
        )

        // Delete data from the new table.
        spark.sql(s"delete from $newTableName where id = 1")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(2, "a2", 10.0, 1000, null)
        )

        val partitionedTable = generateTableName
        spark.sql(
          s"""
             |create table $partitionedTable (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$partitionedTable'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by (dt)
       """.stripMargin)
        spark.sql(s"insert into $partitionedTable values(1, 'a1', 10, 1000, '2021-07-25')")
        spark.sql(s"alter table $partitionedTable add columns(ext0 double)")
        checkAnswer(s"select id, name, price, ts, dt, ext0 from $partitionedTable")(
          Seq(1, "a1", 10.0, 1000, "2021-07-25", null)
        )

        spark.sql(s"insert into $partitionedTable values(2, 'a2', 10, 1000, 1, '2021-07-25')");
        checkAnswer(s"select id, name, price, ts, dt, ext0 from $partitionedTable order by id")(
          Seq(1, "a1", 10.0, 1000, "2021-07-25", null),
          Seq(2, "a2", 10.0, 1000, "2021-07-25", 1.0)
        )
      }
    }
  }
}
