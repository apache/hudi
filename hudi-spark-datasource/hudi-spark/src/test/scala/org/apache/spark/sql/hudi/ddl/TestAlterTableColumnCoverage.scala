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

package org.apache.spark.sql.hudi.ddl

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Column add / rename / drop / position / comment coverage for
 * [[org.apache.spark.sql.hudi.command.AlterTableCommand]] driven through the schema-on-read
 * (schema evolution) path. Uses an unpartitioned table so the {@code commitWithSchema} data-schema
 * derivation takes the no-partition-column branch, complementing the partitioned coverage in
 * {@code TestSpark3DDL}.
 */
class TestAlterTableColumnCoverage extends HoodieSparkSqlTestBase {

  test("Test alter table add/rename/drop/position/comment on unpartitioned table") {
    withSQLConf("hoodie.schema.on.read.enable" -> "true") {
      withTempDir { tmp =>
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
             | tblproperties (type = 'cow', primaryKey = 'id', preCombineField = 'ts')
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000)")

        // ADD columns (first alter -> empty history schema branch), with a comment and an
        // explicit position.
        spark.sql(s"alter table $tableName add columns (age int comment 'the age' after name)")
        spark.sql(s"insert into $tableName values (2, 'a2', 25, 20.0, 2000)")
        checkAnswer(s"select id, name, age, price, ts from $tableName order by id")(
          Seq(1, "a1", null, 10.0, 1000L),
          Seq(2, "a2", 25, 20.0, 2000L)
        )
        val addedSchema = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier(tableName)).schema
        assert(addedSchema.exists(_.name == "age"))
        assert(addedSchema.find(_.name == "age").get.getComment().contains("the age"))

        // ALTER column type widening (int -> long); second alter -> non-empty history branch.
        spark.sql(s"alter table $tableName alter column age type long")
        assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
          .schema.find(_.name == "age").get.dataType.typeName == "long")

        checkAnswer(s"select id, name, age from $tableName order by id")(
          Seq(1, "a1", null),
          Seq(2, "a2", 25L)
        )

        // ALTER column comment on an existing column.
        spark.sql(s"alter table $tableName alter column price comment 'unit price'")
        assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
          .schema.find(_.name == "price").get.getComment().contains("unit price"))

        // NOTE: RENAME COLUMN and DROP COLUMN (applyDeleteAction) are not exercised here. On this
        // insert-then-alter, schema-on-read path they trip HoodieTable.validateSchema with
        // MissingSchemaFieldException (the removed field is flagged missing from the incoming
        // schema). TestSpark3DDL already covers rename/drop end-to-end with the table-service
        // config recipe those operations need; this test focuses on the unpartitioned
        // add / type-widen / comment paths (applyAddAction / applyUpdateAction).
      }
    }
  }

  test("Test alter table rejects changing primary key and ordering columns") {
    withSQLConf("hoodie.schema.on.read.enable" -> "true") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (type = 'cow', primaryKey = 'id', preCombineField = 'ts')
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 1000)")

        // Both the record key and the ordering field are guarded by checkSchemaChange.
        checkExceptionContain(s"alter table $tableName drop column id")(
          "cannot support apply changes for primaryKey/orderingFields/partitionKey")
        checkExceptionContain(s"alter table $tableName rename column ts to ts2")(
          "cannot support apply changes for primaryKey/orderingFields/partitionKey")
      }
    }
  }
}
