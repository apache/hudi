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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{HoodieCatalogTable, SessionCatalog}
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand.buildHoodieInsertConfig

import scala.reflect.ClassTag

abstract class TestOperationConfig extends HoodieSparkSqlTestBase {
  val catalog: SessionCatalog = spark.sessionState.catalog

  def checkOperation(config: Map[String, String])(expected: String): Unit = {
    assertResult(expected)(config("hoodie.datasource.write.operation"))
  }

  def checkExceptionMsg[T <: Throwable](f: => Any)(errorMsg: String)(implicit classTag: ClassTag[T]): Unit = {
    val e = intercept[T](f)
    assert(e.getMessage.contains(errorMsg))
  }
}

class TestInsertOperationConfig extends TestOperationConfig {
  test("prefer custom write operation than auto-deduce") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           |options (primaryKey = 'id', preCombineField = 'ts')
           |location '${tmp.getCanonicalPath}'
      """.stripMargin)
      val tableMeta = catalog.getTableMetadata(new TableIdentifier(tableName))
      val table = new HoodieCatalogTable(spark, tableMeta)

      Seq("upsert", "insert", "bulk_insert").foreach { operation =>
        withSQLConf("hoodie.datasource.write.operation" -> operation) {
          checkOperation(buildHoodieInsertConfig(table, spark, false, false))(operation)
        }
      }
    }
  }

  test("upsert to table without preCombineKey is not supported") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string
           |) using hudi
           |options (primaryKey = 'id')
           |location '${tmp.getCanonicalPath}'
      """.stripMargin)
      val tableMeta = catalog.getTableMetadata(new TableIdentifier(tableName))
      val table = new HoodieCatalogTable(spark, tableMeta)

      withSQLConf("hoodie.datasource.write.operation" -> "upsert") {
        checkExceptionMsg[IllegalArgumentException](buildHoodieInsertConfig(table, spark, false, false))(
          "Table without preCombineKey can not use upsert operation.")
      }
    }
  }

  test("upsert operation with INSERT_DROP_DUPS enabled should convert to insert") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           |options (primaryKey = 'id', preCombineField = 'ts')
           |location '${tmp.getCanonicalPath}'
      """.stripMargin)
      val tableMeta = catalog.getTableMetadata(new TableIdentifier(tableName))
      val table = new HoodieCatalogTable(spark, tableMeta)
      withSQLConf("hoodie.datasource.write.operation" -> "upsert",
                  "hoodie.datasource.write.insert.drop.duplicates" -> "true") {
        checkOperation(buildHoodieInsertConfig(table, spark, false, false))("insert")
      }
    }
  }

  test("auto-deduce for bulk_insert operation") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long,
           |  dt string
           |) using hudi
           |options (primaryKey = 'id', preCombineField = 'ts')
           |location '${tmp.getCanonicalPath}/$tableName'
           |partitioned by (dt)
      """.stripMargin)
      val tableMeta = catalog.getTableMetadata(new TableIdentifier(tableName))
      val partitionedTable = new HoodieCatalogTable(spark, tableMeta)

      val tableName2 = generateTableName
      spark.sql(
        s"""
           |create table $tableName2 (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           |options (primaryKey = 'id', preCombineField = 'ts')
           |location '${tmp.getCanonicalPath}/$tableName2'
        """.stripMargin)
      val tableMeta2 = catalog.getTableMetadata(new TableIdentifier(tableName2))
      val nonPartitionedTable = new HoodieCatalogTable(spark, tableMeta2)

      withSQLConf("hoodie.sql.bulk.insert.enable" -> "true") {
        Seq("upsert", "strict").foreach { insertMode =>
          withSQLConf("hoodie.sql.insert.mode" -> insertMode) {
            checkExceptionMsg[IllegalArgumentException](buildHoodieInsertConfig(partitionedTable, spark, false, false))(
              s"Table with primaryKey can not use bulk insert in $insertMode mode.")
          }
        }
        Seq("non-strict").foreach { insertMode =>
          withSQLConf("hoodie.sql.insert.mode" -> insertMode) {
            checkExceptionMsg[IllegalArgumentException](buildHoodieInsertConfig(partitionedTable, spark, true, false))(
              "Insert Overwrite Partition can not use bulk insert.")
            checkOperation(buildHoodieInsertConfig(nonPartitionedTable, spark, false, false))(
              "bulk_insert")
            checkOperation(buildHoodieInsertConfig(nonPartitionedTable ,spark, false, true))(
              "bulk_insert")
          }
        }
      }
    }
  }

  test("auto-deduce for upsert operation") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long,
           |  dt string
           |) using hudi
           |options (primaryKey = 'id', preCombineField = 'ts')
           |location '${tmp.getCanonicalPath}/$tableName'
           |partitioned by (dt)
      """.stripMargin)
      val tableMeta = catalog.getTableMetadata(new TableIdentifier(tableName))
      val table = new HoodieCatalogTable(spark, tableMeta)
      checkOperation(buildHoodieInsertConfig(table ,spark, false, false))(
        "upsert")
    }
  }

  test("auto-deduce for insert operation") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string
           |) using hudi
           |options (primaryKey = 'id')
           |location '${tmp.getCanonicalPath}/$tableName'
      """.stripMargin)
      val tableMeta = catalog.getTableMetadata(new TableIdentifier(tableName))
      val table = new HoodieCatalogTable(spark, tableMeta)
      checkOperation(buildHoodieInsertConfig(table ,spark, false, false))(
        "insert")
    }
  }
}
