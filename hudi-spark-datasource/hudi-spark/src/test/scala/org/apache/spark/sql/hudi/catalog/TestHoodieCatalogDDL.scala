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

package org.apache.spark.sql.hudi.catalog

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, TableCapability, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.hudi.command.ShowHoodieCreateTableCommand
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.io.File

import scala.collection.JavaConverters._

/**
 * DDL-level coverage for [[HoodieCatalog]], [[HoodieStagedTable]], [[HoodieInternalV2Table]]
 * and the create/show-create table commands, exercised end-to-end through the V2 session
 * catalog wired up by [[HoodieSparkSqlTestBase]] (spark_catalog = HoodieCatalog).
 */
class TestHoodieCatalogDDL extends HoodieSparkSqlTestBase {

  private def hoodieCatalog: HoodieCatalog =
    spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[HoodieCatalog]

  private def userFields(names: Array[String]): Seq[String] =
    names.filterNot(_.startsWith("_hoodie")).toSeq

  test("HoodieCatalog create, load, alter, rename and drop via the V2 catalog API") {
    withTempDir { tmp =>
      val catalog = hoodieCatalog
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      val ident = Identifier.of(Array("default"), tableName)
      val schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("ts", LongType)))
      val props = Map(
        "provider" -> "hudi",
        "primaryKey" -> "id",
        "preCombineField" -> "ts",
        "location" -> tablePath).asJava

      // createTable routes to createHoodieTable(CREATE) and initializes the table on disk.
      catalog.createTable(ident, schema, Array.empty[Transform], props)
      assertTrue(catalog.tableExists(ident))
      assertTrue(new File(s"$tablePath/.hoodie/hoodie.properties").exists())

      // loadTable returns a Hudi-backed table exposing the user schema.
      val loaded = catalog.loadTable(ident)
      assertEquals(Seq("id", "name", "ts"), userFields(loaded.schema().fieldNames))

      // alterTable: add a column.
      catalog.alterTable(ident, TableChange.addColumn(Array("age"), IntegerType, true))
      assertTrue(catalog.loadTable(ident).schema().fieldNames.contains("age"))

      // alterTable: update a column comment.
      catalog.alterTable(ident, TableChange.updateColumnComment(Array("name"), "the name column"))
      val commented = catalog.loadTable(ident).schema().fields.find(_.name == "name").get
      assertEquals("the name column", commented.getComment().getOrElse(""))

      // alterTable: changing a column type is rejected by the V2 alter path, which routes to
      // AlterHoodieTableChangeColumnCommand and does not support column type changes.
      val typeChange = intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.updateColumnType(Array("age"), LongType))
      }
      assertTrue(typeChange.getMessage.contains(
        "ALTER TABLE CHANGE COLUMN is not supported for changing column 'age'"),
        typeChange.getMessage)
      assertEquals(IntegerType,
        catalog.loadTable(ident).schema().fields.find(_.name == "age").get.dataType)

      // renameTable moves the catalog entry.
      val renamed = Identifier.of(Array("default"), s"${tableName}_renamed")
      catalog.renameTable(ident, renamed)
      assertFalse(catalog.tableExists(ident))
      assertTrue(catalog.tableExists(renamed))

      // dropTable removes the Hudi table from the catalog.
      assertTrue(catalog.dropTable(renamed))
      assertFalse(catalog.tableExists(renamed))
    }
  }

  test("CTAS through the staged table commits managed and partitioned Hudi tables") {
    val nonPartitioned = generateTableName
    spark.sql(
      s"""
         |create table $nonPartitioned using hudi
         |tblproperties (primaryKey = 'id', preCombineField = 'ts')
         |as select 1 as id, 'a1' as name, 10 as price, 1000 as ts
         |""".stripMargin)
    checkAnswer(s"select id, name, price, ts from $nonPartitioned")(Seq(1, "a1", 10, 1000))

    val partitioned = generateTableName
    spark.sql(
      s"""
         |create table $partitioned using hudi
         |partitioned by (dt)
         |tblproperties (primaryKey = 'id', preCombineField = 'ts')
         |as select 1 as id, 'a1' as name, 1000 as ts, '2024-01-01' as dt
         |     union all select 2, 'a2', 2000, '2024-01-02'
         |""".stripMargin)
    checkAnswer(s"select id, name, dt from $partitioned")(
      Seq(1, "a1", "2024-01-01"), Seq(2, "a2", "2024-01-02"))
  }

  test("A failing CTAS aborts staged changes and cleans up the table path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      checkExceptionContain(
        s"""
           |create table $tableName using hudi
           |tblproperties (primaryKey = 'id', type = 'cow', hoodie.compact.inline = 'true')
           |location '$tablePath'
           |as select 1 as id, 'a1' as name, 1000 as ts
           |""".stripMargin)("Compaction is not supported on a CopyOnWrite table")
      assertFalse(existsPath(tablePath))
    }
  }

  test("SHOW CREATE TABLE regenerates Hudi DDL") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  ts long,
         |  dt string
         |) using hudi
         |partitioned by (dt)
         |comment 'a hudi table'
         |tblproperties (primaryKey = 'id', preCombineField = 'ts')
         |""".stripMargin)

    // SHOW CREATE TABLE resolves through Spark's native command for the Hudi V2 table, which
    // emits `CREATE TABLE <table>` with a USING/TBLPROPERTIES body. The catalog qualifier differs
    // by Spark version (`spark_catalog.default.` on 3.5+, `default.` on 3.4/3.3), so match either.
    val ddl = spark.sql(s"show create table $tableName").head().getString(0)
    assertTrue(ddl.contains("CREATE TABLE") && ddl.contains(s"default.$tableName"), ddl)
    assertTrue(ddl.contains("USING hudi"), ddl)
    assertTrue(ddl.contains("PARTITIONED BY (dt)"), ddl)
    assertTrue(ddl.contains("COMMENT 'a hudi table'"), ddl)
    assertTrue(ddl.contains("TBLPROPERTIES"), ddl)
    assertTrue(ddl.contains("primaryKey"), ddl)

    // The command reports a missing table through NoSuchTableException.
    intercept[NoSuchTableException] {
      ShowHoodieCreateTableCommand(TableIdentifier("does_not_exist_tbl")).run(spark)
    }
  }

  test("CREATE over an existing location validates conflicting table properties") {
    withTempDir { tmp =>
      val basePath = s"${tmp.getCanonicalPath}/shared"
      val first = generateTableName
      spark.sql(
        s"""
           |create table $first (id int, name string, ts long) using hudi
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |location '$basePath'
           |""".stripMargin)

      // A second table over the same location that keeps the on-disk table config resolves
      // to the persisted schema (existing-location reuse path).
      val reuse = generateTableName
      spark.sql(
        s"""
           |create table $reuse (id int, name string, ts long) using hudi
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |location '$basePath'
           |""".stripMargin)
      assertEquals(Seq("id", "name", "ts"), userFields(spark.table(reuse).schema.fieldNames))

      // A conflicting primaryKey against the on-disk table config is rejected with a config-conflict
      // error surfaced from the write-path validation (HoodieWriterUtils).
      val conflicting = generateTableName
      checkExceptionContain(
        s"""
           |create table $conflicting (id int, name string, ts long) using hudi
           |tblproperties (primaryKey = 'name', preCombineField = 'ts')
           |location '$basePath'
           |""".stripMargin)("hoodie.table.recordkey.fields")
    }
  }

  test("HoodieInternalV2Table exposes v2 capabilities and handles reads and writes") {
    withTempDir { tmp =>
      withSQLConf("hoodie.schema.on.read.enable" -> "true") {
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (id int, name string, ts long) using hudi
             |tblproperties (primaryKey = 'id', preCombineField = 'ts')
             |location '$tablePath'
             |""".stripMargin)

        // With schema evolution enabled, loadTable returns the V2 table directly.
        val ident = Identifier.of(Array("default"), tableName)
        val loaded = hoodieCatalog.loadTable(ident)
        assertTrue(loaded.isInstanceOf[HoodieInternalV2Table])
        val v2 = loaded.asInstanceOf[HoodieInternalV2Table]
        assertTrue(v2.capabilities().contains(TableCapability.BATCH_READ))
        assertTrue(v2.capabilities().contains(TableCapability.V1_BATCH_WRITE))
        assertTrue(v2.schema().fieldNames.contains("id"))
        assertTrue(v2.partitioning().isEmpty)
        assertFalse(v2.properties().isEmpty)
        // v2.name() is catalog-qualified on Spark 3.4+ (spark_catalog.default.<t>) but only
        // db-qualified on Spark 3.3 (TableIdentifier has no catalog field there), so match either.
        assertTrue(
          v2.name() == s"spark_catalog.default.$tableName" || v2.name() == s"default.$tableName",
          v2.name())

        // Append then overwrite through the V1-fallback write builder.
        spark.sql(s"insert into $tableName values (1, 'a1', 1000), (2, 'a2', 2000)")
        checkAnswer(s"select id, name from $tableName")(Seq(1, "a1"), Seq(2, "a2"))
        spark.sql(s"insert overwrite table $tableName values (3, 'a3', 3000)")
        checkAnswer(s"select id, name from $tableName")(Seq(3, "a3"))
      }
    }
  }
}
