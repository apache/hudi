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
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.connector.catalog.TableCapability.{ACCEPT_ANY_SCHEMA, BATCH_READ, OVERWRITE_BY_FILTER, TRUNCATE, V1_BATCH_WRITE}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.command.ShowHoodieCreateTableCommand
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.io.File

import scala.collection.JavaConverters._

/**
 * DDL-level coverage for [[HoodieCatalog]], [[HoodieInternalV2Table]] and the
 * create/show-create table commands, exercised end-to-end through the V2 session
 * catalog wired up by [[HoodieSparkSqlTestBase]] (spark_catalog = HoodieCatalog).
 */
class TestHoodieCatalogDDL extends HoodieSparkSqlTestBase {

  private def hoodieCatalog: HoodieCatalog =
    spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[HoodieCatalog]

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
      assertEquals(Seq("id", "name", "ts"),
        HoodieSqlCommonUtils.removeMetaFields(loaded.schema()).fieldNames.toSeq)

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

      // alterTable: any change that is neither AddColumn nor a ColumnChange falls through to the
      // default arm of HoodieCatalog.alterTable and is reported as unsupported.
      val unsupportedChange = intercept[UnsupportedOperationException] {
        catalog.alterTable(ident, TableChange.setProperty("some.key", "v"))
      }
      assertTrue(unsupportedChange.getMessage.contains("SetProperty"), unsupportedChange.getMessage)

      // renameTable moves the catalog entry.
      val renamed = Identifier.of(Array("default"), s"${tableName}_renamed")
      catalog.renameTable(ident, renamed)
      assertFalse(catalog.tableExists(ident))
      assertTrue(catalog.tableExists(renamed))

      // dropTable removes the Hudi table from the catalog.
      assertTrue(catalog.dropTable(renamed))
      assertFalse(catalog.tableExists(renamed))
      // HoodieCatalog.dropTable passes purge = false, so the external location survives the drop.
      assertTrue(existsPath(tablePath))
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

    // Spark's ResolveSessionCatalog only emits the V1 ShowCreateTableCommand under
    // `spark.sql.legacy.useV1Command`; HoodieAnalysis then rewrites it to ShowHoodieCreateTableCommand.
    withSQLConf("spark.sql.legacy.useV1Command" -> "true") {
      val ddl = spark.sql(s"show create table $tableName").head().getString(0)
      assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS"), ddl)
      assertTrue(ddl.contains(s"`default`.`$tableName`"), ddl)
      assertTrue(ddl.contains("USING hudi"), ddl)
      assertTrue(ddl.contains("PARTITIONED BY (dt)"), ddl)
      assertTrue(ddl.contains("COMMENT 'a hudi table'"), ddl)
      assertTrue(ddl.contains("TBLPROPERTIES"), ddl)
      assertTrue(ddl.contains("primaryKey='id'"), ddl)
    }

    intercept[NoSuchTableException] {
      ShowHoodieCreateTableCommand(TableIdentifier("does_not_exist_tbl")).run(spark)
    }
  }

  test("CREATE over an existing location rejects conflicting table properties") {
    withTempDir { tmp =>
      val basePath = s"${tmp.getCanonicalPath}/shared"
      val first = generateTableName
      spark.sql(
        s"""
           |create table $first (id int, name string, ts long) using hudi
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |location '$basePath'
           |""".stripMargin)

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
        assertEquals(
          Set(BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE, ACCEPT_ANY_SCHEMA).asJava,
          v2.capabilities())
        assertTrue(v2.schema().fieldNames.contains("id"))
        assertTrue(v2.partitioning().isEmpty)
        assertFalse(v2.properties().isEmpty)
        // v2.name() is catalog-qualified on Spark 3.4+ (spark_catalog.default.<t>) but only
        // db-qualified on Spark 3.3 (TableIdentifier has no catalog field there), so match either.
        assertTrue(
          v2.name() == s"spark_catalog.default.$tableName" || v2.name() == s"default.$tableName",
          v2.name())

        // HoodieSpark35Analysis (and its per-version HoodieSpark3xAnalysis siblings) rewrites the
        // V2 relation behind an InsertIntoStatement into the V1 LogicalRelation before any write
        // builder is created, so these inserts cover the V2-to-V1 relation conversion rather than
        // HoodieV1WriteBuilder.
        spark.sql(s"insert into $tableName values (1, 'a1', 1000), (2, 'a2', 2000)")
        checkAnswer(s"select id, name from $tableName")(Seq(1, "a1"), Seq(2, "a2"))
        spark.sql(s"insert overwrite table $tableName values (3, 'a3', 3000)")
        checkAnswer(s"select id, name from $tableName")(Seq(3, "a3"))
      }
    }
  }
}
