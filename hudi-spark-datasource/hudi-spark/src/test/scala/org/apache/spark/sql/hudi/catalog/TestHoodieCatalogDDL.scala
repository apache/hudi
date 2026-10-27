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

import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableCapability.{ACCEPT_ANY_SCHEMA, BATCH_READ, OVERWRITE_BY_FILTER, TRUNCATE, V1_BATCH_WRITE}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.command.ShowHoodieCreateTableCommand
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.io.File
import java.util.Locale

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

  test("alterTable ignores re-sent table properties and rejects changes to persisted table configs") {
    withTempDir { tmp =>
      val catalog = hoodieCatalog
      val tableName = generateTableName
      val ident = Identifier.of(Array("default"), tableName)
      val tableIdent = TableIdentifier(tableName, Some("default"))
      val schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("ts", LongType)))
      // The properties Spark Declarative Pipelines passes on create and re-sends as SetProperty on every run.
      val props = Map(
        "provider" -> "hudi",
        "comment" -> "a hudi table",
        "hoodie.datasource.write.recordkey.field" -> "id",
        "hoodie.datasource.write.precombine.field" -> "ts",
        "hoodie.table.type" -> "COPY_ON_WRITE")
      catalog.createTable(ident, schema, Array.empty[Transform],
        (props + ("location" -> s"${tmp.getCanonicalPath}/$tableName")).asJava)
      val propertiesBefore = spark.sessionState.catalog.getTableMetadata(tableIdent).properties

      // Re-sending unchanged values, including SQL aliases and differently cased values, is a no-op.
      catalog.alterTable(ident, props.map { case (k, v) => TableChange.setProperty(k, v) }.toSeq: _*)
      catalog.alterTable(ident,
        TableChange.setProperty("PRIMARYKEY", "id"),
        TableChange.setProperty("TYPE", "cow"),
        TableChange.setProperty("hoodie.table.type", "copy_on_write"),
        TableChange.setProperty("PRECOMBINEFIELD", "ts"),
        TableChange.setProperty("hoodie.table.precombine.field", "ts"),
        TableChange.setProperty("hoodie.table.ordering.fields", "ts"))
      assertEquals(propertiesBefore, spark.sessionState.catalog.getTableMetadata(tableIdent).properties)

      // Changing or removing a property persisted in hoodie.properties is rejected.
      val recordKeyChange = intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.setProperty("hoodie.datasource.write.recordkey.field", "name"))
      }
      assertTrue(recordKeyChange.getMessage.contains("hoodie.table.recordkey.fields"), recordKeyChange.getMessage)
      intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.setProperty("type", "mor"))
      }
      intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.setProperty("hoodie.datasource.write.precombine.field", "name"))
      }
      intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.removeProperty("primaryKey"))
      }
      Seq("PRIMARYKEY", "PrimaryKey").foreach { key =>
        intercept[HoodieAnalysisException] {
          catalog.alterTable(ident, TableChange.setProperty(key, "name"))
        }
        intercept[HoodieAnalysisException] {
          catalog.alterTable(ident, TableChange.removeProperty(key))
        }
      }
      // Field names and class names are case-sensitive, unlike table-type enum values.
      intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.setProperty("primaryKey", "ID"))
      }
      intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.setProperty("primaryKey", " id "))
      }
      val tableConfig = HoodieCatalogTable(spark, spark.sessionState.catalog.getTableMetadata(tableIdent)).tableConfig
      intercept[HoodieAnalysisException] {
        catalog.alterTable(ident, TableChange.setProperty("hoodie.table.keygenerator.class",
          tableConfig.getKeyGeneratorClassName.toUpperCase(Locale.ROOT)))
      }
      // A rejected change does not apply the other changes of the same call.
      intercept[HoodieAnalysisException] {
        catalog.alterTable(ident,
          TableChange.setProperty("some.key", "v"), TableChange.setProperty("primaryKey", "name"))
      }
      assertEquals(propertiesBefore, spark.sessionState.catalog.getTableMetadata(tableIdent).properties)

      // Other properties and the comment are applied to the catalog table.
      catalog.alterTable(ident,
        TableChange.setProperty("some.key", "v"), TableChange.setProperty("comment", "new comment"))
      val altered = spark.sessionState.catalog.getTableMetadata(tableIdent)
      assertEquals((propertiesBefore - "comment") + ("some.key" -> "v"), altered.properties - "comment")
      assertEquals(Some("v"), altered.properties.get("some.key"))
      assertEquals(Some("new comment"), altered.comment)
      catalog.alterTable(ident, TableChange.removeProperty("some.key"))
      assertFalse(spark.sessionState.catalog.getTableMetadata(tableIdent).properties.contains("some.key"))

      // The table is still writable with its original configuration.
      spark.sql(s"insert into $tableName values (1, 'a1', 1000)")
      checkAnswer(s"select id, name, ts from $tableName")(Seq(1, "a1", 1000))
    }
  }

  test("alterTable applies mutable property changes in order, including table-service write configs") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val ident = Identifier.of(Array("default"), tableName)
      val tableIdent = TableIdentifier(tableName, Some("default"))
      val catalog = hoodieCatalog
      catalog.createTable(ident, StructType(Seq(StructField("id", IntegerType))), Array.empty[Transform],
        Map("provider" -> "hudi", "primaryKey" -> "id", "location" -> s"${tmp.getCanonicalPath}/$tableName").asJava)
      def properties: Map[String, String] = spark.sessionState.catalog.getTableMetadata(tableIdent).properties
      val before = properties

      catalog.alterTable(ident, TableChange.setProperty("some.key", "v"), TableChange.removeProperty("some.key"))
      assertEquals(before, properties)
      catalog.alterTable(ident, TableChange.setProperty("some.key", "old"))
      catalog.alterTable(ident, TableChange.setProperty("some.key", "new"), TableChange.setProperty("some.key", "old"))
      assertEquals(Some("old"), properties.get("some.key"))
      catalog.alterTable(ident, TableChange.removeProperty("some.key"), TableChange.setProperty("some.key", "old"))
      assertEquals(Some("old"), properties.get("some.key"))
      // Net no-ops on one key must also remain no-ops when another key is changed in the same call.
      catalog.alterTable(ident, TableChange.setProperty("some.key", "new"), TableChange.setProperty("some.key", "old"),
        TableChange.setProperty("another.key", "v"), TableChange.removeProperty("absent.key"))
      assertEquals(Some("old"), properties.get("some.key"))
      assertEquals(Some("v"), properties.get("another.key"))

      val writeConfigs = Seq("hoodie.table.services.enabled", "hoodie.table.services.incremental.enabled",
        "hoodie.table.service.manager.enabled")
      catalog.alterTable(ident, writeConfigs.map(TableChange.setProperty(_, "false")): _*)
      writeConfigs.foreach(key => assertEquals(Some("false"), properties.get(key)))
      catalog.alterTable(ident, writeConfigs.map(TableChange.removeProperty): _*)
      writeConfigs.foreach(key => assertFalse(properties.contains(key)))
    }
  }

  test("alterTable preserves unrelated properties changed after the initial catalog read") {
    withTempDir { tmp =>
      val session = spark.sessionState.catalog
      val tableName = generateTableName
      val ident = Identifier.of(Array("default"), tableName)
      val tableIdent = TableIdentifier(tableName, Some("default"))
      hoodieCatalog.createTable(ident, StructType(Seq(StructField("id", IntegerType))), Array.empty[Transform],
        Map("provider" -> "hudi", "primaryKey" -> "id", "a" -> "old", "location" -> s"${tmp.getCanonicalPath}/$tableName").asJava)
      val delegate = new V2SessionCatalog(session)
      def setA(value: String): Unit = {
        val current = session.getTableMetadata(tableIdent)
        session.alterTable(current.copy(properties = current.properties + ("a" -> value)))
      }
      // The delegate preserves an unrelated update committed before its own read.
      setA("concurrent")
      delegate.alterTable(ident, TableChange.setProperty("b", "control"))
      assertEquals("concurrent", session.getTableMetadata(tableIdent).properties("a"))
      setA("old")

      val interleaved = new HoodieCatalog {
        private var inject = true
        override def loadTable(id: Identifier): Table = {
          val snapshot = super.loadTable(id)
          if (inject) {
            inject = false
            setA("concurrent")
          }
          snapshot
        }
      }
      interleaved.setDelegateCatalog(delegate)
      interleaved.alterTable(ident, TableChange.setProperty("b", "requested"))
      val properties = session.getTableMetadata(tableIdent).properties
      assertEquals("concurrent", properties("a"))
      assertEquals("requested", properties("b"))
    }
  }

  test("alterTable preserves separately stored comments unless explicitly removed") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val ident = Identifier.of(Array("default"), tableName)
      val tableIdent = TableIdentifier(tableName, Some("default"))
      val catalog = hoodieCatalog
      catalog.createTable(ident, StructType(Seq(StructField("id", IntegerType))), Array.empty[Transform],
        Map("provider" -> "hudi", "primaryKey" -> "id", "comment" -> "keep me",
          "location" -> s"${tmp.getCanonicalPath}/$tableName").asJava)
      def metadata = spark.sessionState.catalog.getTableMetadata(tableIdent)
      def storeCommentSeparately(): Unit = {
        // HiveClientImpl excludes "comment" from properties and returns it only in CatalogTable.comment.
        spark.sessionState.catalog.alterTable(metadata.copy(properties = metadata.properties - "comment"))
      }

      storeCommentSeparately()
      catalog.alterTable(ident, TableChange.setProperty("some.key", "v"))
      assertEquals(Some("keep me"), metadata.comment)
      storeCommentSeparately()
      catalog.alterTable(ident, TableChange.setProperty("comment", "keep me"), TableChange.removeProperty("some.key"))
      assertEquals(Some("keep me"), metadata.comment)
      storeCommentSeparately()
      catalog.alterTable(ident, TableChange.removeProperty("comment"), TableChange.setProperty("comment", "keep me"),
        TableChange.setProperty("some.key", "v2"))
      assertEquals(Some("keep me"), metadata.comment)
      storeCommentSeparately()
      catalog.alterTable(ident, TableChange.removeProperty("comment"))
      assertEquals(None, metadata.comment)
      catalog.alterTable(ident, TableChange.setProperty("some.key", "v3"))
      assertEquals(None, metadata.comment)
    }
  }

  test("createTable with the Column-based overload initializes Hudi tables") {
    // The Column-based overload exists from Spark 3.4 on; it is invoked reflectively so that this suite also
    // compiles against Spark 3.3. Spark 4.1's createTable(Identifier, TableInfo) delegates to it.
    if (HoodieSparkUtils.gteqSpark3_4) {
      withTempDir { tmp =>
        val catalog = hoodieCatalog
        val columnClass = Class.forName("org.apache.spark.sql.connector.catalog.Column")
        val createColumn = columnClass.getMethod("create", classOf[String], classOf[DataType])
        def columns(fields: (String, DataType)*): AnyRef = {
          val array = java.lang.reflect.Array.newInstance(columnClass, fields.size)
          fields.zipWithIndex.foreach { case ((name, dataType), i) =>
            java.lang.reflect.Array.set(array, i, createColumn.invoke(null, name, dataType))
          }
          array
        }
        val createTable = classOf[TableCatalog].getMethod("createTable", classOf[Identifier],
          java.lang.reflect.Array.newInstance(columnClass, 0).getClass, classOf[Array[Transform]], classOf[java.util.Map[_, _]])
        val cols = columns("id" -> IntegerType, "name" -> StringType, "ts" -> LongType)

        val hudiTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$hudiTable"
        createTable.invoke(catalog, Identifier.of(Array("default"), hudiTable), cols, Array.empty[Transform],
          Map("provider" -> "hudi", "primaryKey" -> "id", "preCombineField" -> "ts", "location" -> tablePath).asJava)
        assertTrue(new File(s"$tablePath/.hoodie/hoodie.properties").exists())
        spark.sql(s"insert into $hudiTable values (1, 'a1', 1000)")
        checkAnswer(s"select id, name, ts from $hudiTable")(Seq(1, "a1", 1000))

        // Non-Hudi tables still go to the session catalog.
        val parquetTable = generateTableName
        createTable.invoke(catalog, Identifier.of(Array("default"), parquetTable), cols, Array.empty[Transform],
          Map("provider" -> "parquet", "location" -> s"${tmp.getCanonicalPath}/$parquetTable").asJava)
        assertEquals(Some("parquet"),
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(parquetTable, Some("default"))).provider)
      }
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
