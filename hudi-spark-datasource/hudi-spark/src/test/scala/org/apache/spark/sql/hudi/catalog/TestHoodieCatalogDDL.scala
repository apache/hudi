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
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.functional.ComplexKeyGenFixtures.COMPLEX_KEYGEN_FIXTURES_PATH
import org.apache.hudi.keygen.constant.KeyGeneratorType

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableCapability.{ACCEPT_ANY_SCHEMA, BATCH_READ, OVERWRITE_BY_FILTER, TRUNCATE, V1_BATCH_WRITE}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.command.ShowHoodieCreateTableCommand
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.io.File
import java.nio.file.Files
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

  for (tableType <- Seq("COPY_ON_WRITE", "MERGE_ON_READ"); version <- Seq(6, 8)) {
    test(s"alterTable reconciles custom partition types: $tableType version $version") {
      withTempDir { tmp =>
        val name = generateTableName
        val ident = Identifier.of(Array("default"), name)
        val tableIdent = TableIdentifier(name, Some("default"))
        val path = s"${tmp.getCanonicalPath}/$name"
        val partitionKey = "hoodie.datasource.write.partitionpath.field"
        val partitionValue = "part:simple"
        spark.sql("SELECT 1 AS id, 'a' AS part").write.format("hudi").options(Map(
          "hoodie.table.name" -> name,
          "hoodie.write.table.version" -> version.toString,
          "hoodie.datasource.write.table.type" -> tableType,
          "hoodie.datasource.write.recordkey.field" -> "id",
          HoodieTableConfig.ORDERING_FIELDS.key -> "id",
          "hoodie.datasource.write.keygenerator.class" -> KeyGeneratorType.CUSTOM.getClassName,
          partitionKey -> partitionValue)).save(path)
        spark.sql(s"CREATE TABLE $name USING hudi LOCATION '$path' TBLPROPERTIES ('$partitionKey' = '$partitionValue')")
        try {
          val session = spark.sessionState.catalog
          val before = session.getTableMetadata(tableIdent)
          val config = HoodieCatalogTable(spark, before).tableConfig
          assertEquals(version, config.getTableVersion.versionCode())
          assertEquals(if (version == 6) "part" else partitionValue,
            HoodieTableConfig.getPartitionFieldPropForKeyGenerator(config).get())
          hoodieCatalog.alterTable(ident, TableChange.setProperty(partitionKey, partitionValue))
          Seq("id:simple", "part:timestamp", "part").foreach { changed =>
            intercept[HoodieAnalysisException] {
              hoodieCatalog.alterTable(ident, TableChange.setProperty(partitionKey, changed))
            }
          }
          assertEquals(before, session.getTableMetadata(tableIdent))

          // Even the catalog's current value must match the partition identity recorded on disk.
          val staleValues = if (version == 6) Seq("id:simple", "part:unknown") else Seq("part:timestamp")
          staleValues.foreach { stale =>
            session.alterTable(before.copy(properties = before.properties + (partitionKey -> stale)))
            intercept[HoodieAnalysisException] {
              hoodieCatalog.alterTable(ident, TableChange.setProperty(partitionKey, stale))
            }
          }
        } finally {
          hoodieCatalog.dropTable(ident)
        }
      }
    }
  }

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

  test("createTable rejects missing partition fields before creating Hudi metadata") {
    withTempDir { tmp =>
      val name = generateTableName
      val ident = Identifier.of(Array("default"), name)
      val path = s"${tmp.getCanonicalPath}/$name"
      val error = intercept[HoodieAnalysisException] {
        hoodieCatalog.createTable(ident, StructType(Seq(StructField("id", IntegerType))),
          Array[Transform](Expressions.identity("missing_partition")),
          Map("provider" -> "hudi", "primaryKey" -> "id", "location" -> path).asJava)
      }
      assertTrue(error.getMessage.contains("missing_partition"), error.getMessage)
      assertFalse(hoodieCatalog.tableExists(ident))
      assertFalse(new File(s"$path/.hoodie/hoodie.properties").exists())
    }
  }

  test("createTable normalizes partition field spelling using the resolver") {
    withSQLConf("spark.sql.caseSensitive" -> "false") {
      withTempDir { tmp =>
        val name = generateTableName
        val ident = Identifier.of(Array("default"), name)
        val path = s"${tmp.getCanonicalPath}/$name"
        hoodieCatalog.createTable(ident,
          StructType(Seq(StructField("id", IntegerType), StructField("part", StringType))),
          Array[Transform](Expressions.identity("PART")),
          Map("provider" -> "hudi", "primaryKey" -> "id", "location" -> path).asJava)
        try {
          spark.sql(s"INSERT INTO $name VALUES (1, 'a')")
          checkAnswer(s"SELECT id, part FROM $name")(Seq(1, "a"))
          val props = new java.util.Properties()
          val stream = new java.io.FileInputStream(s"$path/.hoodie/hoodie.properties")
          try props.load(stream) finally stream.close()
          assertEquals("part", props.getProperty("hoodie.table.partition.fields"))
        } finally {
          hoodieCatalog.dropTable(ident)
        }
      }
    }
  }

  Seq(false, true).foreach { operationMetadata =>
    test(s"alterTable preserves metadata columns during repeated schema reconciliation, operation=$operationMetadata") {
      withSQLConf("hoodie.schema.on.read.enable" -> "true") {
        withTempDir { tmp =>
          val catalog = hoodieCatalog
          val tableName = generateTableName
          val ident = Identifier.of(Array("default"), tableName)
          catalog.createTable(ident, StructType(Seq(StructField("id", IntegerType, nullable = false))),
            Array.empty[Transform], Map("provider" -> "hudi", "primaryKey" -> "id",
              "hoodie.allow.operation.metadata.field" -> operationMetadata.toString,
              "location" -> s"${tmp.getCanonicalPath}/$tableName").asJava)
          spark.sql("select 1 as id").write.format("hudi")
            .option("hoodie.table.name", tableName)
            .option("hoodie.datasource.write.recordkey.field", "id")
            .option("hoodie.allow.operation.metadata.field", operationMetadata.toString)
            .option("hoodie.datasource.write.operation", "upsert")
            .mode("append").save(s"${tmp.getCanonicalPath}/$tableName")
          val schemaBefore = catalog.loadTable(ident).schema()
          assertEquals(operationMetadata, schemaBefore.fieldNames.contains(HoodieRecord.OPERATION_METADATA_FIELD))
          val deletes = schemaBefore.fieldNames.filter(_.startsWith("_hoodie_")).toSeq.map { name =>
            TableChange.deleteColumn(Array(name), false)
          }
          (1 to 3).foreach { _ =>
            catalog.alterTable(ident, (deletes ++ Seq(TableChange.setProperty("primaryKey", "id"),
              TableChange.updateColumnNullability(Array("id"), false))): _*)
            catalog.alterTable(ident, TableChange.updateColumnNullability(Array("id"), true))
            assertEquals(schemaBefore, catalog.loadTable(ident).schema())
          }

          val unsupported = Seq(
            TableChange.deleteColumn(Array("id"), false),
            TableChange.deleteColumn(Array("_hoodie_custom"), false),
            TableChange.deleteColumn(Array("nested", "_hoodie_record_key"), false),
            TableChange.renameColumn(Array("id"), "renamed"),
            TableChange.updateColumnNullability(Array("missing"), false),
            TableChange.updateColumnPosition(Array("id"), TableChange.ColumnPosition.first()))
          unsupported.foreach { change =>
            val error = intercept[UnsupportedOperationException] {
              catalog.alterTable(ident, TableChange.addColumn(Array("added"), StringType), change)
            }
            assertTrue(error.getMessage.contains(change.getClass.getName), error.getMessage)
            assertEquals(schemaBefore, catalog.loadTable(ident).schema())
          }
          Seq(true, false).foreach { addFirst =>
            val add = TableChange.addColumn(Array("added"), StringType)
            val widen = TableChange.updateColumnType(Array("id"), LongType)
            intercept[HoodieAnalysisException] {
              catalog.alterTable(ident, (if (addFirst) Seq(add, widen) else Seq(widen, add)): _*)
            }
            assertEquals(schemaBefore, catalog.loadTable(ident).schema())
          }
          catalog.dropTable(ident)
        }
      }
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

  for (tableType <- Seq("cow", "mor"); version <- Seq("6", "8")) {
    test(s"alterTable protects partition and key-generator aliases: $tableType version $version") {
      withTempDir { tmp =>
        val catalog = hoodieCatalog
        val name = generateTableName
        val ident = Identifier.of(Array("default"), name)
        val tableIdent = TableIdentifier(name, Some("default"))
        val path = s"${tmp.getCanonicalPath}/$name"
        val classKey = "hoodie.datasource.write.keygenerator.class"
        val typeKey = "hoodie.datasource.write.keygenerator.type"
        val partitionKey = "hoodie.datasource.write.partitionpath.field"
        val aliases = Map(classKey -> KeyGeneratorType.SIMPLE.getClassName, typeKey -> "SIMPLE", partitionKey -> "part")
        catalog.createTable(ident,
          StructType(Seq(StructField("id", IntegerType), StructField("ts", LongType), StructField("part", StringType))),
          Array[Transform](Expressions.identity("part")),
          (aliases ++ Map("provider" -> "hudi", "primaryKey" -> "id", "preCombineField" -> "ts",
            "type" -> tableType, "hoodie.write.table.version" -> version, "location" -> path)).asJava)
        try {
          spark.sql(s"INSERT INTO $name VALUES (1, 1L, 'a')")
          val session = spark.sessionState.catalog
          val before = session.getTableMetadata(tableIdent)
          val schemaBefore = catalog.loadTable(ident).schema()
          val propertiesFile = new File(s"$path/.hoodie/hoodie.properties").toPath
          val persistedBefore = Files.readAllBytes(propertiesFile).toSeq
          val config = HoodieCatalogTable(spark, before).tableConfig
          assertEquals(version.toInt, config.getTableVersion.versionCode())
          assertEquals(KeyGeneratorType.SIMPLE.getClassName, config.getKeyGeneratorClassName)
          (1 to 3).foreach { _ =>
            catalog.alterTable(ident, (aliases ++ Map(
              typeKey -> "simple",
              HoodieTableConfig.PARTITION_FIELDS.key -> "part",
              HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key -> KeyGeneratorType.SIMPLE.getClassName,
              HoodieTableConfig.KEY_GENERATOR_TYPE.key -> "SIMPLE")).toSeq.map {
              case (key, value) => TableChange.setProperty(key, value)
            }: _*)
          }
          assertEquals(before, session.getTableMetadata(tableIdent))

          val changes = Seq(
            TableChange.setProperty(classKey, KeyGeneratorType.COMPLEX.getClassName),
            TableChange.setProperty(classKey, KeyGeneratorType.SIMPLE.getClassName.toUpperCase(Locale.ROOT)),
            TableChange.setProperty(typeKey, "COMPLEX"),
            TableChange.setProperty(partitionKey, "id")) ++ aliases.keys.toSeq.map(TableChange.removeProperty)
          changes.foreach { change =>
            val error = intercept[HoodieAnalysisException] {
              catalog.alterTable(ident, TableChange.setProperty("some.key", "changed"),
                TableChange.addColumn(Array("added"), StringType), change)
            }
            assertTrue(error.getMessage.contains("hoodie.properties"), error.getMessage)
            assertEquals(before, session.getTableMetadata(tableIdent))
            assertEquals(schemaBefore, catalog.loadTable(ident).schema())
            assertEquals(persistedBefore, Files.readAllBytes(propertiesFile).toSeq)
          }

          // An old catalog entry may already disagree with the persisted table. Re-sending it is not a safe no-op.
          session.alterTable(before.copy(properties = before.properties + (partitionKey -> "id")))
          try {
            intercept[HoodieAnalysisException] {
              catalog.alterTable(ident, TableChange.setProperty(partitionKey, "id"))
            }
          } finally {
            session.alterTable(before)
          }
          assertEquals(persistedBefore, Files.readAllBytes(propertiesFile).toSeq)
          spark.sql(s"INSERT INTO $name VALUES (2, 2L, 'b')")
          checkAnswer(s"SELECT id, ts, part FROM $name ORDER BY id")(Seq(1, 1L, "a"), Seq(2, 2L, "b"))
        } finally {
          catalog.dropTable(ident)
        }
      }
    }
  }

  test("alterTable accepts equivalent Spark and Avro key generators") {
    Seq(KeyGeneratorType.SIMPLE_AVRO -> KeyGeneratorType.SIMPLE,
      KeyGeneratorType.COMPLEX_AVRO -> KeyGeneratorType.COMPLEX).foreach { case (avroGen, sparkGen) =>
      withTempDir { tmp =>
        val catalog = hoodieCatalog
        val name = generateTableName
        val ident = Identifier.of(Array("default"), name)
        val tableIdent = TableIdentifier(name, Some("default"))
        val classKey = "hoodie.datasource.write.keygenerator.class"
        val typeKey = "hoodie.datasource.write.keygenerator.type"
        val createProps = Map(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key -> avroGen.getClassName,
          classKey -> avroGen.getClassName, typeKey -> sparkGen.name())
        catalog.createTable(ident, StructType(Seq(StructField("id", IntegerType), StructField("part", StringType))),
          Array[Transform](Expressions.identity("part")), (createProps ++ Map(
            "provider" -> "hudi", "primaryKey" -> "id", "location" -> s"${tmp.getCanonicalPath}/$name")).asJava)
        try {
          val before = spark.sessionState.catalog.getTableMetadata(tableIdent)
          val tableConfig = HoodieCatalogTable(spark, before).tableConfig
          assertEquals(avroGen.name(), tableConfig.getString(HoodieTableConfig.KEY_GENERATOR_TYPE))
          catalog.alterTable(ident, createProps.toSeq.map { case (k, v) => TableChange.setProperty(k, v) }: _*)
          catalog.alterTable(ident, TableChange.setProperty(classKey, sparkGen.getClassName),
            TableChange.setProperty(typeKey, avroGen.name()))
          assertEquals(before, spark.sessionState.catalog.getTableMetadata(tableIdent))
          intercept[HoodieAnalysisException] {
            catalog.alterTable(ident, TableChange.setProperty(typeKey, KeyGeneratorType.NON_PARTITION.name()))
          }
        } finally {
          catalog.dropTable(ident)
        }
      }
    }
  }

  test("alterTable accepts unchanged identity aliases absent from an unpartitioned catalog entry") {
    withTempDir { tmp =>
      val catalog = hoodieCatalog
      val name = generateTableName
      val ident = Identifier.of(Array("default"), name)
      val tableIdent = TableIdentifier(name, Some("default"))
      catalog.createTable(ident, StructType(Seq(StructField("id", IntegerType))), Array.empty[Transform],
        Map("provider" -> "hudi", "primaryKey" -> "id", "location" -> s"${tmp.getCanonicalPath}/$name").asJava)
      try {
        val before = spark.sessionState.catalog.getTableMetadata(tableIdent)
        val config = HoodieCatalogTable(spark, before).tableConfig
        catalog.alterTable(ident,
          TableChange.setProperty("hoodie.datasource.write.partitionpath.field", ""),
          TableChange.setProperty("hoodie.datasource.write.keygenerator.class", config.getKeyGeneratorClassName),
          TableChange.setProperty("hoodie.datasource.write.keygenerator.type", "non_partition"))
        assertEquals(before, spark.sessionState.catalog.getTableMetadata(tableIdent))
        intercept[HoodieAnalysisException] {
          catalog.alterTable(ident, TableChange.setProperty("hoodie.datasource.write.partitionpath.field", "id"))
        }
        assertEquals(before, spark.sessionState.catalog.getTableMetadata(tableIdent))
      } finally {
        catalog.dropTable(ident)
      }
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

  test("alterTable accepts the full CREATE property set after upgrading a legacy precombine table") {
    withTempDir { tmp =>
      val fixture = "hudi-v6-table-complex-keygen"
      HoodieTestUtils.extractZipToDirectory(s"$COMPLEX_KEYGEN_FIXTURES_PATH$fixture.zip", tmp.toPath, getClass)
      val path = tmp.toPath.resolve(fixture)
      val name = generateTableName
      val ident = Identifier.of(Array("default"), name)
      val tableIdent = TableIdentifier(name, Some("default"))
      val props = Map(
        "provider" -> "hudi", "location" -> path.toString, "type" -> "mor", "primaryKey" -> "id",
        "preCombineField" -> "ts", HoodieTableConfig.PRECOMBINE_FIELD.key -> "ts",
        "hoodie.datasource.write.partitionpath.field" -> "partition,category",
        "hoodie.datasource.write.keygenerator.class" -> KeyGeneratorType.COMPLEX.getClassName,
        "payloadClass" -> classOf[OverwriteWithLatestAvroPayload].getName,
        "recordMergeMode" -> "EVENT_TIME_ORDERING")
      hoodieCatalog.createTable(ident, new StructType(), Array.empty[Transform], props.asJava)
      try {
        val legacy = HoodieCatalogTable(spark, tableIdent).tableConfig
        assertEquals(HoodieTableVersion.SIX, legacy.getTableVersion)
        assertEquals("ts", legacy.getProps.getProperty(HoodieTableConfig.PRECOMBINE_FIELD.key))
        assertFalse(legacy.getProps.containsKey(HoodieTableConfig.ORDERING_FIELDS.key))

        val target = HoodieTableVersion.current()
        checkAnswer(s"CALL upgrade_table(table => '$name', to_version => '${target.name()}')")(Seq(true))
        val upgraded = HoodieCatalogTable(spark, tableIdent).tableConfig
        assertEquals(target, upgraded.getTableVersion)
        assertEquals("ts", upgraded.getProps.getProperty(HoodieTableConfig.ORDERING_FIELDS.key))
        assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, upgraded.getLegacyPayloadClass)
        val before = spark.sessionState.catalog.getTableMetadata(tableIdent)
        val propertiesFile = path.resolve(".hoodie/hoodie.properties")
        val persistedBefore = Files.readAllBytes(propertiesFile).toSeq
        hoodieCatalog.alterTable(ident, props.toSeq.map { case (k, v) => TableChange.setProperty(k, v) }: _*)
        intercept[HoodieAnalysisException] {
          hoodieCatalog.alterTable(ident, TableChange.setProperty("payloadClass", classOf[DefaultHoodieRecordPayload].getName))
        }
        assertEquals(before, spark.sessionState.catalog.getTableMetadata(tableIdent))
        assertEquals(persistedBefore, Files.readAllBytes(propertiesFile).toSeq)
      } finally {
        hoodieCatalog.dropTable(ident)
      }
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
