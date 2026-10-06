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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.{HoodieSchemaConversionUtils, HoodieTableSchema}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieAvroRecord, HoodieFileFormat, HoodieKey, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.read.CustomPayloadForTesting
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import scala.collection.JavaConverters._

/**
 * Verifies nested schema pruning on Hudi tables. The pruning itself is performed by Spark's
 * built-in SchemaPruning rule: the file-group-reader based file format extends ParquetFileFormat,
 * so Hudi does not ship a nested schema pruning rule of its own.
 */
class TestNestedSchemaPruningOptimization extends HoodieSparkSqlTestBase {

  // NOTE: We disable WCE once for the whole suite so the executed plans stay a plain Project over the
  //       scan node (no WholeStageCodegenExec wrapper); every test here pattern-matches the scan to
  //       read its required schema, so this keeps the plan-inspection helpers free of side effects.
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)
  }

  test("Test NestedSchemaPruning optimization successful") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        createTableWithNestedStructSchema(tableType, tableName, tablePath)

        // Only "item.name" is referenced, so the read schema prunes "item" down to a single leaf
        val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")

        val expectedSchema = StructType(Seq(
          StructField("id", IntegerType, nullable = true),
          StructField("item", StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
        ))

        assertPrunedReadSchema(selectDF, tableName, expectedSchema)

        checkAnswer(s"SELECT id, item.name FROM $tableName")(Seq(1, "a1"))
      }
    }
  }

  test("Test nested schema pruning with a projection-incompatible custom payload") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // NOTE: A payload class outside the well-known set puts the table in CUSTOM merge mode, whose
      //       merger is not projection compatible, so the file group reader merges on the full
      //       table schema internally (FileGroupReaderSchemaHandler#generateRequiredSchema) and
      //       projects the merged rows back down to the pruned read schema afterwards
      createTableWithNestedStructSchema("mor", tableName, tablePath,
        Map(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> classOf[CustomPayloadForTesting].getName),
        populateMetaFields = true)

      // The update writes a log file, so the pruned reads below actually merge through that gate
      spark.sql(s"UPDATE $tableName SET ts = 123457 WHERE id = 1")

      val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")

      // Spark still prunes the scan schema; the full-schema requirement is internal to the reader
      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("item", StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
      ))
      assertPrunedReadSchema(selectDF, tableName, expectedSchema)

      checkAnswer(s"SELECT id, item.name FROM $tableName")(Seq(1, "a1"))
      // A second query pruned to different leaves still observes the correctly merged values
      checkAnswer(s"SELECT id, item.price, ts FROM $tableName")(Seq(1, 10, 123457))
    }
  }

  test("Test nested schema pruning with array, map and partition columns") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // NOTE: Meta fields stay enabled here since the SQL UPDATE on MOR needs them to locate the file group
      createTableWithNestedStructSchema("mor", tableName, tablePath,
        extraColumns = ", array(named_struct('k', 'k0', 'v', 'v0')) AS tags," +
          " map('m0', named_struct('a', 1, 'b', 2)) AS props",
        partitionCol = "part",
        populateMetaFields = true)

      // The update writes a log file, so the pruned reads below merge base and log records
      // (the historically fragile path of HUDI-5443)
      spark.sql(s"UPDATE $tableName SET ts = 123457 WHERE id = 1")

      // Only "item.name" is referenced, so "item" is pruned down to a single leaf and the
      // unreferenced "tags" (array<struct>) and "props" (map<string,struct>) columns are dropped
      val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")
      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("item", StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
      ))
      assertPrunedReadSchema(selectDF, tableName, expectedSchema)

      // Partition column values must survive the pruned read (see #18570)
      checkAnswer(s"SELECT id, item.name, part FROM $tableName")(Seq(1, "a1", "p1"))

      // Projecting through the array keeps only the referenced leaf of the element struct
      val tagsDF = spark.sql(s"SELECT tags.k FROM $tableName")
      val expectedTagsSchema = StructType(Seq(
        StructField("tags",
          ArrayType(StructType(Seq(StructField("k", StringType, nullable = false))), containsNull = false),
          nullable = true)
      ))
      assertPrunedReadSchema(tagsDF, tableName, expectedTagsSchema)
      checkAnswer(s"SELECT tags.k FROM $tableName")(Seq(Seq("k0")))

      // Projecting through the map keeps only the referenced leaf of the value struct
      val propsDF = spark.sql(s"SELECT props['m0'].a FROM $tableName")
      val expectedPropsSchema = StructType(Seq(
        StructField("props",
          MapType(StringType, StructType(Seq(StructField("a", IntegerType, nullable = false))), valueContainsNull = false),
          nullable = true)
      ))
      assertPrunedReadSchema(propsDF, tableName, expectedPropsSchema)
      checkAnswer(s"SELECT props['m0'].a FROM $tableName")(Seq(1))

      // Filter on a nested field combined with pruning
      checkAnswer(s"SELECT id, item.price FROM $tableName WHERE item.name = 'a1'")(Seq(1, 10))
    }
  }

  test("Test nested schema pruning through a union column") {
    withTempDir { tmp =>
      Seq(HoodieTableType.COPY_ON_WRITE, HoodieTableType.MERGE_ON_READ).foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        // A union with two or more non-null branches reaches a table only from a writer that takes a raw
        // Avro schema (Streamer, Flink, Kafka Connect, the Java client): a Spark write round-trips its
        // writer schema through InternalSchema, which keeps the first branch only. Spark reads the union
        // as the struct of nullable member0..memberN fields spark-avro gives it, and its nested schema
        // pruning can then ask for a subset of those members, which pruneDataSchema cannot honour: a
        // union is kept whole (#19825), so the scan's output projection has to drop the rest by name.
        val schema = HoodieSchema.parse(
          """{"type":"record","name":"union_record","namespace":"hoodie.test","fields":[
            |{"name":"id","type":"int"},
            |{"name":"choice","type":["null","string","int"],"default":null},
            |{"name":"pick","type":["null","string","int","long"],"default":null},
            |{"name":"nested","type":["null","int",{"type":"record","name":"nested_branch","fields":[
            |{"name":"x","type":["null","string"],"default":null},
            |{"name":"y","type":["null","string"],"default":null}]}],"default":null},
            |{"name":"ts","type":"long"}]}""".stripMargin)
        HoodieTableMetaClient.newTableBuilder()
          .setTableType(tableType)
          .setTableName(tableName)
          .setRecordKeyFields("id")
          .setOrderingFields("ts")
          .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)
        val jsc = new JavaSparkContext(spark.sparkContext)
        val writeConfig = HoodieWriteConfig.newBuilder().withPath(tablePath).withSchema(schema.toString).forTable(tableName).build()
        val client = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), writeConfig)
        def nestedBranch(x: String, y: String): GenericRecord = {
          val branchSchema = schema.getField("nested").get().schema().getTypes.get(2).getAvroSchema
          val branchRecord = new GenericData.Record(branchSchema)
          branchRecord.put("x", x)
          branchRecord.put("y", y)
          branchRecord
        }
        def record(id: Int, choice: AnyRef, pick: AnyRef, nested: AnyRef, ts: Long): HoodieRecord[HoodieAvroPayload] = {
          val avroRecord = new GenericData.Record(schema.getAvroSchema)
          avroRecord.put("id", Int.box(id))
          avroRecord.put("choice", choice)
          avroRecord.put("pick", pick)
          avroRecord.put("nested", nested)
          avroRecord.put("ts", Long.box(ts))
          new HoodieAvroRecord(new HoodieKey(id.toString, ""), new HoodieAvroPayload(HOption.of[GenericRecord](avroRecord)))
        }
        try {
          val insertInstant = client.startCommit()
          client.commit(insertInstant, client.insert(jsc.parallelize(Seq(
            record(1, "a1", Long.box(100L), nestedBranch("n1", "n2"), 1000L),
            record(2, Int.box(7), "b2", Int.box(9), 1000L)).asJava), insertInstant))
          if (tableType == HoodieTableType.MERGE_ON_READ) {
            // The update writes a log file, so the pruned reads below merge base and log records
            val updateInstant = client.startCommit()
            client.commit(updateInstant, client.upsert(jsc.parallelize(Seq(
              record(1, "a1", Long.box(100L), nestedBranch("n1", "n2"), 1001L)).asJava), updateInstant))
          }
        } finally {
          client.close()
        }
        val choiceSchema = new TableSchemaResolver(createMetaClient(spark, tablePath)).getTableSchema.getField("choice").get().schema()
        assertEquals(HoodieSchemaType.UNION, choiceSchema.getType)
        assertEquals(3, choiceSchema.getTypes.size())

        spark.read.format("hudi").load(tablePath).createOrReplaceTempView(tableName)
        // One member out of two: Spark prunes the struct down to it while the reader still emits both
        val member1DF = spark.sql(s"SELECT id, choice.member1 FROM $tableName")
        assertEquals(StructType(Seq(StructField("member1", IntegerType, nullable = true))), prunedStructTypeOf(member1DF, "choice"))
        checkAnswer(s"SELECT id, choice.member1 FROM $tableName")(Seq(1, null), Seq(2, 7))
        checkAnswer(s"SELECT id, choice.member0 FROM $tableName")(Seq(1, "a1"), Seq(2, null))
        // Two members out of three, and not the leading ones
        checkAnswer(s"SELECT id, pick.member0, pick.member2 FROM $tableName")(Seq(1, null, 100L), Seq(2, "b2", null))
        checkAnswer(s"SELECT id, pick.member2 FROM $tableName")(Seq(1, 100L), Seq(2, null))
        // A record branch: the required schema converts back to a union over a record, and the projection
        // has to drop both the other member and the inner field Spark did not ask for
        val nestedDF = spark.sql(s"SELECT id, nested.member1.x FROM $tableName")
        assertEquals(
          StructType(Seq(StructField("member1", StructType(Seq(StructField("x", StringType, nullable = true))), nullable = true))),
          prunedStructTypeOf(nestedDF, "nested"))
        checkAnswer(s"SELECT id, nested.member1.x FROM $tableName")(Seq(1, "n1"), Seq(2, null))
        checkAnswer(s"SELECT id, nested.member0 FROM $tableName")(Seq(1, null), Seq(2, 9))
        checkAnswer(s"SELECT id, nested.member1 FROM $tableName")(Seq(1, Row("n1", "n2")), Seq(2, null))
        // The whole struct is not pruned and comes back as written, next to the ordering value the log record carries
        val tsOfFirstRecord = if (tableType == HoodieTableType.MERGE_ON_READ) 1001L else 1000L
        checkAnswer(s"SELECT id, choice, ts FROM $tableName")(Seq(1, Row("a1", null), tsOfFirstRecord), Seq(2, Row(null, 7), 1000L))
      }
    }
  }

  test("Test a projection of columns named memberN is not read as a union") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        // canBeUnion matches a struct whose fields are all nullable and named member0..memberN. At the
        // root that is a projection of columns that happen to be named that way, not a union, and
        // converting it to one made pruneDataSchema hand the reader the whole table schema -- so
        // SELECT member0 came back with _hoodie_commit_time -- or made Avro reject the duplicate
        // branch types outright.
        spark.sql(
          s"""
             |CREATE TABLE $tableName USING HUDI
             |TBLPROPERTIES (type = '$tableType', primaryKey = 'id', orderingFields = 'ts')
             |LOCATION '$tablePath'
             |AS SELECT 1 AS id, 'a1' AS member0, 'b1' AS member1, 123456 AS ts
             """.stripMargin)
        // The update writes a log file on MOR, so the reads below go through the merge path
        spark.sql(s"UPDATE $tableName SET ts = 123457 WHERE id = 1")

        checkAnswer(s"SELECT member0 FROM $tableName")(Seq("a1"))
        checkAnswer(s"SELECT member0, member1 FROM $tableName")(Seq("a1", "b1"))
        checkAnswer(s"SELECT id, member0 FROM $tableName")(Seq(1, "a1"))
      }
    }
  }

  test("Test no nested schema pruning when disabled") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      createTableWithNestedStructSchema("mor", tableName, tablePath)

      // With the optimizer flag off no nested schema pruning happens, so "item" keeps "price" even
      // though only "item.name" is projected
      val expectedItemStruct = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("price", IntegerType, nullable = false)
      ))

      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")
        assertEquals(expectedItemStruct, prunedStructTypeOf(selectDF, "item"))
        selectDF.count
      }
    }
  }

  test("Nested struct predicate pushdown") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        // NOTE: Meta fields stay enabled since the SQL UPDATE on MOR needs them to locate the file group
        createTableWithNestedStructSchema(tableType, tableName, tablePath, populateMetaFields = true)
        // Move id 1 off the filtered value and add id 2 on it; for MOR the update lands in a log
        // file, so the read has to merge a base record that matches with a log record that does not.
        spark.sql(s"INSERT INTO $tableName SELECT 2 AS id, named_struct('name', 'a1', 'price', 20) AS item, 123457 AS ts")
        spark.sql(s"UPDATE $tableName SET item = named_struct('name', 'zz', 'price', 10) WHERE id = 1")
        if (tableType == "mor") {
          assertHasLogFiles(tablePath)
        }

        val query = s"SELECT id FROM $tableName WHERE item.name = 'a1' AND item.price > 5"
        // NOTE: Spark infers a top-level IsNotNull(item) from the nested predicates and pushes it
        //       regardless of the allowlist, so assert on the nested predicates themselves.
        val nestedFilters = Seq("EqualTo(item.name,a1)", "GreaterThan(item.price,5)")

        val pushedFilters = pushedFiltersOf(spark.sql(query))
        nestedFilters.foreach { f =>
          assertTrue(pushedFilters.contains(f), s"[$tableType] $f should be pushed down, got $pushedFilters")
        }
        checkAnswer(query)(Seq(2))
        checkAnswer(s"SELECT id FROM $tableName WHERE item.name = 'zz'")(Seq(1))

        // Negative leg: with an empty allowlist the nested predicates must not be pushed.
        withSQLConf(SQLConf.NESTED_PREDICATE_PUSHDOWN_FILE_SOURCE_LIST.key -> "") {
          val prunedFilters = pushedFiltersOf(spark.sql(query))
          nestedFilters.foreach { f =>
            assertFalse(prunedFilters.contains(f), s"[$tableType] $f should not be pushed down, got $prunedFilters")
          }
          checkAnswer(query)(Seq(2))
        }
      }
    }
  }

  test("Nested struct predicate pushdown with schema on read and a renamed nested column") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withSQLConf("hoodie.schema.on.read.enable" -> "true") {
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"

          createTableWithNestedStructSchema(tableType, tableName, tablePath, populateMetaFields = true)
          spark.sql(s"ALTER TABLE $tableName RENAME COLUMN item.name TO title")
          // For MOR, also merge in a log file written under the new name. COW is left alone so its
          // base file keeps the old name (an update would rewrite it under the new one).
          if (tableType == "mor") {
            spark.sql(s"UPDATE $tableName SET ts = ts + 1 WHERE id = 1")
            assertHasLogFiles(tablePath)
          }

          // The base file still stores item.name, so the pushed filter must be rewritten to the
          // file-side name rather than dropped or failing the read.
          val query = s"SELECT id FROM $tableName WHERE item.title = 'a1'"
          val pushedFilters = pushedFiltersOf(spark.sql(query))
          assertTrue(pushedFilters.contains("EqualTo(item.title,a1)"),
            s"[$tableType] EqualTo(item.title,a1) should be pushed down, got $pushedFilters")
          checkAnswer(query)(Seq(1))
          checkAnswer(s"SELECT id FROM $tableName WHERE item.title = 'a2'")()
        }
      }
    }
  }

  test("Nested struct predicate pushdown with schema on read and quoted column names") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        // NOTE: A top-level rename fails the incoming-schema check unless column drops are allowed
        withSQLConf("hoodie.schema.on.read.enable" -> "true",
          "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true") {
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          // Names that are not plain identifiers reach the reader backtick-quoted (`名字`,
          // item.`名称`) once nested predicate pushdown is enabled for the source. Only names
          // Avro accepts can be written, so hyphens and dots are covered by the unit tests.
          spark.sql(
            s"""
               |CREATE TABLE $tableName USING HUDI
               |TBLPROPERTIES (type = '$tableType', primaryKey = 'id', orderingFields = 'ts')
               |LOCATION '$tablePath'
               |AS SELECT
               |  1 AS id,
               |  '李明' AS `名字`,
               |  10 AS `旧分数`,
               |  named_struct('名称', 'a1', 'price', 10) AS item,
               |  123456 AS ts
               """.stripMargin)
          spark.sql(s"ALTER TABLE $tableName RENAME COLUMN `旧分数` TO `新分数`")
          spark.sql(s"ALTER TABLE $tableName RENAME COLUMN item.`名称` TO `标题`")
          if (tableType == "mor") {
            spark.sql(s"UPDATE $tableName SET ts = ts + 1 WHERE id = 1")
            assertHasLogFiles(tablePath)
          }

          val nestedQuery = s"SELECT id FROM $tableName WHERE item.`标题` = 'a1'"
          val pushedFilters = pushedFiltersOf(spark.sql(nestedQuery))
          assertTrue(pushedFilters.contains("EqualTo(item.`标题`,a1)"),
            s"[$tableType] EqualTo(item.`标题`,a1) should be pushed down, got $pushedFilters")
          checkAnswer(nestedQuery)(Seq(1))
          checkAnswer(s"SELECT id FROM $tableName WHERE item.`标题` = 'a2'")()
          checkAnswer(s"SELECT id FROM $tableName WHERE `名字` = '李明'")(Seq(1))
          checkAnswer(s"SELECT id FROM $tableName WHERE `新分数` > 5")(Seq(1))
          checkAnswer(s"SELECT id FROM $tableName WHERE `新分数` > 50")()
        }
      }
    }
  }

  test("shortName matches the base file format") {
    val unusedSchema = StructType.fromDDL("id int, ts long")
    def formatFor(fileFormat: HoodieFileFormat, multipleBaseFileFormats: Boolean = false) =
      new HoodieFileGroupReaderBasedFileFormat(
        "unused", HoodieTableSchema(unusedSchema,
          HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(unusedSchema, "unused", "hoodie.test")),
        "unused", "000", Seq.empty, isMOR = false, isBootstrap = false, isIncremental = false,
        "", shouldUseRecordPosition = false, Seq.empty, multipleBaseFileFormats, fileFormat)

    assertEquals("parquet", formatFor(HoodieFileFormat.PARQUET).shortName())
    assertEquals("orc", formatFor(HoodieFileFormat.ORC).shortName())
    assertEquals("lance", formatFor(HoodieFileFormat.LANCE).shortName())
    assertEquals("parquet", formatFor(HoodieFileFormat.ORC, multipleBaseFileFormats = true).shortName())
  }

  private def pushedFiltersOf(selectDF: DataFrame): String =
    findFileScan(selectDF.queryExecution.executedPlan).metadata.getOrElse("PushedFilters", "")

  private def assertHasLogFiles(tablePath: String): Unit = {
    val stream = java.nio.file.Files.walk(java.nio.file.Paths.get(tablePath))
    try {
      val hasLogFile = stream.iterator().asScala.exists { p =>
        !p.toString.contains("/.hoodie/") && p.getFileName.toString.contains(".log.")
      }
      assertTrue(hasLogFile, s"Expected log files under $tablePath")
    } finally {
      stream.close()
    }
  }

  private def findFileScan(plan: SparkPlan): FileSourceScanExec = {
    val scans = plan.collect { case f: FileSourceScanExec => f }
    assert(scans.nonEmpty, s"No FileSourceScanExec found in plan:\n$plan")
    scans.head
  }

  private def assertPrunedReadSchema(selectDF: DataFrame,
                                     tableName: String,
                                     expectedSchema: StructType): Unit = {
    val fileScan = fileScanOf(selectDF)
    assertEquals(tableName, fileScan.tableIdentifier.get.table)
    assertEquals(expectedSchema, fileScan.requiredSchema)
  }

  private def prunedStructTypeOf(selectDF: DataFrame, fieldName: String): StructType =
    fileScanOf(selectDF).requiredSchema(fieldName).dataType.asInstanceOf[StructType]

  private def fileScanOf(selectDF: DataFrame): FileSourceScanExec =
    selectDF.queryExecution.executedPlan match {
      case ProjectExec(_, fileScan: FileSourceScanExec) => fileScan
      case other => fail(s"Unexpected plan shape (expected Project over FileSourceScanExec):\n$other")
    }

  private def createTableWithNestedStructSchema(tableType: String,
                                                tableName: String,
                                                tablePath: String,
                                                opts: Map[String, String] = Map.empty,
                                                extraColumns: String = "",
                                                partitionCol: String = "",
                                                populateMetaFields: Boolean = false): Unit = {
    val partitionedByClause = if (partitionCol.nonEmpty) s"PARTITIONED BY ($partitionCol)" else ""
    val partitionSelectExpr = if (partitionCol.nonEmpty) s", 'p1' AS $partitionCol" else ""
    val optsClause = if (opts.nonEmpty) "," + opts.map { case (k, v) => s"'$k' = '$v'" }.mkString(",") else ""
    spark.sql(
      s"""
         |CREATE TABLE $tableName USING HUDI
         |$partitionedByClause
         |TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.populate.meta.fields = '$populateMetaFields'
         |  $optsClause
         |)
         |LOCATION '$tablePath'
         |AS SELECT
         |  1 AS id,
         |  named_struct('name', 'a1', 'price', 10) AS item$extraColumns,
         |  123456 AS ts$partitionSelectExpr
             """.stripMargin)
  }
}
