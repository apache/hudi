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

package org.apache.spark.sql.hudi.dml.schema

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.internal.schema.HoodieSchemaException
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hudi.command.CreateHoodieTableCommand
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, LongType, MapType, MetadataBuilder, StringType, StructField, StructType}


class TestVariantDataType extends HoodieSparkSqlTestBase {

  test(s"Test Table with Variant Data Type") {
    // Variant type is only supported in Spark 4.0+
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    Seq("cow", "mor").foreach { tableType =>
      withRecordType()(withTempDir { tmp =>
        val tableName = generateTableName
        // Create a table with a Variant column
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  v variant,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
         """.stripMargin)

        // Insert data with Variant values using parse_json (Spark 4.0+)
        spark.sql(
          s"""
             |insert into $tableName
             |values
             |  (1, 'row1', parse_json('{"key": "value1", "num": 1}'), 1000),
             |  (2, 'row2', parse_json('{"key": "value2", "list": [1, 2, 3]}'), 1000)
         """.stripMargin)

        // Verify the data by casting Variant to String for deterministic comparison
        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"key\":\"value1\",\"num\":1}", 1000),
          Seq(2, "row2", "{\"key\":\"value2\",\"list\":[1,2,3]}", 1000)
        )

        // Test Updates on Variant column, MOR will generate logs
        spark.sql(
          s"""
             |update $tableName
             |set v = parse_json('{"updated": true, "new_field": 123}')
             |where id = 1
         """.stripMargin)

        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"new_field\":123,\"updated\":true}", 1000),
          Seq(2, "row2", "{\"key\":\"value2\",\"list\":[1,2,3]}", 1000)
        )

        // Test Delete
        spark.sql(s"delete from $tableName where id = 2")

        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"new_field\":123,\"updated\":true}", 1000)
        )

        // Test MergeInto: exercises both MATCHED (UPDATE SET on the Variant
        // column) and NOT MATCHED (INSERT of a new row carrying a Variant
        // literal).
        spark.sql(
          s"""
             |merge into $tableName t
             |using (
             |  select 1 as id, 'row1' as name, parse_json('{"key":"v1-merged"}') as v, 2000L as ts
             |  union all
             |  select 3 as id, 'row3' as name, parse_json('{"key":"v3"}') as v, 2000L as ts
             |) s
             |on t.id = s.id
             |when matched then update set t.v = s.v, t.ts = s.ts
             |when not matched then insert (id, name, v, ts) values (s.id, s.name, s.v, s.ts)
             """.stripMargin)

        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"key\":\"v1-merged\"}", 2000),
          Seq(3, "row3", "{\"key\":\"v3\"}", 2000)
        )
      })
    }
  }

  test("Test Query Log Only MOR Table With VARIANT column triggers compaction") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  v variant,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.index.type = 'INMEMORY',
           |  hoodie.compact.inline = 'true',
           |  hoodie.compact.inline.max.delta.commits = '5',
           |  hoodie.clean.commits.retained = '1'
           | )
       """.stripMargin)

      spark.sql(
        s"insert into $tableName values " +
          "(1, parse_json('{\"key\":\"value1\"}'), 1000)")
      spark.sql(
        s"insert into $tableName values " +
          "(2, parse_json('{\"key\":\"value2\"}'), 1000)")
      spark.sql(
        s"insert into $tableName values " +
          "(3, parse_json('{\"key\":\"value3\"}'), 1000)")
      // 3 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))
      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, "{\"key\":\"value1\"}", 1000),
        Seq(2, "{\"key\":\"value2\"}", 1000),
        Seq(3, "{\"key\":\"value3\"}", 1000)
      )

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 1 as id,
           |         parse_json('{"key":"v1-merged"}') as v,
           |         1001L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      // 4 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))
      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, "{\"key\":\"v1-merged\"}", 1001),
        Seq(2, "{\"key\":\"value2\"}", 1000),
        Seq(3, "{\"key\":\"value3\"}", 1000)
      )

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 4 as id,
           |         parse_json('{"key":"value4"}') as v,
           |         1000L as ts
           |) s0
           | on h0.id = s0.id
           | when not matched then insert *
           |""".stripMargin)

      // 5 commits will trigger compaction.
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tablePath))
      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, "{\"key\":\"v1-merged\"}", 1001),
        Seq(2, "{\"key\":\"value2\"}", 1000),
        Seq(3, "{\"key\":\"value3\"}", 1000),
        Seq(4, "{\"key\":\"value4\"}", 1000)
      )

      // VARIANT must round-trip as native VariantType through the compacted base-file read path.
      val variantField = spark.table(tableName).schema.find(_.name == "v").get
      assertResult("variant")(variantField.dataType.typeName)

      // 6th commit drives an auto-clean that retires the now-superseded log-only slice.
      // Inline compaction on commit 5 ran AFTER its own postCommit clean, so the prior
      // slice was not yet superseded when that clean fired and no .clean instant was
      // written. This deltacommit's postCommit clean writes the .clean instant.
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 2 as id,
           |         parse_json('{"key":"v2-merged"}') as v,
           |         1002L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, "{\"key\":\"v1-merged\"}", 1001),
        Seq(2, "{\"key\":\"v2-merged\"}", 1002),
        Seq(3, "{\"key\":\"value3\"}", 1000),
        Seq(4, "{\"key\":\"value4\"}", 1000)
      )

      val metaClient = createMetaClient(spark, tablePath)
      metaClient.reloadActiveTimeline()
      assert(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() > 0,
        "Expected at least one .clean instant on the timeline after compaction")
    })
  }

  test("Test toHiveCompatibleSchema converts VariantType to physical struct") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    val variantType = DataType.fromDDL("variant")
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType),
      StructField("variant_col", variantType, nullable = true),
      StructField("nested_struct", StructType(Seq(
        StructField("inner_variant", variantType)
      ))),
      StructField("variant_array", ArrayType(variantType)),
      StructField("variant_map", MapType(StringType, variantType)),
      StructField("ts", LongType)
    ))

    val hiveSchema = CreateHoodieTableCommand.toHiveCompatibleSchema(schema)

    // Non-variant fields should be unchanged
    assert(hiveSchema("id").dataType == LongType)
    assert(hiveSchema("name").dataType == StringType)
    assert(hiveSchema("ts").dataType == LongType)

    // Top-level variant should be converted with canonical (metadata, value) field order.
    val variantStruct = assertVariantStruct(hiveSchema("variant_col").dataType)
    assert(variantStruct.fields(0).name == HoodieSchema.Variant.VARIANT_METADATA_FIELD)
    assert(variantStruct.fields(1).name == HoodieSchema.Variant.VARIANT_VALUE_FIELD)

    // Variant nested inside a StructType should be converted recursively.
    val nestedStruct = hiveSchema("nested_struct").dataType.asInstanceOf[StructType]
    assertVariantStruct(nestedStruct("inner_variant").dataType)

    // Variant as ArrayType element should be converted.
    val arrayType = hiveSchema("variant_array").dataType.asInstanceOf[ArrayType]
    assertVariantStruct(arrayType.elementType)

    // Variant as MapType value should be converted.
    val mapType = hiveSchema("variant_map").dataType.asInstanceOf[MapType]
    assert(mapType.keyType == StringType)
    assertVariantStruct(mapType.valueType)
  }

  private def assertVariantStruct(dataType: DataType): StructType = {
    assert(dataType.isInstanceOf[StructType])
    val structType = dataType.asInstanceOf[StructType]
    assert(structType.length == 2)
    assert(structType(HoodieSchema.Variant.VARIANT_METADATA_FIELD).dataType == BinaryType)
    assert(structType(HoodieSchema.Variant.VARIANT_VALUE_FIELD).dataType == BinaryType)
    structType
  }

  test("Test buildHiveCompatibleCatalogTable converts schema and merges properties") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    val variantType = DataType.fromDDL("variant")
    val table = CatalogTable(
      identifier = TableIdentifier("test_table", Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("variant_col", variantType, nullable = true)
      )),
      provider = Some("hudi"),
      properties = Map("existing_key" -> "table_value", "shared_key" -> "table_value"))

    val dataSourceProps = Map(
      "spark.sql.sources.provider" -> "hudi",
      "shared_key" -> "datasource_value")

    val result = CreateHoodieTableCommand.buildHiveCompatibleCatalogTable(table, dataSourceProps)

    // VariantType replaced with the canonical (metadata, value) struct.
    assertVariantStruct(result.schema("variant_col").dataType)
    // Non-variant columns preserved.
    assert(result.schema("id").dataType == LongType)
    // Existing-only table properties survive.
    assert(result.properties("existing_key") == "table_value")
    // dataSource-only keys are merged in.
    assert(result.properties("spark.sql.sources.provider") == "hudi")
    // On conflict, CatalogTable.properties wins over dataSourceProps (right-biased `++`).
    assert(result.properties("shared_key") == "table_value")
    // Identity/provider fields pass through unchanged.
    assert(result.identifier == table.identifier)
    assert(result.provider == table.provider)
  }

  test("Test DataFrame writer with native VariantType round-trips through the V1 save path") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    withTempDir { tmp =>
      val df = spark.sql(
        """
          |SELECT
          |  1L AS id,
          |  'row1' AS name,
          |  parse_json('{"key":"value1"}') AS variant_data,
          |  1000L AS ts
          |UNION ALL
          |SELECT
          |  2L AS id,
          |  'row2' AS name,
          |  parse_json('{"key":"value2"}') AS variant_data,
          |  1000L AS ts
          |""".stripMargin)

      // Sanity: the DataFrame carries a native VariantType column, not a metadata-tagged struct.
      assert(df.schema("variant_data").dataType.typeName == "variant",
        s"expected native VariantType, got ${df.schema("variant_data").dataType}")

      df.write.format("hudi")
        .option("hoodie.table.name", "variant_native_df_test")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(tmp.getCanonicalPath)

      val readDf = spark.read.format("hudi").load(tmp.getCanonicalPath)
      assert(readDf.schema("variant_data").dataType.typeName == "variant",
        s"variant_data should round-trip as native VariantType, got ${readDf.schema("variant_data").dataType}")
      assert(readDf.count() == 2)

      val rows = readDf.selectExpr("id", "cast(variant_data as string) as v")
        .orderBy("id").collect()
      assert(rows(0).getString(1) == "{\"key\":\"value1\"}")
      assert(rows(1).getString(1) == "{\"key\":\"value2\"}")
    }
  }

  test("Test StructType with hudi_type=VARIANT metadata is promoted to VARIANT logical type") {
    // A StructType field in the DataFrame API tagged with hudi_type=VARIANT is treated as a first-class
    // VARIANT (like BLOB/VECTOR), not a plain struct. On Spark 4.0+ the column round-trips as native VariantType.
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    withTempDir { tmp =>
      val variantMetadata = new MetadataBuilder()
        .putString(HoodieSchema.TYPE_METADATA_FIELD, "VARIANT")
        .build()

      val variantStruct = StructType(Seq(
        StructField("metadata", BinaryType, nullable = false),
        StructField("value", BinaryType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType),
        StructField("variant_data", variantStruct, nullable = false, metadata = variantMetadata),
        StructField("ts", LongType)
      ))

      val data = Seq(
        Row(1L, "row1", Row(Array[Byte](1, 0), """{"key":"value1"}""".getBytes), 1000L)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write.format("hudi")
        .option("hoodie.table.name", "variant_struct_test")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(tmp.getCanonicalPath)

      val readDf = spark.read.format("hudi").load(tmp.getCanonicalPath)
      val readFieldType = readDf.schema("variant_data").dataType
      assert(readFieldType.typeName == "variant",
        s"variant_data should round-trip as native VariantType on Spark 4.0+, got $readFieldType")
      assert(readDf.count() == 1)
    }
  }

  test("Test StructType with hudi_type=VARIANT metadata rejects malformed struct") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    withTempDir { tmp =>
      val variantMetadata = new MetadataBuilder()
        .putString(HoodieSchema.TYPE_METADATA_FIELD, "VARIANT")
        .build()

      // VARIANT structure must be {metadata: binary, value: binary}; a single string field is malformed.
      val malformedVariantStruct = StructType(Seq(
        StructField("wrong_field", StringType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("variant_data", malformedVariantStruct, nullable = false, metadata = variantMetadata),
        StructField("ts", LongType)
      ))

      val data = Seq(Row(1L, Row("oops"), 1000L))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      val ex = intercept[Exception] {
        df.write.format("hudi")
          .option("hoodie.table.name", "variant_malformed_test")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.precombine.field", "ts")
          .mode(SaveMode.Overwrite)
          .save(tmp.getCanonicalPath)
      }
      val causes = Iterator.iterate[Throwable](ex)(e => e.getCause).takeWhile(_ != null).toList
      assert(causes.exists(c => c.isInstanceOf[IllegalArgumentException]
        && c.getMessage != null
        && c.getMessage.contains("Invalid variant schema structure")),
        s"Expected IllegalArgumentException with 'Invalid variant schema structure', got: ${causes.map(_.getMessage)}")
    }
  }

  test("Test Spark 3.x throws when auto-resolving Variant schema from commit metadata") {
    assume(HoodieSparkUtils.isSpark3, "This test verifies Spark 3.x rejects VARIANT type during schema resolution")

    withTempDir { tmpDir =>
      HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", tmpDir.toPath, getClass)
      val cowPath = tmpDir.toPath.resolve("variant_cow").toString

      // Read without specifying schema — Hudi resolves it from commit metadata,
      // which contains the Avro VariantLogicalType. This triggers the
      // HoodieSchema → Spark StructType conversion that throws on Spark 3.x.
      val ex = intercept[HoodieSchemaException] {
        spark.read.format("hudi").load(cowPath).collect()
      }
      assert(ex.getCause.getMessage.contains("VARIANT type is only supported in Spark 4.0+"))
    }
  }

  test(s"Test Backward Compatibility: Read Spark 4.0 Variant Table in Spark 3.x") {
    // This test only runs on Spark 3.x to verify backward compatibility
    assume(HoodieSparkUtils.isSpark3, "This test verifies Spark 3.x can read Spark 4.0 Variant tables")

    withTempDir { tmpDir =>
      // Test COW table - record type does not affect file metadata for COW, only need one test
      HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", tmpDir.toPath, getClass)
      val cowPath = tmpDir.toPath.resolve("variant_cow").toString
      verifyVariantBackwardCompatibility(cowPath, "cow", "COW table")

      // Test MOR table with AVRO record type
      HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_mor_avro.zip", tmpDir.toPath, getClass)
      val morAvroPath = tmpDir.toPath.resolve("variant_mor_avro").toString
      verifyVariantBackwardCompatibility(morAvroPath, "mor", "MOR table with AVRO record type")

      // Test MOR table with SPARK record type
      HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_mor_spark.zip", tmpDir.toPath, getClass)
      val morSparkPath = tmpDir.toPath.resolve("variant_mor_spark").toString
      verifyVariantBackwardCompatibility(morSparkPath, "mor", "MOR table with SPARK record type")
    }
  }

  /**
   * Helper method to verify backward compatibility of reading Spark 4.0 Variant tables in Spark 3.x
   */
  private def verifyVariantBackwardCompatibility(resourcePath: String, tableType: String, testDescription: String): Unit = {
    val tableName = generateTableName

    // Create a Hudi table pointing to the saved data location
    // In Spark 3.x, we define the Variant column as a struct with binary fields since Variant type is not available
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  v struct<value: binary, metadata: binary>,
         |  ts long
         |) using hudi
         |location '$resourcePath'
         |tblproperties (
         |  primaryKey = 'id',
         |  tableType = '$tableType',
         |  preCombineField = 'ts'
         |)
       """.stripMargin)

    // Verify we can read the basic columns
    checkAnswer(s"select id, name, ts from $tableName order by id")(Seq(1, "row1", 1000))

    // Read and verify the variant column as a struct with binary fields
    val rows = spark.sql(s"select id, v from $tableName order by id").collect()
    assert(rows.length == 1, s"Should have 1 row after delete operation in Spark 4.0 ($testDescription)")
    assert(rows(0).getInt(0) == 1, "First column should be id=1")
    assert(!rows(0).isNullAt(1), "Variant column should not be null")

    val variantStruct = rows(0).getStruct(1)
    assert(variantStruct.size == 2, "Variant struct should have 2 fields: value and metadata")

    val valueBytes = variantStruct.getAs[Array[Byte]](0)
    val metadataBytes = variantStruct.getAs[Array[Byte]](1)

    // Expected byte values from Spark 4.0 Variant representation: {"updated": true, "new_field": 123}
    val expectedValueBytes = Array[Byte](0x02, 0x02, 0x01, 0x00, 0x01, 0x00, 0x03, 0x04, 0x0C, 0x7B)
    val expectedMetadataBytes = Array[Byte](0x01, 0x02, 0x00, 0x07, 0x10, 0x75, 0x70, 0x64, 0x61,
      0x74, 0x65, 0x64, 0x6E, 0x65, 0x77, 0x5F, 0x66, 0x69, 0x65, 0x6C, 0x64)

    assert(valueBytes.sameElements(expectedValueBytes),
      s"Variant value bytes mismatch ($testDescription). " +
        s"Expected: ${StringUtils.encodeHex(expectedValueBytes).mkString("Array(", ", ", ")")}, " +
        s"Got: ${StringUtils.encodeHex(valueBytes).mkString("Array(", ", ", ")")}")

    assert(metadataBytes.sameElements(expectedMetadataBytes),
      s"Variant metadata bytes mismatch ($testDescription). " +
        s"Expected: ${StringUtils.encodeHex(expectedMetadataBytes).mkString("Array(", ", ", ")")}, " +
        s"Got: ${StringUtils.encodeHex(metadataBytes).mkString("Array(", ", ", ")")}")

    // Verify we can select all columns without errors
    assert(spark.sql(s"select * from $tableName").count() == 1, "Should be able to read all columns including variant")

    spark.sql(s"drop table $tableName")
  }
}
