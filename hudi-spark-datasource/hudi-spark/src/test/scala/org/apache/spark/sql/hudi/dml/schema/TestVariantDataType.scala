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

import org.apache.hudi.{DataSourceReadOptions, HoodieSparkUtils}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.internal.HoodieSchemaException
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.parquet.schema.Type
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hudi.command.CreateHoodieTableCommand
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, LongType, MapType, MetadataBuilder, StringType, StructField, StructType}


class TestVariantDataType extends HoodieSparkSqlTestBase with VariantShreddingTestSupport {

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
    // Gated on Spark >= 4.1. Compaction writes the base file via the AVRO shredding writer, which
    // lays the variant group out as [metadata, value, typed_value]. Spark 4.0's read support
    // (Spark40HoodieParquetReadSupport.reorderVariantFields) rebuilds that group as [value, metadata]
    // and drops typed_value, so the subsequent native read fails with MALFORMED_VARIANT. Spark 4.1+
    // reads variant fields by name (SPARK-54410) and reconstructs correctly.
    // TODO(voon): drop this comment once Spark 4.0 is removed.
    assume(HoodieSparkUtils.gteqSpark4_1, "Shredded variant base-file read requires Spark 4.1 or higher")

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      // Shred variants on write so the compacted base file is shredded, then read it back. Note the
      // Spark SQL read here goes through the InternalRow reader (SparkFileFormatInternalRowReaderContext),
      // which reconstructs the shredded variant natively - it does NOT exercise the AVRO read-path
      // reconstruction (#18931, HoodieVariantReconstruction), which Spark compaction never reaches.
      // That path is covered directly by TestSpark4VariantShreddingProvider and
      // TestHoodieVariantReconstruction.
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
           |  hoodie.parquet.variant.write.shredding.enabled = 'true',
           |  hoodie.parquet.variant.force.shredding.schema.for.test = 'key string',
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

      // Round 2: the compaction above compacted a log-only slice, so it never read a
      // parquet base file. Pin that the compacted base file is shredded, then drive a
      // second compaction that reads it through the internal reader
      // (SparkFileFormatInternalRowReaderContext) and must carry rows 3 and 4, which
      // exist only in that base file, forward (#19556).
      val compactedFiles = listDataParquetFiles(tablePath)
      assert(compactedFiles.nonEmpty, "Should have a compacted base parquet file")
      compactedFiles.foreach { filePath =>
        val parquetSchema = readParquetSchema(filePath)
        val variantGroup = getFieldAsGroup(parquetSchema, "v")
        assert(variantGroup.containsField("typed_value"),
          s"Compacted base file should carry typed_value. Schema:\n$variantGroup")
      }

      // The v2-merged deltacommit above was the first after the compaction; four more
      // reach max.delta.commits = 5 and trip the second compaction inline.
      spark.sql(s"""update $tableName set v = parse_json('{"key":"v1-r2"}'), ts = 1003 where id = 1""")
      spark.sql(s"""update $tableName set v = parse_json('{"key":"v2-r2"}'), ts = 1004 where id = 2""")
      spark.sql(s"""update $tableName set v = parse_json('{"key":"v1-r3"}'), ts = 1005 where id = 1""")
      spark.sql(s"""update $tableName set v = parse_json('{"key":"v2-r3"}'), ts = 1006 where id = 2""")

      metaClient.reloadActiveTimeline()
      assertResult(2)(metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants.countInstants)

      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, "{\"key\":\"v1-r3\"}", 1005),
        Seq(2, "{\"key\":\"v2-r3\"}", 1006),
        Seq(3, "{\"key\":\"value3\"}", 1000),
        Seq(4, "{\"key\":\"value4\"}", 1000)
      )

      // Incremental round trip over the shredded table: batch incremental reads the
      // shredded base file through the file-group-reader file format with a catalyst
      // schema, a stack nothing else in this suite pins for variant columns. The
      // no-catalyst-schema legs are covered by the CDC round trip below and, for
      // streaming with hoodie.file.group.reader.enabled=false, by
      // TestStreamingSource#"test mor stream source reads shredded variant with legacy
      // file group reader disabled".
      val incRows = spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, "000")
        .load(tablePath)
        .selectExpr("id", "cast(v as string) as v", "ts")
        .orderBy("id")
        .collect()
      assertResult(4)(incRows.length)
      assertResult("{\"key\":\"v1-r3\"}")(incRows(0).getString(1))
      assertResult("{\"key\":\"v2-r3\"}")(incRows(1).getString(1))
      assertResult("{\"key\":\"value3\"}")(incRows(2).getString(1))
      assertResult("{\"key\":\"value4\"}")(incRows(3).getString(1))
    })
  }

  test("Test COW clustering preserves VARIANT values") {
    // Same Spark 4.1 gate as the compaction test above: clustering reads the shredded
    // base files back through the native reader, which rejects the 3-field shredded
    // layout before SPARK-54410 (Spark 4.1+).
    assume(HoodieSparkUtils.gteqSpark4_1, "Shredded variant base-file read requires Spark 4.1 or higher")

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      // Clustering rewrites ALL rows of the clustered file groups through the internal
      // write-side reader context (SparkReaderContextFactory ->
      // SparkFileFormatInternalRowReaderContext), the stack whose blob handling silently
      // lost bytes in #19232. Nothing pinned its VARIANT behavior: this is the first
      // clustering coverage for the type. Shredding is forced so the rewrite reads and
      // rewrites the shredded layout, the default in production.
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
           |  type = 'cow',
           |  preCombineField = 'ts',
           |  hoodie.parquet.variant.write.shredding.enabled = 'true',
           |  hoodie.parquet.variant.force.shredding.schema.for.test = 'key string',
           |  hoodie.index.type = 'INMEMORY',
           |  hoodie.parquet.small.file.limit = '0',
           |  hoodie.clustering.inline = 'true',
           |  hoodie.clustering.inline.max.commits = '2'
           | )
       """.stripMargin)

      // small.file.limit = 0 keeps the second insert in its own file group. Otherwise the
      // second commit bin-packs into the first file group and rewrites it through the CoW
      // small-file MERGE (a different internal read stack), conflating that path's variant
      // handling with the clustering rewrite this test isolates.
      spark.sql(s"insert into $tableName values " +
        "(1, parse_json('{\"key\":\"value1\"}'), 1000), " +
        "(2, parse_json('{\"key\":\"value2\"}'), 1000)")

      // The pre-clustering base file must actually carry the shredded layout; without this
      // check the test silently degrades into the unshredded twin below if the forced
      // shredding schema ever stops taking effect.
      val preClusteringFiles = listDataParquetFiles(tablePath)
      assert(preClusteringFiles.nonEmpty, "Should have at least one data parquet file before clustering")
      preClusteringFiles.foreach { filePath =>
        val parquetSchema = readParquetSchema(filePath)
        val variantGroup = getFieldAsGroup(parquetSchema, "v")
        assert(variantGroup.containsField("typed_value"),
          s"Pre-clustering base file should carry typed_value. Schema:\n$variantGroup")
      }

      // Second commit trips inline clustering (max.commits = 2), which rewrites the rows
      // of the first commit too.
      spark.sql(s"insert into $tableName values " +
        "(3, parse_json('{\"key\":\"value3\"}'), 1000), " +
        "(4, parse_json('{\"key\":\"value4\"}'), 1000)")

      // getLastClusteringInstant filters by action only, so a REQUESTED/INFLIGHT instant
      // satisfies isPresent; isCompleted confirms the rewrite finished.
      val metaClient = createMetaClient(spark, tablePath)
      val lastClustering = metaClient.getActiveTimeline.getLastClusteringInstant
      assert(lastClustering.isPresent && lastClustering.get.isCompleted,
        "A COMPLETED clustering (replacecommit) instant must exist after inline clustering; " +
          "without a completed rewrite the round-trip below proves nothing")

      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, "{\"key\":\"value1\"}", 1000),
        Seq(2, "{\"key\":\"value2\"}", 1000),
        Seq(3, "{\"key\":\"value3\"}", 1000),
        Seq(4, "{\"key\":\"value4\"}", 1000)
      )

      // VARIANT must still surface as the native type after the clustering rewrite.
      val variantField = spark.table(tableName).schema.find(_.name == "v").get
      assertResult("variant")(variantField.dataType.typeName)
    })
  }

  test("Test COW clustering preserves unshredded VARIANT values") {
    // Companion to the shredded clustering test above, with shredding disabled. If this
    // passes while the shredded one fails, the loss is specific to reading the shredded
    // layout inside the clustering rewrite, not to variant clustering in general.
    assume(HoodieSparkUtils.gteqSpark4_1, "Variant clustering read-back requires Spark 4.1 or higher")

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
           |  type = 'cow',
           |  preCombineField = 'ts',
           |  hoodie.parquet.variant.write.shredding.enabled = 'false',
           |  hoodie.index.type = 'INMEMORY',
           |  hoodie.parquet.small.file.limit = '0',
           |  hoodie.clustering.inline = 'true',
           |  hoodie.clustering.inline.max.commits = '2'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values " +
        "(1, parse_json('{\"key\":\"value1\"}'), 1000), " +
        "(2, parse_json('{\"key\":\"value2\"}'), 1000)")

      // Pin the layout: these files must NOT carry typed_value, or this twin silently
      // becomes a copy of the shredded test above and the unshredded leg of the rewrite
      // goes uncovered.
      val preClusteringFiles = listDataParquetFiles(tablePath)
      assert(preClusteringFiles.nonEmpty, "Should have at least one data parquet file before clustering")
      preClusteringFiles.foreach { filePath =>
        val parquetSchema = readParquetSchema(filePath)
        val variantGroup = getFieldAsGroup(parquetSchema, "v")
        assert(!variantGroup.containsField("typed_value"),
          s"Unshredded base file must not carry typed_value. Schema:\n$variantGroup")
      }

      spark.sql(s"insert into $tableName values " +
        "(3, parse_json('{\"key\":\"value3\"}'), 1000), " +
        "(4, parse_json('{\"key\":\"value4\"}'), 1000)")

      val metaClient = createMetaClient(spark, tablePath)
      val lastClustering = metaClient.getActiveTimeline.getLastClusteringInstant
      assert(lastClustering.isPresent && lastClustering.get.isCompleted,
        "A COMPLETED clustering (replacecommit) instant must exist after inline clustering")

      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, "{\"key\":\"value1\"}", 1000),
        Seq(2, "{\"key\":\"value2\"}", 1000),
        Seq(3, "{\"key\":\"value3\"}", 1000),
        Seq(4, "{\"key\":\"value4\"}", 1000)
      )
    })
  }

  test("Test CDC captures VARIANT values from shredded and unshredded base files") {
    // #19578: CDC is the only default-config query path that builds the internal reader
    // context without a catalyst schema, and its BASE_FILE_INSERT case additionally reads
    // the new base file directly, bypassing the context, so it needs its own full-variant
    // rewrite. That rewrite is picked from the spark adapter and the requested schema alone,
    // never from the file, so the unshredded leg runs through it too; both layouts are swept
    // below so neither side of that branch goes uncovered.
    assume(HoodieSparkUtils.gteqSpark4_1, "Shredded variant base-file read requires Spark 4.1 or higher")

    // OP_KEY_ONLY reconstructs both images by reading the file slices; DATA_BEFORE_AFTER
    // (the default) reads the update images from the cdc log instead. The insert leg takes
    // BASE_FILE_INSERT in both modes.
    Seq(true, false).foreach { shredded =>
      Seq("OP_KEY_ONLY", "DATA_BEFORE_AFTER").foreach { loggingMode =>
        // SPARK is pinned instead of sweeping both record types: a cdc-enabled table always
        // writes through FileGroupReaderBasedMergeHandle, and the merger's record type picks
        // that handle's reader context. AVRO would route the update's base-file read through
        // HoodieAvroParquetReader, whose shredded read is the separate defect tracked as
        // #19567/#19582; SPARK routes it through SparkFileFormatInternalRowReaderContext,
        // fixed in #19558. The CDC read path under test is independent of that choice.
        withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
          val leg = s"$loggingMode, shredded=$shredded"
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath
          // The forced shredding schema belongs to the shredded leg only; the unshredded leg
          // must reach the writer with shredding off and no forced schema.
          val forceShreddingProp =
            if (shredded) "hoodie.parquet.variant.force.shredding.schema.for.test = 'key string'," else ""
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
               |  type = 'cow',
               |  preCombineField = 'ts',
               |  'hoodie.table.cdc.enabled' = 'true',
               |  'hoodie.table.cdc.supplemental.logging.mode' = '$loggingMode',
               |  hoodie.parquet.variant.write.shredding.enabled = '$shredded',
               |  $forceShreddingProp
               |  hoodie.index.type = 'INMEMORY'
               | )
           """.stripMargin)

          spark.sql(s"""insert into $tableName values (1, parse_json('{"key":"value1"}'), 1000)""")

          // Pin the layout the CDC reads: without this, one leg silently degrades into a
          // second copy of the other and its half of the rewrite branch goes uncovered.
          assertVariantLayout(tablePath, shredded, leg)

          spark.sql(s"""update $tableName set v = parse_json('{"key":"value2"}'), ts = 1001 where id = 1""")

          val cdc = spark.read.format("hudi")
            .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
            .option(DataSourceReadOptions.INCREMENTAL_FORMAT.key, DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
            .option(DataSourceReadOptions.START_COMMIT.key, "000")
            .load(tablePath)

          // Insert leg: the after-image comes from BASE_FILE_INSERT's direct read of the
          // base file; a null here means that read dropped the variant payload.
          val insertRows = cdc.where("op = 'i'")
            .selectExpr("get_json_object(after, '$.v.key') as after_key").collect()
          assert(insertRows.length == 1, s"[$leg] expected exactly one insert cdc row")
          assert(insertRows(0).getString(0) == "value1",
            s"[$leg] insert after-image lost the variant payload: ${insertRows(0)}")

          // Update leg: images come from slice reads (OP_KEY_ONLY) or the cdc log
          // (DATA_BEFORE_AFTER); both must carry the variant payloads.
          val updateRows = cdc.where("op = 'u'")
            .selectExpr(
              "get_json_object(before, '$.v.key') as before_key",
              "get_json_object(after, '$.v.key') as after_key")
            .collect()
          assert(updateRows.length == 1, s"[$leg] expected exactly one update cdc row")
          assert(updateRows(0).getString(0) == "value1",
            s"[$leg] update before-image lost the variant payload: ${updateRows(0)}")
          assert(updateRows(0).getString(1) == "value2",
            s"[$leg] update after-image lost the variant payload: ${updateRows(0)}")
        })
      }
    }
  }

  test("Test COW small-file merge preserves VARIANT values") {
    // The #19567 repro. The second insert bin-packs into the existing small file group, and the
    // small-file merge (HoodieConcatHandle -> HoodieMergeHelper) rewrites the old base file.
    // Before the fix the footer-derived reader schema lost the variant logical type, so the
    // strict-projection check failed and degenerated the requested schema to the footer schema
    // itself; reconstruction had no variant column to anchor on, and the writer-schema rewrite
    // silently dropped typed_value, nulling rows 1-2 after the second commit.
    //
    // Swept over both dimensions because each leg pins something different:
    // - AVRO + shredded: the bug itself. Goes red without the HoodieMergeHelper alignment.
    // - AVRO + unshredded: the other direction. alignShreddedVariants runs on every runMerge, so
    //   an ordinary variant table must round-trip exactly as it did before. Red here while the
    //   shredded leg stays green means the alignment is reaching columns it should not.
    // - SPARK: HoodieSparkParquetReader.getSchema returns a nullable UNION rather than a RECORD,
    //   so alignShreddedVariants bails at its RECORD/RECORD guard and is a strict no-op on that
    //   reader - this leg is byte-for-byte unchanged by the fix, and Spark's own parquet read
    //   handles the shredded column. Swept anyway because nothing else covers it, and because
    //   that guard is what scopes the fix to the AVRO reader; if it ever changes, this catches it.
    //
    // Unlike the clustering tests above, small.file.limit stays at its default on purpose: the
    // bin-pack is the trigger.
    assume(HoodieSparkUtils.gteqSpark4_1, "Shredded variant base-file read requires Spark 4.1 or higher")

    Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK).foreach { recordType =>
      Seq(true, false).foreach { shredded =>
        withRecordType(Seq(recordType))(withTempDir { tmp =>
          val leg = s"$recordType, shredded=$shredded"
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath
          // The forced shredding schema belongs to the shredded leg only; the unshredded leg must
          // reach the writer with shredding off and no forced schema.
          val forceShreddingProp =
            if (shredded) "hoodie.parquet.variant.force.shredding.schema.for.test = 'key string'," else ""
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
               |  type = 'cow',
               |  preCombineField = 'ts',
               |  hoodie.parquet.variant.write.shredding.enabled = '$shredded',
               |  $forceShreddingProp
               |  hoodie.index.type = 'INMEMORY'
               | )
           """.stripMargin)

          spark.sql(s"insert into $tableName values " +
            "(1, parse_json('{\"key\":\"value1\"}'), 1000), " +
            "(2, parse_json('{\"key\":\"value2\"}'), 1000)")

          // Pin the trigger: without this the shredded leg can silently degenerate into a plain
          // unshredded merge, or the unshredded leg into a copy of the shredded one.
          assertVariantLayout(tablePath, shredded, leg)

          spark.sql(s"insert into $tableName values " +
            "(3, parse_json('{\"key\":\"value3\"}'), 1000), " +
            "(4, parse_json('{\"key\":\"value4\"}'), 1000)")

          // Pin that the second commit went through the small-file merge: both parquet versions
          // belong to one file group, no second group was created.
          assertSingleFileGroup(tablePath, leg)

          // Rows 1 and 2 survive only if the merge carried them out of the base file; nulls here
          // mean the read path dropped typed_value (#19567).
          checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
            Seq(1, "{\"key\":\"value1\"}", 1000),
            Seq(2, "{\"key\":\"value2\"}", 1000),
            Seq(3, "{\"key\":\"value3\"}", 1000),
            Seq(4, "{\"key\":\"value4\"}", 1000)
          )
        })
      }
    }
  }

  test("Test COW small-file merge preserves shredded VARIANT values when the schema evolves") {
    // The evolving-schema leg of the same merge. Adding a column makes the writer schema stop
    // being a strict projection of the base file's, so runMerge takes the other branch of
    // recordSchema = isPureProjection ? writerSchema : readerSchema. That branch used to hand the
    // reader the raw footer schema, where the variant column is a plain record, so reconstruction
    // could not anchor and rewriteRecordWithNewSchema copied {metadata, value} by name and dropped
    // typed_value - the same silent corruption as the un-evolved case, reached without any
    // schema-on-read config. Aligning the reader schema up front covers both branches.
    assume(HoodieSparkUtils.gteqSpark4_1, "Shredded variant base-file read requires Spark 4.1 or higher")

    withRecordType(Seq(HoodieRecordType.AVRO))(withTempDir { tmp =>
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
           |  type = 'cow',
           |  preCombineField = 'ts',
           |  hoodie.parquet.variant.write.shredding.enabled = 'true',
           |  hoodie.parquet.variant.force.shredding.schema.for.test = 'key string',
           |  hoodie.index.type = 'INMEMORY'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values " +
        "(1, parse_json('{\"key\":\"value1\"}'), 1000), " +
        "(2, parse_json('{\"key\":\"value2\"}'), 1000)")

      assertVariantLayout(tablePath, shredded = true, "schema evolves")

      // The added column is what breaks the strict projection: the writer schema now carries a
      // field the base file does not have.
      spark.sql(s"alter table $tableName add columns (note string)")

      spark.sql(s"insert into $tableName values " +
        "(3, parse_json('{\"key\":\"value3\"}'), 1000, 'n3'), " +
        "(4, parse_json('{\"key\":\"value4\"}'), 1000, 'n4')")

      assertSingleFileGroup(tablePath, "schema evolves")

      // Rows 1-2 keep their variants and pick up a null for the new column; rows 3-4 carry it.
      checkAnswer(s"select id, cast(v as string), ts, note from $tableName order by id")(
        Seq(1, "{\"key\":\"value1\"}", 1000, null),
        Seq(2, "{\"key\":\"value2\"}", 1000, null),
        Seq(3, "{\"key\":\"value3\"}", 1000, "n3"),
        Seq(4, "{\"key\":\"value4\"}", 1000, "n4")
      )
    })
  }

  test("Test bulk_insert row-writer round-trips VARIANT") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    // bulk_insert takes the row-writer path (HoodieRowParquetWriteSupport), which has its
    // own variant writers (unshredded and shredded); no end-to-end test covered it for
    // either layout, only writer-level units (TestHoodieRowParquetWriteSupportVariant).

    // Unshredded bulk_insert: full round trip on any Spark 4.x.
    withTempDir { tmp =>
      val df = spark.sql(
        """
          |SELECT 1L AS id, parse_json('{"key":"value1"}') AS v, 1000L AS ts
          |UNION ALL
          |SELECT 2L AS id, parse_json('{"key":"value2"}') AS v, 1000L AS ts
          |""".stripMargin)
      df.write.format("hudi")
        .option("hoodie.table.name", "variant_bulk_insert_unshredded")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.operation", "bulk_insert")
        .option("hoodie.datasource.write.row.writer.enable", "true")
        .option("hoodie.parquet.variant.write.shredding.enabled", "false")
        .mode(SaveMode.Overwrite)
        .save(tmp.getCanonicalPath)

      val readDf = spark.read.format("hudi").load(tmp.getCanonicalPath)
      assert(readDf.schema("v").dataType.typeName == "variant",
        s"v should round-trip as native VariantType, got ${readDf.schema("v").dataType}")
      val rows = readDf.selectExpr("id", "cast(v as string) as v").orderBy("id").collect()
      assert(rows.length == 2)
      assert(rows(0).getString(1) == "{\"key\":\"value1\"}")
      assert(rows(1).getString(1) == "{\"key\":\"value2\"}")
    }

    // Shredded bulk_insert exercises the row-writer shredding path end to end; reading
    // the shredded file back needs Spark 4.1+ (SPARK-54410).
    if (HoodieSparkUtils.gteqSpark4_1) {
      withTempDir { tmp =>
        val df = spark.sql(
          """
            |SELECT 1L AS id, parse_json('{"key":"value1"}') AS v, 1000L AS ts
            |UNION ALL
            |SELECT 2L AS id, parse_json('{"key":"value2"}') AS v, 1000L AS ts
            |""".stripMargin)
        df.write.format("hudi")
          .option("hoodie.table.name", "variant_bulk_insert_shredded")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.precombine.field", "ts")
          .option("hoodie.datasource.write.operation", "bulk_insert")
          .option("hoodie.datasource.write.row.writer.enable", "true")
          .option("hoodie.parquet.variant.write.shredding.enabled", "true")
          .option("hoodie.parquet.variant.force.shredding.schema.for.test", "key string")
          .mode(SaveMode.Overwrite)
          .save(tmp.getCanonicalPath)

        // The written parquet must actually carry the shredded layout; otherwise the
        // read-back below silently validates the unshredded path a second time.
        val parquetFiles = listDataParquetFiles(tmp.getCanonicalPath)
        assert(parquetFiles.nonEmpty, "Should have at least one data parquet file")
        parquetFiles.foreach { filePath =>
          val schema = readParquetSchema(filePath)
          val variantGroup = getFieldAsGroup(schema, "v")
          assert(variantGroup.containsField("typed_value"),
            s"bulk_insert with shredding forced should write typed_value. Schema:\n$variantGroup")
        }

        val readDf = spark.read.format("hudi").load(tmp.getCanonicalPath)
        val rows = readDf.selectExpr("id", "cast(v as string) as v").orderBy("id").collect()
        assert(rows.length == 2)
        assert(rows(0).getString(1) == "{\"key\":\"value1\"}")
        assert(rows(1).getString(1) == "{\"key\":\"value2\"}")
      }
    }
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

  test("Test Shredded Variant Write and Read + Validate Parquet Schema after Write") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    // Test 1: Shredding enabled with forced schema → parquet should have typed_value
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
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
           |  primaryKey = 'id',
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
        """.stripMargin)

      spark.sql("set hoodie.parquet.variant.write.shredding.enabled = true")
      spark.sql("set hoodie.parquet.variant.allow.reading.shredded = true")
      spark.sql("set hoodie.parquet.variant.force.shredding.schema.for.test = a int, b string")

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, 'row1', parse_json('{"a": 1, "b": "hello"}'), 1000)
        """.stripMargin)
      // Reading shredded variants back via Spark SQL needs Spark 4.1+ (spark.sql.variant.allowReadingShredded,
      // SPARK-54410); Spark 4.0's native reader rejects the 3-field shredded layout. The shredded write is
      // still validated below.
      if (HoodieSparkUtils.gteqSpark4_1) {
        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"a\":1,\"b\":\"hello\"}", 1000)
        )
      } else {
        // Spark 4.0 cannot reconstruct shredded variants; the read must fail loudly instead of
        // returning a partial payload. Depending on the path that is Spark's own
        // INVALID_VARIANT_FROM_PARQUET.WRONG_NUM_FIELDS (schema conversion rejects the 3-field
        // group) or Hudi's read-support guard naming the shredded column; both name the variant.
        assertQueryFailsWith(s"select id, name, cast(v as string), ts from $tableName order by id",
          "ariant", "spark 4.0 shredded read")
      }

      // Verify parquet schema has shredded structure with typed_value
      val parquetFiles = listDataParquetFiles(tmp.getCanonicalPath)
      assert(parquetFiles.nonEmpty, "Should have at least one data parquet file")

      parquetFiles.foreach { filePath =>
        val schema = readParquetSchema(filePath)
        val variantGroup = getFieldAsGroup(schema, "v")
        assert(variantGroup.containsField("typed_value"),
          s"Shredded variant should have typed_value field. Schema:\n$variantGroup")
        val valueField = variantGroup.getType(variantGroup.getFieldIndex("value"))
        assert(valueField.getRepetition == Type.Repetition.OPTIONAL,
          "Shredded variant value field should be OPTIONAL")
        val metadataField = variantGroup.getType(variantGroup.getFieldIndex("metadata"))
        assert(metadataField.getRepetition == Type.Repetition.REQUIRED,
          "Shredded variant metadata field should be REQUIRED")
      }
    })
  }

  test("Test Unshredded Variant Write and Read + Validate Parquet Schema after Write") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")
    // Shredding disabled parquet should NOT have typed_value
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
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
           |  primaryKey = 'id',
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
              """.stripMargin)

      spark.sql(s"set hoodie.parquet.variant.write.shredding.enabled = false")

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, 'row1', parse_json('{"a": 1, "b": "hello"}'), 1000)
              """.stripMargin)

      checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
        Seq(1, "row1", "{\"a\":1,\"b\":\"hello\"}", 1000)
      )

      // Verify parquet schema does NOT have typed_value
      val parquetFiles = listDataParquetFiles(tmp.getCanonicalPath)
      assert(parquetFiles.nonEmpty, "Should have at least one data parquet file")

      parquetFiles.foreach { filePath =>
        val schema = readParquetSchema(filePath)
        val variantGroup = getFieldAsGroup(schema, "v")
        assert(!variantGroup.containsField("typed_value"),
          s"Non-shredded variant should NOT have typed_value field. Schema:\n$variantGroup")
        val valueField = variantGroup.getType(variantGroup.getFieldIndex("value"))
        assert(valueField.getRepetition == Type.Repetition.REQUIRED,
          "Non-shredded variant value field should be REQUIRED")
      }

      // Verify data can still be read back for the non-shredded case
      checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
        Seq(1, "row1", "{\"a\":1,\"b\":\"hello\"}", 1000)
      )
    })
  }

}
