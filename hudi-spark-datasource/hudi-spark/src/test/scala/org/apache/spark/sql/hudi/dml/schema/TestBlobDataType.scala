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

import org.apache.hudi.blob.BlobTestHelpers
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.catalyst.plans.logical.FormatClasses
import org.apache.spark.sql.hudi.common.{ExtendedParserTestHelpers, HoodieSparkSqlTestBase}
import org.apache.spark.sql.types._

import java.io.File

class TestBlobDataType extends HoodieSparkSqlTestBase with ExtendedParserTestHelpers {

  private val referenceStructType =
    "struct<external_path:string, offset:bigint, length:bigint, managed:boolean>"

  private def inlineBlobLiteral(hex: String): String =
    s"""named_struct(
       |  'type', 'INLINE',
       |  'data', cast(X'$hex' as binary),
       |  'reference', cast(null as $referenceStructType)
       |)""".stripMargin

  private def outOfLineBlobLiteral(externalPath: String, offset: Long, length: Long): String =
    s"""named_struct(
       |  'type', 'OUT_OF_LINE',
       |  'data', cast(null as binary),
       |  'reference', named_struct(
       |    'external_path', '$externalPath',
       |    'offset', cast($offset as bigint),
       |    'length', cast($length as bigint),
       |    'managed', false
       |  )
       |)""".stripMargin

  test("Test Query Log Only MOR Table With BLOB INLINE column triggers compaction") {
    withRecordType()(withTempDir { tmp =>
      val tablePath = new File(tmp, "hudi").getCanonicalPath
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  data blob,
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

      spark.sql(s"insert into $tableName values (1, ${inlineBlobLiteral("01")}, 1000)")
      spark.sql(s"insert into $tableName values (2, ${inlineBlobLiteral("02")}, 1000)")
      spark.sql(s"insert into $tableName values (3, ${inlineBlobLiteral("03")}, 1000)")
      // 3 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 1 as id, ${inlineBlobLiteral("11")} as data, 1001L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      // 4 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 4 as id, ${inlineBlobLiteral("04")} as data, 1000L as ts
           |) s0
           | on h0.id = s0.id
           | when not matched then insert *
           |""".stripMargin)

      // 5 commits will trigger compaction.
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tablePath))

      // read_blob() on an INLINE column returns the inline bytes directly, verify the
      // post-compaction bytes match what was written.
      val bytesById = spark.sql(
        s"select id, read_blob(data) as bytes from $tableName order by id"
      ).collect().map(r => r.getInt(0) -> r.getAs[Array[Byte]]("bytes")).toMap
      assertResult(4)(bytesById.size)
      assert(bytesById(1).sameElements(Array(0x11.toByte)))
      assert(bytesById(2).sameElements(Array(0x02.toByte)))
      assert(bytesById(3).sameElements(Array(0x03.toByte)))
      assert(bytesById(4).sameElements(Array(0x04.toByte)))

      // Verify inline shape: type='INLINE', data non-null, reference null.
      spark.sql(s"select id, data from $tableName order by id").collect().foreach { row =>
        val blob = row.getStruct(1)
        assertResult("INLINE")(blob.getString(blob.fieldIndex(HoodieSchema.Blob.TYPE)))
        assert(!blob.isNullAt(blob.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)))
        assert(blob.isNullAt(blob.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)))
      }

      // BLOB custom-type descriptor must survive the compacted base-file read path.
      val blobField = spark.table(tableName).schema.find(_.name == "data").get
      assert(blobField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD),
        s"Expected BLOB type metadata on data field after compaction, " +
          s"got: ${blobField.metadata}")
      assertResult(HoodieSchemaType.BLOB.name())(
        blobField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))

      // 6th commit drives an auto-clean that retires the now-superseded log-only slice.
      // Inline compaction on commit 5 ran AFTER its own postCommit clean, so the prior
      // slice was not yet superseded when that clean fired and no .clean instant was
      // written. This deltacommit's postCommit clean writes the .clean instant.
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 2 as id, ${inlineBlobLiteral("22")} as data, 1002L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      val updatedBytesById = spark.sql(
        s"select id, read_blob(data) as bytes from $tableName order by id"
      ).collect().map(r => r.getInt(0) -> r.getAs[Array[Byte]]("bytes")).toMap
      assert(updatedBytesById(2).sameElements(Array(0x22.toByte)))

      val metaClient = createMetaClient(spark, tablePath)
      metaClient.reloadActiveTimeline()
      assert(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() > 0,
        "Expected at least one .clean instant on the timeline after compaction")
    })
  }

  test("Test Query Log Only MOR Table With BLOB OUT_OF_LINE column triggers compaction") {
    withRecordType()(withTempDir { tmp =>
      val tablePath = new File(tmp, "hudi").getCanonicalPath
      val blobDir = new File(tmp, "blobs")
      blobDir.mkdirs()
      // createTestFile writes bytes where byte[i] = i % 256, assertBytesContent
      // checks round-trip against that pattern.
      val file1 = BlobTestHelpers.createTestFile(blobDir.toPath, "blob1.bin", 100)
      val file2 = BlobTestHelpers.createTestFile(blobDir.toPath, "blob2.bin", 100)
      val file3 = BlobTestHelpers.createTestFile(blobDir.toPath, "blob3.bin", 100)
      val file4 = BlobTestHelpers.createTestFile(blobDir.toPath, "blob4.bin", 100)
      val file1Updated = BlobTestHelpers.createTestFile(blobDir.toPath, "blob1_updated.bin", 80)
      val file2Updated = BlobTestHelpers.createTestFile(blobDir.toPath, "blob2_updated.bin", 60)

      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  data blob,
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
        s"insert into $tableName values (1, ${outOfLineBlobLiteral(file1, 0L, 100L)}, 1000)")
      spark.sql(
        s"insert into $tableName values (2, ${outOfLineBlobLiteral(file2, 0L, 100L)}, 1000)")
      spark.sql(
        s"insert into $tableName values (3, ${outOfLineBlobLiteral(file3, 0L, 100L)}, 1000)")
      // 3 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 1 as id, ${outOfLineBlobLiteral(file1Updated, 0L, 80L)} as data, 1001L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      // 4 commits will not trigger compaction, so it should be log only.
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 4 as id, ${outOfLineBlobLiteral(file4, 0L, 100L)} as data, 1000L as ts
           |) s0
           | on h0.id = s0.id
           | when not matched then insert *
           |""".stripMargin)

      // 5 commits will trigger compaction.
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tablePath))

      // read_blob() on an OUT_OF_LINE column must dereference external_path and read
      // the referenced byte range, verify bytes from the compacted base-file plan.
      val bytesById = spark.sql(
        s"select id, read_blob(data) as bytes from $tableName order by id"
      ).collect().map(r => r.getInt(0) -> r.getAs[Array[Byte]]("bytes")).toMap
      assertResult(4)(bytesById.size)
      assertResult(80)(bytesById(1).length)
      BlobTestHelpers.assertBytesContent(bytesById(1))
      assertResult(100)(bytesById(2).length)
      BlobTestHelpers.assertBytesContent(bytesById(2))
      assertResult(100)(bytesById(3).length)
      BlobTestHelpers.assertBytesContent(bytesById(3))
      assertResult(100)(bytesById(4).length)
      BlobTestHelpers.assertBytesContent(bytesById(4))

      // Verify out-of-line shape: type='OUT_OF_LINE', data null, reference non-null.
      spark.sql(s"select id, data from $tableName order by id").collect().foreach { row =>
        val blob = row.getStruct(1)
        assertResult("OUT_OF_LINE")(blob.getString(blob.fieldIndex(HoodieSchema.Blob.TYPE)))
        assert(blob.isNullAt(blob.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)))
        assert(!blob.isNullAt(blob.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)))
      }

      // BLOB custom-type descriptor must survive the compacted base-file read path.
      val blobField = spark.table(tableName).schema.find(_.name == "data").get
      assert(blobField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD),
        s"Expected BLOB type metadata on data field after compaction, " +
          s"got: ${blobField.metadata}")
      assertResult(HoodieSchemaType.BLOB.name())(
        blobField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))

      // 6th commit drives an auto-clean that retires the now-superseded log-only slice.
      // Inline compaction on commit 5 ran AFTER its own postCommit clean, so the prior
      // slice was not yet superseded when that clean fired and no .clean instant was
      // written. This deltacommit's postCommit clean writes the .clean instant.
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 2 as id, ${outOfLineBlobLiteral(file2Updated, 0L, 60L)} as data, 1002L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      val updatedBytesById = spark.sql(
        s"select id, read_blob(data) as bytes from $tableName order by id"
      ).collect().map(r => r.getInt(0) -> r.getAs[Array[Byte]]("bytes")).toMap
      assertResult(60)(updatedBytesById(2).length)
      BlobTestHelpers.assertBytesContent(updatedBytesById(2))

      val metaClient = createMetaClient(spark, tablePath)
      metaClient.reloadActiveTimeline()
      assert(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() > 0,
        "Expected at least one .clean instant on the timeline after compaction")
    })
  }

  // The following cases are parser-coverage only: a BLOB column routes the whole CREATE TABLE
  // through the extended AST builder, so its clause visitors run. parsePlan is purely syntactic
  // (no catalog, no execution), which lets us exercise clauses Hudi does not support at execution
  // time (transform partitioning, STORED AS / ROW FORMAT, interval columns). The BLOB column type
  // itself proves routing because the stock Spark parser rejects the BLOB type name.

  test("Test parse CREATE TABLE with BLOB column and primitive data types") {
    // Exercises the primitive-data-type match arms plus NOT NULL and column COMMENT.
    val plan = parseCreateTable(
      s"""
         |CREATE TABLE blob_prim_tbl (
         |  c_bool BOOLEAN,
         |  c_tiny TINYINT,
         |  c_small SMALLINT,
         |  c_int INT,
         |  c_big BIGINT,
         |  c_float FLOAT,
         |  c_double DOUBLE,
         |  c_date DATE,
         |  c_str STRING,
         |  c_char CHAR(5),
         |  c_varchar VARCHAR(10),
         |  c_bin BINARY,
         |  c_dec DECIMAL,
         |  c_dec1 DECIMAL(12),
         |  c_dec2 DECIMAL(12, 3),
         |  c_notnull INT NOT NULL,
         |  c_comment INT COMMENT 'a comment',
         |  data BLOB
         |) USING hudi
       """.stripMargin)
    val schema = plan.tableSchema
    assertResult(BooleanType)(schema("c_bool").dataType)
    assertResult(ByteType)(schema("c_tiny").dataType)
    assertResult(ShortType)(schema("c_small").dataType)
    assertResult(IntegerType)(schema("c_int").dataType)
    assertResult(LongType)(schema("c_big").dataType)
    assertResult(FloatType)(schema("c_float").dataType)
    assertResult(DoubleType)(schema("c_double").dataType)
    assertResult(DateType)(schema("c_date").dataType)
    assertResult(StringType)(schema("c_str").dataType)
    // CHAR/VARCHAR may be preserved or replaced with STRING depending on the Spark version.
    assert(Seq[DataType](CharType(5), StringType).contains(schema("c_char").dataType))
    assert(Seq[DataType](VarcharType(10), StringType).contains(schema("c_varchar").dataType))
    assertResult(BinaryType)(schema("c_bin").dataType)
    assertResult(DecimalType(10, 0))(schema("c_dec").dataType)
    assertResult(DecimalType(12, 0))(schema("c_dec1").dataType)
    assertResult(DecimalType(12, 3))(schema("c_dec2").dataType)
    assertResult(BlobType())(schema("data").dataType)
    assert(!schema("c_notnull").nullable)
    assertResult("a comment")(schema("c_comment").metadata.getString("comment"))
  }

  test("Test parse CREATE TABLE with BLOB column and complex data types") {
    // Exercises the ARRAY / MAP / STRUCT arms. BLOB-in-struct metadata handling is already
    // covered by "test BLOB in nested struct".
    val plan = parseCreateTable(
      s"""
         |CREATE TABLE blob_complex_tbl (
         |  c_arr ARRAY<INT>,
         |  c_map MAP<STRING, INT>,
         |  c_struct STRUCT<a: INT, b: STRING>,
         |  data BLOB
         |) USING hudi
       """.stripMargin)
    val schema = plan.tableSchema
    assertResult(ArrayType(IntegerType))(schema("c_arr").dataType)
    assertResult(MapType(StringType, IntegerType))(schema("c_map").dataType)
    val inner = schema("c_struct").dataType.asInstanceOf[StructType]
    assertResult(IntegerType)(inner("a").dataType)
    assertResult(StringType)(inner("b").dataType)
    assertResult(BlobType())(schema("data").dataType)
  }

  test("Test parse CREATE TABLE with BLOB column and interval data types") {
    val plan = parseCreateTable(
      s"""
         |CREATE TABLE blob_ivl_tbl (
         |  i_year INTERVAL YEAR,
         |  i_ym INTERVAL YEAR TO MONTH,
         |  i_day INTERVAL DAY,
         |  i_ds INTERVAL DAY TO SECOND,
         |  data BLOB
         |) USING hudi
       """.stripMargin)
    val schema = plan.tableSchema
    assertResult(YearMonthIntervalType(YearMonthIntervalType.YEAR))(schema("i_year").dataType)
    assertResult(YearMonthIntervalType(YearMonthIntervalType.YEAR, YearMonthIntervalType.MONTH))(
      schema("i_ym").dataType)
    assertResult(DayTimeIntervalType(DayTimeIntervalType.DAY))(schema("i_day").dataType)
    assertResult(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))(
      schema("i_ds").dataType)
    assertResult(BlobType())(schema("data").dataType)

    // Endpoints where the end field does not follow the start are rejected by both interval
    // data-type visitors. The grammar only allows YEAR/MONTH -> MONTH and DAY/HOUR/MINUTE/SECOND
    // -> HOUR/MINUTE/SECOND, so these stay grammatical yet still hit the builder's end <= start guard.
    interceptParse(
      "CREATE TABLE blob_bad_ym (id BIGINT, bad INTERVAL MONTH TO MONTH, data BLOB) USING hudi")(
      "are not supported")
    interceptParse(
      "CREATE TABLE blob_bad_dt (id BIGINT, bad INTERVAL SECOND TO HOUR, data BLOB) USING hudi")(
      "are not supported")
  }

  test("Test parse CREATE TABLE with BLOB column and partition transforms") {
    val plan = parseCreateTable(
      s"""
         |CREATE TABLE blob_tf_tbl (
         |  id BIGINT,
         |  ts DATE,
         |  region STRING,
         |  data BLOB
         |) USING hudi
         |PARTITIONED BY (region, years(ts), months(ts), days(ts), hours(ts), myfunc(id))
       """.stripMargin)
    assertResult(BlobType())(plan.tableSchema("data").dataType)
    assertResult(Seq(Seq("region")))(transformFieldRefs(transformByName(plan, "identity")))
    assertResult(Seq(Seq("ts")))(transformFieldRefs(transformByName(plan, "years")))
    assertResult(Seq(Seq("ts")))(transformFieldRefs(transformByName(plan, "months")))
    assertResult(Seq(Seq("ts")))(transformFieldRefs(transformByName(plan, "days")))
    assertResult(Seq(Seq("ts")))(transformFieldRefs(transformByName(plan, "hours")))
    // an arbitrary function transform falls through to the generic apply-transform arm
    assertResult(Seq(Seq("id")))(transformFieldRefs(transformByName(plan, "myfunc")))

    // bucket(numBuckets, col) with int, long and short number-of-buckets literals exercises the
    // three numeric arms of the bucket handling.
    Seq("4", "4L", "4S").foreach { numLiteral =>
      val bp = parseCreateTable(
        s"CREATE TABLE blob_bkt_tbl (id BIGINT, data BLOB) USING hudi " +
          s"PARTITIONED BY (bucket($numLiteral, id))")
      val bkt = transformByName(bp, "bucket")
      assertResult("4")(firstLiteralArg(bkt).value.toString)
      assertResult(Seq(Seq("id")))(transformFieldRefs(bkt))
    }
  }

  test("Test parse CREATE TABLE with BLOB column and partition columns") {
    // The partition-COLUMN arm is the only partitioning branch that changes the emitted schema:
    // the parsed partition columns are appended to the table columns and each becomes an
    // identity transform.
    val plan = parseCreateTable(
      "CREATE TABLE blob_pcol_tbl (id BIGINT, data BLOB) USING hudi PARTITIONED BY (p STRING)")
    assertResult(StringType)(plan.tableSchema("p").dataType)
    assertResult(BlobType())(plan.tableSchema("data").dataType)
    assertResult(Seq(Seq("p")))(transformFieldRefs(transformByName(plan, "identity")))

    // Mixing partition columns and transform expressions is rejected.
    checkExceptionContain(
      "CREATE TABLE blob_mix_tbl (id BIGINT, data BLOB) USING hudi " +
        "PARTITIONED BY (p STRING, bucket(4, id))")(
      "Cannot mix partition expressions and partition columns")

    // SKEWED BY and CREATE TEMPORARY TABLE are rejected by the clause and header visitors.
    checkExceptionContain(
      "CREATE TABLE blob_skew_tbl (id BIGINT, data BLOB) USING hudi SKEWED BY (id) ON (1, 2)")(
      "CREATE TABLE ... SKEWED BY")
    checkExceptionContain(
      "CREATE TEMPORARY TABLE blob_tmp_tbl (id BIGINT, data BLOB) USING hudi")(
      "use CREATE TEMPORARY VIEW instead")
  }

  test("Test parse CREATE TABLE with BLOB column and typed transform-argument literals") {
    // Constant transform arguments exercise the literal visitors: string, integer, big-integer and
    // exponent numerics (the private numeric-literal helper), the typed date constructor, and both
    // interval forms (multi-unit and unit-to-unit). A typed TIMESTAMP constructor is skipped due
    // to #19449: the extended parser enables ANSI reserved-keyword enforcement whenever
    // spark.sql.ansi.enabled is set (the Spark 4.x default) instead of following Spark's
    // spark.sql.ansi.enforceReservedKeywords, which makes a bare TIMESTAMP token unparseable
    // there (a bug, not a Spark 4 constraint). Boolean and null literals are keyword-mode
    // dependent: only TRUE is ansiNonReserved, so a bare true always parses as a column
    // reference, while false and null are column references under the default non-ANSI keyword
    // mode but typed literals under ANSI mode (both cases are asserted below).
    val plan = parseCreateTable(
      s"""
         |CREATE TABLE blob_lit_tbl (
         |  id BIGINT,
         |  data BLOB
         |) USING hudi
         |PARTITIONED BY (
         |  str_t('x', id),
         |  int_t(7, id),
         |  long_t(9000000000L, id),
         |  exp_t(1E3, id),
         |  date_t(DATE '2020-01-01', id),
         |  mu_ivl_t(INTERVAL '1' DAY, id),
         |  uu_ivl_t(INTERVAL '1-2' YEAR TO MONTH, id)
         |)
       """.stripMargin)
    assertResult(StringType)(firstLiteralArg(transformByName(plan, "str_t")).dataType)
    assertResult("x")(firstLiteralArg(transformByName(plan, "str_t")).value.toString)
    assertResult(IntegerType)(firstLiteralArg(transformByName(plan, "int_t")).dataType)
    assertResult(LongType)(firstLiteralArg(transformByName(plan, "long_t")).dataType)
    assertResult(DoubleType)(firstLiteralArg(transformByName(plan, "exp_t")).dataType)
    assertResult(DateType)(firstLiteralArg(transformByName(plan, "date_t")).dataType)
    assertResult(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY))(
      firstLiteralArg(transformByName(plan, "mu_ivl_t")).dataType)
    assertResult(YearMonthIntervalType(YearMonthIntervalType.YEAR, YearMonthIntervalType.MONTH))(
      firstLiteralArg(transformByName(plan, "uu_ivl_t")).dataType)

    // Under ANSI keyword mode a bare false and a bare null must survive visitBooleanLiteral
    // and visitNullLiteral as typed literals, while a bare true stays a column reference
    // (TRUE is ansiNonReserved; FALSE and NULL are not).
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      val ansiPlan = parseCreateTable(
        "CREATE TABLE blob_bool_tbl (id BIGINT, data BLOB) USING hudi " +
          "PARTITIONED BY (bool_t(false, id), true_t(true, id), null_t(null, id))")
      assertResult(BooleanType)(firstLiteralArg(transformByName(ansiPlan, "bool_t")).dataType)
      assertResult(false)(firstLiteralArg(transformByName(ansiPlan, "bool_t")).value)
      assertResult(NullType)(firstLiteralArg(transformByName(ansiPlan, "null_t")).dataType)
      assertResult(null)(firstLiteralArg(transformByName(ansiPlan, "null_t")).value)
      assertResult(Seq(Seq("true"), Seq("id")))(
        transformFieldRefs(transformByName(ansiPlan, "true_t")))
    }
  }

  test("Test parse CREATE TABLE with BLOB column and invalid partition transforms") {
    // Each case pins a distinct visitor arm of the extended AST builders; all must surface as a
    // clean ParseException on every Spark profile (#19450). Assertions stay substring-based
    // because the Spark 4.x builders add an "Operation not allowed: " prefix.
    // Non-numeric number of buckets.
    interceptParse("CREATE TABLE blob_e1 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (bucket('x', id))")(
      "Invalid number of buckets")
    // A non-column-reference where a column is required. The buggy interpolation rendered the
    // literal text "5.describe" (a superstring of the expected message), so pin its absence too.
    val e2 = interceptParse("CREATE TABLE blob_e2 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (bucket(4, 5))")(
      "Expected a column reference for transform bucket: 5")
    assert(!e2.getMessage.contains(".describe"))
    // A single-field transform given more than one argument.
    interceptParse("CREATE TABLE blob_e3 (id BIGINT, ts DATE, data BLOB) USING hudi PARTITIONED BY (years(id, ts))")(
      "Too many arguments")
    // Typed literal that fails to parse (visitTypeConstructor arm).
    interceptParse("CREATE TABLE blob_e4 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(DATE 'nope', id))")(
      "Cannot parse the DATE value: nope")
    // Invalid INTERVAL literal: the builders copy the triggering exception's stack trace onto the
    // ParseException (the construct-then-setStackTrace arm), so the thrower must be visible.
    val e5 = interceptParse("CREATE TABLE blob_e5 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(INTERVAL 'x', id))")(
      "Cannot parse the INTERVAL value: x")
    assert(e5.getStackTrace.exists(_.getClassName.contains("IntervalUtils")))
    // Out-of-range fractional literal; pre-fix the message's interior dots broke Spark 3.4+
    // error-class lookup and surfaced as a bare AssertionError.
    interceptParse("CREATE TABLE blob_e6 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (bucket(1e40F, id))")(
      "does not fit in range")
    // Mixed year-month and day-time interval fields.
    interceptParse("CREATE TABLE blob_e7 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(INTERVAL '1 year 2 hours', id))")(
      "Cannot mix year-month and day-time fields")
  }

  test("Test parse CREATE TABLE with BLOB column and invalid literal transform arguments") {
    // Remaining error arms of the literal and interval visitors, one SQL per throw site; all must
    // surface as a clean ParseException on every Spark profile (#19450).
    // A typed literal whose type keyword has no visitor arm.
    interceptParse("CREATE TABLE blob_e8 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(FOO 'bar', id))")(
      "Literals of type 'FOO' are currently not supported")
    // A hex literal with a non-hex character (the IllegalArgumentException fallback arm).
    interceptParse("CREATE TABLE blob_e9 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(X'zz', id))")(
      "hexBinary")
    // Multi-unit interval combined with a from-to unit.
    interceptParse("CREATE TABLE blob_e10 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(INTERVAL 1 DAY 2 HOUR TO MINUTE, id))")(
      "Can only have a single from-to unit in the interval literal syntax")
    // From-to unit combined with a trailing multi-unit interval (the error-recovery arm).
    interceptParse("CREATE TABLE blob_e11 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(INTERVAL '1' DAY TO HOUR '2' MINUTE, id))")(
      "Can only have a single from-to unit in the interval literal syntax")
    // A non-numeric value in a unit-value pair.
    interceptParse("CREATE TABLE blob_e12 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(INTERVAL 'x' DAY, id))")(
      "Can only use numbers in the interval value part")
    // A from-to interval whose value is not a string literal.
    interceptParse("CREATE TABLE blob_e13 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(INTERVAL 1 DAY TO HOUR, id))")(
      "The value of from-to unit must be a string")
    // A from-to unit pair outside the supported YEAR TO MONTH / DAY TO SECOND family.
    interceptParse("CREATE TABLE blob_e14 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (myfunc(INTERVAL '1' MONTH TO HOUR, id))")(
      "Intervals FROM month TO hour are not supported")
  }

  test("Test parse CREATE TABLE with BLOB column and file-format / row-format clauses") {
    // Each positive case asserts the SerdeInfo the clause visitors produced (portable across
    // Spark 3.3 through 4.2 via tableSpec.serde); asserting only the BLOB column type would pass
    // even if the file/row-format visitors dropped their clause.
    // Generic STORED AS format.
    val ff1 = parseCreateTable("CREATE TABLE blob_ff1 (id BIGINT, data BLOB) STORED AS PARQUET")
    assertResult(BlobType())(ff1.tableSchema("data").dataType)
    assertResult(Some("PARQUET"))(ff1.tableSpec.serde.get.storedAs)
    // STORED AS INPUTFORMAT ... OUTPUTFORMAT ... (the table-file-format arm).
    val ff2 = parseCreateTable("CREATE TABLE blob_ff2 (id BIGINT, data BLOB) " +
      "STORED AS INPUTFORMAT 'com.example.InFmt' OUTPUTFORMAT 'com.example.OutFmt'")
    assertResult(Some(FormatClasses("com.example.InFmt", "com.example.OutFmt")))(
      ff2.tableSpec.serde.get.formatClasses)
    // ROW FORMAT SERDE on its own.
    val ff3 = parseCreateTable("CREATE TABLE blob_ff3 (id BIGINT, data BLOB) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'")
    assertResult(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))(
      ff3.tableSpec.serde.get.serde)
    // ROW FORMAT DELIMITED on its own.
    val ff4 = parseCreateTable("CREATE TABLE blob_ff4 (id BIGINT, data BLOB) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    assertResult(",")(ff4.tableSpec.serde.get.serdeProperties("field.delim"))
    // Compatible ROW FORMAT SERDE + STORED AS SEQUENCEFILE merges both into one SerdeInfo.
    val ff5 = parseCreateTable("CREATE TABLE blob_ff5 (id BIGINT, data BLOB) " +
      "ROW FORMAT SERDE 'com.example.Serde' STORED AS SEQUENCEFILE")
    assertResult(Some("SEQUENCEFILE"))(ff5.tableSpec.serde.get.storedAs)
    assertResult(Some("com.example.Serde"))(ff5.tableSpec.serde.get.serde)
    // Compatible ROW FORMAT DELIMITED + STORED AS TEXTFILE.
    val ff6 = parseCreateTable("CREATE TABLE blob_ff6 (id BIGINT, data BLOB) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    assertResult(Some("TEXTFILE"))(ff6.tableSpec.serde.get.storedAs)
    assertResult(",")(ff6.tableSpec.serde.get.serdeProperties("field.delim"))
    // Any ROW FORMAT combined with STORED AS INPUTFORMAT/OUTPUTFORMAT is accepted (the
    // table-file-format arm of validateRowFormatFileFormat).
    val ff7 = parseCreateTable("CREATE TABLE blob_ff7 (id BIGINT, data BLOB) " +
      "ROW FORMAT SERDE 'com.example.Serde' " +
      "STORED AS INPUTFORMAT 'com.example.InFmt' OUTPUTFORMAT 'com.example.OutFmt'")
    assertResult(Some("com.example.Serde"))(ff7.tableSpec.serde.get.serde)
    assertResult(Some(FormatClasses("com.example.InFmt", "com.example.OutFmt")))(
      ff7.tableSpec.serde.get.formatClasses)

    // ROW FORMAT DELIMITED with a non-text file format is rejected.
    checkExceptionContain(
      "CREATE TABLE blob_ferr1 (id BIGINT, data BLOB) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET")(
      "only compatible with 'textfile'")
    // ROW FORMAT SERDE with a format that also specifies a serde is rejected.
    checkExceptionContain(
      "CREATE TABLE blob_ferr2 (id BIGINT, data BLOB) " +
        "ROW FORMAT SERDE 'com.example.Serde' STORED AS PARQUET")(
      "incompatible with format")
    // STORED BY (a storage handler) is not allowed. The full "Operation not allowed" prefix is
    // asserted because ParseException.getMessage echoes the SQL text, so a bare "STORED BY"
    // expectation would match any failure of this statement.
    checkExceptionContain(
      "CREATE TABLE blob_ferr3 (id BIGINT, data BLOB) STORED BY 'com.example.Handler'")(
      "Operation not allowed: STORED BY")
    // ROW FORMAT combined with STORED BY leaves no file format for the row format to pair with;
    // validateRowFormatFileFormat's catch-all arm rejects the combination before the STORED BY
    // error can fire.
    checkExceptionContain(
      "CREATE TABLE blob_ferr5 (id BIGINT, data BLOB) " +
        "ROW FORMAT SERDE 'com.example.Serde' STORED BY 'com.example.Handler'")(
      "Unexpected combination of")
    // A USING provider combined with a serde clause is not allowed.
    checkExceptionContain(
      "CREATE TABLE blob_ferr4 (id BIGINT, data BLOB) USING hudi STORED AS PARQUET")(
      "CREATE TABLE ... USING")
  }
}
