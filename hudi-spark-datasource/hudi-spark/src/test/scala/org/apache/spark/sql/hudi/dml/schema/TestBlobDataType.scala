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

import org.apache.spark.sql.catalyst.plans.logical.CreateTable
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

  private def parse(sql: String): CreateTable =
    spark.sessionState.sqlParser.parsePlan(sql).asInstanceOf[CreateTable]

  test("Test parse CREATE TABLE with BLOB column and primitive data types") {
    // Exercises the primitive-data-type match arms plus NOT NULL and column COMMENT.
    val plan = parse(
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
    // Exercises the ARRAY / MAP / STRUCT arms and BLOB-in-struct metadata handling.
    val plan = parse(
      s"""
         |CREATE TABLE blob_complex_tbl (
         |  c_arr ARRAY<INT>,
         |  c_map MAP<STRING, INT>,
         |  c_struct STRUCT<a: INT, b: STRING>,
         |  c_nested_blob STRUCT<x: BLOB>,
         |  data BLOB
         |) USING hudi
       """.stripMargin)
    val schema = plan.tableSchema
    assertResult(ArrayType(IntegerType))(schema("c_arr").dataType)
    assertResult(MapType(StringType, IntegerType))(schema("c_map").dataType)
    val inner = schema("c_struct").dataType.asInstanceOf[StructType]
    assertResult(IntegerType)(inner("a").dataType)
    assertResult(StringType)(inner("b").dataType)
    // A BLOB nested inside a struct still carries the BLOB type descriptor.
    val nested = schema("c_nested_blob").dataType.asInstanceOf[StructType]("x")
    assertResult(BlobType())(nested.dataType)
    assertResult(HoodieSchemaType.BLOB.name())(
      nested.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
  }

  test("Test parse CREATE TABLE with BLOB column and interval data types") {
    val plan = parse(
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
    checkExceptionContain(
      "CREATE TABLE blob_bad_ym (id BIGINT, bad INTERVAL MONTH TO MONTH, data BLOB) USING hudi")(
      "are not supported")
    checkExceptionContain(
      "CREATE TABLE blob_bad_dt (id BIGINT, bad INTERVAL SECOND TO HOUR, data BLOB) USING hudi")(
      "are not supported")

    // An unknown primitive type name is rejected.
    checkExceptionContain(
      "CREATE TABLE blob_bad_type (id BIGINT, weird sometype, data BLOB) USING hudi")(
      "is not supported")
  }

  test("Test parse CREATE TABLE with BLOB column and partition transforms") {
    val plan = parse(
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
      val bp = parse(
        s"CREATE TABLE blob_bkt_tbl (id BIGINT, data BLOB) USING hudi " +
          s"PARTITIONED BY (bucket($numLiteral, id))")
      val bkt = transformByName(bp, "bucket")
      assertResult("4")(firstLiteralArg(bkt).value.toString)
      assertResult(Seq(Seq("id")))(transformFieldRefs(bkt))
    }
  }

  test("Test parse CREATE TABLE with BLOB column and typed transform-argument literals") {
    // Constant transform arguments exercise the literal visitors: string, integer, big-integer and
    // exponent numerics (the private numeric-literal helper), the typed date constructor, and both
    // interval forms (multi-unit and unit-to-unit). A typed timestamp constructor is not used
    // because the Spark 4.x extended parser rejects a bare TIMESTAMP token. Note: a bare
    // true/false/null in this position is parsed as a column reference (qualifiedName takes
    // precedence over a constant in the grammar under the default non-ANSI config), so the boolean
    // and null literal visitors are not reachable from a CREATE TABLE statement.
    val plan = parse(
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
  }

  test("Test parse CREATE TABLE with BLOB column and invalid partition transforms") {
    // Non-numeric number of buckets.
    checkExceptionContain(
      "CREATE TABLE blob_e1 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (bucket('x', id))")(
      "Invalid number of buckets")
    // A non-column-reference where a column is required.
    checkExceptionContain(
      "CREATE TABLE blob_e2 (id BIGINT, data BLOB) USING hudi PARTITIONED BY (bucket(4, 5))")(
      "Expected a column reference")
    // A single-field transform given more than one argument.
    checkExceptionContain(
      "CREATE TABLE blob_e3 (id BIGINT, ts DATE, data BLOB) USING hudi " +
        "PARTITIONED BY (years(id, ts))")(
      "Too many arguments")
  }

  test("Test parse CREATE TABLE with BLOB column and file-format / row-format clauses") {
    // Generic STORED AS format.
    assertResult(BlobType())(
      parse("CREATE TABLE blob_ff1 (id BIGINT, data BLOB) STORED AS PARQUET")
        .tableSchema("data").dataType)
    // STORED AS INPUTFORMAT ... OUTPUTFORMAT ... (the table-file-format arm).
    assertResult(BlobType())(
      parse("CREATE TABLE blob_ff2 (id BIGINT, data BLOB) " +
        "STORED AS INPUTFORMAT 'com.example.InFmt' OUTPUTFORMAT 'com.example.OutFmt'")
        .tableSchema("data").dataType)
    // ROW FORMAT SERDE on its own.
    assertResult(BlobType())(
      parse("CREATE TABLE blob_ff3 (id BIGINT, data BLOB) " +
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'")
        .tableSchema("data").dataType)
    // ROW FORMAT DELIMITED on its own.
    assertResult(BlobType())(
      parse("CREATE TABLE blob_ff4 (id BIGINT, data BLOB) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
        .tableSchema("data").dataType)
    // Compatible ROW FORMAT SERDE + STORED AS SEQUENCEFILE.
    assertResult(BlobType())(
      parse("CREATE TABLE blob_ff5 (id BIGINT, data BLOB) " +
        "ROW FORMAT SERDE 'com.example.Serde' STORED AS SEQUENCEFILE")
        .tableSchema("data").dataType)
    // Compatible ROW FORMAT DELIMITED + STORED AS TEXTFILE.
    assertResult(BlobType())(
      parse("CREATE TABLE blob_ff6 (id BIGINT, data BLOB) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
        .tableSchema("data").dataType)

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
    // STORED BY (a storage handler) is not allowed.
    checkExceptionContain(
      "CREATE TABLE blob_ferr3 (id BIGINT, data BLOB) STORED BY 'com.example.Handler'")(
      "STORED BY")
    // A USING provider combined with a serde clause is not allowed.
    checkExceptionContain(
      "CREATE TABLE blob_ferr4 (id BIGINT, data BLOB) USING hudi STORED AS PARQUET")(
      "CREATE TABLE ... USING")
  }
}
