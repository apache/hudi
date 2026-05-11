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

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.io.File

class TestBlobDataType extends HoodieSparkSqlTestBase {

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
}
