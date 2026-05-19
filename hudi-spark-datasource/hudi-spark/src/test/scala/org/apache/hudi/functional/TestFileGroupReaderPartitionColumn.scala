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

package org.apache.hudi.functional

import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull}
import org.junit.jupiter.api.Test

/**
 * Regression test for the FileGroupReader bug where mandatory partition columns
 * were dropped from `dataSchema` before pruning, causing the column to read back
 * as null for untouched rows in MOR file slices containing both a base file and
 * a log file.
 *
 * Scenario: MOR + `CustomKeyGenerator` (`country:simple`) + `PostgresDebeziumAvroPayload`
 * + `GLOBAL_SIMPLE` index with `update.partition.path=true`, then a round-2 write
 * that moves two records out of the `country=IN` partition via a partition-key
 * change. This produces a file slice in `country=IN` with both a base parquet
 * (round 1) and a log file (round 2 delete markers for the moved records),
 * while the two untouched records in that slice (id=12 and id=14) remain in
 * the base file.
 *
 * Before the fix: `buildReaderWithPartitionValues` augmented only `requestedStructType`
 * with mandatory partition fields before pruning, leaving `dataSchema` missing
 * `country`. The FileGroupReader path then skipped reading the column from
 * parquet, and the `FileGroupReaderSchemaHandler` (which sets `requiredSchema` to
 * `this.tableSchema` for a non-projection-compatible CUSTOM merger like Postgres)
 * propagated that through the output converter, which wrote `null` for every
 * untouched row in the base+log slice.
 *
 * After the fix: `dataSchema` contains `country`, so id=12 and id=14 read back
 * with the correct `country="IN"`.
 *
 * Partitions without log files (`country=US`, `country=CN`) hit the `readBaseFile`
 * path that appends partition values from the directory and are unaffected either
 * way — they're included here as negative controls.
 */
class TestFileGroupReaderPartitionColumn extends SparkClientFunctionalTestHarness {

  @Test
  def testMandatoryPartitionColumnReadFromLogFileSlice(): Unit = {
    val commonOpts = Map(
      "hoodie.table.name" -> "test_fg_reader_partition_col",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.write.table.version" -> "6",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombine.field" -> "_event_lsn",
      "hoodie.datasource.write.partitionpath.field" -> "country:simple",
      "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.CustomKeyGenerator",
      "hoodie.datasource.write.hive_style_partitioning" -> "true",
      "hoodie.datasource.write.partitionpath.urlencode" -> "true",
      "hoodie.datasource.write.payload.class" ->
        "org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload",
      "hoodie.index.type" -> "GLOBAL_SIMPLE",
      "hoodie.simple.index.update.partition.path" -> "true",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.metadata.enable" -> "true",
      "hoodie.compact.inline" -> "false",
      "hoodie.clean.automatic" -> "false",
      "hoodie.datasource.write.reconcile.schema" -> "false"
    )

    val schema = StructType(Array(
      StructField("_change_operation_type", StringType, nullable = true),
      StructField("_event_lsn", LongType, nullable = true),
      StructField("id", LongType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true)
    ))

    // Round 1: 8 records across 3 partitions.
    //  country=IN: id=2,6,12,14 (4 rows in one base parquet)
    //  country=US: id=4,10
    //  country=CN: id=8,16
    val round1Rows = Seq(
      Row("c", 2550218872L,  2L, "IN", "0x100000021ce30", "Acme Corp",       "plan change",       11.0),
      Row("c", 2550219144L,  4L, "US", "0x100000052e763", "Delta corp",      "telecoms activity", 12.0),
      Row("c", 2550219424L,  6L, "IN", "0x10000008e92b8", "Xyzzy Inc.",      "plan change",       13.0),
      Row("c", 2550219696L,  8L, "CN", "0x10000008a5eba", "Xyzzy Inc.",      "plan change",       14.0),
      Row("c", 2550219968L, 10L, "US", "0x10000008df79c", "Lakehouse Ltd",   "device error",      15.0),
      Row("c", 2550220240L, 12L, "IN", "0x10000007ddfb1", "Embanks Devices", "plan change",       16.0),
      Row("c", 2550220512L, 14L, "IN", "0x10000008d8892", "Acme Corp",       "deactivation",      17.0),
      Row("c", 2550220784L, 16L, "CN", "0x10000007352cd", "Acme Corp",       "telecoms activity", 18.0)
    )
    spark.createDataFrame(spark.sparkContext.parallelize(round1Rows, 1), schema)
      .write.format("hudi")
      .options(commonOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // Round 2: partition-key changes for id=2 (IN->US), id=6 (IN->US), and a delete
    // for id=4 (stays in US). With GLOBAL_SIMPLE + update.partition.path=true, the
    // partition-key changes leave delete markers in the old country=IN partition —
    // which gives us the base+log file layout in country=IN. id=12 and id=14 are
    // untouched and must read back with country=IN after the fix.
    val round2Rows = Seq(
      Row("u", 2650218872L, 2L, "US", "0x100000021ce30", "Acme Corp",  "plan change",       11.0),
      Row("d", 2650219144L, 4L, "US", "0x100000052e763", "Delta corp", "telecoms activity", 12.0),
      Row("u", 2650219424L, 6L, "US", "0x10000008e92b8", "Xyzzy Inc.", "plan change",       13.0)
    )
    spark.createDataFrame(spark.sparkContext.parallelize(round2Rows, 1), schema)
      .write.format("hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val rows = spark.read.format("hudi").load(basePath)
      .select("id", "country")
      .collect()
      .map(r => r.getLong(0) -> (if (r.isNullAt(1)) null else r.getString(1)))
      .toMap

    // The moved records land in US.
    assertEquals("US", rows(2L), "id=2 moved to US")
    assertEquals("US", rows(6L), "id=6 moved to US")

    // id=4 was deleted.
    assertFalse(rows.contains(4L), "id=4 was deleted")

    // Untouched rows whose slice has no log file — negative controls, these were
    // never broken.
    assertEquals("CN", rows(8L), "id=8 untouched in CN (no log file)")
    assertEquals("US", rows(10L), "id=10 untouched in US (no log file)")
    assertEquals("CN", rows(16L), "id=16 untouched in CN (no log file)")

    // The regression: untouched rows in the IN slice that now has base+log. Before
    // the fix these read back as null.
    assertNotNull(rows(12L),
      "id=12 (untouched, country=IN slice with base+log) must not be null")
    assertNotNull(rows(14L),
      "id=14 (untouched, country=IN slice with base+log) must not be null")
    assertEquals("IN", rows(12L), "id=12 partition column must be IN")
    assertEquals("IN", rows(14L), "id=14 partition column must be IN")
  }
}
