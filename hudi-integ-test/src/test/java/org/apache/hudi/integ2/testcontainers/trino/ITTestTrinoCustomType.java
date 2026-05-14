/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.integ2.testcontainers.trino;

import org.apache.hudi.integ2.testcontainers.ITTestBaseTestcontainers;
import org.apache.hudi.integ2.testcontainers.ITTestCustomTypeHiveSync;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.apache.hudi.integ2.testcontainers.TestcontainersConfig.Paths;

/**
 * Trino read coverage for Hudi's custom logical types (BLOB struct, VECTOR
 * fixed_len_byte_array, VARIANT), complementing {@link ITTestCustomTypeHiveSync}
 * which asserts the same fixtures round-trip through the Hive serde. A flip in
 * either direction (e.g. VECTOR decoded as array&lt;float&gt; instead of
 * binary, BLOB struct field projection broken, VARIANT row count off) shows up
 * here.
 *
 * <p>This test reuses the same {@code sparksql-*-sql.commands} fixtures that
 * {@code ITTestCustomTypeHiveSync} drives, so the two tests can run in either
 * order without cross-contamination (each has its own {@code @BeforeAll} that
 * re-seeds, and an {@code @AfterAll} that cleans up).
 *
 * <p>VARIANT seeding and tests only fire on a Spark 4.x compose; on Spark 3.5
 * the BLOB and VECTOR coverage still runs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITTestTrinoCustomType extends ITTestBaseTestcontainers {

  private static final String BLOB_TEST_PATH = "/user/hive/warehouse/blob_test";
  private static final String BLOB_TEST_DF_PATH = "/user/hive/warehouse/blob_test_df";
  private static final String VECTOR_TEST_PATH = "/user/hive/warehouse/vector_test";
  private static final String VARIANT_TEST_PATH = "/user/hive/warehouse/variant_test";
  private static final String SPARKSQL_BLOB_TYPE_SQL_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-blob-type-sql.commands";
  private static final String SPARKSQL_BLOB_TYPE_DF_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-blob-type-df.commands";
  private static final String SPARKSQL_VECTOR_TYPE_SQL_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-vector-type-sql.commands";
  private static final String SPARKSQL_VARIANT_TYPE_SQL_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-variant-type-sql.commands";

  @BeforeAll
  public void setupOnce() throws Exception {
    assumeTrinoPluginBuilt();
    initializeServices();
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + Paths.DEMO_SETUP).expectToSucceed();
    sparkAdhoc1.executeSQLFile(SPARKSQL_BLOB_TYPE_SQL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContainsLine("BLOB_SQL_TEST_SUCCESS");
    // The DF fixture writes blob_test_df with the INLINE branch of the BLOB struct
    // (data field non-null, reference null). The SQL fixture exercises only the
    // OUT_OF_LINE branch, so seeding both gives Trino read coverage of both shapes.
    sparkAdhoc1.executeSQLFile(SPARKSQL_BLOB_TYPE_DF_COMMANDS)
        .expectToSucceed()
        .assertStdOutContainsLine("BLOB_DF_TEST_SUCCESS");
    sparkAdhoc1.executeSQLFile(SPARKSQL_VECTOR_TYPE_SQL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContainsLine("VECTOR_SQL_TEST_SUCCESS");
    if (isSpark4Compose()) {
      // VARIANT type is Spark 4.x only - guard the seed so the BLOB/VECTOR
      // coverage still runs on a Spark 3.5 stack.
      sparkAdhoc1.executeSQLFile(SPARKSQL_VARIANT_TYPE_SQL_COMMANDS)
          .expectToSucceed()
          .assertStdOutContainsLine("VARIANT_SQL_TEST_SUCCESS");
    }
    trino.waitUntilReady();
  }

  @AfterAll
  public void clean() throws Exception {
    // -f silently skips non-existent paths so the variant_test cleanup is safe
    // even on Spark 3.5 runs where the table was never created.
    sparkAdhoc1.executeShellCommand("hdfs dfs -rm -R -f "
        + BLOB_TEST_PATH + " " + BLOB_TEST_DF_PATH + " "
        + VECTOR_TEST_PATH + " " + VARIANT_TEST_PATH).expectToSucceed();
  }

  // ---------- BLOB OUT_OF_LINE (blob_test) ----------

  @Test
  public void testTrinoCountBlob() throws Exception {
    // Post-DELETE state of sparksql-blob-type-sql.commands is 2 rows (id=1 updated,
    // id=2 merged, id=3 inserted then deleted) - parity with the Hive count assertion
    // in ITTestCustomTypeHiveSync#testBlobTypeWithHiveSyncSQL.
    trino.execute("SELECT count(*) FROM blob_test")
        .expectToSucceed()
        .assertStdOutContains("2");
  }

  @Test
  public void testTrinoProjectsBlobUpdatedRow() throws Exception {
    // Full per-row shape for id=1 (post-UPDATE state): type discriminator +
    // every reference subfield + the OUT_OF_LINE invariant that data IS NULL.
    // One query, one substring assertion - catches column-order shifts,
    // nested-struct field renames, and per-field decoding bugs.
    trino.execute("SELECT blob_data.type, blob_data.data IS NULL, "
            + "blob_data.reference.external_path, blob_data.reference.offset, "
            + "blob_data.reference.length, blob_data.reference.managed "
            + "FROM blob_test WHERE id = 1")
        .expectToSucceed()
        .assertStdOutContains("OUT_OF_LINE,true,blobs/updated-1,10,100,true");
  }

  @Test
  public void testTrinoProjectsBlobMergedRow() throws Exception {
    // id=2 was MATCHED by the MERGE clause and rewritten to 'blobs/merged-2'.
    // Same full-shape assertion as id=1 - confirms both UPDATE and MERGE write
    // paths land at an identical on-disk OUT_OF_LINE shape.
    trino.execute("SELECT blob_data.type, blob_data.data IS NULL, "
            + "blob_data.reference.external_path, blob_data.reference.offset, "
            + "blob_data.reference.length, blob_data.reference.managed "
            + "FROM blob_test WHERE id = 2")
        .expectToSucceed()
        .assertStdOutContains("OUT_OF_LINE,true,blobs/merged-2,20,200,true");
  }

  @Test
  public void testTrinoBlobDeletedRowAbsent() throws Exception {
    // id=3 was MERGE-inserted into dt=2024-01-02 then DELETEd. A DELETE that
    // leaves the row visible (e.g. tombstone not honored on read) shows up as
    // count = 1 here. Pairs with testTrinoCountBlob = 2 (total post-delete)
    // to catch the case where DELETE silently no-ops.
    trino.execute("SELECT count(*) FROM blob_test WHERE id = 3")
        .expectToSucceed()
        .assertStdOutContains("0");
  }

  @Test
  public void testTrinoBlobBothPartitionsVisible() throws Exception {
    // MERGE created dt=2024-01-02 (id=3), then DELETE emptied it. The native
    // trino-hudi connector filters to partitions with files, so the empty
    // partition is invisible to Trino - we only verify the data-bearing
    // partition's row count here. Catches regressions in partition pruning or
    // GROUP BY against partition columns.
    trino.execute("SELECT dt, count(*) FROM blob_test GROUP BY dt ORDER BY dt")
        .expectToSucceed()
        .assertStdOutContains("2024-01-01,2");
  }

  // ---------- BLOB INLINE (blob_test_df) ----------

  @Test
  public void testTrinoCountBlobInline() throws Exception {
    // Post-DELETE state of sparksql-blob-type-df.commands is 2 rows (id=1 kept,
    // id=2 updated to "updated payload", id=3 upserted then deleted). Parity
    // with testTrinoCountBlob for the INLINE-shape table.
    trino.execute("SELECT count(*) FROM blob_test_df")
        .expectToSucceed()
        .assertStdOutContains("2");
  }

  @Test
  public void testTrinoProjectsBlobInlineRow() throws Exception {
    // INLINE shape for id=1 (never mutated): type=INLINE, data is the UTF-8
    // seed "hello world", and the INLINE invariant that reference IS NULL.
    // from_utf8(data) is the right decoder - raw cast(varbinary as varchar)
    // is rejected by Trino.
    trino.execute("SELECT blob_data.type, from_utf8(blob_data.data), "
            + "blob_data.reference IS NULL FROM blob_test_df WHERE id = 1")
        .expectToSucceed()
        .assertStdOutContains("INLINE,hello world,true");
  }

  @Test
  public void testTrinoProjectsBlobInlineUpdatedRow() throws Exception {
    // id=2 was UPSERT-rewritten to "updated payload" in the DF fixture. Same
    // full-shape assertion as id=1 - confirms the UPSERT write path for the
    // INLINE branch round-trips end-to-end through Trino.
    trino.execute("SELECT blob_data.type, from_utf8(blob_data.data), "
            + "blob_data.reference IS NULL FROM blob_test_df WHERE id = 2")
        .expectToSucceed()
        .assertStdOutContains("INLINE,updated payload,true");
  }

  @Test
  public void testTrinoBlobInlineDeletedRowAbsent() throws Exception {
    // id=3 was upserted into dt=2024-01-02 then DELETEd in the DF fixture.
    // Parity with testTrinoBlobDeletedRowAbsent for the INLINE-shape table.
    trino.execute("SELECT count(*) FROM blob_test_df WHERE id = 3")
        .expectToSucceed()
        .assertStdOutContains("0");
  }

  // ---------- VECTOR (vector_test) ----------

  @Test
  public void testTrinoCountVector() throws Exception {
    // Post-DELETE state of sparksql-vector-type-sql.commands is 2 rows.
    // Parity with testTrinoCountBlob / testTrinoCountVariant.
    trino.execute("SELECT count(*) FROM vector_test")
        .expectToSucceed()
        .assertStdOutContains("2");
  }

  @Test
  public void testTrinoVectorDeletedRowAbsent() throws Exception {
    // id=3 was MERGE-inserted then DELETEd in the vector fixture. Parity with
    // the BLOB/VARIANT delete-absence checks.
    trino.execute("SELECT count(*) FROM vector_test WHERE id = 3")
        .expectToSucceed()
        .assertStdOutContains("0");
  }

  @Test
  public void testTrinoVectorRoundTripsAsBinary() throws Exception {
    // Per RFC-99, VECTOR(3) is stored on disk as fixed_len_byte_array(12) and Hive
    // sync maps it to BINARY. The native plugin should expose the column as VARBINARY
    // of the same 12 bytes (3 floats * 4 bytes). length() returning 12 confirms the
    // round-trip; a return of 3 would mean the plugin decoded it as array<float>,
    // a real regression worth a separate ticket. Pairs with the Hive assertion at
    // ITTestCustomTypeHiveSync:228-235.
    trino.execute("SELECT length(embedding) FROM vector_test WHERE id = 1")
        .expectToSucceed()
        .assertStdOutContains("12");
  }

  @Test
  public void testTrinoVectorBytesDecodeToExpectedFloats() throws Exception {
    // The 12 bytes of VECTOR(3) are 3 IEEE-754 floats in little-endian (Parquet's
    // default). reverse(substr(..., n, 4)) flips each 4-byte chunk to big-endian
    // so from_ieee754_32 returns the actual value. round(., 1) sidesteps float-
    // precision noise in Trino's CSV rendering (0.9f decodes to ~0.90000004).
    // For id=1's post-UPDATE state the fixture writes (0.9f, 0.8f, 0.7f).
    trino.execute("SELECT round(from_ieee754_32(reverse(substr(embedding, 1, 4))), 1), "
            + "round(from_ieee754_32(reverse(substr(embedding, 5, 4))), 1), "
            + "round(from_ieee754_32(reverse(substr(embedding, 9, 4))), 1) "
            + "FROM vector_test WHERE id = 1")
        .expectToSucceed()
        .assertStdOutContains("0.9,0.8,0.7");
  }

  @Test
  public void testTrinoVectorMergedRowDecodes() throws Exception {
    // id=2 was MATCHED by the MERGE clause and rewritten to (0.41f, 0.51f, 0.61f).
    // round(., 2) keeps it readable; complements id=1's UPDATE path to confirm
    // both write paths land at the same on-disk layout.
    trino.execute("SELECT round(from_ieee754_32(reverse(substr(embedding, 1, 4))), 2), "
            + "round(from_ieee754_32(reverse(substr(embedding, 5, 4))), 2), "
            + "round(from_ieee754_32(reverse(substr(embedding, 9, 4))), 2) "
            + "FROM vector_test WHERE id = 2")
        .expectToSucceed()
        .assertStdOutContains("0.41,0.51,0.61");
  }

  @Test
  public void testTrinoVectorAllRowsAreFixedLength12() throws Exception {
    // Invariant: every row's embedding is exactly 12 bytes. count(DISTINCT length(...))
    // = 1 AND min/max = 12 catches the case where some rows decode at a different
    // width (e.g. an older row written before a layout fix). Stronger than the
    // single-row length() check in testTrinoVectorRoundTripsAsBinary.
    trino.execute("SELECT count(DISTINCT length(embedding)), min(length(embedding)), "
            + "max(length(embedding)) FROM vector_test")
        .expectToSucceed()
        .assertStdOutContains("1,12,12");
  }

  // ---------- VARIANT (Spark 4.x only) ----------

  @Test
  public void testTrinoCountVariant() throws Exception {
    assumeSpark4Compose();
    // Post-DELETE state of sparksql-variant-type-sql.commands is 2 rows (id=1
    // updated, id=2 merged, id=3 inserted then deleted) - parity with the Hive
    // count assertion in ITTestCustomTypeHiveSync#testVariantTypeWithHiveSyncSQL.
    // count(*) doesn't deserialize the variant column so it's safe even if the
    // plugin's variant decoding has gaps.
    trino.execute("SELECT count(*) FROM variant_test")
        .expectToSucceed()
        .assertStdOutContains("2");
  }

  @Test
  public void testTrinoIntrospectsVariantColumn() throws Exception {
    assumeSpark4Compose();
    // Schema-level smoke: the variant_data column must surface in Trino's view of
    // the table. Catches metastore-side regressions where Hive sync drops or
    // mistypes the variant column entirely, separately from the
    // value-projection question (which depends on how the plugin maps VARIANT).
    trino.execute("DESCRIBE variant_test")
        .expectToSucceed()
        .assertStdOutContains("variant_data");
  }

  @Test
  public void testTrinoProjectsVariantValue() throws Exception {
    assumeSpark4Compose();
    // The native trino-hudi plugin exposes VARIANT as ROW(metadata VARBINARY,
    // value VARBINARY) with no top-level JSON/string decoding. But Spark's
    // Variant binary format stores leaf string values as UTF-8 bytes inside
    // the value component, so we can decode the bytes with from_utf8() and
    // LIKE-match the seeded payload. Non-UTF8 framing bytes around the leaf
    // become U+FFFD replacement chars, which the wildcard tolerates. This is
    // real content round-trip: write {"key":"value1-updated"}, read the same
    // payload back through the plugin.
    trino.execute("SELECT from_utf8(variant_data.value) LIKE '%value1-updated%' "
            + "FROM variant_test WHERE id = 1")
        .expectToSucceed()
        .assertStdOutContains("true");
  }

  @Test
  public void testTrinoProjectsVariantMergedRow() throws Exception {
    assumeSpark4Compose();
    // Same content-level round-trip for the MERGE-rewritten row. id=2's seed
    // is {"key":"value2-merged"}; verifying that exact payload survives the
    // MERGE write path -> hive sync -> Trino read end-to-end.
    trino.execute("SELECT from_utf8(variant_data.value) LIKE '%value2-merged%' "
            + "FROM variant_test WHERE id = 2")
        .expectToSucceed()
        .assertStdOutContains("true");
  }

  @Test
  public void testTrinoVariantValuesDifferAcrossRows() throws Exception {
    assumeSpark4Compose();
    // Invariant: id=1 ({"key":"value1-updated"}) and id=2 ({"key":"value2-merged"})
    // must produce different value bytes. Trino's count(DISTINCT ...) supports
    // VARBINARY natively, so no base64 wrapping is needed. Catches the
    // "projection returns same bytes for every row" regression that per-row
    // content matches would silently miss.
    trino.execute("SELECT count(DISTINCT variant_data.value) FROM variant_test")
        .expectToSucceed()
        .assertStdOutContains("2");
  }

  @Test
  public void testTrinoVariantDeletedRowAbsent() throws Exception {
    assumeSpark4Compose();
    // id=3 was MERGE-inserted into dt=2024-01-02 then DELETEd. Mirrors
    // testTrinoBlobDeletedRowAbsent for the variant table - catches the case
    // where DELETE silently no-ops on a Variant-bearing row.
    trino.execute("SELECT count(*) FROM variant_test WHERE id = 3")
        .expectToSucceed()
        .assertStdOutContains("0");
  }

  @Test
  public void testTrinoVariantBothPartitionsVisible() throws Exception {
    assumeSpark4Compose();
    // Same partition-pruning behavior as the BLOB variant: dt=2024-01-02 is
    // invisible after its only row was deleted. Verify the data-bearing
    // partition's row count surfaces correctly.
    trino.execute("SELECT dt, count(*) FROM variant_test GROUP BY dt ORDER BY dt")
        .expectToSucceed()
        .assertStdOutContains("2024-01-01,2");
  }
}
