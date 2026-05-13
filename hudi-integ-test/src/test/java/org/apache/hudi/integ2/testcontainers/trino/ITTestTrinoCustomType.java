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
 * fixed_len_byte_array), complementing {@link ITTestCustomTypeHiveSync} which
 * asserts the same fixtures round-trip through the Hive serde. A flip in either
 * direction (e.g. VECTOR decoded as array&lt;float&gt; instead of binary, BLOB
 * struct field projection broken) shows up here.
 *
 * <p>This test reuses the same {@code sparksql-*-sql.commands} fixtures that
 * {@code ITTestCustomTypeHiveSync} drives, so the two tests can run in either
 * order without cross-contamination (each has its own {@code @BeforeAll} that
 * re-seeds, and an {@code @AfterAll} that cleans up).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITTestTrinoCustomType extends ITTestBaseTestcontainers {

  private static final String BLOB_TEST_PATH = "/user/hive/warehouse/blob_test";
  private static final String VECTOR_TEST_PATH = "/user/hive/warehouse/vector_test";
  private static final String SPARKSQL_BLOB_TYPE_SQL_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-blob-type-sql.commands";
  private static final String SPARKSQL_VECTOR_TYPE_SQL_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-vector-type-sql.commands";

  @BeforeAll
  public void setupOnce() throws Exception {
    assumeTrinoPluginBuilt();
    initializeServices();
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + Paths.DEMO_SETUP).expectToSucceed();
    sparkAdhoc1.executeSQLFile(SPARKSQL_BLOB_TYPE_SQL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContainsLine("BLOB_SQL_TEST_SUCCESS");
    sparkAdhoc1.executeSQLFile(SPARKSQL_VECTOR_TYPE_SQL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContainsLine("VECTOR_SQL_TEST_SUCCESS");
    trino.waitUntilReady();
  }

  @AfterAll
  public void clean() throws Exception {
    sparkAdhoc1.executeShellCommand("hdfs dfs -rm -R -f "
        + BLOB_TEST_PATH + " " + VECTOR_TEST_PATH).expectToSucceed();
  }

  @Test
  public void testTrinoCountBlob() throws Exception {
    // Post-DELETE state of sparksql-blob-type-sql.commands is 2 rows (id=1 updated,
    // id=2 merged, id=3 inserted then deleted) — parity with the Hive count assertion
    // in ITTestCustomTypeHiveSync#testBlobTypeWithHiveSyncSQL.
    trino.execute("SELECT count(*) FROM blob_test")
        .expectToSucceed()
        .assertStdOutContains("2");
  }

  @Test
  public void testTrinoProjectsBlobStructField() throws Exception {
    // The BLOB column is stored as a Spark struct<type, data, reference>. Projecting
    // the nested `type` field through the native plugin confirms struct decoding is
    // wired correctly. id=1's final state per the fixture is OUT_OF_LINE.
    trino.execute("SELECT blob_data.type FROM blob_test WHERE id = 1")
        .expectToSucceed()
        .assertStdOutContains("OUT_OF_LINE");
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
}
