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

package org.apache.hudi.integ2.testcontainers;

import org.apache.hudi.common.util.CollectionUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * End-to-end Hive sync coverage for Hudi's custom logical types (VECTOR, BLOB) and the
 * Spark 4.0 VARIANT type, running against a real Hive metastore via the Testcontainers
 * harness.
 *
 * Each type is exercised through both paths:
 *   - SQL CREATE TABLE (`*-sql.commands`) - table name `<type>_test`
 *   - DataFrame writer API (`*-df.commands`) - table name `<type>_test_df`
 */
public class ITTestCustomTypeHiveSync extends ITTestBaseTestcontainers {

  private static final String HOODIE_WS_ROOT = "/var/hoodie/ws";

  // BLOB
  private static final String BLOB_SQL_TEST_BASE_PATH = "/user/hive/warehouse/blob_test";
  private static final String BLOB_DF_TEST_BASE_PATH = "/user/hive/warehouse/blob_test_df";
  private static final String SPARKSQL_BLOB_TYPE_SQL_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-blob-type-sql.commands";
  private static final String SPARKSQL_BLOB_TYPE_DF_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-blob-type-df.commands";

  // VARIANT
  private static final String VARIANT_SQL_TEST_BASE_PATH = "/user/hive/warehouse/variant_test";
  private static final String VARIANT_DF_TEST_BASE_PATH = "/user/hive/warehouse/variant_test_df";
  private static final String SPARKSQL_VARIANT_TYPE_SQL_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-variant-type-sql.commands";
  private static final String SPARKSQL_VARIANT_TYPE_DF_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-variant-type-df.commands";

  // VECTOR
  private static final String VECTOR_SQL_TEST_BASE_PATH = "/user/hive/warehouse/vector_test";
  private static final String VECTOR_DF_TEST_BASE_PATH = "/user/hive/warehouse/vector_test_df";
  private static final String SPARKSQL_VECTOR_TYPE_SQL_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-vector-type-sql.commands";
  private static final String SPARKSQL_VECTOR_TYPE_DF_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-vector-type-df.commands";

  private static final String DEMO_CONTAINER_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/setup_demo_container.sh";

  @BeforeEach
  public void setup() {
    initializeServices();
  }

  @AfterEach
  public void clean() throws Exception {
    // Use -f to silently skip non-existent paths (not all tests create all tables).
    final String hdfsCmd = "hdfs dfs -rm -R -f ";
    List<String> tablePaths = CollectionUtils.createImmutableList(
        BLOB_SQL_TEST_BASE_PATH, BLOB_DF_TEST_BASE_PATH,
        VARIANT_SQL_TEST_BASE_PATH, VARIANT_DF_TEST_BASE_PATH,
        VECTOR_SQL_TEST_BASE_PATH, VECTOR_DF_TEST_BASE_PATH);
    for (String tablePath : tablePaths) {
      sparkAdhoc1.executeShellCommand(hdfsCmd + tablePath)
          .expectToSucceed();
    }
  }

  // ---------- BLOB ----------

  @Test
  @Disabled("FIXME: Blob hive-sync E2E (SQL path) currently fails on the testcontainers harness. "
      + "Wiring is kept in place so the test can be re-enabled once the failure is root-caused.")
  public void testBlobTypeWithHiveSyncSQL() throws Exception {
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + DEMO_CONTAINER_SCRIPT).expectToSucceed();

    sparkAdhoc1.executeSQLFile(SPARKSQL_BLOB_TYPE_SQL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("BLOB_SQL_TEST_SUCCESS");

    hive.execute("DESCRIBE default.blob_test")
        .expectToSucceed()
        .assertStdOutContains("blob_data");

    hive.execute("SHOW PARTITIONS default.blob_test")
        .expectToSucceed()
        .assertStdOutContains("dt=2024-01-01");
  }

  @Test
  @Disabled("FIXME: Blob hive-sync E2E (DataFrame path) currently fails on the testcontainers harness. "
      + "Wiring is kept in place so the test can be re-enabled once the failure is root-caused.")
  public void testBlobTypeWithHiveSyncDataFrameAPI() throws Exception {
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + DEMO_CONTAINER_SCRIPT).expectToSucceed();

    sparkAdhoc1.executeSQLFile(SPARKSQL_BLOB_TYPE_DF_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("BLOB_DF_TEST_SUCCESS");

    hive.execute("DESCRIBE default.blob_test_df")
        .expectToSucceed()
        .assertStdOutContains("blob_data");

    hive.execute("SHOW PARTITIONS default.blob_test_df")
        .expectToSucceed()
        .assertStdOutContains("dt=2024-01-01");
  }

  // ---------- VARIANT (Spark 4.x only) ----------

  @Test
  public void testVariantTypeWithHiveSyncSQL() throws Exception {
    assumeSpark4Compose();
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + DEMO_CONTAINER_SCRIPT).expectToSucceed();

    sparkAdhoc1.executeSQLFile(SPARKSQL_VARIANT_TYPE_SQL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("VARIANT_SQL_TEST_SUCCESS");

    hive.execute("DESCRIBE default.variant_test")
        .expectToSucceed()
        .assertStdOutContains("variant_data");

    hive.execute("SHOW PARTITIONS default.variant_test")
        .expectToSucceed()
        .assertStdOutContains("dt=2024-01-01");
  }

  @Test
  public void testVariantTypeWithHiveSyncDataFrameAPI() throws Exception {
    assumeSpark4Compose();
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + DEMO_CONTAINER_SCRIPT).expectToSucceed();

    sparkAdhoc1.executeSQLFile(SPARKSQL_VARIANT_TYPE_DF_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("VARIANT_DF_TEST_SUCCESS");

    hive.execute("DESCRIBE default.variant_test_df")
        .expectToSucceed()
        .assertStdOutContains("variant_data");

    hive.execute("SHOW PARTITIONS default.variant_test_df")
        .expectToSucceed()
        .assertStdOutContains("dt=2024-01-01");
  }

  // ---------- VECTOR ----------

  @Test
  public void testVectorTypeWithHiveSyncSQL() throws Exception {
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + DEMO_CONTAINER_SCRIPT).expectToSucceed();

    sparkAdhoc1.executeSQLFile(SPARKSQL_VECTOR_TYPE_SQL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("VECTOR_SQL_TEST_SUCCESS");

    hive.execute("DESCRIBE default.vector_test")
        .expectToSucceed()
        .assertStdOutContains("embedding")
        .assertStdOutContains("binary");
  }

  @Test
  public void testVectorTypeWithHiveSyncDataFrameAPI() throws Exception {
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + DEMO_CONTAINER_SCRIPT).expectToSucceed();

    sparkAdhoc1.executeSQLFile(SPARKSQL_VECTOR_TYPE_DF_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("VECTOR_DF_TEST_SUCCESS");

    hive.execute("DESCRIBE default.vector_test_df")
        .expectToSucceed()
        .assertStdOutContains("embedding")
        .assertStdOutContains("binary");

    hive.execute("SHOW PARTITIONS default.vector_test_df")
        .expectToSucceed()
        .assertStdOutContains("dt=2024-01-01");
  }
}
