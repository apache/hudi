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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Smoke coverage for the native trino-hudi connector running inside the integ2
 * testcontainers harness. Cheapest signal that the plugin loaded, the metastore
 * is reachable, and the CLI can round-trip a query.
 *
 * <p>Skipped when {@code hudi-trino-plugin/target/trino-hudi-472} is absent (the
 * plugin module is outside the parent reactor).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITTestTrinoSmoke extends ITTestBaseTestcontainers {

  @BeforeAll
  public void setupOnce() throws Exception {
    initializeServices();
    trino.waitUntilReady();
  }

  @Test
  public void testShowCatalogsListsHudi() throws Exception {
    // Asserts the native plugin registered. With connector.name=hudi (per
    // HudiConnectorFactory#getName) the catalog appears under that name; if the
    // plugin failed to load the catalog file would have made Trino fail to start
    // and we will never reach this assertion.
    trino.execute("system", "runtime", "SHOW CATALOGS")
        .expectToSucceed()
        .assertStdOutContains("hudi");
  }

  @Test
  public void testShowSchemasFromHudiReachesMetastore() throws Exception {
    // The `default` schema is created by Hive at first contact with the metastore.
    // Asserting it appears here proves the connector can talk to thrift://hivemetastore:9083.
    trino.execute("hudi", "default", "SHOW SCHEMAS")
        .expectToSucceed()
        .assertStdOutContains("default");
  }

  @Test
  public void testSelectOneRoundtrip() throws Exception {
    trino.execute("system", "runtime", "SELECT 1")
        .expectToSucceed()
        .assertStdOutContains("1");
  }
}
