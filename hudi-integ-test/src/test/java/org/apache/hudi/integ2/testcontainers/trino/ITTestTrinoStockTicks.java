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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.apache.hudi.integ2.testcontainers.TestcontainersConfig.Paths;

/**
 * End-to-end coverage that the native trino-hudi connector can read both COW and
 * MOR tables that came from Spark + Hive sync. Mirrors the historical
 * {@code docker/demo/trino-batch1.commands} demo flow but uses a self-contained
 * spark-sql fixture (see {@code sparksql-stock-ticks-trino.commands}) instead of
 * the full Kafka/streaming pipeline, which integ2 doesn't otherwise exercise.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITTestTrinoStockTicks extends ITTestBaseTestcontainers {

  private static final String STOCK_TICKS_COW_PATH = "/user/hive/warehouse/stock_ticks_cow";
  private static final String STOCK_TICKS_MOR_PATH = "/user/hive/warehouse/stock_ticks_mor";
  private static final String SPARKSQL_STOCK_TICKS_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-stock-ticks-trino.commands";

  @BeforeAll
  public void setupOnce() throws Exception {
    assumeTrinoPluginBuilt();
    initializeServices();
    waitForHdfs();
    sparkAdhoc1.executeShellCommand("/bin/bash " + Paths.DEMO_SETUP).expectToSucceed();
    sparkAdhoc1.executeSQLFile(SPARKSQL_STOCK_TICKS_COMMANDS)
        .expectToSucceed()
        .assertStdOutContainsLine("STOCK_TICKS_COW_SETUP_SUCCESS")
        .assertStdOutContainsLine("STOCK_TICKS_MOR_SETUP_SUCCESS")
        .assertStdOutContainsLine("STOCK_TICKS_TRINO_SETUP_SUCCESS");
    trino.waitUntilReady();
  }

  @AfterAll
  public void clean() throws Exception {
    sparkAdhoc1.executeShellCommand("hdfs dfs -rm -R -f "
        + STOCK_TICKS_COW_PATH + " " + STOCK_TICKS_MOR_PATH).expectToSucceed();
  }

  // ---------- Queries reproduced from docker/demo/trino-batch1.commands ----------

  @Test
  public void testTrinoReadsCowMaxTs() throws Exception {
    // Original: select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'
    trino.execute("SELECT symbol, max(ts) FROM stock_ticks_cow GROUP BY symbol HAVING symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG")
        .assertStdOutContains("2018-08-31 10:29:00");
  }

  @Test
  public void testTrinoReadsMorRoMaxTs() throws Exception {
    // Hive sync produces stock_ticks_mor_ro (RO view of base files). Trino reads it
    // through the same hudi connector path; this catches MOR-specific regressions.
    trino.execute("SELECT symbol, max(ts) FROM stock_ticks_mor_ro GROUP BY symbol HAVING symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG")
        .assertStdOutContains("2018-08-31 10:29:00");
  }

  @Test
  public void testTrinoReadsCowProjectedColumns() throws Exception {
    trino.execute("SELECT symbol, ts, volume, open, close FROM stock_ticks_cow WHERE symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG")
        .assertStdOutContains("6330")
        .assertStdOutContains("1230.5");
  }

  @Test
  public void testTrinoReadsMorRoProjectedColumns() throws Exception {
    trino.execute("SELECT symbol, ts, volume, open, close FROM stock_ticks_mor_ro WHERE symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG")
        .assertStdOutContains("6330")
        .assertStdOutContains("1230.5");
  }
}
