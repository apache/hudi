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
 * MOR tables that came from Spark + Hive sync. Mirrors the retired
 * {@code docker/demo/trino-batch1.commands} demo flow (removed together with the
 * rest of the legacy trino-coordinator path) but uses a self-contained spark-sql
 * fixture (see {@code sparksql-stock-ticks-trino.commands}) instead of the full
 * Kafka/streaming pipeline, which integ2 doesn't otherwise exercise.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITTestTrinoStockTicks extends ITTestBaseTestcontainers {

  private static final String STOCK_TICKS_COW_PATH = "/user/hive/warehouse/stock_ticks_cow";
  private static final String STOCK_TICKS_MOR_PATH = "/user/hive/warehouse/stock_ticks_mor";
  private static final String SPARKSQL_STOCK_TICKS_COMMANDS =
      Paths.DEMO_DIR + "/sparksql-stock-ticks-trino.commands";

  @BeforeAll
  public void setupOnce() throws Exception {
    assumeTrinoProfile();
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
    // JUnit runs @AfterAll even when the @BeforeAll assumption aborted setupOnce()
    // before initializeServices(); nothing was seeded then, so nothing to clean.
    if (sparkAdhoc1 == null) {
      return;
    }
    sparkAdhoc1.executeShellCommand("hdfs dfs -rm -R -f "
        + STOCK_TICKS_COW_PATH + " " + STOCK_TICKS_MOR_PATH).expectToSucceed();
  }

  // ---------- Queries reproduced from the retired docker/demo/trino-batch1.commands ----------

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
    // Hive sync produces stock_ticks_mor_ro (RO view of base files). The fixture's
    // UPDATE (ts 10:59:00) lives only in a log file, so _ro must keep serving the
    // 10:29:00 base row - a 10:59:00 here means log records leaked into the RO view.
    trino.execute("SELECT symbol, max(ts) FROM stock_ticks_mor_ro GROUP BY symbol HAVING symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG")
        .assertStdOutContains("2018-08-31 10:29:00")
        .assertStdOutContains("2018-08-31 10:59:00", 0);
  }

  @Test
  public void testTrinoReadsCowProjectedColumns() throws Exception {
    // open == close == 1230.50 in the seed row, so "1230.5" appears twice in CSV output.
    trino.execute("SELECT symbol, ts, volume, open, close FROM stock_ticks_cow WHERE symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG,2018-08-31 10:29:00,6330,1230.5,1230.5");
  }

  @Test
  public void testTrinoReadsMorRoProjectedColumns() throws Exception {
    // Same symbol-count pin as the _rt case below: the base row assert on its own
    // would still pass with the log row returned next to it.
    trino.execute("SELECT symbol, ts, volume, open, close FROM stock_ticks_mor_ro WHERE symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG,2018-08-31 10:29:00,6330,1230.5,1230.5")
        .assertStdOutContains("GOOG", 1);
  }

  @Test
  public void testTrinoReadsMorRtMergedMaxTs() throws Exception {
    // The fixture's UPDATE lands as a log-only delta; the _rt view must merge it
    // on read. Pairs with testTrinoReadsMorRoMaxTs pinning _ro to the base row,
    // so together they prove the connector takes different read paths for the
    // two views instead of serving base files for both.
    trino.execute("SELECT symbol, max(ts) FROM stock_ticks_mor_rt GROUP BY symbol HAVING symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG")
        .assertStdOutContains("2018-08-31 10:59:00");
  }

  @Test
  public void testTrinoReadsMorRtMergedProjectedColumns() throws Exception {
    // Full merged row: every non-key column must come from the log record. The row
    // assert alone would still pass if the base row came back alongside it, so pin
    // the symbol count too - exactly one GOOG row may survive the merge.
    trino.execute("SELECT symbol, ts, volume, open, close FROM stock_ticks_mor_rt WHERE symbol = 'GOOG'")
        .expectToSucceed()
        .assertStdOutContains("GOOG,2018-08-31 10:59:00,9021,1227.25,1227.5")
        .assertStdOutContains("GOOG", 1);
  }
}
