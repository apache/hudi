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

package org.apache.hudi.delta.sync;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestDeltaLakeSyncTool extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void cleanup() {
    metaClient = null;

  }

  @Test
  public void testDeltaSync() {
    basePath = TestDeltaLakeSyncTool.class.getClassLoader().getResource("hudi-testing-data/stock_ticks_cow").getPath();
    metaClient = HoodieTableMetaClient.builder().setConf(metaClient.getFs().getConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    System.out.println(basePath);
    System.out.println(metaClient.getTableConfig().getProps());
    // createTestDataForPartitionedTable(metaClient, 10);
    DeltaLakeSyncTool syncTool = new DeltaLakeSyncTool(metaClient);
    syncTool.syncHoodieTable();
    System.out.println(getClass().getResource("/hudi-testing-data/stock_ticks_cow/").toString());
    SparkSession sparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName(TestDeltaLakeSyncTool.class.getName())
        .getOrCreate();
    /*final Dataset<Row> hudiRows = sparkSession.read().format("hudi").load(metaClient.getBasePath());
    System.out.println(hudiRows.count());*/
    final Dataset<Row> deltaRows = sparkSession.read().format("delta").load(getClass().getResource("/hudi-testing-data/stock_ticks_cow/").toString());
    System.out.println(deltaRows.count());
  }
}
