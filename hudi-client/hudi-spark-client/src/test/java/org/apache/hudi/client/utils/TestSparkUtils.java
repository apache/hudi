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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Date;
import java.util.List;

public class TestSparkUtils extends HoodieClientTestBase {
  private HoodieTestTable testTable;

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieSparkWriteableTestTable.of(metaClient);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testEmptyCommitCreation(boolean autoCommit) throws Exception {
    HoodieWriteConfig writeConfig = getConfigBuilder().withAutoCommit(autoCommit).build();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);

    long goBackMs = Duration.ofHours(25).getSeconds() * 1000;
    String firstCommit = HoodieInstantTimeGenerator.formatDate(new Date(System.currentTimeMillis() - goBackMs));

    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(false, writeConfig, dataGen::generateInserts);
    writeBatch(writeClient, firstCommit, "000", Option.empty(), "000", 10,
        recordGenFunction, SparkRDDWriteClient::bulkInsert, true, 10, 10,
        1, !autoCommit, false);

    metaClient.reloadActiveTimeline();
    if (autoCommit) {
      Assertions.assertThrows(IllegalStateException.class,
          () -> SparkCommitUtils.checkAndCreateEmptyCommit(writeClient, metaClient, Option.empty()));
    } else {
      SparkCommitUtils.checkAndCreateEmptyCommit(writeClient, metaClient, Option.empty());
      metaClient.reloadActiveTimeline();
      Assertions.assertTrue(metaClient.getActiveTimeline().countInstants() == 2);
    }
  }
}
