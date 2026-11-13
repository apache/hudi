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

package org.apache.hudi.table.action.deltacommit;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.ListBasedHoodieBloomIndexHelper;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code upsertPreppedRecords} on a merge-on-read table of the Java client.
 */
public class TestJavaUpsertPreppedDeltaCommitActionExecutor extends HoodieJavaClientTestHarness {

  private static final String[] PARTITIONS = {"2020/01/01", "2020/01/02"};

  private HoodieTestDataGenerator generator;

  @BeforeEach
  void setUpMergeOnReadTable() throws IOException {
    initMetaClient(HoodieTableType.MERGE_ON_READ);
    generator = new HoodieTestDataGenerator(PARTITIONS);
    CountingIndex.CALLS.set(0);
  }

  private HoodieWriteConfig config() {
    return getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexClass(CountingIndex.class.getName()).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).build())
        .build();
  }

  private List<HoodieRecord> insertAndTagUpdates(HoodieJavaWriteClient client, HoodieWriteConfig config) throws IOException {
    String first = client.startCommit();
    List<HoodieRecord> inserts = generator.generateInserts(first, 20);
    assertTrue(client.commit(first, client.insert(inserts, first)));
    HoodieTable table = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
    List<HoodieRecord> updates = tagLocation(table.getIndex(), context, generator.generateUpdates(first, inserts), table);
    assertTrue(updates.stream().allMatch(HoodieRecord::isCurrentLocationKnown));
    return updates;
  }

  private void assertCompletedDeltaCommit(String instant, int expectedRecords, HoodieWriteConfig config) {
    HoodieTimeline completed = HoodieTableMetaClient.reload(metaClient).getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants();
    assertTrue(completed.containsInstant(instant), "The prepped upsert is a completed delta commit");
    assertEquals(expectedRecords, GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, generator).size());
  }

  @Test
  void testUpdatesOnlyBatchCommits() throws IOException {
    HoodieWriteConfig config = config();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, config)) {
      List<HoodieRecord> updates = insertAndTagUpdates(client, config);
      String instant = client.startCommit();
      List<WriteStatus> statuses = client.upsertPreppedRecords(updates, instant);
      assertTrue(client.commit(instant, statuses));
      assertCompletedDeltaCommit(instant, 20, config);
    }
  }

  @Test
  void testMixedBatchUpdatesTheIndexOnce() throws IOException {
    HoodieWriteConfig config = config();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, config)) {
      List<HoodieRecord> records = new ArrayList<>(insertAndTagUpdates(client, config));
      String instant = client.startCommit();
      records.addAll(generator.generateInserts(instant, 10));
      CountingIndex.CALLS.set(0);
      List<WriteStatus> statuses = client.upsertPreppedRecords(records, instant);
      assertEquals(1, CountingIndex.CALLS.get(), "The index is updated once for the write");
      assertTrue(client.commit(instant, statuses));
      assertCompletedDeltaCommit(instant, 30, config);
    }
  }

  /**
   * A bloom index that counts its location updates.
   */
  public static class CountingIndex extends HoodieBloomIndex {
    static final AtomicInteger CALLS = new AtomicInteger();

    public CountingIndex(HoodieWriteConfig config) {
      super(config, ListBasedHoodieBloomIndexHelper.getInstance());
    }

    @Override
    public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context, HoodieTable hoodieTable) {
      CALLS.incrementAndGet();
      return HoodieListData.eager(writeStatuses.collectAsList());
    }
  }
}
