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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.tableformat.TestTableFormat;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the pluggable table format receives lifecycle callbacks for merge-on-read tables,
 * in particular the compaction completion, which a table format must observe to reflect the
 * rewritten base files.
 */
public class TestHoodieJavaClientMergeOnReadForTestFormat extends HoodieJavaClientTestHarness {

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieTestTable.of(metaClient);
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Override
  protected void initMetaClient() throws IOException {
    if (basePath == null) {
      initPath();
    }
    storageConf.set(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    storageConf.set(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    storageConf.set(HoodieMetadataConfig.ENABLE.key(), "false");
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, properties);
  }

  @AfterAll
  public static void tearDownAll() throws IOException {
    TestTableFormat.tearDown();
    FileSystem.closeAll();
  }

  @Test
  public void testTableFormatReceivesCompactionCommit() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    String prevCommit = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, commitTime, prevCommit, Option.of(Arrays.asList(prevCommit)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    // The deltacommits must have been reported to the table format.
    List<HoodieInstant> recorded = TestTableFormat.getRecordedInstants(metaClient.getBasePath().toString());
    assertEquals(2, recorded.stream()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)).count(),
        "both deltacommits must be reported to the table format");

    // Schedule, execute and commit compaction; the completion must be reported as well.
    Option<String> compactionTime = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTime.isPresent(), "expected a compaction plan");
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime.get());
    client.commitCompaction(compactionTime.get(), writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants()
        .containsInstant(compactionTime.get()));

    List<HoodieInstant> compactionInstants =
        TestTableFormat.getRecordedInstants(metaClient.getBasePath().toString()).stream()
            .filter(instant -> instant.requestedTime().equals(compactionTime.get()))
            .collect(Collectors.toList());
    assertEquals(1, compactionInstants.size(),
        "the compaction completion must be reported to the table format exactly once");
    assertEquals(HoodieTimeline.COMMIT_ACTION, compactionInstants.get(0).getAction());
    assertTrue(compactionInstants.get(0).isCompleted());
    assertNotNull(compactionInstants.get(0).getCompletionTime(),
        "the completed instant handed to the table format must carry its completion time");
  }

  @Test
  public void testTableFormatObservesRollbackOnMergeOnRead() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(10).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);
    assertTrue(TestTableFormat.getRecordedInstants(metaClient.getBasePath().toString()).stream()
        .anyMatch(instant -> instant.requestedTime().equals(updateTime)));

    assertTrue(client.rollback(updateTime), "rollback of the deltacommit must succeed");

    // The table format must have been told to revert the instant before its files were removed.
    List<HoodieInstant> recorded =
        TestTableFormat.getRecordedInstants(metaClient.getBasePath().toString());
    assertTrue(recorded.stream().noneMatch(instant -> instant.requestedTime().equals(updateTime)),
        "the rolled back deltacommit must no longer be recorded in the table format");
  }
}
