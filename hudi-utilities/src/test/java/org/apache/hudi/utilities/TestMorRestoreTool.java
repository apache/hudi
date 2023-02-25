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

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMorRestoreTool extends SparkClientFunctionalTestHarness implements SparkProvider {

  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(0L);
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void init() throws IOException {
    this.metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ);
  }

  @AfterAll
  public static void cleanup() {
    DATA_GENERATOR.close();
  }

  @Test
  public void testSimpleRestore() throws Exception {
    HoodieFileFormat fileFormat = HoodieFileFormat.PARQUET;

    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build());
    HoodieWriteConfig cfg = cfgBuilder.build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = DATA_GENERATOR.generateInserts(newCommitTime, 200);
      upsertToMorTable(client, records, newCommitTime);

      /*
       * Write 2 (updates)
       */
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = DATA_GENERATOR.generateUpdates(newCommitTime, 100);
      upsertToMorTable(client, records, newCommitTime);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = DATA_GENERATOR.generateUpdates(newCommitTime, 50);
      upsertToMorTable(client, records, newCommitTime);
      String toRestoreCommit = newCommitTime;

      // collect file slices to validate later
      HoodieTable table = HoodieSparkTable.create(cfg, context(), metaClient);
      table.getHoodieView().sync();
      Map<String, List<HoodieLogFile>> beforeRestoreFileSlice = new HashMap<>();
      for (String partitionPath : DATA_GENERATOR.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        beforeRestoreFileSlice.put(partitionPath, new ArrayList<>());
        for (FileSlice fileSlice : groupedLogFiles) {
          beforeRestoreFileSlice.get(partitionPath).addAll(fileSlice.getLogFiles().collect(Collectors.toList()));
        }
      }

      String firstCompactionInstant = client.scheduleCompaction(Option.empty()).get().toString();
      client.compact(firstCompactionInstant);

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
      assertTrue(HoodieTimeline.compareTimestamps("000", HoodieTimeline.LESSER_THAN, latestCompactionCommitTime));
      /*
       * Write 1 (only inserts)
       */
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = DATA_GENERATOR.generateUpdates(newCommitTime, 100);
      upsertToMorTable(client, records, newCommitTime);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = DATA_GENERATOR.generateUpdates(newCommitTime, 50);
      upsertToMorTable(client, records, newCommitTime);

      String secondCompactionInstant = client.scheduleCompaction(Option.empty()).get().toString();
      client.compact(secondCompactionInstant);

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(2, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting two commits.");

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = DATA_GENERATOR.generateUpdates(newCommitTime, 100);
      upsertToMorTable(client, records, newCommitTime);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = DATA_GENERATOR.generateUpdates(newCommitTime, 50);
      upsertToMorTable(client, records, newCommitTime);

      // trigger restore
      MORRestoreTool.Config restoreToolConfig = new MORRestoreTool.Config();
      restoreToolConfig.basePath = cfg.getBasePath();
      restoreToolConfig.commitTime = toRestoreCommit;
      restoreToolConfig.execute = true;
      MORRestoreTool restoreTool = new MORRestoreTool(jsc(), restoreToolConfig);
      restoreTool.run();

      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieSparkTable.create(cfg, context(), metaClient);
      table.getHoodieView().sync();
      Map<String, List<HoodieLogFile>> afterRestoreFileSlice = new HashMap<>();
      for (String partitionPath : DATA_GENERATOR.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        afterRestoreFileSlice.put(partitionPath, new ArrayList<>());
        for (FileSlice fileSlice : groupedLogFiles) {
          afterRestoreFileSlice.get(partitionPath).addAll(fileSlice.getLogFiles().collect(Collectors.toList()));
        }
        // validate
        List<HoodieLogFile> beforeLogFiles = beforeRestoreFileSlice.get(partitionPath);
        List<HoodieLogFile> afterLogFiles = afterRestoreFileSlice.get(partitionPath);
        for (HoodieLogFile beforeLogFile : beforeLogFiles) {
          assertTrue(afterLogFiles.contains(beforeLogFile));
        }
        for (HoodieLogFile afterLogFile : afterLogFiles) {
          assertTrue(beforeLogFiles.contains(afterLogFile));
        }
      }
    }
  }

  private void upsertToMorTable(SparkRDDWriteClient client, List<HoodieRecord> records, String commitTime) {
    JavaRDD<WriteStatus> statusesRdd = client.upsert(jsc().parallelize(records, 1), commitTime);
    List<WriteStatus> statuses = statusesRdd.collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
  }
}
