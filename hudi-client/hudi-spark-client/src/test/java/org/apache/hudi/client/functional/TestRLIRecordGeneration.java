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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.BaseFileUtils;
import org.apache.hudi.metadata.RecordKeyInfo;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRLIRecordGeneration extends HoodieClientTestBase {

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testGeneratingRLIRecordsFromBaseFile(HoodieTableType tableType) throws IOException {
    cleanupClients();
    initMetaClient(tableType);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = tableType == HoodieTableType.COPY_ON_WRITE ? getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).build()
        : getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2)
            .withInlineCompaction(true)
            .compactionSmallFileSize(0).build()).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // Insert
      String commitTime = client.createNewInstantTime();
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, 100);
      client.startCommitWithTime(commitTime);
      List<WriteStatus> writeStatuses1 = client.insert(jsc.parallelize(records1, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses1);

      // assert RLI records for a base file from 1st commit
      String finalCommitTime = commitTime;
      Map<String, String> recordKeyToPartitionMapping1 = new HashMap<>();
      Map<String, String> fileIdToFileNameMapping1 = new HashMap<>();
      writeStatuses1.forEach(writeStatus -> {
        assertEquals(writeStatus.getStat().getNumDeletes(), 0);
        // Fetch record keys for all
        try {
          String writeStatFileId = writeStatus.getFileId();
          if (!fileIdToFileNameMapping1.containsKey(writeStatFileId)) {
            fileIdToFileNameMapping1.put(writeStatFileId, writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf("/") + 1));
          }

          Iterator<RecordKeyInfo> rliRecordsItr = BaseFileUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(),
              writeStatus.getStat(), metaClient.getStorage()).getRecordKeyInfoList().iterator();
          while (rliRecordsItr.hasNext()) {
            RecordKeyInfo rliRecord = rliRecordsItr.next();
            String key = rliRecord.getRecordKey();
            String partition = rliRecord.getPartition();
            recordKeyToPartitionMapping1.put(key, partition);
          }
        } catch (IOException e) {
          throw new HoodieException("Should not have failed ", e);
        }
      });

      Map<String, String> expectedRecordToPartitionMapping1 = new HashMap<>();
      records1.forEach(record -> expectedRecordToPartitionMapping1.put(record.getRecordKey(), record.getPartitionPath()));

      assertEquals(expectedRecordToPartitionMapping1, recordKeyToPartitionMapping1);

      // lets update some records and assert RLI records.
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      String finalCommitTime2 = commitTime;
      List<HoodieRecord> deletes2 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(commitTime, 30);
      List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime, 30);
      List<HoodieRecord> records2 = new ArrayList<>();
      records2.addAll(inserts2);
      records2.addAll(updates2);
      records2.addAll(deletes2);

      List<WriteStatus> writeStatuses2 = client.upsert(jsc.parallelize(records2, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses2);

      if (tableType == HoodieTableType.COPY_ON_WRITE) {
        List<String> expectedRLIInserts = inserts2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
        List<String> expectedRLIDeletes = deletes2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
        generateRliRecordsAndAssert(writeStatuses2, fileIdToFileNameMapping1, finalCommitTime2, writeConfig, expectedRLIInserts, expectedRLIDeletes);
      } else {
        // trigger 2nd commit followed by compaction.
        commitTime = client.createNewInstantTime();
        client.startCommitWithTime(commitTime);
        String finalCommitTime3 = commitTime;
        List<HoodieRecord> deletes3 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
        List<HoodieRecord> updates3 = dataGen.generateUniqueUpdates(commitTime, 30);
        List<HoodieRecord> inserts3 = dataGen.generateInserts(commitTime, 30);
        List<HoodieRecord> records3 = new ArrayList<>();
        records3.addAll(inserts3);
        records3.addAll(updates3);
        records3.addAll(deletes3);

        List<WriteStatus> writeStatuses3 = client.upsert(jsc.parallelize(records3, 1), commitTime).collect();
        assertNoWriteErrors(writeStatuses3);

        // trigger compaction
        Option<String> compactionInstantOpt = client.scheduleCompaction(Option.empty());
        assertTrue(compactionInstantOpt.isPresent());
        List<HoodieWriteStat> compactionWriteStats = (List<HoodieWriteStat>) client.compact(compactionInstantOpt.get()).getWriteStats().get();

        List<String> expectedRLIDeletes = deletes3.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
        // since deletes are lazily realized when compaction kicks in, we need to account for deletes from 2nd commit as well.
        expectedRLIDeletes.addAll(deletes2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList()));
        List<String> actualRLIDeletes = new ArrayList<>();

        compactionWriteStats.forEach(writeStat -> {
          try {
            for (RecordKeyInfo rliRecord : BaseFileUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(), writeStat,
                metaClient.getStorage()).getRecordKeyInfoList()) {
              String key = rliRecord.getRecordKey();
              if (rliRecord.getRecordStatus() == RecordKeyInfo.RecordStatus.DELETE) {
                actualRLIDeletes.add(key);
              }
            }
          } catch (IOException e) {
            throw new HoodieException("Should not have failed ", e);
          }
        });

        // it may not be easy to assert inserts to RLI. bcoz, if there are no deletes, both inserts and updates to data table result in RLI records.
        // but if there are deleted, we only ingest inserts and deletes from data table to RLI partition.
        Collections.sort(expectedRLIDeletes);
        Collections.sort(actualRLIDeletes);
        assertEquals(expectedRLIDeletes, actualRLIDeletes);
      }
    }
  }

  private void generateRliRecordsAndAssert(List<WriteStatus> writeStatuses, Map<String, String> fileIdToFileNameMapping, String commitTime,
                                           HoodieWriteConfig writeConfig, List<String> expectedRLIInserts, List<String> expectedRLIDeletes) {
    List<String> actualRLIDeletes = new ArrayList<>();
    List<String> actualRLIInserts = new ArrayList<>();
    writeStatuses.forEach(writeStatus -> {
      assertTrue(writeStatus.getStat().getNumDeletes() != 0);
      // Fetch record keys for all
      try {
        String writeStatFileId = writeStatus.getFileId();
        assertEquals(writeStatus.getStat().getPrevBaseFile(), fileIdToFileNameMapping.get(writeStatFileId));

        Iterator<RecordKeyInfo> rliRecordsItr =
            BaseFileUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(), writeStatus.getStat(), metaClient.getStorage())
                .getRecordKeyInfoList().iterator();
        while (rliRecordsItr.hasNext()) {
          RecordKeyInfo rliRecord = rliRecordsItr.next();
          String key = rliRecord.getRecordKey();
          if (rliRecord.getRecordStatus() == RecordKeyInfo.RecordStatus.DELETE) {
            actualRLIDeletes.add(key);
          } else {
            actualRLIInserts.add(key);
          }
        }
      } catch (IOException e) {
        throw new HoodieException("Should not have failed ", e);
      }
    });

    Collections.sort(expectedRLIInserts);
    Collections.sort(actualRLIInserts);
    Collections.sort(expectedRLIDeletes);
    Collections.sort(actualRLIDeletes);
    assertEquals(expectedRLIInserts, actualRLIInserts);
    assertEquals(expectedRLIDeletes, actualRLIDeletes);
  }
}
