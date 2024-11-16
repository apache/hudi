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
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.BaseFileUtils;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBaseFileUtils extends HoodieClientTestBase {

  @Test
  public void testGeneratingRLIRecordsFromBaseFile() throws IOException {

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).build();

    // metadata enabled with only FILES partition
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
      Map<String, String> recordKeyToFileIdMapping1 = new HashMap<>();
      Map<String, Set<String>> partitionToFileIdMapping = new HashMap<>();
      Map<String, String> fileIdToFileNameMapping1 = new HashMap<>();
      writeStatuses1.forEach(writeStatus -> {
        assertEquals(writeStatus.getStat().getNumDeletes(), 0);
        // Fetch record keys for all
        try {
          String writeStatFileId = writeStatus.getFileId();
          if (!fileIdToFileNameMapping1.containsKey(writeStatFileId)) {
            fileIdToFileNameMapping1.put(writeStatFileId, writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf("/") + 1));
          }

          Iterator<HoodieRecord> rliRecordsItr = BaseFileUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(),
              writeStatus.getStat(), writeConfig.getWritesFileIdEncoding(), finalCommitTime, metaClient.getStorage());
          while (rliRecordsItr.hasNext()) {
            HoodieRecord rliRecord = rliRecordsItr.next();
            String key = rliRecord.getRecordKey();
            String partition = ((HoodieMetadataPayload) rliRecord.getData()).getRecordGlobalLocation().getPartitionPath();
            String fileId = ((HoodieMetadataPayload) rliRecord.getData()).getRecordGlobalLocation().getFileId();
            recordKeyToPartitionMapping1.put(key, partition);
            recordKeyToFileIdMapping1.put(key, fileId);
            partitionToFileIdMapping.computeIfAbsent(partition, new Function<String, Set<String>>() {
              @Override
              public Set<String> apply(String s) {
                Set<String> fileIds = new HashSet<>();
                fileIds.add(s);
                return fileIds;
              }
            }).add(fileId);
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

      List<String> expectedRLIInserts = inserts2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      List<String> expectedRLIDeletes = deletes2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      List<String> actualRLIInserts = new ArrayList<>();
      List<String> actualRLIDeletes = new ArrayList<>();

      writeStatuses2.forEach(writeStatus -> {
        assertTrue(writeStatus.getStat().getNumDeletes() != 0);
        // Fetch record keys for all
        try {
          String writeStatFileId = writeStatus.getFileId();
          assertEquals(writeStatus.getStat().getPrevBaseFile(), fileIdToFileNameMapping1.get(writeStatFileId));

          Iterator<HoodieRecord> rliRecordsItr = BaseFileUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(), writeStatus.getStat(),
              writeConfig.getWritesFileIdEncoding(), finalCommitTime2, metaClient.getStorage());
          while (rliRecordsItr.hasNext()) {
            HoodieRecord rliRecord = rliRecordsItr.next();
            String key = rliRecord.getRecordKey();
            if (rliRecord.getData() instanceof EmptyHoodieRecordPayload) {
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
}
