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

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Bucket Index with Java Write Client
 */
public class TestHoodieJavaClientBucketIndex extends HoodieJavaClientTestHarness {

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Test
  public void testWriteData() throws Exception {
    HoodieWriteConfig config = getConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum("8")
            .withIndexKeyField("_row_key")
            .build())
        .build();
    HoodieJavaWriteClient writeClient = getHoodieWriteClient(config);

    int totalRecords = 20;
    List<HoodieRecord> records = dataGen.generateInserts("001", totalRecords);

    // Insert totalRecords records
    List<WriteStatus> writeStatuses = writeData(writeClient, records, true);
    long numFileGroups = writeStatuses.stream().map(WriteStatus::getFileId).distinct().count();
    assertTrue(numFileGroups > 0, "Should create file groups");

    // Verify records written
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Map<String, GenericRecord> recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(totalRecords, recordMap.size(), "Should have written " + totalRecords + " records");

    // Upsert the same set of records, the number of records should remain the same (deduplication)
    writeData(writeClient, dataGen.generateUpdates("002", totalRecords), true);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(totalRecords, recordMap.size(), "Should still have " + totalRecords + " records after deduplication");

    // Upsert new set of records, and validate the total number of records
    writeData(writeClient, dataGen.generateInserts("003", totalRecords), true);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(totalRecords * 2, recordMap.size(), "Should have " + (totalRecords * 2) + " total records");
  }

  private List<WriteStatus> writeData(HoodieJavaWriteClient writeClient, List<HoodieRecord> records, boolean doCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String commitTime = writeClient.startCommit(HoodieActiveTimeline.DELTA_COMMIT_ACTION);
    List<WriteStatus> writeStatuses = writeClient.upsert(records, commitTime);
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);

    if (doCommit) {
      boolean success = writeClient.commit(commitTime, writeStatuses, Option.empty());
      assertTrue(success, "Commit should succeed");
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }
}
