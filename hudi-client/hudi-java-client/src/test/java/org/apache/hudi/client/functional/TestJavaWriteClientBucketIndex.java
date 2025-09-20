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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.upgrade.JavaUpgradeDowngradeHelper;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJavaWriteClientBucketIndex extends HoodieJavaClientTestHarness {

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieMetadataTestTable.of(metaClient);
  }

  protected EngineType getEngineType() {
    return EngineType.JAVA;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Override
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType) {
    return super.getConfigBuilder(schemaStr, HoodieIndex.IndexType.BUCKET)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum("4")
            .build());
  }

  @Test
  public void testUpsertsPrepped() throws Exception {
    testUpsertsInternal((writeClient, recordRDD, instantTime) -> writeClient.upsertPreppedRecords(recordRDD, instantTime),
        true, true, JavaUpgradeDowngradeHelper.getInstance());
  }

  @Override
  protected Object castInsertFirstBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                        String initCommitTime, int numRecordsInThisCommit,
                                        Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                        boolean assertForCommit, int expRecordsInThisCommit,
                                        boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    return insertFirstBatch(writeConfig, (HoodieJavaWriteClient) client, newCommitTime, initCommitTime, numRecordsInThisCommit,
        (writeClient, records, commitTime) -> (List<WriteStatus>) writeFn.apply(writeClient, records, commitTime),
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, filterForCommitTimeWithAssert, instantGenerator);
  }

  @Override
  protected Object castWriteBatch(BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                  Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                  Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                  Function3<Object, BaseHoodieWriteClient, Object, String> writeFn,
                                  boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                  boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator, boolean skipCommit) throws Exception {
    return writeBatch((HoodieJavaWriteClient) client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        (writeClient, records, commitTime) -> (List<WriteStatus>) writeFn.apply(writeClient, records, commitTime),
        assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, filterForCommitTimeWithAssert, instantGenerator, skipCommit);
  }

  @Override
  protected Object castUpdateBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                   Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                   Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                   boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueUpdates);

    return writeBatch((HoodieJavaWriteClient) client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        (writeClient, records, commitTime) -> (List<WriteStatus>) writeFn.apply(writeClient, records, commitTime), assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, filterForCommitTimeWithAssert, instantGenerator);
  }

  @Override
  protected Object castDeleteBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                   String prevCommitTime, String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                   boolean filterForCommitTimeWithAssert, TimelineFactory timelineFactory, InstantGenerator instantGenerator) throws Exception {
    return deleteBatch(writeConfig, (HoodieJavaWriteClient) client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit,
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, expTotalRecords, filterForCommitTimeWithAssert, timelineFactory, instantGenerator);
  }

  @Override
  protected String[] assertTheEntireDatasetHasAllRecordsStill(int expectedRecords) {
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(expectedRecords, countRowsInPaths(basePath, storage, fullPartitionPaths), "Must contain " + expectedRecords + " records");
    return fullPartitionPaths;
  }
}
