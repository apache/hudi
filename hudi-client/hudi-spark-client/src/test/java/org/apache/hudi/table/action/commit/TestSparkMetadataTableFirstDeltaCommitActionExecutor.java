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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.Partitioner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests SparkMetadataTableUpsertCommitActionExecutor.
 */
public class TestSparkMetadataTableFirstDeltaCommitActionExecutor extends SparkClientFunctionalTestHarness {

  @Test
  public void testMetadataTableUpsertCommitActionExecutor() throws IOException {

    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ);
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    HoodieTable table = HoodieSparkTable.create(writeConfig, context(), metaClient);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts("0001", 100);
    HoodieData<HoodieRecord> recordHoodieData = context().parallelize(records, 1);

    List<HoodieFileGroupId> hoodieFileGroupIdList = new ArrayList<>();
    hoodieFileGroupIdList.add(new HoodieFileGroupId(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "record-index-00001"));

    HoodieData<WriteStatus> statusHoodieData = mock(HoodieData.class);

    // create requested in timeline
    metaClient.getActiveTimeline().createNewInstant(metaClient.createNewInstant(HoodieInstant.State.REQUESTED, DELTA_COMMIT_ACTION,
        "0001"));

    BaseSparkCommitActionExecutor commitActionExecutor = new MockSparkMetadataTableFirstDeltaCommitActionExecutor(context(),
        writeConfig, table, "0001", recordHoodieData, hoodieFileGroupIdList, statusHoodieData);
    commitActionExecutor.execute(recordHoodieData);
    // since this is initial call, inflight instant should be added.
    assertTrue(metaClient.reloadActiveTimeline().getWriteTimeline().filterInflights().containsInstant("0001"));

    hoodieFileGroupIdList.clear();
    hoodieFileGroupIdList.add(new HoodieFileGroupId(MetadataPartitionType.FILES.getPartitionPath(), "files-00001"));

    commitActionExecutor = new MockSparkMetadataTableSecondaryDeltaCommitActionExecutor(context(),
        writeConfig, table, "0001", recordHoodieData, statusHoodieData, false);
    commitActionExecutor.execute(recordHoodieData);
    // ensure inflight is still intact and is not complete yet unless we commit
    HoodieActiveTimeline reloadedActiveTimeline = metaClient.reloadActiveTimeline();
    assertTrue(reloadedActiveTimeline.getWriteTimeline().filterInflights().containsInstant("0001"));
    assertFalse(reloadedActiveTimeline.getWriteTimeline().filterCompletedInstants().containsInstant("0001"));
  }

  static class MockSparkMetadataTableFirstDeltaCommitActionExecutor<T> extends SparkMetadataTableFirstDeltaCommitActionExecutor<T> {
    private final HoodieData<WriteStatus> writeStatusHoodieData;
    public MockSparkMetadataTableFirstDeltaCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                                String instantTime, HoodieData<HoodieRecord<T>> preppedRecords,
                                                                List<HoodieFileGroupId> hoodieFileGroupIdList,
                                                                HoodieData<WriteStatus> writeStatusHoodieData) {
      super(context, config, table, instantTime, preppedRecords, hoodieFileGroupIdList);
      this.writeStatusHoodieData = writeStatusHoodieData;
    }

    @Override
    protected HoodieData<WriteStatus> mapPartitionsAsRDD(HoodieData<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
      return writeStatusHoodieData;
    }
  }

  static class MockSparkMetadataTableSecondaryDeltaCommitActionExecutor<T> extends SparkMetadataTableSecondaryDeltaCommitActionExecutor<T> {
    private final HoodieData<WriteStatus> writeStatusHoodieData;

    public MockSparkMetadataTableSecondaryDeltaCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                                    String instantTime, HoodieData<HoodieRecord<T>> preppedRecords,
                                                                    HoodieData<WriteStatus> writeStatusHoodieData,
                                                                    boolean initialCall) {
      super(context, config, table, instantTime, preppedRecords, initialCall);
      this.writeStatusHoodieData = writeStatusHoodieData;
    }

    @Override
    protected HoodieData<WriteStatus> mapPartitionsAsRDD(HoodieData<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
      return writeStatusHoodieData;
    }
  }
}
