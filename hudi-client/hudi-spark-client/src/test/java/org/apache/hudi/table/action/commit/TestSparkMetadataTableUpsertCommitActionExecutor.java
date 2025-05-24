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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.Pair;
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
public class TestSparkMetadataTableUpsertCommitActionExecutor extends SparkClientFunctionalTestHarness {

  @Test
  public void testMetadataTableUpsertCommitActionExecutor() throws IOException {

    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ);
    HoodieWriteConfig writeConfig = getConfigBuilder(false).build();
    HoodieTable table = HoodieSparkTable.create(writeConfig, context(), metaClient);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts("0001", 100);
    HoodieData<HoodieRecord> recordHoodieData = context().parallelize(records, 1);

    List<Pair<String, String>> mdtPartitionPathFileGroupIdList = new ArrayList<>();
    mdtPartitionPathFileGroupIdList.add(Pair.of(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), "record-index-00001"));

    HoodieData<WriteStatus> statusHoodieData = mock(HoodieData.class);

    // create requested in timeline
    metaClient.getActiveTimeline().createNewInstant(metaClient.createNewInstant(HoodieInstant.State.REQUESTED, DELTA_COMMIT_ACTION,
        "0001"));

    SparkMetadataTableUpsertCommitActionExecutor commitActionExecutor = new MockSparkMetadataTableUpsertCommitActionExecutor(context(),
        writeConfig, table, "0001", recordHoodieData, mdtPartitionPathFileGroupIdList, statusHoodieData);
    commitActionExecutor.execute(recordHoodieData);
    // since the mdt partitions does not contain FILES partition, inflight instant may not be added.
    assertFalse(metaClient.reloadActiveTimeline().getWriteTimeline().filterInflights().containsInstant("0001"));

    // just add FILES partition. we should expect the inflight instant to be added to timeline.
    mdtPartitionPathFileGroupIdList.clear();
    mdtPartitionPathFileGroupIdList.add(Pair.of(MetadataPartitionType.FILES.getPartitionPath(), "files-00001"));

    commitActionExecutor.execute(recordHoodieData);
    assertTrue(metaClient.reloadActiveTimeline().getWriteTimeline().filterInflights().containsInstant("0001"));
  }

  static class MockSparkMetadataTableUpsertCommitActionExecutor<T> extends SparkMetadataTableUpsertCommitActionExecutor<T> {

    private final HoodieData<WriteStatus> writeStatusHoodieData;

    public MockSparkMetadataTableUpsertCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                            String instantTime, HoodieData<HoodieRecord<T>> preppedRecords,
                                                            List<Pair<String, String>> mdtPartitionPathFileGroupIdList,
                                                            HoodieData<WriteStatus> writeStatusHoodieData) {
      super(context, config, table, instantTime, preppedRecords, mdtPartitionPathFileGroupIdList);
      this.writeStatusHoodieData = writeStatusHoodieData;
    }

    @Override
    protected HoodieData<WriteStatus> mapPartitionsAsRDD(HoodieData<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
      return writeStatusHoodieData;
    }

  }

}


