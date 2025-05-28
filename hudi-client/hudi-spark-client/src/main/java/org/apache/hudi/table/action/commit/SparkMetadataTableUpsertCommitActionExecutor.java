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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.deltacommit.SparkUpsertPreppedDeltaCommitActionExecutor;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Upsert commit action executor for Metadata table.
 *
 * @param <T>
 */
public class SparkMetadataTableUpsertCommitActionExecutor<T> extends SparkUpsertPreppedDeltaCommitActionExecutor<T> {

  private static final WorkloadStat PLACEHOLDER_GLOBAL_STAT = new WorkloadStat();
  private final List<HoodieFileGroupId> mdtFileGroupIdList;
  private final boolean initialCall;

  public SparkMetadataTableUpsertCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table, String instantTime,
                                                      HoodieData<HoodieRecord<T>> preppedRecords, List<HoodieFileGroupId> mdtFileGroupIdList,
                                                      boolean initialCall) {
    super(context, config, table, instantTime, preppedRecords);
    this.mdtFileGroupIdList = mdtFileGroupIdList;
    this.initialCall = initialCall;
  }

  @Override
  protected boolean shouldPersistInputRecords(JavaRDD<HoodieRecord<T>> inputRDD) {
    return inputRDD.getStorageLevel() == StorageLevel.NONE();
  }

  @Override
  protected WorkloadProfile prepareWorkloadProfile(HoodieData<HoodieRecord<T>> inputRecordsWithClusteringUpdate) {
    // create workload profile only when we are writing to FILES partition in Metadata table.
    WorkloadProfile workloadProfile = new WorkloadProfile(Pair.of(Collections.emptyMap(), PLACEHOLDER_GLOBAL_STAT));
    return workloadProfile;
  }

  protected void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, String instantTime)
      throws HoodieCommitException {
    // with streaming writes support, we might write to metadata table multiple times for the same instant times.
    // ie. writeClient.startCommit(t1), writeClient.upsert(batch1, t1), writeClient.upsert(batch2, t1), writeClient.commit(t1, ...)
    // So, here we are generating inflight file only in the last known writes, which we know will only have FILES partition.
    if (initialCall) {
      super.saveWorkloadProfileMetadataToInflight(profile, instantTime);
    }
  }

  @Override
  protected Partitioner getPartitioner(WorkloadProfile profile) {
    List<BucketInfo> bucketInfoList = new ArrayList<>();
    Map<String, Integer> fileIdToSparkPartitionIndexMap = new HashMap<>();
    int counter = 0;
    while (counter < mdtFileGroupIdList.size()) {
      HoodieFileGroupId fileGroupId = mdtFileGroupIdList.get(counter);
      fileIdToSparkPartitionIndexMap.put(fileGroupId.getFileId(), counter);
      bucketInfoList.add(new BucketInfo(BucketType.UPDATE, fileGroupId.getFileId(), fileGroupId.getPartitionPath()));
      counter++;
    }
    return new SparkMetadataTableUpsertPartitioner<>(bucketInfoList, fileIdToSparkPartitionIndexMap);
  }

  @Override
  protected HoodieData<HoodieRecord<T>> clusteringHandleUpdate(HoodieData<HoodieRecord<T>> inputRecords) {
    return inputRecords;
  }

}
