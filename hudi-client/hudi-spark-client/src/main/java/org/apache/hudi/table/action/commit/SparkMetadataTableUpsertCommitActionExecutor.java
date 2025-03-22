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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.deltacommit.SparkUpsertPreppedDeltaCommitActionExecutor;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

/**
 * Upsert commit action executor for Metadata table.
 *
 * @param <T>
 */
public class SparkMetadataTableUpsertCommitActionExecutor<T> extends SparkUpsertPreppedDeltaCommitActionExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMetadataTableUpsertCommitActionExecutor.class);

  private static final HashMap<String, WorkloadStat> EMPTY_MAP = new HashMap<>();
  private static final WorkloadStat PLACEHOLDER_GLOBAL_STAT = new WorkloadStat();

  private final List<Pair<String, String>> mdtPartitionPathFileGroupIdList;

  public SparkMetadataTableUpsertCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table, String instantTime,
                                                      HoodieData<HoodieRecord<T>> preppedRecords, List<Pair<String, String>> mdtPartitionPathFileGroupIdList) {
    super(context, config, table, instantTime, preppedRecords);
    this.mdtPartitionPathFileGroupIdList = mdtPartitionPathFileGroupIdList;
  }

  @Override
  protected boolean shouldPersistInputRecords(JavaRDD<HoodieRecord<T>> inputRDD) {
    return inputRDD.getStorageLevel() == StorageLevel.NONE();
  }

  @Override
  protected WorkloadProfile prepareWorkloadProfileAndSaveToInflight(HoodieData<HoodieRecord<T>> inputRecordsWithClusteringUpdate) {
    // create workload profile only when we are writing to FILES partition in Metadata table.
    WorkloadProfile workloadProfile = new WorkloadProfile(Pair.of(EMPTY_MAP, PLACEHOLDER_GLOBAL_STAT));
    if (mdtPartitionPathFileGroupIdList.size() == 1 && mdtPartitionPathFileGroupIdList.get(0).getKey().equals(FILES.getPartitionPath())) {
      saveWorkloadProfileMetadataToInflight(workloadProfile, instantTime);
    }
    return workloadProfile;
  }

  @Override
  protected Partitioner getPartitioner(WorkloadProfile profile) {
    List<BucketInfo> bucketInfoList = new ArrayList<>();
    Map<String, Integer> fileIdToSparkPartitionIndexMap = new HashMap<>();
    int counter = 0;
    while (counter < mdtPartitionPathFileGroupIdList.size()) {
      Pair<String, String> partitionPathFileIdPair = mdtPartitionPathFileGroupIdList.get(counter);
      fileIdToSparkPartitionIndexMap.put(partitionPathFileIdPair.getValue(), counter);
      bucketInfoList.add(new BucketInfo(BucketType.UPDATE, partitionPathFileIdPair.getValue(), partitionPathFileIdPair.getKey()));
      counter++;
    }
    return new SparkMetadataTableUpsertPartitioner(bucketInfoList, fileIdToSparkPartitionIndexMap);
  }

  @Override
  protected HoodieData<HoodieRecord<T>> clusteringHandleUpdate(HoodieData<HoodieRecord<T>> inputRecords) {
    return inputRecords;
  }

}
