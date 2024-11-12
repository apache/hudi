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
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.deltacommit.SparkUpsertPreppedDeltaCommitActionExecutor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

public class SparkMetadataTableUpsertCommitActionExecutor<T> extends SparkUpsertPreppedDeltaCommitActionExecutor<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMetadataTableUpsertCommitActionExecutor.class);

  private final boolean saveWorkloadProfileToInflight;
  private final boolean writesToMetadataTable;
  private final List<Pair<String, String>> mdtPartitionPathFileGroupIdList;

  public SparkMetadataTableUpsertCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table, String instantTime,
                                                      HoodieData<HoodieRecord<T>> preppedRecords, boolean saveWorkloadProfileToInflight,
                                                      boolean writesToMetadataTable,
                                                      List<Pair<String, String>> mdtPartitionPathFileGroupIdList) {
    super(context, config, table, instantTime, preppedRecords);
    this.saveWorkloadProfileToInflight = saveWorkloadProfileToInflight;
    this.writesToMetadataTable = writesToMetadataTable;
    this.mdtPartitionPathFileGroupIdList = mdtPartitionPathFileGroupIdList;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    return execute(preppedRecords, Option.empty(), saveWorkloadProfileToInflight, writesToMetadataTable, mdtPartitionPathFileGroupIdList);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(HoodieData<HoodieRecord<T>> inputRecords, Option<HoodieTimer> sourceReadAndIndexTimer,
                                                              boolean saveWorkloadProfileToInflight, boolean writesToMetadata,
                                                              List<Pair<String, String>> mdtPartitionPathFileGroupIdList) {
    if (!writesToMetadata) {
      System.out.println("");
    }
    ValidationUtils.checkState(writesToMetadata, "SparkMetadataTableUpsertCommitActionExecutor can only be used for Metadata table writes");

    // Cache the tagged records, so we don't end up computing both
    JavaRDD<HoodieRecord<T>> inputRDD = HoodieJavaRDD.getJavaRDD(inputRecords);
    if (inputRDD.getStorageLevel() == StorageLevel.NONE()) {
      HoodieJavaRDD.of(inputRDD).persist(config.getTaggedRecordStorageLevel(),
          context, HoodieData.HoodieDataCacheKey.of(config.getBasePath(), instantTime));
    } else {
      LOG.info("RDD PreppedRecords was persisted at: " + inputRDD.getStorageLevel());
    }

    if (mdtPartitionPathFileGroupIdList.size() == 1 && mdtPartitionPathFileGroupIdList.get(0).getKey().equals(FILES.getPartitionPath())) {
      HashMap<String, WorkloadStat> partitionPathStatMap = new HashMap<>();
      WorkloadStat globalStat = new WorkloadStat();
      WorkloadProfile workloadProfile = new WorkloadProfile(Pair.of(partitionPathStatMap, globalStat));
      saveWorkloadProfileMetadataToInflight(workloadProfile, instantTime);
    }
    context.setJobStatus(this.getClass().getSimpleName(), "Doing partition and writing data: " + config.getTableName());
    HoodieData<WriteStatus> writeStatuses = mapPartitionsAsRDD(HoodieJavaRDD.of(inputRDD), getMetadataFileIdPartitioner(mdtPartitionPathFileGroupIdList));
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();
    updateIndexAndCommitIfNeeded(writeStatuses, result);
    return result;
  }

  private SparkHoodiePartitioner getMetadataFileIdPartitioner(List<Pair<String, String>> mdtPartitionPathFileGroupIdList) {
    List<BucketInfo> bucketInfoList = new ArrayList<>();
    Map<String, Integer> fileIdToPartitionIndexMap = new HashMap<>();
    int counter = 0;
    while (counter < mdtPartitionPathFileGroupIdList.size()) {
      Pair<String, String> partitionPathFileIdPair = mdtPartitionPathFileGroupIdList.get(counter);
      fileIdToPartitionIndexMap.put(partitionPathFileIdPair.getValue(), counter);
      bucketInfoList.add(new BucketInfo(BucketType.UPDATE, partitionPathFileIdPair.getValue(), partitionPathFileIdPair.getKey()));
      counter++;
    }
    return new SparkMetadataUpsertPartitioner(bucketInfoList, fileIdToPartitionIndexMap);
  }
}
