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

package org.apache.hudi.commit;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.BucketStrategist;
import org.apache.hudi.index.bucket.BucketStrategistFactory;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;

public class LSMBucketRescaleCommitActionExecutor extends LSMDatasetBulkInsertOverwriteCommitActionExecutor {

  private Map<String, List<String>> partitionAndReplaceFiledIds = null;
  private BucketStrategist bucketStrategist;
  private HoodieStorageStrategy hoodieStorageStrategy;

  public LSMBucketRescaleCommitActionExecutor(HoodieWriteConfig config, SparkRDDWriteClient writeClient, String instantTime) {
    super(config, writeClient, instantTime);
  }

  @Override
  protected void preExecute() {
    super.preExecute();
    this.bucketStrategist = BucketStrategistFactory.getInstant(writeConfig, table.getMetaClient().getFs());
    this.hoodieStorageStrategy = HoodieStorageStrategyFactory.getInstant(table.getMetaClient());
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieData<WriteStatus> writeStatuses) {
    partitionAndReplaceFiledIds = super.getPartitionToReplacedFileIds(writeStatuses);
    return partitionAndReplaceFiledIds;
  }

  @Override
  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    super.afterExecute(result);
    // modify affected partitions related meta file.
    if (partitionAndReplaceFiledIds != null) {
      partitionAndReplaceFiledIds.keySet().forEach(partitionPath -> {
        int bucketNumber = bucketStrategist.computeBucketNumber(partitionPath);
        hoodieStorageStrategy.getAllLocations(partitionPath, true).forEach(path -> {
          HoodiePartitionMetadata meta = new HoodiePartitionMetadata(table.getMetaClient().getFs(), path);
          meta.updatePartitionBucketNumber(instantTime, bucketNumber);
        });
      });
    }
  }
}
