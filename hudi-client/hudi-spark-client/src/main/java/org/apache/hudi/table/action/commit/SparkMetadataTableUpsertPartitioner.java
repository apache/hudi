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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;

import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Upsert Partitioner to be used for metadata table in spark. All records are prepped (location known) already wrt metadata table. So, we could optimize the upsert partitioner by avoiding certain
 * unnecessary computations.
 * @param <T>
 */
public class SparkMetadataTableUpsertPartitioner<T> extends SparkHoodiePartitioner<T> {

  private final List<BucketInfo> bucketInfoList;
  private final int totalPartitions;
  private final Map<String, Integer> fileIdToSparkPartitionIndexMap;

  public SparkMetadataTableUpsertPartitioner(List<BucketInfo> bucketInfoList, Map<String, Integer> fileIdToSparkPartitionIndexMap) {
    super(null, null); // passing null since these are never used from {@link SparkHoodiePartitioner}.
    this.bucketInfoList = bucketInfoList;
    this.totalPartitions = bucketInfoList.size();
    this.fileIdToSparkPartitionIndexMap = fileIdToSparkPartitionIndexMap;
  }

  @Override
  public int numPartitions() {
    return totalPartitions;
  }

  @Override
  public int getPartition(Object key) {
    // all records to metadata table are prepped. So, we just fetch the fileId from the incoming key and lookup in the map we constructed
    // to find the index.
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    HoodieRecordLocation location = keyLocation._2().get();
    return fileIdToSparkPartitionIndexMap.get(location.getFileId());
  }

  @Override
  public BucketInfo getBucketInfo(int bucketNumber) {
    return bucketInfoList.get(bucketNumber);
  }
}
