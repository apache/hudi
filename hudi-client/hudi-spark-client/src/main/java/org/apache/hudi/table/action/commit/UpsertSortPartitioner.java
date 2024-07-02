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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

public class UpsertSortPartitioner<T> extends UpsertPartitioner<T> {
  private static final Logger LOG = LoggerFactory.getLogger(UpsertSortPartitioner.class);

  public UpsertSortPartitioner(WorkloadProfile profile, HoodieEngineContext context, HoodieTable table,
                               HoodieWriteConfig config) {
    super(profile, context, table, config);
  }

  @Override
  protected List<String> getPartitionPath(WorkloadProfile profile) {
    return profile.getPartitionPaths().stream().sorted().collect(Collectors.toList());
  }

  @Override
  public int getPartition(Object key) {
    Tuple2<HoodieKey, Long> keyAndIndex = (Tuple2<HoodieKey, Long>) key;

    // Fetch the targetBuckets for this record's Hudi partition-path
    String partitionPath = keyAndIndex._1().getPartitionPath();
    List<InsertBucketCumulativeWeightPair> targetBuckets = partitionPathToInsertBucketInfos.get(partitionPath);

    // Total number of inserts based on workload profile
    final long totalInserts = Math.max(1, profile.getWorkloadStat(partitionPath).getNumInserts());

    // The index of this among the sorted input records
    Long recordIndex = keyAndIndex._2();

    // Calculate the weight (or position) of this record based on the cumulative weight of buckets
    // assigned to this Hudi partition-path
    Long startingRecordIndexForPartition = partitionPathToStartingRecordIndex.get(partitionPath);
    final double r =  ((recordIndex - startingRecordIndexForPartition + 1.0)) / totalInserts;
    int index = Collections.binarySearch(targetBuckets, new InsertBucketCumulativeWeightPair(new InsertBucket(), r));

    // return first one, by default
    int bucketNumber = targetBuckets.get(0).getKey().bucketNumber;
    if (index >= 0) {
      bucketNumber = targetBuckets.get(index).getKey().bucketNumber;
    } else if ((-1 * index - 1) < targetBuckets.size()) {
      bucketNumber = targetBuckets.get((-1 * index - 1)).getKey().bucketNumber;
    }

    return bucketNumber;
  }
}