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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.BaseConsistentHashingBucketClusteringPlanStrategy;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Consistent hashing bucket index clustering plan for spark engine.
 */
public class SparkConsistentBucketClusteringPlanStrategy<T extends HoodieRecordPayload<T>>
    extends BaseConsistentHashingBucketClusteringPlanStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  public SparkConsistentBucketClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * @VisibleForTesting
   */
  @Override
  protected Triple<List<HoodieClusteringGroup>, Integer, List<FileSlice>> buildMergeClusteringGroup(
      ConsistentBucketIdentifier identifier, List<FileSlice> fileSlices, int mergeSlot) {
    return super.buildMergeClusteringGroup(identifier, fileSlices, mergeSlot);
  }

  /**
   * @VisibleForTesting
   */
  @Override
  protected Triple<List<HoodieClusteringGroup>, Integer, List<FileSlice>> buildSplitClusteringGroups(
      ConsistentBucketIdentifier identifier, List<FileSlice> fileSlices, int splitSlot) {
    return super.buildSplitClusteringGroups(identifier, fileSlices, splitSlot);
  }

}
