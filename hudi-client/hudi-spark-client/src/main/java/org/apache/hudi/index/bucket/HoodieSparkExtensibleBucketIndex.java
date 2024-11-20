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

package org.apache.hudi.index.bucket;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import java.util.stream.Collectors;

// TODO: complete the implementation of HoodieSparkExtensibleBucketIndex for extensible bucket
public class HoodieSparkExtensibleBucketIndex extends HoodieExtensibleBucketIndex {
  public HoodieSparkExtensibleBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
                                                HoodieTable hoodieTable, String instantTime) throws HoodieIndexException {
    HoodieInstant instant = hoodieTable.getMetaClient().getActiveTimeline().findInstantsAfterOrEquals(instantTime, 1).firstInstant().get();
    ValidationUtils.checkState(instant.getTimestamp().equals(instantTime), "Cannot get the same instant, instantTime: " + instantTime);
    if (!ClusteringUtils.isClusteringOrReplaceCommitAction(instant.getAction())) {
      return writeStatuses;
    }

    // Double-check if it is a clustering operation by trying to obtain the clustering plan
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlanPair =
        ClusteringUtils.getClusteringPlan(hoodieTable.getMetaClient(), instant);
    if (!instantPlanPair.isPresent()) {
      return writeStatuses;
    }

    HoodieClusteringPlan plan = instantPlanPair.get().getRight();

    HoodieJavaRDD.getJavaRDD(context.parallelize(plan.getInputGroups().stream().map(HoodieClusteringGroup::getExtraMetadata).collect(Collectors.toList())))
        .foreach(m -> {
          String partition = m.get(ExtensibleBucketIndexUtils.METADATA_PARTITION_PATH);
          // Generate new bucket metadata
          HoodieExtensibleBucketMetadata clusteringBucketMetadata =
              ExtensibleBucketIndexUtils.deconstructExtensibleExtraMetadata(m, instantPlanPair.get().getLeft().getTimestamp());
          // Checking version continuity
          Option<HoodieExtensibleBucketMetadata> oldMetadata = ExtensibleBucketIndexUtils.loadMetadata(hoodieTable, partition);
          ValidationUtils.checkArgument(oldMetadata.isPresent(), "No metadata found for partition " + partition);
          ValidationUtils.checkArgument(oldMetadata.get().getBucketVersion() == clusteringBucketMetadata.getBucketVersion() - 1,
              "Version continuity check failed for partition " + partition + ", old version: " + oldMetadata.get()
                  + ", new version: " + clusteringBucketMetadata);
          // Save the metadata
          // TODO: consider fault-tolerance, success to save metadata but instant failed to commit or be rolled back
          ExtensibleBucketIndexUtils.saveMetadata(hoodieTable, clusteringBucketMetadata, true);
        });
    return writeStatuses;
  }
}
