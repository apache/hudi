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

package org.apache.hudi.client.clustering.update.strategy;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.bucket.ExtensibleBucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class SparkExtensibleBucketDuplicateUpdateStrategy<T extends HoodieRecordPayload<T>> extends UpdateStrategy<T, HoodieData<HoodieRecord<T>>> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExtensibleBucketDuplicateUpdateStrategy.class);

  public SparkExtensibleBucketDuplicateUpdateStrategy(HoodieEngineContext engineContext, HoodieTable table,
                                                      Set<HoodieFileGroupId> fileGroupsInPendingClustering) {
    super(engineContext, table, fileGroupsInPendingClustering);
  }

  @Override
  public Pair<HoodieData<HoodieRecord<T>>, Set<HoodieFileGroupId>> handleUpdate(HoodieData<HoodieRecord<T>> taggedRecordsRDD) {
    // no file groups in pending clustering, return directly
    if (fileGroupsInPendingClustering.isEmpty()) {
      return Pair.of(taggedRecordsRDD, Collections.emptySet());
    }

    // find that records which need to be updated
    HoodieData<HoodieRecord<T>> filteredRecordsRDD = taggedRecordsRDD.filter(r -> {
      checkState(r.getCurrentLocation() != null);
      HoodieFileGroupId hoodieFileGroupId = new HoodieFileGroupId(r.getPartitionPath(), r.getCurrentLocation().getFileId());
      return fileGroupsInPendingClustering.contains(hoodieFileGroupId);
    });

    // no records need to be updated, return directly
    if (filteredRecordsRDD.count() == 0) {
      return Pair.of(taggedRecordsRDD, Collections.emptySet());
    }

    List<HoodieFileGroupId> fileGroupIds = filteredRecordsRDD.map(r -> new HoodieFileGroupId(r.getPartitionPath(), r.getCurrentLocation().getFileId())).distinct().collectAsList();

    if (!table.getConfig().isBucketResizingConcurrentWriteEnable()) {
      // disallow concurrent writing to the bucket-resizing bucket
      LOG.warn("Disallow concurrent writing to the bucket-resizing bucket: " + fileGroupIds);
      return Pair.of(taggedRecordsRDD, fileGroupIds.stream().collect(Collectors.toSet()));
    }

    // find bucket-identifier for each partition
    final Set<String> partitions = fileGroupIds.stream().map(HoodieFileGroupId::getPartitionPath).collect(Collectors.toSet());

    Map<String, ExtensibleBucketIdentifier> bucketIdentifier = ExtensibleBucketIndexUtils.fetchLatestUncommittedExtensibleBucketIdentifier(table, partitions);

    // produce records tagged with new record location
    List<String> indexKeyFields = Arrays.asList(table.getConfig().getBucketIndexHashField().split(","));
    HoodieData<HoodieRecord<T>> redirectedRecordsRDD = filteredRecordsRDD.map(r -> {
      ExtensibleBucketIdentifier identifier = bucketIdentifier.get(r.getPartitionPath());
      String fileIdPrefix = identifier.getFileIdPrefix(r.getKey().getRecordKey(), indexKeyFields);
      return ExtensibleBucketIndexUtils.tagAsDualWriteRecordIfNeeded(new HoodieAvroRecord(r.getKey(), r.getData(), r.getOperation()),
          Option.ofNullable(new HoodieRecordLocation(identifier.getMetadata().getInstant(), FSUtils.createNewFileId(fileIdPrefix, 0))));
    });

    // Return combined iterator (the original and records with new location)
    return Pair.of(taggedRecordsRDD.union(redirectedRecordsRDD), Collections.emptySet());
  }
}
