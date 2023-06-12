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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Consistent hashing bucket index implementation, with auto-adjust bucket number.
 * NOTE: bucket resizing is triggered by clustering.
 */
public class HoodieConsistentBucketIndex extends HoodieBucketIndex {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieConsistentBucketIndex.class);

  public HoodieConsistentBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
                                                HoodieEngineContext context,
                                                HoodieTable hoodieTable)
      throws HoodieIndexException {
    throw new HoodieIndexException("Consistent hashing index does not support update location without the instant parameter");
  }

  /**
   * Do nothing.
   * A failed write may create a hashing metadata for a partition. In this case, we still do nothing when rolling back
   * the failed write. Because the hashing metadata created by a writer must have 00000000000000 timestamp and can be viewed
   * as the initialization of a partition rather than as a part of the failed write.
   */
  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  protected BucketIndexLocationMapper getLocationMapper(HoodieTable table, List<String> partitionPath) {
    return new ConsistentBucketIndexLocationMapper(table, partitionPath);
  }

  public class ConsistentBucketIndexLocationMapper implements BucketIndexLocationMapper {

    /**
     * Mapping from partitionPath -> bucket identifier
     */
    private final Map<String, ConsistentBucketIdentifier> partitionToIdentifier;

    public ConsistentBucketIndexLocationMapper(HoodieTable table, List<String> partitions) {
      // TODO maybe parallel
      partitionToIdentifier = partitions.stream().collect(Collectors.toMap(p -> p, p -> {
        HoodieConsistentHashingMetadata metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(table, p, getNumBuckets());
        return new ConsistentBucketIdentifier(metadata);
      }));
    }

    @Override
    public Option<HoodieRecordLocation> getRecordLocation(HoodieKey key) {
      String partitionPath = key.getPartitionPath();
      ConsistentHashingNode node = partitionToIdentifier.get(partitionPath).getBucket(key, indexKeyFields);
      if (!StringUtils.isNullOrEmpty(node.getFileIdPrefix())) {
        // Dynamic Bucket Index doesn't need the instant time of the latest file group.
        // We add suffix 0 here to the file uuid, following the naming convention, i.e., fileId = [uuid]_[numWrites]
        return Option.of(new HoodieRecordLocation(null, FSUtils.createNewFileId(node.getFileIdPrefix(), 0)));
      }

      LOG.error("Consistent hashing node has no file group, partition: {}, meta: {}, record_key: {}",
          partitionPath, partitionToIdentifier.get(partitionPath).getMetadata().getFilename(), key.toString());
      throw new HoodieIndexException("Failed to getBucket as hashing node has no file group");
    }
  }
}
