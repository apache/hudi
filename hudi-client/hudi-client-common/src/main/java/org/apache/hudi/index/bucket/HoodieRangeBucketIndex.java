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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Range Bucket indexing mechanism.
 */
public class HoodieRangeBucketIndex extends HoodieBucketIndex {

  private final int bucketRangeStepSize;

  public HoodieRangeBucketIndex(HoodieWriteConfig config) {
    super(config);
    bucketRangeStepSize = config.getBucketRangeStepSize();
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  protected BucketIndexLocationMapper getLocationMapper(HoodieTable table, List<String> partitionPath) {
    return new RangeBucketIndexLocationMapper(table, partitionPath);
  }

  public class RangeBucketIndexLocationMapper implements BucketIndexLocationMapper {

    /**
     * Mapping from partitionPath -> bucketId -> fileInfo
     */
    private final Map<String, Map<Integer, HoodieRecordLocation>> partitionPathFileIDList;

    public RangeBucketIndexLocationMapper(HoodieTable table, List<String> partitions) {
      partitionPathFileIDList = partitions.stream().collect(Collectors.toMap(p -> p, p -> loadPartitionBucketIdFileIdMapping(table, p)));
    }

    @Override
    public Option<HoodieRecordLocation> getRecordLocation(HoodieKey key) {
      int bucketId = BucketIdentifier.getRangeBucketId(key, bucketRangeStepSize);
      Map<Integer, HoodieRecordLocation> bucketIdToFileIdMapping = partitionPathFileIDList.get(key.getPartitionPath());
      if (bucketIdToFileIdMapping.containsKey(bucketId)) {
        return Option.ofNullable(bucketIdToFileIdMapping.get(bucketId));
      } else {
        return Option.ofNullable(new HoodieRecordLocation(null, BucketIdentifier.bucketIdStr(bucketId)));
      }
    }
  }
}
