/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.index.bucket.BucketIdentifier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Companion class to SparkBucketIndexPartitioner
 */
public class SparkBucketIndexBucketInfoGetter implements SparkBucketInfoGetter {
  private final int numBucketsPerPartition;
  private final List<String> partitionPaths;
  private final Map<String, Set<String>> updatePartitionPathFileIds;
  private final boolean isOverwrite;

  public SparkBucketIndexBucketInfoGetter(int numBucketsPerPartition, List<String> partitionPaths, Map<String, Set<String>> updatePartitionPathFileIds, boolean isOverwrite) {
    this.numBucketsPerPartition = numBucketsPerPartition;
    this.partitionPaths = partitionPaths;
    this.updatePartitionPathFileIds = updatePartitionPathFileIds;
    this.isOverwrite = isOverwrite;
  }

  @Override
  public BucketInfo getBucketInfo(int bucketNumber) {
    String partitionPath = partitionPaths.get(bucketNumber / numBucketsPerPartition);
    String bucketId = BucketIdentifier.bucketIdStr(bucketNumber % numBucketsPerPartition);
    // Insert overwrite always generates new bucket file id
    if (isOverwrite) {
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketId), partitionPath);
    }
    Option<String> fileIdOption = Option.fromJavaOptional(updatePartitionPathFileIds
        .getOrDefault(partitionPath, Collections.emptySet()).stream()
        .filter(e -> e.startsWith(bucketId))
        .findFirst());
    if (fileIdOption.isPresent()) {
      return new BucketInfo(BucketType.UPDATE, fileIdOption.get(), partitionPath);
    } else {
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketId), partitionPath);
    }
  }
}
