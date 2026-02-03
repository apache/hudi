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

package org.apache.hudi.client.transaction;

import org.apache.hudi.index.bucket.BucketIdentifier;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is a basic implementation of a conflict resolution strategy for concurrent writes {@link ConflictResolutionStrategy} using bucket index.
 */
@Slf4j
public class BucketIndexConcurrentFileWritesConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // TODO : UUID's can clash even for insert/insert, handle that case.
    Set<String> partitionBucketIdSetForFirstInstant = thisOperation
        .getMutatedPartitionAndFileIds()
        .stream()
        .map(partitionAndFileId ->
            BucketIdentifier.partitionBucketIdStr(partitionAndFileId.getLeft(), BucketIdentifier.bucketIdFromFileId(partitionAndFileId.getRight()))
        ).collect(Collectors.toSet());
    Set<String> partitionBucketIdSetForSecondInstant = otherOperation
        .getMutatedPartitionAndFileIds()
        .stream()
        .map(partitionAndFileId ->
            BucketIdentifier.partitionBucketIdStr(partitionAndFileId.getLeft(), BucketIdentifier.bucketIdFromFileId(partitionAndFileId.getRight()))
        ).collect(Collectors.toSet());
    Set<String> intersection = new HashSet<>(partitionBucketIdSetForFirstInstant);
    intersection.retainAll(partitionBucketIdSetForSecondInstant);
    if (!intersection.isEmpty()) {
      log.info("Found conflicting writes between first operation = " + thisOperation
          + ", second operation = " + otherOperation + " , intersecting bucket ids " + intersection);
      return true;
    }
    return false;
  }
}
