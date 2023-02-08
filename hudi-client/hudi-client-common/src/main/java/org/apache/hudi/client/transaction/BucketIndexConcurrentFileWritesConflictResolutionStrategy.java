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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is a basic implementation of a conflict resolution strategy for concurrent writes {@link ConflictResolutionStrategy} using bucket index.
 */
public class BucketIndexConcurrentFileWritesConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(BucketIndexConcurrentFileWritesConflictResolutionStrategy.class);

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // TODO : UUID's can clash even for insert/insert, handle that case.
    Set<String> bucketIdsSetForFirstInstant = extractBucketIds(thisOperation.getMutatedFileIds());
    Set<String> bucketIdsSetForSecondInstant = extractBucketIds(otherOperation.getMutatedFileIds());
    Set<String> intersection = new HashSet<>(bucketIdsSetForFirstInstant);
    intersection.retainAll(bucketIdsSetForSecondInstant);
    if (!intersection.isEmpty()) {
      LOG.info("Found conflicting writes between first operation = " + thisOperation
          + ", second operation = " + otherOperation + " , intersecting bucket ids " + intersection);
      return true;
    }
    return false;
  }

  private static Set<String> extractBucketIds(Set<String> fileIds) {
    return fileIds.stream().map(BucketIdentifier::bucketIdStrFromFileId).collect(Collectors.toSet());
  }
}
