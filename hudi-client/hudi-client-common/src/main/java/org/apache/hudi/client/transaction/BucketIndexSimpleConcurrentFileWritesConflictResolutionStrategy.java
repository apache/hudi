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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class implements a conflict checking of bucket index for concurrent writes {@link ConflictResolutionStrategy}.
 */
public class BucketIndexSimpleConcurrentFileWritesConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  private static final Logger LOG = LogManager.getLogger(BucketIndexSimpleConcurrentFileWritesConflictResolutionStrategy.class);

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    Set<Integer> bucketIdsForFirstInstant = thisOperation.getMutatedFileIds()
        .stream().map(fileId -> BucketIdentifier.bucketIdFromFileId(fileId)).collect(Collectors.toSet());
    Set<Integer> bucketIdsForSecondInstant = otherOperation.getMutatedFileIds()
        .stream().map(fileId -> BucketIdentifier.bucketIdFromFileId(fileId)).collect(Collectors.toSet());
    Set<Integer> intersection = new HashSet<>(bucketIdsForFirstInstant);
    intersection.retainAll(bucketIdsForSecondInstant);

    if (!intersection.isEmpty()) {
      LOG.info("Found conflicting writes between first operation = " + thisOperation
          + ", second operation = " + otherOperation + " , intersecting bucket ids " + intersection);
      return true;
    }
    return false;
  }

}
