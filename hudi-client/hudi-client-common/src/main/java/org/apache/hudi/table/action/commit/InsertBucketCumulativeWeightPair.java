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

import org.apache.hudi.common.util.collection.Pair;

/**
 * Each InsertBucket has a weight, InsertBucketCumulativeWeightPair stored here is the cumulativeWeight of the
 * InsertBucket. If there are multiple InsertBuckets in a partition, the InsertBuckets are numbered from 1,
 * the cumulativeWeight of a InsertBucket is the sum of the InsertBucket weights from number 1 to its own number.
 *
 * Example, there are three InsertBucket in a partition, each bucketNumber and weight is:
 * 1) bucketNumber: 1, weight: 0.2
 * 2) bucketNumber: 2, weight: 0.3
 * 3) bucketNumber: 3, weight: 0.5
 *
 * Each cumulativeWeight of the bucket is:
 * 1) bucketNumber: 1, cumulativeWeight: 0.2
 * 2) bucketNumber: 2, cumulativeWeight: 0.5
 * 3) bucketNumber: 3, cumulativeWeight: 1.0
 */
public class InsertBucketCumulativeWeightPair extends Pair<InsertBucket, Double> {
  InsertBucket insertBucket;
  Double cumulativeWeight;

  public InsertBucketCumulativeWeightPair(final InsertBucket insertBucket, final Double cumulativeWeight) {
    super();
    this.insertBucket = insertBucket;
    this.cumulativeWeight = cumulativeWeight;
  }

  @Override
  public InsertBucket getLeft() {
    return insertBucket;
  }

  @Override
  public Double getRight() {
    return cumulativeWeight;
  }

  @Override
  public int compareTo(final Pair<InsertBucket, Double> other) {
    // Only need to compare the cumulativeWeight.
    return cumulativeWeight.compareTo(other.getRight());
  }

  @Override
  public Double setValue(Double value) {
    this.cumulativeWeight = value;
    return value;
  }
}
