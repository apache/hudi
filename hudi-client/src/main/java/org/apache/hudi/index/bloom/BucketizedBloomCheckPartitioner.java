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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Partitions bloom filter checks by spreading out comparisons across buckets of work.
 *
 * Each bucket incurs the following cost
 * 
 * <pre>
 *   1) Read bloom filter from file footer
 *   2) Check keys against bloom filter
 *   3) [Conditional] If any key had a hit, open file and check
 * </pre>
 *
 * The partitioner performs a two phase bin packing algorithm, to pack enough work into each bucket such that cost of
 * (1) & (3) is amortized. Also, avoids any skews in the sort based approach, by directly partitioning by the file to be
 * checked against and ensuring each partition has similar number of buckets. Performance tests show that this approach
 * could bound the amount of skew to std_dev(numberOfBucketsPerPartition) * cost of (3), lower than sort partitioning.
 *
 * Approach has two goals :
 * 
 * <pre>
 *   1) Pack as many buckets from same file group into same partition, to amortize cost of (1) and (2) further
 *   2) Spread buckets across partitions evenly to achieve skew reduction
 * </pre>
 */
public class BucketizedBloomCheckPartitioner extends Partitioner {

  private static final Logger LOG = LogManager.getLogger(BucketizedBloomCheckPartitioner.class);

  private int partitions;

  /**
   * Stores the final mapping of a file group to a list of partitions for its keys.
   */
  private Map<String, List<Integer>> fileGroupToPartitions;

  /**
   * Create a partitioner that computes a plan based on provided workload characteristics.
   *
   * @param targetPartitions maximum number of partitions to target
   * @param fileGroupToComparisons number of expected comparisons per file group
   * @param keysPerBucket maximum number of keys to pack in a single bucket
   */
  public BucketizedBloomCheckPartitioner(int targetPartitions, Map<String, Long> fileGroupToComparisons,
      int keysPerBucket) {
    this.fileGroupToPartitions = new HashMap<>();

    Map<String, Integer> bucketsPerFileGroup = new HashMap<>();
    // Compute the buckets needed per file group, using simple uniform distribution
    fileGroupToComparisons.forEach((f, c) -> bucketsPerFileGroup.put(f, (int) Math.ceil((c * 1.0) / keysPerBucket)));
    int totalBuckets = bucketsPerFileGroup.values().stream().mapToInt(i -> i).sum();
    // If totalBuckets > targetPartitions, no need to have extra partitions
    this.partitions = Math.min(targetPartitions, totalBuckets);

    // PHASE 1 : start filling upto minimum number of buckets into partitions, taking all but one bucket from each file
    // This tries to first optimize for goal 1 above, with knowledge that each partition needs a certain minimum number
    // of buckets and assigns buckets in the same order as file groups. If we were to simply round robin, then buckets
    // for a file group is more or less guaranteed to be placed on different partitions all the time.
    int minBucketsPerPartition = Math.max((int) Math.floor((1.0 * totalBuckets) / partitions), 1);
    LOG.info(String.format("TotalBuckets %d, min_buckets/partition %d", totalBuckets, minBucketsPerPartition));
    int[] bucketsFilled = new int[partitions];
    Map<String, AtomicInteger> bucketsFilledPerFileGroup = new HashMap<>();
    int partitionIndex = 0;
    for (Map.Entry<String, Integer> e : bucketsPerFileGroup.entrySet()) {
      for (int b = 0; b < Math.max(1, e.getValue() - 1); b++) {
        // keep filled counts upto date
        bucketsFilled[partitionIndex]++;
        AtomicInteger cnt = bucketsFilledPerFileGroup.getOrDefault(e.getKey(), new AtomicInteger(0));
        cnt.incrementAndGet();
        bucketsFilledPerFileGroup.put(e.getKey(), cnt);

        // mark this partition against the file group
        List<Integer> partitionList = this.fileGroupToPartitions.getOrDefault(e.getKey(), new ArrayList<>());
        partitionList.add(partitionIndex);
        this.fileGroupToPartitions.put(e.getKey(), partitionList);

        // switch to new partition if needed
        if (bucketsFilled[partitionIndex] >= minBucketsPerPartition) {
          partitionIndex = (partitionIndex + 1) % partitions;
        }
      }
    }

    // PHASE 2 : for remaining unassigned buckets, round robin over partitions once. Since we withheld 1 bucket from
    // each file group uniformly, this remaining is also an uniform mix across file groups. We just round robin to
    // optimize for goal 2.
    for (Map.Entry<String, Integer> e : bucketsPerFileGroup.entrySet()) {
      int remaining = e.getValue() - bucketsFilledPerFileGroup.get(e.getKey()).intValue();
      for (int r = 0; r < remaining; r++) {
        // mark this partition against the file group
        this.fileGroupToPartitions.get(e.getKey()).add(partitionIndex);
        bucketsFilled[partitionIndex]++;
        partitionIndex = (partitionIndex + 1) % partitions;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Partitions assigned per file groups :" + fileGroupToPartitions);
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < bucketsFilled.length; i++) {
        str.append("p" + i + " : " + bucketsFilled[i] + ",");
      }
      LOG.debug("Num buckets assigned per file group :" + str);
    }
  }

  @Override
  public int numPartitions() {
    return partitions;
  }

  @Override
  public int getPartition(Object key) {
    final Pair<String, String> parts = (Pair<String, String>) key;
    final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", parts.getRight());
    final List<Integer> candidatePartitions = fileGroupToPartitions.get(parts.getLeft());
    final int idx = (int) Math.floorMod((int) hashOfKey, candidatePartitions.size());
    assert idx >= 0;
    return candidatePartitions.get(idx);
  }

  Map<String, List<Integer>> getFileGroupToPartitions() {
    return fileGroupToPartitions;
  }
}
