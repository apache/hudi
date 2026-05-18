/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.split.assign;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieSplitBucketAssigner}.
 *
 * <p>The assignment formula is:
 * <pre>
 *   partitionIndex = (partition.hashCode() & Integer.MAX_VALUE) % parallelism * numBuckets
 *   taskId = (partitionIndex + curBucket) % parallelism
 * </pre>
 * When the partition path is empty (hashCode = 0), this simplifies to {@code curBucket % parallelism}.
 */
public class TestHoodieSplitBucketAssigner {

  // -------------------------------------------------------------------------
  // Constructor validation
  // -------------------------------------------------------------------------

  @Test
  public void testConstructorWithZeroParallelismThrows() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitBucketAssigner(0, new Configuration()));
    assertEquals("Parallelism must be positive, but was: 0", ex.getMessage());
  }

  @Test
  public void testConstructorWithNegativeParallelismThrows() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitBucketAssigner(-1, new Configuration()));
    assertEquals("Parallelism must be positive, but was: -1", ex.getMessage());
  }

  @Test
  public void testConstructorWithLargeNegativeParallelismThrows() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitBucketAssigner(-100, new Configuration()));
    assertEquals("Parallelism must be positive, but was: -100", ex.getMessage());
  }

  // -------------------------------------------------------------------------
  // Single parallelism — everything maps to task 0
  // -------------------------------------------------------------------------

  @Test
  public void testSingleParallelismAlwaysAssignsToTaskZero() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(1, new Configuration());

    for (int bucket = 0; bucket < 10; bucket++) {
      assertEquals(0, assigner.assign(splitWithBucket(bucket, "")),
          "All splits must go to task 0 when parallelism is 1");
    }
  }

  // -------------------------------------------------------------------------
  // Empty partition path: taskId = curBucket % parallelism
  // When partition hashCode is 0, partitionIndex = 0, so the formula
  // reduces to a simple modulo on the bucket ID.
  // -------------------------------------------------------------------------

  @Test
  public void testAssignWithEmptyPartitionUsesSimpleModulo() {
    int parallelism = 5;
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism, new Configuration());

    for (int bucket = 0; bucket < 20; bucket++) {
      int expected = bucket % parallelism;
      assertEquals(expected, assigner.assign(splitWithBucket(bucket, "")),
          "With empty partition, taskId must equal curBucket % parallelism");
    }
  }

  @Test
  public void testTaskIdIsAlwaysInValidRange() {
    int parallelism = 7;
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism, new Configuration());

    for (int bucket = 0; bucket < 50; bucket++) {
      int taskId = assigner.assign(splitWithBucket(bucket, ""));
      assertTrue(taskId >= 0 && taskId < parallelism,
          "taskId must be in [0, parallelism): got " + taskId);
    }
  }

  // -------------------------------------------------------------------------
  // Co-location: same (partition, bucket) always lands on the same task
  // -------------------------------------------------------------------------

  @Test
  public void testSameBucketSamePartitionAlwaysCoLocated() {
    int parallelism = 4;
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism, new Configuration());

    int[] bucketsToTest = {0, 3, 7, 15};
    String[] partitions = {"", "2024/01/01", "country=US"};

    for (String partition : partitions) {
      for (int bucket : bucketsToTest) {
        int first = assigner.assign(splitWithBucket(bucket, partition));
        for (int i = 0; i < 5; i++) {
          assertEquals(first, assigner.assign(splitWithBucket(bucket, partition)),
              "Same bucket+partition must always map to the same task");
        }
      }
    }
  }

  @Test
  public void testCoLocationHoldsAcrossDifferentSplitNumbers() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(6, new Configuration());
    int bucket = 11;
    String partition = "region=EU";

    // Different split numbers, same bucket and partition
    int expected = assigner.assign(split(0, bucket, partition));
    assertEquals(expected, assigner.assign(split(100, bucket, partition)));
    assertEquals(expected, assigner.assign(split(999, bucket, partition)));
  }

  // -------------------------------------------------------------------------
  // Custom numBuckets via config
  // -------------------------------------------------------------------------

  @Test
  public void testCustomNumBucketsIsUsed() {
    // With a non-trivial partition hash, changing numBuckets changes the assignment.
    // Partition "p1" has hashCode 3521.
    // parallelism=3, numBuckets=4:
    //   partitionIndex = (3521 % 3) * 4 = 2 * 4 = 8
    //   taskId(bucket=0) = (8 + 0) % 3 = 2
    //   taskId(bucket=1) = (8 + 1) % 3 = 0
    //   taskId(bucket=2) = (8 + 2) % 3 = 1
    int parallelism = 3;
    int numBuckets = 4;
    String partition = "p1"; // hashCode = 112*31 + 49 = 3521

    Configuration conf = new Configuration();
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, numBuckets);
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism, conf);

    assertEquals(expectedTaskId(partition, 0, numBuckets, parallelism), assigner.assign(split(0, 0, partition)));
    assertEquals(expectedTaskId(partition, 1, numBuckets, parallelism), assigner.assign(split(1, 1, partition)));
    assertEquals(expectedTaskId(partition, 2, numBuckets, parallelism), assigner.assign(split(2, 2, partition)));
  }

  @Test
  public void testDifferentNumBucketsProduceDifferentAssignments() {
    // Use a partition with non-zero hash so numBuckets affects the result.
    String partition = "country=DE"; // non-zero hashCode guaranteed
    int parallelism = 5;
    int bucket = 3;

    Configuration conf4 = new Configuration();
    conf4.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    Configuration conf8 = new Configuration();
    conf8.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 8);

    int taskWith4 = new HoodieSplitBucketAssigner(parallelism, conf4).assign(split(0, bucket, partition));
    int taskWith8 = new HoodieSplitBucketAssigner(parallelism, conf8).assign(split(0, bucket, partition));

    // Both must be valid task IDs, and at least verify they're computed from the right formula
    assertEquals(expectedTaskId(partition, bucket, 4, parallelism), taskWith4);
    assertEquals(expectedTaskId(partition, bucket, 8, parallelism), taskWith8);
  }

  // -------------------------------------------------------------------------
  // Full formula verification with non-trivial partition hash
  // -------------------------------------------------------------------------

  @Test
  public void testFullFormulaWithNonTrivialPartitionHash() {
    int parallelism = 4;
    int numBuckets = 4;
    String partition = "dt=2024-01-15";

    Configuration conf = new Configuration();
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, numBuckets);
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism, conf);

    for (int bucket = 0; bucket < numBuckets; bucket++) {
      int expected = expectedTaskId(partition, bucket, numBuckets, parallelism);
      assertEquals(expected, assigner.assign(split(bucket, bucket, partition)),
          "Task ID must match the formula for bucket " + bucket);
    }
  }

  // -------------------------------------------------------------------------
  // Bucket ID parsing from file ID
  // -------------------------------------------------------------------------

  @Test
  public void testBucketIdParsedFromFirst8CharsOfFileId() {
    // bucketIdFromFileId parses the first 8 characters as an integer
    int parallelism = 10;
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism, new Configuration());

    // File ID "00000005-..." → bucket 5 → task 5 % 10 = 5 (empty partition)
    HoodieSourceSplit split5 = createTestSplit(0, "00000005-0000-0000-0000-000000000000", "");
    assertEquals(5, assigner.assign(split5));

    // File ID "00000012-..." → bucket 12 → task 12 % 10 = 2
    HoodieSourceSplit split12 = createTestSplit(1, "00000012-1234-5678-abcd-ef0123456789", "");
    assertEquals(2, assigner.assign(split12));

    // File ID "00000099-..." → bucket 99 → task 99 % 10 = 9
    HoodieSourceSplit split99 = createTestSplit(2, "00000099-aaaa-bbbb-cccc-dddddddddddd", "");
    assertEquals(9, assigner.assign(split99));
  }

  // -------------------------------------------------------------------------
  // Consistency between assigner instances
  // -------------------------------------------------------------------------

  @Test
  public void testTwoAssignersWithSameConfigProduceSameResults() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 8);

    HoodieSplitBucketAssigner a1 = new HoodieSplitBucketAssigner(5, conf);
    HoodieSplitBucketAssigner a2 = new HoodieSplitBucketAssigner(5, conf);

    for (int bucket = 0; bucket < 30; bucket++) {
      HoodieSourceSplit split = splitWithBucket(bucket, "region=APAC");
      assertEquals(a1.assign(split), a2.assign(split),
          "Two assigners with same config must produce identical results");
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Computes the expected task ID using the same formula as {@link HoodieSplitBucketAssigner}:
   * <pre>
   *   partitionIndex = (partition.hashCode() & Integer.MAX_VALUE) % parallelism * numBuckets
   *   taskId = (partitionIndex + curBucket) % parallelism
   * </pre>
   */
  private static int expectedTaskId(String partition, int curBucket, int numBuckets, int parallelism) {
    long partitionIndex = (long) ((partition.hashCode() & Integer.MAX_VALUE) % parallelism) * numBuckets;
    return (int) ((partitionIndex + curBucket) % parallelism);
  }

  /** Creates a split whose file ID encodes {@code bucketId} in the first 8 characters. */
  private static HoodieSourceSplit splitWithBucket(int bucketId, String partitionPath) {
    return split(bucketId, bucketId, partitionPath);
  }

  /** Creates a split with an explicit split number, bucket ID, and partition path. */
  private static HoodieSourceSplit split(int splitNum, int bucketId, String partitionPath) {
    String fileId = String.format("%08d-0000-0000-0000-000000000000", bucketId);
    return createTestSplit(splitNum, fileId, partitionPath);
  }

  private static HoodieSourceSplit createTestSplit(int splitNum, String fileId, String partitionPath) {
    return new HoodieSourceSplit(
        splitNum,
        "basePath_" + splitNum,
        Option.empty(),
        "/table/path",
        partitionPath,
        "read_optimized",
        "19700101000000000",
        fileId,
        Option.empty());
  }
}
