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

package org.apache.hudi.source.assign;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link HoodieSplitBucketAssigner}.
 */
public class TestHoodieSplitBucketAssigner {

  @Test
  public void testAssignWithSingleParallelism() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(1);

    HoodieSourceSplit split1 = createTestSplitWithBucket(0, 0);
    HoodieSourceSplit split2 = createTestSplitWithBucket(1, 5);
    HoodieSourceSplit split3 = createTestSplitWithBucket(2, 10);

    assertEquals(0, assigner.assign(split1), "All splits should be assigned to task 0");
    assertEquals(0, assigner.assign(split2), "All splits should be assigned to task 0");
    assertEquals(0, assigner.assign(split3), "All splits should be assigned to task 0");
  }

  @Test
  public void testAssignWithMultipleParallelism() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(3);

    HoodieSourceSplit split0 = createTestSplitWithBucket(0, 0);
    HoodieSourceSplit split1 = createTestSplitWithBucket(1, 1);
    HoodieSourceSplit split2 = createTestSplitWithBucket(2, 2);
    HoodieSourceSplit split3 = createTestSplitWithBucket(3, 3);
    HoodieSourceSplit split4 = createTestSplitWithBucket(4, 4);
    HoodieSourceSplit split5 = createTestSplitWithBucket(5, 5);

    assertEquals(0, assigner.assign(split0), "Bucket 0 should be assigned to task 0");
    assertEquals(1, assigner.assign(split1), "Bucket 1 should be assigned to task 1");
    assertEquals(2, assigner.assign(split2), "Bucket 2 should be assigned to task 2");
    assertEquals(0, assigner.assign(split3), "Bucket 3 should be assigned to task 0 (round-robin)");
    assertEquals(1, assigner.assign(split4), "Bucket 4 should be assigned to task 1 (round-robin)");
    assertEquals(2, assigner.assign(split5), "Bucket 5 should be assigned to task 2 (round-robin)");
  }

  @Test
  public void testAssignBucketBasedCoLocation() {
    int parallelism = 4;
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism);

    // Splits with the same bucket ID should always be assigned to the same task
    HoodieSourceSplit split1a = createTestSplitWithBucket(0, 5);
    HoodieSourceSplit split1b = createTestSplitWithBucket(1, 5);
    HoodieSourceSplit split1c = createTestSplitWithBucket(2, 5);

    int taskId = assigner.assign(split1a);
    assertEquals(taskId, assigner.assign(split1b),
        "Splits with same bucket should be co-located on same task");
    assertEquals(taskId, assigner.assign(split1c),
        "Splits with same bucket should be co-located on same task");
    assertEquals(1, taskId, "Bucket 5 % 4 = 1");
  }

  @Test
  public void testAssignDifferentBucketsDistributed() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(5);

    // Different buckets should be distributed across different tasks
    HoodieSourceSplit split0 = createTestSplitWithBucket(0, 0);
    HoodieSourceSplit split1 = createTestSplitWithBucket(1, 1);
    HoodieSourceSplit split2 = createTestSplitWithBucket(2, 2);

    assertNotEquals(assigner.assign(split0), assigner.assign(split1),
        "Different buckets should typically be on different tasks");
    assertNotEquals(assigner.assign(split1), assigner.assign(split2),
        "Different buckets should typically be on different tasks");
  }

  @Test
  public void testAssignWithHighParallelism() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(100);

    HoodieSourceSplit split0 = createTestSplitWithBucket(0, 0);
    HoodieSourceSplit split50 = createTestSplitWithBucket(1, 50);
    HoodieSourceSplit split99 = createTestSplitWithBucket(2, 99);
    HoodieSourceSplit split100 = createTestSplitWithBucket(3, 100);

    assertEquals(0, assigner.assign(split0));
    assertEquals(50, assigner.assign(split50));
    assertEquals(99, assigner.assign(split99));
    assertEquals(0, assigner.assign(split100), "Bucket 100 should wrap around to task 0");
  }

  @Test
  public void testAssignSameBucketMultipleTimes() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(5);

    HoodieSourceSplit split1 = createTestSplitWithBucket(0, 7);
    HoodieSourceSplit split2 = createTestSplitWithBucket(1, 7);

    // Assigning splits with the same bucket multiple times should return the same task ID
    int taskId = assigner.assign(split1);
    assertEquals(2, taskId, "Bucket 7 % 5 = 2");
    assertEquals(taskId, assigner.assign(split2),
        "Same bucket should always return same task ID");
    assertEquals(taskId, assigner.assign(split1),
        "Same bucket should always return same task ID");
  }

  @Test
  public void testConstructorWithZeroParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitBucketAssigner(0),
        "Should throw exception for zero parallelism"
    );
    assertEquals("Parallelism must be positive, but was: 0", exception.getMessage());
  }

  @Test
  public void testConstructorWithNegativeParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitBucketAssigner(-1),
        "Should throw exception for negative parallelism"
    );
    assertEquals("Parallelism must be positive, but was: -1", exception.getMessage());
  }

  @Test
  public void testConstructorWithNegativeLargeParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitBucketAssigner(-100),
        "Should throw exception for large negative parallelism"
    );
    assertEquals("Parallelism must be positive, but was: -100", exception.getMessage());
  }

  @Test
  public void testAssignmentConsistency() {
    HoodieSplitBucketAssigner assigner1 = new HoodieSplitBucketAssigner(5);
    HoodieSplitBucketAssigner assigner2 = new HoodieSplitBucketAssigner(5);

    // Two assigners with same parallelism should assign splits identically
    for (int i = 0; i < 20; i++) {
      HoodieSourceSplit split = createTestSplitWithBucket(i, i);
      assertEquals(
          assigner1.assign(split),
          assigner2.assign(split),
          "Different assigner instances should assign same bucket to same task"
      );
    }
  }

  @Test
  public void testBucketCoLocationAcrossMultipleSplits() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(8);

    // Create multiple splits for the same bucket
    int bucketId = 15;
    HoodieSourceSplit[] splits = new HoodieSourceSplit[10];
    for (int i = 0; i < 10; i++) {
      splits[i] = createTestSplitWithBucket(i, bucketId);
    }

    // All splits with the same bucket should be assigned to the same task
    int expectedTaskId = bucketId % 8;
    for (HoodieSourceSplit split : splits) {
      assertEquals(expectedTaskId, assigner.assign(split),
          "All splits for bucket " + bucketId + " should be on task " + expectedTaskId);
    }
  }

  @Test
  public void testManyBucketsDistribution() {
    int parallelism = 4;
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(parallelism);

    // Test distribution of many buckets
    int[] taskCounts = new int[parallelism];
    int totalBuckets = 100;

    for (int bucketId = 0; bucketId < totalBuckets; bucketId++) {
      HoodieSourceSplit split = createTestSplitWithBucket(bucketId, bucketId);
      int taskId = assigner.assign(split);
      taskCounts[taskId]++;

      // Verify task ID is within valid range
      assertEquals(bucketId % parallelism, taskId,
          "Bucket should be assigned using modulo operation");
    }

    // Verify even distribution
    for (int count : taskCounts) {
      assertEquals(totalBuckets / parallelism, count,
          "Buckets should be evenly distributed");
    }
  }

  @Test
  public void testFileIdParsing() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(10);

    // Test with various file ID formats that have bucket ID in first 8 characters
    HoodieSourceSplit split1 = createTestSplit(0, "00000005-0000-0000-0000-000000000000");
    HoodieSourceSplit split2 = createTestSplit(1, "00000012-1234-5678-abcd-ef0123456789");
    HoodieSourceSplit split3 = createTestSplit(2, "00000099-aaaa-bbbb-cccc-dddddddddddd");

    assertEquals(5, assigner.assign(split1), "Should extract bucket 5 from file ID");
    assertEquals(2, assigner.assign(split2), "Should extract bucket 12 and assign to task 2 (12 % 10)");
    assertEquals(9, assigner.assign(split3), "Should extract bucket 99 and assign to task 9 (99 % 10)");
  }

  @Test
  public void testDifferentSplitNumbersSameBucket() {
    HoodieSplitBucketAssigner assigner = new HoodieSplitBucketAssigner(5);

    // Different split numbers but same bucket should go to same task
    HoodieSourceSplit split1 = createTestSplitWithBucket(0, 8);
    HoodieSourceSplit split2 = createTestSplitWithBucket(100, 8);
    HoodieSourceSplit split3 = createTestSplitWithBucket(999, 8);

    int taskId = assigner.assign(split1);
    assertEquals(taskId, assigner.assign(split2),
        "Split number should not affect bucket-based assignment");
    assertEquals(taskId, assigner.assign(split3),
        "Split number should not affect bucket-based assignment");
    assertEquals(3, taskId, "Bucket 8 % 5 = 3");
  }

  @Test
  public void testComparisonWithDefaultAssigner() {
    int parallelism = 5;
    HoodieSplitBucketAssigner bucketAssigner = new HoodieSplitBucketAssigner(parallelism);
    HoodieSplitNumberAssigner defaultAssigner = new HoodieSplitNumberAssigner(parallelism);

    // Create splits where bucket ID != split number
    HoodieSourceSplit split1 = createTestSplitWithBucket(0, 3);
    HoodieSourceSplit split2 = createTestSplitWithBucket(3, 0);

    // Bucket assigner uses bucket ID from file ID
    assertEquals(3, bucketAssigner.assign(split1), "Should use bucket 3");
    assertEquals(0, bucketAssigner.assign(split2), "Should use bucket 0");

    // Default assigner uses split number
    assertEquals(0, defaultAssigner.assign(split1), "Should use split number 0");
    assertEquals(3, defaultAssigner.assign(split2), "Should use split number 3");
  }

  /**
   * Creates a test split with a specific bucket ID encoded in the file ID.
   * File ID format: "{8-digit bucket ID}-{rest of UUID}"
   */
  private HoodieSourceSplit createTestSplitWithBucket(int splitNum, int bucketId) {
    String fileId = String.format("%08d-0000-0000-0000-000000000000", bucketId);
    return createTestSplit(splitNum, fileId);
  }

  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "basePath_" + splitNum,
        Option.empty(),
        "/table/path",
        "/table/path/partition1",
        "read_optimized",
        "19700101000000000",
        fileId,
        Option.empty()
    );
  }
}
