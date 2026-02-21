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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link HoodieSplitNumberAssigner}.
 */
public class TestHoodieSplitNumberAssigner {

  @Test
  public void testAssignWithSingleParallelism() {
    HoodieSplitNumberAssigner assigner = new HoodieSplitNumberAssigner(1);

    HoodieSourceSplit split1 = createTestSplit(0, "file1");
    HoodieSourceSplit split2 = createTestSplit(1, "file2");
    HoodieSourceSplit split3 = createTestSplit(2, "file3");

    assertEquals(0, assigner.assign(split1), "All splits should be assigned to task 0");
    assertEquals(0, assigner.assign(split2), "All splits should be assigned to task 0");
    assertEquals(0, assigner.assign(split3), "All splits should be assigned to task 0");
  }

  @Test
  public void testAssignWithMultipleParallelism() {
    HoodieSplitNumberAssigner assigner = new HoodieSplitNumberAssigner(3);

    HoodieSourceSplit split0 = createTestSplit(0, "file0");
    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3");
    HoodieSourceSplit split4 = createTestSplit(4, "file4");
    HoodieSourceSplit split5 = createTestSplit(5, "file5");

    assertEquals(0, assigner.assign(split0), "Split 0 should be assigned to task 0");
    assertEquals(1, assigner.assign(split1), "Split 1 should be assigned to task 1");
    assertEquals(2, assigner.assign(split2), "Split 2 should be assigned to task 2");
    assertEquals(0, assigner.assign(split3), "Split 3 should be assigned to task 0 (round-robin)");
    assertEquals(1, assigner.assign(split4), "Split 4 should be assigned to task 1 (round-robin)");
    assertEquals(2, assigner.assign(split5), "Split 5 should be assigned to task 2 (round-robin)");
  }

  @Test
  public void testAssignRoundRobinDistribution() {
    int parallelism = 4;
    HoodieSplitNumberAssigner assigner = new HoodieSplitNumberAssigner(parallelism);

    // Test that splits are evenly distributed across all tasks
    int[] taskCounts = new int[parallelism];
    int totalSplits = 100;

    for (int i = 0; i < totalSplits; i++) {
      HoodieSourceSplit split = createTestSplit(i, "file" + i);
      int taskId = assigner.assign(split);
      taskCounts[taskId]++;

      // Verify task ID is within valid range
      assertEquals(i % parallelism, taskId, "Split should be assigned using modulo operation");
    }

    // Verify even distribution
    for (int count : taskCounts) {
      assertEquals(totalSplits / parallelism, count, "Splits should be evenly distributed");
    }
  }

  @Test
  public void testAssignWithHighParallelism() {
    HoodieSplitNumberAssigner assigner = new HoodieSplitNumberAssigner(100);

    HoodieSourceSplit split0 = createTestSplit(0, "file0");
    HoodieSourceSplit split50 = createTestSplit(50, "file50");
    HoodieSourceSplit split99 = createTestSplit(99, "file99");
    HoodieSourceSplit split100 = createTestSplit(100, "file100");

    assertEquals(0, assigner.assign(split0));
    assertEquals(50, assigner.assign(split50));
    assertEquals(99, assigner.assign(split99));
    assertEquals(0, assigner.assign(split100), "Split 100 should wrap around to task 0");
  }

  @Test
  public void testAssignSameSplitMultipleTimes() {
    HoodieSplitNumberAssigner assigner = new HoodieSplitNumberAssigner(5);

    HoodieSourceSplit split = createTestSplit(7, "file7");

    // Assigning the same split multiple times should return the same task ID
    int taskId = assigner.assign(split);
    assertEquals(2, taskId, "Split 7 % 5 = 2");
    assertEquals(taskId, assigner.assign(split), "Same split should always return same task ID");
    assertEquals(taskId, assigner.assign(split), "Same split should always return same task ID");
  }

  @Test
  public void testConstructorWithZeroParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitNumberAssigner(0),
        "Should throw exception for zero parallelism"
    );
    assertEquals("Parallelism must be positive, but was: 0", exception.getMessage());
  }

  @Test
  public void testConstructorWithNegativeParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitNumberAssigner(-1),
        "Should throw exception for negative parallelism"
    );
    assertEquals("Parallelism must be positive, but was: -1", exception.getMessage());
  }

  @Test
  public void testConstructorWithNegativeLargeParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieSplitNumberAssigner(-100),
        "Should throw exception for large negative parallelism"
    );
    assertEquals("Parallelism must be positive, but was: -100", exception.getMessage());
  }

  @Test
  public void testAssignmentConsistency() {
    HoodieSplitNumberAssigner assigner1 = new HoodieSplitNumberAssigner(5);
    HoodieSplitNumberAssigner assigner2 = new HoodieSplitNumberAssigner(5);

    // Two assigners with same parallelism should assign splits identically
    for (int i = 0; i < 20; i++) {
      HoodieSourceSplit split = createTestSplit(i, "file" + i);
      assertEquals(
          assigner1.assign(split),
          assigner2.assign(split),
          "Different assigner instances should assign same split to same task"
      );
    }
  }

  @Test
  public void testAssignWithDifferentFileIds() {
    HoodieSplitNumberAssigner assigner = new HoodieSplitNumberAssigner(3);

    // Different file IDs should not affect assignment (only split number matters)
    HoodieSourceSplit split1 = createTestSplit(5, "fileA");
    HoodieSourceSplit split2 = createTestSplit(5, "fileB");
    HoodieSourceSplit split3 = createTestSplit(5, "fileC");

    assertEquals(assigner.assign(split1), assigner.assign(split2),
        "File ID should not affect assignment");
    assertEquals(assigner.assign(split1), assigner.assign(split3),
        "File ID should not affect assignment");
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
