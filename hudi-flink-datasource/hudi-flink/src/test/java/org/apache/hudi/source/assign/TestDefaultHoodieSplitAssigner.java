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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link DefaultHoodieSplitAssigner}.
 */
public class TestDefaultHoodieSplitAssigner {

  @Test
  public void testAssignWithSingleParallelism() {
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(1);

    HoodieSourceSplit split1 = createTestSplit(0, "file1");
    HoodieSourceSplit split2 = createTestSplit(1, "file2");
    HoodieSourceSplit split3 = createTestSplit(2, "file3");

    assertEquals(0, assigner.assign(split1), "All splits should be assigned to task 0");
    assertEquals(0, assigner.assign(split2), "All splits should be assigned to task 0");
    assertEquals(0, assigner.assign(split3), "All splits should be assigned to task 0");
  }

  @Test
  public void testAssignWithMultipleParallelism() {
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(3);

    // DefaultHoodieSplitAssigner uses KeyGroupRangeAssignment which uses hash-based assignment
    // We test that it assigns consistently and all assignments are within range
    HoodieSourceSplit split0 = createTestSplit(0, "file0");
    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");

    // Verify assignments are within valid range [0, parallelism)
    int task0 = assigner.assign(split0);
    int task1 = assigner.assign(split1);
    int task2 = assigner.assign(split2);

    assertTrue(task0 >= 0 && task0 < 3, "Task ID should be in range [0, 3)");
    assertTrue(task1 >= 0 && task1 < 3, "Task ID should be in range [0, 3)");
    assertTrue(task2 >= 0 && task2 < 3, "Task ID should be in range [0, 3)");
  }

  @Test
  public void testAssignBasedOnFileIdHash() {
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(5);

    // Same file ID should always map to same task
    HoodieSourceSplit split1a = createTestSplit(0, "sameFileId");
    HoodieSourceSplit split1b = createTestSplit(1, "sameFileId");
    HoodieSourceSplit split1c = createTestSplit(2, "sameFileId");

    int taskId = assigner.assign(split1a);
    assertEquals(taskId, assigner.assign(split1b), "Same file ID should map to same task");
    assertEquals(taskId, assigner.assign(split1c), "Same file ID should map to same task");
  }

  @Test
  public void testAssignWithDifferentFileIds() {
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(3);

    // Different file IDs with same split number should potentially map to different tasks
    HoodieSourceSplit split1 = createTestSplit(5, "fileA");
    HoodieSourceSplit split2 = createTestSplit(5, "fileB");
    HoodieSourceSplit split3 = createTestSplit(5, "fileC");

    // Just verify all are within range - hash distribution may vary
    int task1 = assigner.assign(split1);
    int task2 = assigner.assign(split2);
    int task3 = assigner.assign(split3);

    assertTrue(task1 >= 0 && task1 < 3, "Task ID should be in range");
    assertTrue(task2 >= 0 && task2 < 3, "Task ID should be in range");
    assertTrue(task3 >= 0 && task3 < 3, "Task ID should be in range");
  }

  @Test
  public void testAssignDistributionAcrossMultipleSplits() {
    int parallelism = 4;
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(parallelism);

    // Test that assignments are within valid range for many splits
    int totalSplits = 100;

    for (int i = 0; i < totalSplits; i++) {
      HoodieSourceSplit split = createTestSplit(i, "file" + i);
      int taskId = assigner.assign(split);

      // Verify task ID is within valid range
      assertTrue(taskId >= 0 && taskId < parallelism,
          "Task ID " + taskId + " should be in range [0, " + parallelism + ")");
    }
  }

  @Test
  public void testAssignWithHighParallelism() {
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(100);

    HoodieSourceSplit split0 = createTestSplit(0, "file0");
    HoodieSourceSplit split50 = createTestSplit(50, "file50");
    HoodieSourceSplit split99 = createTestSplit(99, "file99");

    int task0 = assigner.assign(split0);
    int task50 = assigner.assign(split50);
    int task99 = assigner.assign(split99);

    assertTrue(task0 >= 0 && task0 < 100, "Task ID should be in range [0, 100)");
    assertTrue(task50 >= 0 && task50 < 100, "Task ID should be in range [0, 100)");
    assertTrue(task99 >= 0 && task99 < 100, "Task ID should be in range [0, 100)");
  }

  @Test
  public void testAssignSameSplitMultipleTimes() {
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(5);

    HoodieSourceSplit split = createTestSplit(7, "file7");

    // Assigning the same split multiple times should return the same task ID
    int taskId = assigner.assign(split);
    assertEquals(taskId, assigner.assign(split), "Same split should always return same task ID");
    assertEquals(taskId, assigner.assign(split), "Same split should always return same task ID");
  }

  @Test
  public void testConstructorWithZeroParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new DefaultHoodieSplitAssigner(0),
        "Should throw exception for zero parallelism"
    );
    assertEquals("Parallelism must be positive, but was: 0", exception.getMessage());
  }

  @Test
  public void testConstructorWithNegativeParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new DefaultHoodieSplitAssigner(-1),
        "Should throw exception for negative parallelism"
    );
    assertEquals("Parallelism must be positive, but was: -1", exception.getMessage());
  }

  @Test
  public void testConstructorWithNegativeLargeParallelism() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new DefaultHoodieSplitAssigner(-100),
        "Should throw exception for large negative parallelism"
    );
    assertEquals("Parallelism must be positive, but was: -100", exception.getMessage());
  }

  @Test
  public void testAssignmentConsistency() {
    DefaultHoodieSplitAssigner assigner1 = new DefaultHoodieSplitAssigner(5);
    DefaultHoodieSplitAssigner assigner2 = new DefaultHoodieSplitAssigner(5);

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
  public void testAssignmentCoLocation() {
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(5);

    // Splits with same file ID should be co-located (assigned to same task)
    // regardless of split number
    HoodieSourceSplit split1 = createTestSplit(0, "commonFileId");
    HoodieSourceSplit split2 = createTestSplit(10, "commonFileId");
    HoodieSourceSplit split3 = createTestSplit(99, "commonFileId");

    int task1 = assigner.assign(split1);
    int task2 = assigner.assign(split2);
    int task3 = assigner.assign(split3);

    assertEquals(task1, task2, "Splits with same file ID should be co-located");
    assertEquals(task1, task3, "Splits with same file ID should be co-located");
  }

  @Test
  public void testKeyGroupRangeAssignment() {
    // Test with a parallelism that is not a power of 2
    // to ensure KeyGroupRangeAssignment works correctly
    DefaultHoodieSplitAssigner assigner = new DefaultHoodieSplitAssigner(7);

    int validAssignments = 0;
    for (int i = 0; i < 50; i++) {
      HoodieSourceSplit split = createTestSplit(i, "file_" + i);
      int taskId = assigner.assign(split);
      if (taskId >= 0 && taskId < 7) {
        validAssignments++;
      }
    }

    assertEquals(50, validAssignments, "All assignments should be within valid range");
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
