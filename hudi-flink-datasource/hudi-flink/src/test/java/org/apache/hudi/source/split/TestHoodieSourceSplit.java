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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieSourceSplit}.
 */
public class TestHoodieSourceSplit {

  @Test
  public void testEqualsWithIdenticalSplits() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file1", "/partition1");

    assertEquals(split1, split2);
    assertEquals(split1.hashCode(), split2.hashCode());
  }

  @Test
  public void testEqualsWithSameInstance() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");

    assertEquals(split, split);
    assertEquals(split.hashCode(), split.hashCode());
  }

  @Test
  public void testEqualsWithNull() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");

    assertNotEquals(split, null);
  }

  @Test
  public void testEqualsWithDifferentClass() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");
    String differentObject = "not a split";

    assertNotEquals(split, differentObject);
  }

  @Test
  public void testEqualsWithDifferentSplitNum() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(2, "file1", "/partition1");

    assertNotEquals(split1, split2);
    assertNotEquals(split1.hashCode(), split2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentFileId() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file2", "/partition1");

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentPartitionPath() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file1", "/partition2");

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentBasePath() {
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1, "base-path-1", Option.empty(), "/table", "/partition1",  "read_optimized", "file1");
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        1, "base-path-2", Option.empty(), "/table", "/partition1", "read_optimized", "file1");

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentLogPaths() {
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1, "base-path", Option.of(Arrays.asList("log1", "log2")), "/table", "/partition1", "payload_combine", "file1");
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        1, "base-path", Option.of(Arrays.asList("log1", "log3")), "/table", "/partition1",  "payload_combine", "file1");

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentTablePath() {
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1, "base-path", Option.empty(), "/table1", "/partition1",  "read_optimized", "file1");
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        1, "base-path", Option.empty(), "/table2", "/partition1", "read_optimized", "file1");

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentMergeType() {
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1, "base-path", Option.empty(), "/table", "/partition1", "read_optimized", "file1");
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        1, "base-path", Option.empty(), "/table", "/partition1", "payload_combine", "file1");

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentConsumedValue() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file1", "/partition1");

    split1.consume();

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentFileOffset() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file1", "/partition1");

    split1.updatePosition(5, 0L);

    assertNotEquals(split1, split2);
  }

  @Test
  public void testEqualsWithDifferentRecordOffset() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file1", "/partition1");

    split1.updatePosition(0, 100L);

    assertNotEquals(split1, split2);
  }

  @Test
  public void testHashCodeConsistency() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");

    int hashCode1 = split.hashCode();
    int hashCode2 = split.hashCode();

    assertEquals(hashCode1, hashCode2);
  }

  @Test
  public void testHashCodeWithIdenticalSplits() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file1", "/partition1");

    assertEquals(split1.hashCode(), split2.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentSplits() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2", "/partition2");

    // While hash codes could theoretically collide, they should be different for different splits
    assertNotEquals(split1.hashCode(), split2.hashCode());
  }

  @Test
  public void testUpdatePosition() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");

    assertEquals(0, split.getFileOffset());
    assertEquals(0L, split.getConsumed());

    split.updatePosition(5, 100L);

    assertEquals(5, split.getFileOffset());
    assertEquals(100L, split.getConsumed());
  }

  @Test
  public void testConsume() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");

    assertFalse(split.isConsumed());
    assertEquals(0L, split.getConsumed());

    split.consume();

    assertTrue(split.isConsumed());
    assertEquals(1L, split.getConsumed());

    split.consume();
    assertEquals(2L, split.getConsumed());
  }

  @Test
  public void testGetters() {
    String basePath = "base-path";
    String tablePath = "/table/path";
    String partitionPath = "/partition/path";
    String mergeType = "payload_combine";
    String fileId = "file-123";

    HoodieSourceSplit split = new HoodieSourceSplit(
        42, basePath, Option.of(Arrays.asList("log1", "log2")),
        tablePath, partitionPath, mergeType, fileId);

    assertTrue(split.getBasePath().isPresent());
    assertEquals(basePath, split.getBasePath().get());
    assertTrue(split.getLogPaths().isPresent());
    assertEquals(2, split.getLogPaths().get().size());
    assertEquals(tablePath, split.getTablePath());
    assertEquals(partitionPath, split.getPartitionPath());
    assertEquals(mergeType, split.getMergeType());
    assertEquals(fileId, split.getFileId());
  }

  @Test
  public void testSetFileId() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");

    assertEquals("file1", split.getFileId());

    split.setFileId("new-file-id");

    assertEquals("new-file-id", split.getFileId());
  }

  @Test
  public void testSplitId() {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");

    // splitId() returns toString()
    String splitId = split.splitId();
    String toString = split.toString();

    assertEquals(toString, splitId);
    assertTrue(splitId.contains("HoodieSourceSplit"));
  }

  @Test
  public void testToString() {
    HoodieSourceSplit split = new HoodieSourceSplit(
        1, "base-path", Option.of(Arrays.asList("log1")),
        "/table", "/partition", "read_optimized", "file1");

    String result = split.toString();

    assertTrue(result.contains("HoodieSourceSplit"));
    assertTrue(result.contains("splitNum=1"));
    assertTrue(result.contains("basePath"));
    assertTrue(result.contains("logPaths"));
    assertTrue(result.contains("tablePath"));
    assertTrue(result.contains("partitionPath"));
    assertTrue(result.contains("mergeType"));
  }

  @Test
  public void testEqualsAfterModification() {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(1, "file1", "/partition1");

    assertEquals(split1, split2);

    // Modify split1
    split1.consume();
    split1.updatePosition(5, 100L);

    assertNotEquals(split1, split2);

    // Apply same modifications to split2
    split2.consume();
    split2.updatePosition(5, 100L);

    assertEquals(split1, split2);
  }

  @Test
  public void testEqualsWithNullBasePath() {
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1, null, Option.empty(), "/table", "/partition","read_optimized", "file1");
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        1, null, Option.empty(), "/table", "/partition","read_optimized", "file1");

    assertEquals(split1, split2);
  }

  @Test
  public void testEqualsOneNullBasePathOneNot() {
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1, null, Option.empty(), "/table", "/partition", "read_optimized", "file1");
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        1, "base-path", Option.empty(), "/table", "/partition", "read_optimized", "file1");

    assertNotEquals(split1, split2);
  }

  /**
   * Helper method to create a test HoodieSourceSplit.
   */
  private HoodieSourceSplit createTestSplit(int splitNum, String fileId, String partitionPath) {
    return new HoodieSourceSplit(
        splitNum,
        "base-path-" + splitNum,
        Option.of(Collections.emptyList()),
        "/test/table",
        partitionPath,
        "read_optimized",
        fileId
    );
  }
}
