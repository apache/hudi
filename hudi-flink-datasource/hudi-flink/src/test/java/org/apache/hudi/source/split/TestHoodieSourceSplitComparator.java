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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieSourceSplitComparator}.
 */
public class TestHoodieSourceSplitComparator {
  private HoodieSourceSplitComparator comparator;

  @BeforeEach
  public void setUp() {
    comparator = new HoodieSourceSplitComparator();
  }

  @Test
  public void testCompareWithDifferentLatestCommits() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Split with earlier commit time should come first");
  }

  @Test
  public void testCompareWithSameLatestCommits() {
    // Both have same latest commit but different base file commit times
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Split with earlier base file commit time should come first");
  }

  @Test
  public void testCompareWithIdenticalCommits() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    int result = comparator.compare(split1, split2);
    assertEquals(0, result, "Identical splits should return 0");
  }

  @Test
  public void testCompareReverseOrder() {
    HoodieSourceSplit split1 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    int result = comparator.compare(split1, split2);
    assertTrue(result > 0, "Split with later commit time should come after");
  }

  @Test
  public void testCompareWithNullLatestCommit() {
    HoodieSourceSplit split1 = createSplit(null, "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    assertThrows(IllegalArgumentException.class, () -> comparator.compare(split1, split2),
        "Should throw exception when first split has null latest commit");
  }

  @Test
  public void testCompareWithEmptyLatestCommit() {
    HoodieSourceSplit split1 = createSplit("", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    assertThrows(IllegalArgumentException.class, () -> comparator.compare(split1, split2),
        "Should throw exception when first split has empty latest commit");
  }

  @Test
  public void testCompareWithBothNullLatestCommits() {
    HoodieSourceSplit split1 = createSplit(null, "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit(null, "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    assertThrows(IllegalArgumentException.class, () -> comparator.compare(split1, split2),
        "Should throw exception when both splits have null latest commit");
  }

  @Test
  public void testCompareMultipleSplitsOrdering() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split3 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split4 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034718000.parquet");

    // Verify ordering
    assertTrue(comparator.compare(split1, split2) < 0);
    assertTrue(comparator.compare(split1, split3) < 0);
    assertTrue(comparator.compare(split1, split4) < 0);
    assertTrue(comparator.compare(split2, split3) < 0);
    assertTrue(comparator.compare(split2, split4) < 0);
    assertTrue(comparator.compare(split3, split4) < 0);
  }

  @Test
  public void testCompareWithOrcFiles() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.orc");
    HoodieSourceSplit split2 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.orc");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Should work with ORC file extensions");
  }

  @Test
  public void testCompareWithLogFiles() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.log");
    HoodieSourceSplit split2 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.log");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Should work with log file extensions");
  }

  @Test
  public void testCompareWithComplexFilePaths() {
    HoodieSourceSplit split1 = createSplit("20260126034716930",
        "/table/path/partition1/09983c46-6160-48e7-a8ec-4e69f742b4ce-0_83-512-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034717000",
        "/table/path/partition2/09983c46-6160-48e7-a8ec-4e69f742b4ce-0_83-512-1_20260126034717000.parquet");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Should work with complex file paths");
  }

  @Test
  public void testCompareWithVeryCloseCommitTimes() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716931", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716931.parquet");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Should correctly order very close commit times");
  }

  @Test
  public void testComparatorIsConsistent() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");

    int result1 = comparator.compare(split1, split2);
    int result2 = comparator.compare(split1, split2);
    assertEquals(result1, result2, "Comparator should be consistent");
  }

  @Test
  public void testComparatorSymmetry() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");

    int result1 = comparator.compare(split1, split2);
    int result2 = comparator.compare(split2, split1);
    assertEquals(-Integer.signum(result1), Integer.signum(result2),
        "Comparator should be symmetric");
  }

  @Test
  public void testComparatorTransitivity() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split3 = createSplit("20260126034718000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034718000.parquet");

    assertTrue(comparator.compare(split1, split2) < 0);
    assertTrue(comparator.compare(split2, split3) < 0);
    assertTrue(comparator.compare(split1, split3) < 0,
        "Comparator should be transitive");
  }

  @Test
  public void testCompareWithNullBasePath() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", null);
    HoodieSourceSplit split2 = createSplit("20260126034716930", null);

    int result = comparator.compare(split1, split2);
    assertEquals(0, result, "Splits with same latest commit and null base paths should be equal");
  }

  @Test
  public void testCompareWithOneNullBasePath() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", null);
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Split with null base path should come before split with base path");
  }

  @Test
  public void testCompareWithDifferentLatestCommitOverridesBasePathOrder() {
    // Latest commit takes precedence over base path commit time
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034718000.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    int result = comparator.compare(split1, split2);
    assertTrue(result < 0, "Latest commit should take precedence over base file commit time");
  }

  @Test
  public void testCompareWithNonStandardFileNames() {
    // Test with file names that might not have standard Hudi naming pattern
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-2_20260126034716930.parquet");

    // Should not throw exception and should compare based on latest commit
    int result = comparator.compare(split1, split2);
    assertEquals(0, result, "When latest commits are equal and base paths don't contain commit times, should return 0");
  }

  @Test
  public void testCompareWithSecondSplitNullLatestCommit() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit(null, "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    assertThrows(IllegalArgumentException.class, () -> comparator.compare(split1, split2),
        "Should throw exception when second split has null latest commit");
  }

  @Test
  public void testCompareWithSecondSplitEmptyLatestCommit() {
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split2 = createSplit("", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    assertThrows(IllegalArgumentException.class, () -> comparator.compare(split1, split2),
        "Should throw exception when second split has empty latest commit");
  }

  @Test
  public void testCompareWithSameLatestCommitDifferentBaseFileCommitTimes() {
    // Testing secondary sort by base file commit time
    HoodieSourceSplit split1 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716000.parquet");
    HoodieSourceSplit split2 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716500.parquet");
    HoodieSourceSplit split3 = createSplit("20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");

    assertTrue(comparator.compare(split1, split2) < 0);
    assertTrue(comparator.compare(split2, split3) < 0);
    assertTrue(comparator.compare(split1, split3) < 0);
  }

  private HoodieSourceSplit createSplit(String latestCommit, String basePath) {
    return new HoodieSourceSplit(
        1,
        basePath,
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        latestCommit,
        "file-1",
        Option.empty()
    );
  }
}
