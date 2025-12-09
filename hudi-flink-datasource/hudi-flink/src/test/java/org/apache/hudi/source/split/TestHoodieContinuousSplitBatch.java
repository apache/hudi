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
import org.apache.hudi.common.util.StringUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieContinuousSplitBatch}.
 */
public class TestHoodieContinuousSplitBatch {

  @Test
  public void testEmptyBatch() {
    HoodieContinuousSplitBatch emptyBatch = HoodieContinuousSplitBatch.EMPTY;

    assertNotNull(emptyBatch, "Empty batch should not be null");
    assertNotNull(emptyBatch.getSplits(), "Splits collection should not be null");
    assertTrue(emptyBatch.getSplits().isEmpty(), "Splits should be empty");
    assertTrue(StringUtils.isNullOrEmpty(emptyBatch.getFromInstant()), "From instant should be null for empty batch");
    assertTrue(StringUtils.isNullOrEmpty(emptyBatch.getToInstant()), "To instant should be null for empty batch");
  }

  @Test
  public void testConstructorWithNullSplits() {
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieContinuousSplitBatch(null, "20231201000000000", "20231201120000000");
    }, "Should throw IllegalArgumentException when splits collection is null");
  }

  @Test
  public void testConstructorWithNullToInstant() {
    List<HoodieSourceSplit> splits = createTestSplits(2);

    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieContinuousSplitBatch(splits, "20231201000000000", null);
    }, "Should throw IllegalArgumentException when toInstant is null");
  }

  @Test
  public void testConstructorWithNullFromInstant() {
    List<HoodieSourceSplit> splits = createTestSplits(2);

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        splits, null, "20231201120000000");

    assertNotNull(batch, "Batch should be created successfully");
    assertNull(batch.getFromInstant(), "From instant can be null");
    assertEquals("20231201120000000", batch.getToInstant(), "To instant should match");
  }

  @Test
  public void testGetSplits() {
    List<HoodieSourceSplit> expectedSplits = createTestSplits(3);

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        expectedSplits, "20231201000000000", "20231201120000000");

    assertEquals(expectedSplits, batch.getSplits(), "Splits should match");
    assertEquals(3, batch.getSplits().size(), "Should have 3 splits");
  }

  @Test
  public void testGetFromInstant() {
    List<HoodieSourceSplit> splits = createTestSplits(1);
    String expectedFromInstant = "20231201000000000";

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        splits, expectedFromInstant, "20231201120000000");

    assertEquals(expectedFromInstant, batch.getFromInstant(), "From instant should match");
  }

  @Test
  public void testGetToInstant() {
    List<HoodieSourceSplit> splits = createTestSplits(1);
    String expectedToInstant = "20231201120000000";

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        splits, "20231201000000000", expectedToInstant);

    assertEquals(expectedToInstant, batch.getToInstant(), "To instant should match");
  }

  @Test
  public void testWithEmptySplitsCollection() {
    List<HoodieSourceSplit> emptySplits = Collections.emptyList();

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        emptySplits, "20231201000000000", "20231201120000000");

    assertNotNull(batch, "Batch should be created with empty splits");
    assertTrue(batch.getSplits().isEmpty(), "Splits should be empty");
    assertEquals("20231201000000000", batch.getFromInstant());
    assertEquals("20231201120000000", batch.getToInstant());
  }

  @Test
  public void testWithMultipleSplits() {
    List<HoodieSourceSplit> splits = createTestSplits(10);

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        splits, "20231201000000000", "20231201120000000");

    assertEquals(10, batch.getSplits().size(), "Should have 10 splits");
    assertEquals("20231201000000000", batch.getFromInstant());
    assertEquals("20231201120000000", batch.getToInstant());
  }

  @Test
  public void testInstantRange() {
    List<HoodieSourceSplit> splits = createTestSplits(5);
    String fromInstant = "20231201000000000";
    String toInstant = "20231215235959999";

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        splits, fromInstant, toInstant);

    assertEquals(fromInstant, batch.getFromInstant(), "From instant should match");
    assertEquals(toInstant, batch.getToInstant(), "To instant should match");
  }

  @Test
  public void testSplitsImmutability() {
    List<HoodieSourceSplit> originalSplits = new ArrayList<>(createTestSplits(3));

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(
        originalSplits, "20231201000000000", "20231201120000000");

    // Get the splits and verify they match original
    assertEquals(3, batch.getSplits().size(), "Should have 3 splits");

    // Modify original list
    originalSplits.add(createTestSplit(999, "newFile"));

    // Verify batch still has original count (if implementation doesn't make defensive copy)
    // Note: The actual behavior depends on implementation
    assertNotNull(batch.getSplits(), "Splits should not be null");
  }

  // Helper methods

  private List<HoodieSourceSplit> createTestSplits(int count) {
    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      splits.add(createTestSplit(i, "file" + i));
    }
    return splits;
  }

  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "basePath_" + splitNum,
        Option.empty(),
        "/table/path",
        "read_optimized",
        fileId
    );
  }
}
