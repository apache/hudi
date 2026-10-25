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

package org.apache.hudi.source.split;

import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.util.StreamerUtil.EMPTY_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieCdcSourceSplit}.
 */
public class TestHoodieCdcSourceSplit {

  private static HoodieCDCFileSplit makeFileSplit(String instant) {
    return new HoodieCDCFileSplit(instant, HoodieCDCInferenceCase.BASE_FILE_INSERT, "cdc.parquet");
  }

  @Test
  public void testGetChangesReturnsConstructorValue() {
    HoodieCDCFileSplit[] changes = {makeFileSplit("20230101000000000"), makeFileSplit("20230102000000000")};
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-1",
            EMPTY_PARTITION_PATH, changes, "read_optimized", "20230101000000000");

    assertArrayEquals(changes, split.getChanges());
    assertSame(changes, split.getChanges());
  }

  @Test
  public void testGetMaxCompactionMemoryInBytes() {
    long memory = 256 * 1024 * 1024L;
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", memory, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    assertEquals(memory, split.getMaxCompactionMemoryInBytes());
  }

  @Test
  public void testGetTablePath() {
    String tablePath = "/warehouse/hudi/my_table";
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, tablePath, 1024L, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    assertEquals(tablePath, split.getTablePath());
  }

  @Test
  public void testGetFileId() {
    String fileId = "my-file-id-abc-123";
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, fileId,
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    assertEquals(fileId, split.getFileId());
  }

  @Test
  public void testSplitIdFormat() {
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    // splitId() is inherited from HoodieSourceSplit and returns "splitNum:fileId"
    assertEquals("1:file-1", split.splitId());
  }

  @Test
  public void testToStringContainsExpectedFields() {
    HoodieCDCFileSplit[] changes = {makeFileSplit("20230101000000000")};
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-xyz",
        EMPTY_PARTITION_PATH, changes, "read_optimized", "20230101000000000");

    String str = split.toString();
    assertNotNull(str);
    // Lombok @ToString uses the actual class name "HoodieCdcSourceSplit"
    assertTrue(str.contains("HoodieCdcSourceSplit"), "toString should mention class name");
    // CDC-specific fields are included
    assertTrue(str.contains("changes"), "toString should mention changes");
    assertTrue(str.contains("maxCompactionMemoryInBytes"), "toString should mention maxCompactionMemoryInBytes");
  }

  @Test
  public void testEmptyChangesArray() {
    HoodieCDCFileSplit[] changes = new HoodieCDCFileSplit[0];
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(5, "/table", 512L, "file-empty",
        EMPTY_PARTITION_PATH, changes, "read_optimized", "20230101000000000");

    assertNotNull(split.getChanges());
    assertEquals(0, split.getChanges().length);
  }

  @Test
  public void testIsInstanceOfHoodieSourceSplit() {
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    assertTrue(split instanceof HoodieSourceSplit);
  }

  @Test
  public void testLargeCompactionMemory() {
    long largeMemory = Long.MAX_VALUE;
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", largeMemory, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    assertEquals(largeMemory, split.getMaxCompactionMemoryInBytes());
  }

  @Test
  public void testDefaultConsumedAndFileOffset() {
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    assertEquals(0L, split.getConsumed());
    assertEquals(0, split.getFileOffset());
    assertFalse(split.isConsumed());
  }

  @Test
  public void testConsumeIncrementsConsumed() {
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    assertFalse(split.isConsumed());
    split.consume();
    assertTrue(split.isConsumed());
    assertEquals(1L, split.getConsumed());
    split.consume();
    split.consume();
    assertEquals(3L, split.getConsumed());
  }

  @Test
  public void testUpdatePositionSetsConsumedAndFileOffset() {
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-1",
        EMPTY_PARTITION_PATH, new HoodieCDCFileSplit[0], "read_optimized", "20230101000000000");

    split.updatePosition(7, 42L);
    assertEquals(7, split.getFileOffset());
    assertEquals(42L, split.getConsumed());
    assertTrue(split.isConsumed());
  }

  @Test
  public void testMultipleChangesWithDifferentInferenceCases() {
    HoodieCDCFileSplit insert = new HoodieCDCFileSplit("20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, "insert.parquet");
    HoodieCDCFileSplit delete = new HoodieCDCFileSplit("20230102000000000", HoodieCDCInferenceCase.BASE_FILE_DELETE, "delete.log");
    HoodieCDCFileSplit logFile = new HoodieCDCFileSplit("20230103000000000", HoodieCDCInferenceCase.LOG_FILE, "log.log");
    HoodieCDCFileSplit replace = new HoodieCDCFileSplit("20230104000000000", HoodieCDCInferenceCase.REPLACE_COMMIT, "replace.parquet");

    HoodieCDCFileSplit[] changes = {insert, delete, logFile, replace};
    HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(1, "/table", 1024L, "file-multi",
        EMPTY_PARTITION_PATH, changes, "read_optimized", "20230101000000000");

    assertEquals(4, split.getChanges().length);
    assertSame(insert, split.getChanges()[0]);
    assertSame(delete, split.getChanges()[1]);
    assertSame(logFile, split.getChanges()[2]);
    assertSame(replace, split.getChanges()[3]);
  }
}
