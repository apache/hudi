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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.table.format.cdc.CdcInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieContinuousSplitBatch}.
 */
public class TestHoodieContinuousSplitBatch {

  private static final String TABLE_PATH = "/warehouse/test_table";
  private static final long MAX_MEMORY = 128 * 1024 * 1024L;

  // -------------------------------------------------------------------------
  //  fromResult tests
  // -------------------------------------------------------------------------

  @Test
  public void testFromResultWithEmptyResult() {
    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.emptyList(), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertNotNull(batch);
    assertTrue(batch.getSplits().isEmpty());
    assertEquals("20230101000000000", batch.getEndInstant());
    assertNull(batch.getOffset());
  }

  @Test
  public void testFromResultWithCdcSplitProducesHoodieCdcSourceSplit() {
    HoodieCDCFileSplit[] changes = {
        new HoodieCDCFileSplit("20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, "insert.parquet")
    };
    CdcInputSplit cdcInputSplit = new CdcInputSplit(1, TABLE_PATH, MAX_MEMORY, "file-cdc", changes);

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.<MergeOnReadInputSplit>singletonList(cdcInputSplit), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(1, batch.getSplits().size());
    HoodieSourceSplit split = batch.getSplits().iterator().next();
    assertTrue(split instanceof HoodieCdcSourceSplit,
        "CDC input split should produce HoodieCdcSourceSplit, got: " + split.getClass());

    HoodieCdcSourceSplit cdcSplit = (HoodieCdcSourceSplit) split;
    assertEquals(TABLE_PATH, cdcSplit.getTablePath());
    assertEquals(MAX_MEMORY, cdcSplit.getMaxCompactionMemoryInBytes());
    assertEquals("file-cdc", cdcSplit.getFileId());
    assertEquals(1, cdcSplit.getChanges().length);
  }

  @Test
  public void testFromResultWithRegularSplitProducesHoodieSourceSplit() {
    MergeOnReadInputSplit morSplit = new MergeOnReadInputSplit(
        1,
        TABLE_PATH + "/base.parquet",
        Option.of(Collections.singletonList(TABLE_PATH + "/log1.log")),
        "20230101000000000",
        TABLE_PATH,
        MAX_MEMORY,
        "payload_combine",
        null,
        "file-mor");

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.singletonList(morSplit), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(1, batch.getSplits().size());
    HoodieSourceSplit split = batch.getSplits().iterator().next();
    // Should be a plain HoodieSourceSplit (not a CDC one)
    assertTrue(split instanceof HoodieSourceSplit);
    assertTrue(!(split instanceof HoodieCdcSourceSplit),
        "MOR split should produce HoodieSourceSplit, not HoodieCdcSourceSplit");
    assertEquals("file-mor", split.getFileId());
    assertEquals(TABLE_PATH, split.getTablePath());
  }

  @Test
  public void testFromResultWithMixedSplitsPreservesOrder() {
    HoodieCDCFileSplit[] changes = {
        new HoodieCDCFileSplit("20230101000000000", HoodieCDCInferenceCase.LOG_FILE, "cdc.log")
    };
    CdcInputSplit cdcInputSplit = new CdcInputSplit(1, TABLE_PATH, MAX_MEMORY, "file-cdc", changes);

    MergeOnReadInputSplit morSplit = new MergeOnReadInputSplit(
        2,
        TABLE_PATH + "/base.parquet",
        Option.empty(),
        "20230101000000000",
        TABLE_PATH,
        MAX_MEMORY,
        "payload_combine",
        null,
        "file-mor");

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Arrays.asList(cdcInputSplit, morSplit), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(2, batch.getSplits().size());
    List<HoodieSourceSplit> splits = (List<HoodieSourceSplit>) batch.getSplits();
    assertTrue(splits.get(0) instanceof HoodieCdcSourceSplit);
    assertTrue(!(splits.get(1) instanceof HoodieCdcSourceSplit));
    assertEquals("file-cdc", splits.get(0).getFileId());
    assertEquals("file-mor", splits.get(1).getFileId());
  }

  @Test
  public void testFromResultPreservesEndInstant() {
    String endInstant = "20230215120000000";
    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.emptyList(), endInstant);

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(endInstant, batch.getEndInstant());
  }

  @Test
  public void testFromResultPreservesOffset() {
    String offset = "20230215120000000";
    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.emptyList(), "20230215120000000", offset);

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(offset, batch.getOffset());
  }

  @Test
  public void testFromResultWithNullOffsetPreservesNullOffset() {
    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.emptyList(), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertNull(batch.getOffset());
  }

  @Test
  public void testFromResultCdcSplitPreservesChangesArray() {
    HoodieCDCFileSplit split1 = new HoodieCDCFileSplit(
        "20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, "insert.parquet");
    HoodieCDCFileSplit split2 = new HoodieCDCFileSplit(
        "20230102000000000", HoodieCDCInferenceCase.BASE_FILE_DELETE, "delete.log");
    HoodieCDCFileSplit[] changes = {split1, split2};

    CdcInputSplit cdcInputSplit = new CdcInputSplit(1, TABLE_PATH, MAX_MEMORY, "file-cdc", changes);

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.<MergeOnReadInputSplit>singletonList(cdcInputSplit), "20230102000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    HoodieCdcSourceSplit cdcSplit = (HoodieCdcSourceSplit) batch.getSplits().iterator().next();
    assertEquals(2, cdcSplit.getChanges().length);
    assertSame(split1, cdcSplit.getChanges()[0]);
    assertSame(split2, cdcSplit.getChanges()[1]);
  }

  @Test
  public void testFromResultWithMultipleCdcSplits() {
    HoodieCDCFileSplit[] changes1 = {
        new HoodieCDCFileSplit("20230101000000000", HoodieCDCInferenceCase.BASE_FILE_INSERT, "insert1.parquet")
    };
    HoodieCDCFileSplit[] changes2 = {
        new HoodieCDCFileSplit("20230101000000000", HoodieCDCInferenceCase.REPLACE_COMMIT, "replace.parquet")
    };

    CdcInputSplit cdc1 = new CdcInputSplit(1, TABLE_PATH, MAX_MEMORY, "file-cdc-1", changes1);
    CdcInputSplit cdc2 = new CdcInputSplit(2, TABLE_PATH, MAX_MEMORY, "file-cdc-2", changes2);

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Arrays.<MergeOnReadInputSplit>asList(cdc1, cdc2), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(2, batch.getSplits().size());
    for (HoodieSourceSplit s : batch.getSplits()) {
      assertTrue(s instanceof HoodieCdcSourceSplit);
    }
  }

  // -------------------------------------------------------------------------
  //  Constructor and EMPTY tests
  // -------------------------------------------------------------------------

  @Test
  public void testEmptyConstant() {
    HoodieContinuousSplitBatch empty = HoodieContinuousSplitBatch.EMPTY;

    assertNotNull(empty);
    assertTrue(empty.getSplits().isEmpty());
    assertEquals("", empty.getEndInstant());
    assertEquals("", empty.getOffset());
  }

  @Test
  public void testConstructorStoresFields() {
    Collection<HoodieSourceSplit> splits = Collections.singletonList(
        new HoodieSourceSplit(1, null, Option.empty(), TABLE_PATH, "", "read_optimized", "", "file-1", Option.empty()));
    String endInstant = "20230215000000000";
    String offset = "some-offset";

    HoodieContinuousSplitBatch batch = new HoodieContinuousSplitBatch(splits, endInstant, offset);

    assertSame(splits, batch.getSplits());
    assertEquals(endInstant, batch.getEndInstant());
    assertEquals(offset, batch.getOffset());
  }

  @Test
  public void testConstructorRejectsNullSplits() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieContinuousSplitBatch(null, "20230101000000000", null));
  }

  @Test
  public void testConstructorRejectsNullEndInstant() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieContinuousSplitBatch(Collections.emptyList(), null, null));
  }

  // -------------------------------------------------------------------------
  //  resolvePartitionPath coverage tests
  // -------------------------------------------------------------------------

  @Test
  public void testFromResultMorSplitWithLogPathsOnlyDerivesPartitionPath() {
    // Split has no base file, only log paths; partition path should be derived from the first log path
    MergeOnReadInputSplit logOnlySplit = new MergeOnReadInputSplit(
        1,
        null,
        Option.of(Collections.singletonList(TABLE_PATH + "/2023/01/log.log")),
        "20230101000000000",
        TABLE_PATH,
        MAX_MEMORY,
        "payload_combine",
        null,
        "file-log-only");

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.singletonList(logOnlySplit), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(1, batch.getSplits().size());
    HoodieSourceSplit split = batch.getSplits().iterator().next();
    // Partition path is derived relative to the table path
    assertEquals("2023/01", split.getPartitionPath());
  }

  @Test
  public void testFromResultMorSplitWithNoFilesHasEmptyPartitionPath() {
    // Split has neither base file nor log paths; partition path must be empty string
    MergeOnReadInputSplit emptyPathSplit = new MergeOnReadInputSplit(
        1,
        null,
        Option.empty(),
        "20230101000000000",
        TABLE_PATH,
        MAX_MEMORY,
        "read_optimized",
        null,
        "file-no-paths");

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.singletonList(emptyPathSplit), "20230101000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(1, batch.getSplits().size());
    HoodieSourceSplit split = batch.getSplits().iterator().next();
    assertEquals("", split.getPartitionPath());
  }

  @Test
  public void testFromResultCdcSplitWithEmptyChangesUsesEndInstantAsLatestCommit() {
    // When a CDC split has no changes, the batch end instant is used as the latest commit
    CdcInputSplit cdcWithNoChanges = new CdcInputSplit(1, TABLE_PATH, MAX_MEMORY, "file-cdc-empty",
        new HoodieCDCFileSplit[0]);
    String endInstant = "20230301000000000";

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.<MergeOnReadInputSplit>singletonList(cdcWithNoChanges), endInstant);

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    assertEquals(1, batch.getSplits().size());
    HoodieCdcSourceSplit cdcSplit = (HoodieCdcSourceSplit) batch.getSplits().iterator().next();
    assertEquals(0, cdcSplit.getChanges().length);
    // With no changes, latestCommit must fall back to the batch end instant
    assertEquals(endInstant, cdcSplit.getLatestCommit());
  }

  @Test
  public void testFromResultMorSplitWithSubdirectoryPartitionPath() {
    // Verifies multi-level partition paths (e.g. year=2023/month=01/day=15) are resolved correctly
    MergeOnReadInputSplit partitionedSplit = new MergeOnReadInputSplit(
        1,
        TABLE_PATH + "/year=2023/month=01/day=15/base.parquet",
        Option.empty(),
        "20230115000000000",
        TABLE_PATH,
        MAX_MEMORY,
        "read_optimized",
        null,
        "file-partitioned");

    IncrementalInputSplits.Result result = IncrementalInputSplits.Result.instance(
        Collections.singletonList(partitionedSplit), "20230115000000000");

    HoodieContinuousSplitBatch batch = HoodieContinuousSplitBatch.fromResult(result);

    HoodieSourceSplit split = batch.getSplits().iterator().next();
    assertEquals("year=2023/month=01/day=15", split.getPartitionPath());
  }
}
