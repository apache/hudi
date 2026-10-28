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

package org.apache.hudi.core.read;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.core.read.BaseHoodieTableFileIndex.PartitionPath;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class BaseHoodieTableFileIndexTest {

  /**
   * A realistic absolute table base path. Listing never produces a relative one, and the difference matters to the
   * weigher: it charges per character of the full file path.
   */
  private static final String TABLE_BASE_PATH = "file:/tmp/junit-6913148002031446690/dataset";

  /**
   * Regression test for the empty-partition NPE that surfaces in {@code getInputFileSlices}
   * when the {@code hoodie.datasource.read.file.index.list.file.statuses.using.ro.path.filter}
   * code path is exercised on a COW (or READ_OPTIMIZED) table that contains a partition
   * holding zero base files.
   *
   * <p>Before the fix, {@link BaseHoodieTableFileIndex#generatePartitionFileSlicesPostROTablePathFilter}
   * built its result map by iterating over the file list, so a partition with no files received
   * no entry. The downstream {@code Collectors.toMap(identity, p -> cache.get(p))} in
   * {@code getInputFileSlices} then dereferenced a null value and threw NPE inside
   * {@code Collectors.uniqKeysMapAccumulator}.
   *
   * <p>After the fix, every input partition appears in the returned map (with an empty list
   * for empty partitions), preserving the contract already honored by the non-RO path
   * ({@code filterFiles}).
   */
  @Test
  public void testGeneratePartitionFileSlicesPostROTablePathFilterIncludesEmptyPartitions() throws Exception {
    BaseHoodieTableFileIndex fileIndex = mock(BaseHoodieTableFileIndex.class,
        org.mockito.Mockito.CALLS_REAL_METHODS);

    StoragePath basePath = new StoragePath("/tmp/hudi_empty_partition_test");
    Field basePathField = BaseHoodieTableFileIndex.class.getDeclaredField("basePath");
    basePathField.setAccessible(true);
    basePathField.set(fileIndex, basePath);

    PartitionPath partitionWithFiles = new PartitionPath("dt=2026-01-01", new Object[]{"2026-01-01"});
    PartitionPath emptyPartition = new PartitionPath("dt=2026-01-02", new Object[]{"2026-01-02"});
    PartitionPath anotherEmpty = new PartitionPath("dt=2026-01-03", new Object[]{"2026-01-03"});
    List<PartitionPath> partitions = Arrays.asList(partitionWithFiles, emptyPartition, anotherEmpty);

    StoragePathInfo file = new StoragePathInfo(
        new StoragePath(basePath, "dt=2026-01-01/file-0_0-0-0_20260101000000001.parquet"),
        100L, false, (short) 1, 1024L, 0L);
    List<StoragePathInfo> allFiles = Collections.singletonList(file);

    Method generateMethod = BaseHoodieTableFileIndex.class.getDeclaredMethod(
        "generatePartitionFileSlicesPostROTablePathFilter", List.class, List.class);
    generateMethod.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<PartitionPath, List<FileSlice>> result =
        (Map<PartitionPath, List<FileSlice>>) generateMethod.invoke(fileIndex, partitions, allFiles);

    assertNotNull(result, "Result map must not be null");
    assertEquals(3, result.size(),
        "Result map must contain an entry for every input partition, including empty ones");
    assertTrue(result.containsKey(partitionWithFiles));
    assertTrue(result.containsKey(emptyPartition),
        "Empty partition must appear in the result so getInputFileSlices does not NPE");
    assertTrue(result.containsKey(anotherEmpty),
        "Empty partition must appear in the result so getInputFileSlices does not NPE");
    assertEquals(1, result.get(partitionWithFiles).size(),
        "Partition with files should retain its file slice");
    assertTrue(result.get(emptyPartition).isEmpty(),
        "Empty partition's file slice list must be present and empty (not null, not missing)");
    assertTrue(result.get(anotherEmpty).isEmpty(),
        "Empty partition's file slice list must be present and empty (not null, not missing)");
  }

  /**
   * {@code cachedAllInputFileSlices} is budgeted in bytes rather than in partitions, because a partition's file-slice
   * list scales with the number of file groups (and, for MOR, log files) the partition holds. Measured with Hudi's own
   * ObjectSizeCalculator, 10000 partitions hold 16 MiB at one file group each and 1205 MiB at a hundred -- so no single
   * partition count can be a sane heap budget for both shapes. This pins that the weight of an entry actually tracks
   * how much the entry holds, along both axes it varies along: file groups, and log files per file group.
   */
  @Test
  public void testFileSliceCacheWeightGrowsWithTheEntryContents() {
    PartitionPath partition = new PartitionPath("dt=2026-01-01", new Object[]{"2026-01-01"});
    List<FileSlice> empty = Collections.emptyList();
    List<FileSlice> one = fileSlices(partition, 1, 0);
    List<FileSlice> many = fileSlices(partition, 50, 0);
    List<FileSlice> oneWithLogs = fileSlices(partition, 1, 10);

    int weightOfEmpty = BaseHoodieTableFileIndex.estimateFileSliceCacheWeight(partition, empty);
    int weightOfOne = BaseHoodieTableFileIndex.estimateFileSliceCacheWeight(partition, one);
    int weightOfMany = BaseHoodieTableFileIndex.estimateFileSliceCacheWeight(partition, many);
    int weightOfOneWithLogs = BaseHoodieTableFileIndex.estimateFileSliceCacheWeight(partition, oneWithLogs);

    assertTrue(weightOfEmpty > 0, "Even an empty entry must have a positive weight");
    assertTrue(weightOfOne > weightOfEmpty, "An entry holding a file slice must outweigh an empty one");
    assertTrue(weightOfMany > weightOfOne * 10,
        "50 file slices (" + weightOfMany + " bytes) must weigh far more than one (" + weightOfOne + " bytes)");
    // MOR: one file group with a stack of log files holds (and must be charged for) far more than a bare one
    assertTrue(weightOfOneWithLogs > weightOfOne * 5,
        "A file slice with 10 log files (" + weightOfOneWithLogs + " bytes) must weigh far more than a bare one ("
            + weightOfOne + " bytes)");
  }

  /**
   * The weigher runs on every single insertion into a cache that lazy listing populates one partition at a time, so it
   * has to be cheap: walking the entry's object graph with {@code ObjectSizeCalculator} measured about 90us per
   * partition at 20 file groups once warm (380us on the first call) and 0.5-0.9ms at 140 file slices, against 0.4us
   * and 0.8us for counting structure -- which is tens of seconds of driver CPU for one cold plan of a wide table.
   * The absolute figures are machine- and load-dependent; the three-orders-of-magnitude ratio is not.
   * This pins that the weigher's cost does not grow with what the entry describes beyond the one unavoidable pass over
   * the file-slice list: the per-slice work must be O(1), which rules out re-walking each slice's files.
   */
  @Test
  public void testFileSliceCacheWeightIsLinearInSliceCountAndFreeOfLogFileCount() {
    PartitionPath partition = new PartitionPath("dt=2026-01-01", new Object[]{"2026-01-01"});
    // Same slice count, 100x the log files: an O(files) weigher would take ~100x longer, an O(slices) one the same
    long fewLogs = timeWeigh(partition, fileSlices(partition, 200, 1));
    long manyLogs = timeWeigh(partition, fileSlices(partition, 200, 100));

    assertTrue(manyLogs < Math.max(fewLogs, 1_000L) * 20,
        "Weighing 200 slices took " + fewLogs + "ns with 1 log file each and " + manyLogs
            + "ns with 100, so the weigher is walking the files rather than counting them");
  }

  private static long timeWeigh(PartitionPath partition, List<FileSlice> slices) {
    for (int i = 0; i < 1000; i++) {
      BaseHoodieTableFileIndex.estimateFileSliceCacheWeight(partition, slices);
    }
    long start = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      BaseHoodieTableFileIndex.estimateFileSliceCacheWeight(partition, slices);
    }
    return (System.nanoTime() - start) / 1000;
  }

  /**
   * Listing hands the index file slices whose paths are <em>absolute</em> -- the table base path, then the partition
   * path, then the file name -- and the weigher is calibrated on that. An earlier version of this helper built paths
   * as {@code partition.getPath() + "/" + name}, i.e. relative ones, which for a {@code dt=0} partition is 12
   * characters standing in for a real 117, and calibrating against that shape is exactly how the weigher came to
   * under-charge every object-store table by around 1.5x. Build them the way listing does.
   */
  private static List<FileSlice> fileSlices(PartitionPath partition, int count, int logFilesPerSlice) {
    List<FileSlice> slices = new ArrayList<>();
    String partitionDir = TABLE_BASE_PATH + "/" + partition.getPath();
    for (int i = 0; i < count; i++) {
      FileSlice slice = new FileSlice(partition.getPath(), "20260101000000001", "file-" + i);
      slice.setBaseFile(new HoodieBaseFile(pathInfo(
          partitionDir + "/file-" + i + "_0-1-2_20260101000000001.parquet")));
      for (int l = 0; l < logFilesPerSlice; l++) {
        slice.addLogFile(new HoodieLogFile(pathInfo(
            partitionDir + "/.file-" + i + "_20260101000000001.log." + (l + 1) + "_0-1-2")));
      }
      slices.add(slice);
    }
    return slices;
  }

  /**
   * Materialises what listing materialises -- the {@code URI}'s parsed components, the cached {@code toString} and
   * the cached parent -- so that an {@code ObjectSizeCalculator} run over these slices sees the same graph a listed
   * entry has.
   */
  private static StoragePathInfo pathInfo(String path) {
    StoragePath storagePath = new StoragePath(path);
    storagePath.toUri().getPath();
    storagePath.toString();
    storagePath.getName();
    storagePath.getParent();
    return new StoragePathInfo(storagePath, 1024L, false, (short) 1, 1024L, 1700000000000L);
  }

  /**
   * The incremental partition listing reads written partitions from the write timeline, so the
   * "has the start been archived" guard must be evaluated against that same timeline. A completed
   * rollback (or clean) older than every active commit must not make an archived start look active,
   * otherwise only the partitions of the active commits in the range are listed.
   *
   * <p>Timeline: rollback 001 (completed 002), commit 005 (completed 006), commit 007 (completed 008).
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIncrementalStartBeforeFirstWriteIsArchivedDespiteOlderNonWriteInstant(boolean completionTimeBased) throws Exception {
    HoodieTimeline activeTimeline = new MockHoodieTimeline(Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "001", "002"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "005", "006"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "007", "008")));

    // start after the rollback but before the first commit: archived
    assertTrue(BaseHoodieTableFileIndex.isBeforeTimelineStarts(activeTimeline, completionTimeBased, "003"));
    // start at or after the first commit: covered by the active write timeline
    assertFalse(BaseHoodieTableFileIndex.isBeforeTimelineStarts(activeTimeline, completionTimeBased, completionTimeBased ? "006" : "005"));
    assertFalse(BaseHoodieTableFileIndex.isBeforeTimelineStarts(activeTimeline, completionTimeBased, "007"));
  }

  /**
   * With archival beyond savepoint, a savepointed commit stays on the active timeline while the commits after it are
   * archived. A start in that hole is archived, so the savepointed commit must not count as the start of the timeline.
   *
   * <p>Timeline: commit 001 (completed 002) with savepoint 001, commit 010 (completed 011), commit 012 (completed 013).
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIncrementalStartInArchivalHoleAfterSavepointIsArchived(boolean completionTimeBased) throws Exception {
    HoodieTimeline activeTimeline = new MockHoodieTimeline(Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001", "002"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "001", "003"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "010", "011"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "012", "013")));

    // start between the savepointed commit and the first active commit after it: archived
    assertTrue(BaseHoodieTableFileIndex.isBeforeTimelineStarts(activeTimeline, completionTimeBased, "005"));
    assertFalse(BaseHoodieTableFileIndex.isBeforeTimelineStarts(activeTimeline, completionTimeBased, completionTimeBased ? "011" : "010"));
  }

  /**
   * Without a completed write instant on the active timeline the written partitions are unknown, so every start is
   * treated as archived and all partitions are listed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIncrementalStartIsArchivedWithoutActiveWriteInstant(boolean completionTimeBased) throws Exception {
    HoodieTimeline activeTimeline = new MockHoodieTimeline(Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "001", "002"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "003", "004")));

    assertTrue(BaseHoodieTableFileIndex.isBeforeTimelineStarts(activeTimeline, completionTimeBased, "000"));
    assertTrue(BaseHoodieTableFileIndex.isBeforeTimelineStarts(activeTimeline, completionTimeBased, "005"));

    // a pending write instant alone does not tell which partitions were written either
    HoodieTimeline pendingOnlyTimeline = new MockHoodieTimeline(Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "001", "002"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "003")));
    assertTrue(BaseHoodieTableFileIndex.isBeforeTimelineStarts(pendingOnlyTimeline, completionTimeBased, "005"));
  }
}
