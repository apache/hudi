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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.read.BaseHoodieTableFileIndex.PartitionPath;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
import static org.mockito.Mockito.when;

public class BaseHoodieTableFileIndexTest {

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
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(new MockHoodieTimeline(Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "001", "002"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "005", "006"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "007", "008"))));

    // start after the rollback but before the first commit: archived
    assertTrue(isBeforeTimelineStarts(metaClient, completionTimeBased, "003"));
    // start at or after the first commit: covered by the active write timeline
    assertFalse(isBeforeTimelineStarts(metaClient, completionTimeBased, completionTimeBased ? "006" : "005"));
    assertFalse(isBeforeTimelineStarts(metaClient, completionTimeBased, "007"));
  }

  private static boolean isBeforeTimelineStarts(HoodieTableMetaClient metaClient, boolean completionTimeBased, String start)
      throws Exception {
    BaseHoodieTableFileIndex fileIndex = mock(BaseHoodieTableFileIndex.class, org.mockito.Mockito.CALLS_REAL_METHODS);
    setField(fileIndex, "metaClient", metaClient);
    setField(fileIndex, "isCompletionTimeBasedQuery", completionTimeBased);
    setField(fileIndex, "incrementalQueryStartTime", Option.of(start));
    Method method = BaseHoodieTableFileIndex.class.getDeclaredMethod("isBeforeTimelineStarts");
    method.setAccessible(true);
    return (boolean) method.invoke(fileIndex);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = BaseHoodieTableFileIndex.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
