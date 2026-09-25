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
import org.apache.hudi.core.read.BaseHoodieTableFileIndex.PartitionPath;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

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
   * {@code cachedAllInputFileSlices} used to be a plain {@link java.util.HashMap} that nothing ever removed from:
   * it was only ever inserted into (via {@code ensurePreloadedPartitions}) and only ever discarded wholesale by
   * {@code doRefresh()}, which is called from the constructor and from {@code refresh()} and never on the query path.
   * With lazy listing and a file index reused for the lifetime of a session, it therefore accumulated one entry per
   * distinct partition ever queried. This asserts the replacement retains at most the configured number of
   * partitions no matter how many distinct partitions are looked up.
   */
  @Test
  public void testBoundedFileSliceCacheDoesNotGrowPastItsBound() {
    int bound = 16;
    BaseHoodieTableFileIndex.BoundedFileSliceCache cache =
        new BaseHoodieTableFileIndex.BoundedFileSliceCache(bound);

    for (int i = 0; i < 2000; i++) {
      PartitionPath partition = new PartitionPath("dt=" + i, new Object[]{String.valueOf(i)});
      cache.put(partition, Collections.emptyList());
      assertTrue(cache.size() <= bound,
          "Cache grew to " + cache.size() + " entries after " + (i + 1) + " distinct partitions, bound is " + bound);
    }

    assertEquals(bound, cache.size(), "A saturated cache should hold exactly its bound");
  }

  /**
   * Eviction has to be least-recently-*used*, not merely least-recently-inserted: a dashboard that keeps querying the
   * same handful of partitions while a backfill sweeps the long tail must not have its hot partitions evicted.
   */
  @Test
  public void testBoundedFileSliceCacheEvictsLeastRecentlyUsed() {
    BaseHoodieTableFileIndex.BoundedFileSliceCache cache =
        new BaseHoodieTableFileIndex.BoundedFileSliceCache(3);

    PartitionPath hot = new PartitionPath("dt=hot", new Object[]{"hot"});
    PartitionPath cold = new PartitionPath("dt=cold", new Object[]{"cold"});
    PartitionPath other = new PartitionPath("dt=other", new Object[]{"other"});
    cache.put(hot, Collections.emptyList());
    cache.put(cold, Collections.emptyList());
    cache.put(other, Collections.emptyList());

    // Read the hot partition so it is the most recently used, then insert past the bound twice
    assertNotNull(cache.get(hot));
    cache.put(new PartitionPath("dt=new-1", new Object[]{"new-1"}), Collections.emptyList());
    cache.put(new PartitionPath("dt=new-2", new Object[]{"new-2"}), Collections.emptyList());

    assertEquals(3, cache.size());
    assertTrue(cache.containsKey(hot), "The most recently read partition must survive eviction");
    assertFalse(cache.containsKey(cold), "The least recently used partition must be the one evicted");
    assertFalse(cache.containsKey(other), "The second least recently used partition must be evicted next");
  }

  /**
   * A non-positive bound means "unbounded" at the call site in {@code doRefresh}, which picks a plain HashMap instead;
   * constructing the bounded cache with such a bound would silently evict on every insert, so it is rejected.
   */
  @Test
  public void testBoundedFileSliceCacheRejectsNonPositiveBound() {
    assertThrows(IllegalArgumentException.class, () -> new BaseHoodieTableFileIndex.BoundedFileSliceCache(0));
    assertThrows(IllegalArgumentException.class, () -> new BaseHoodieTableFileIndex.BoundedFileSliceCache(-1));
  }

  /**
   * The bounded cache has to remain a {@link java.util.HashMap} subtype: {@code TestHoodieIncrSource} reads the
   * private field by reflection and casts it to {@code HashMap} on the non-spillable path.
   */
  @Test
  public void testBoundedFileSliceCacheIsAHashMap() {
    assertTrue(new BaseHoodieTableFileIndex.BoundedFileSliceCache(4) instanceof java.util.HashMap,
        "Bounded cache must stay assignable to HashMap for existing reflective consumers");
  }
}
