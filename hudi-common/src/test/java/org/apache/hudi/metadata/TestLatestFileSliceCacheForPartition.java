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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.LatestFileSliceCacheForPartition.CacheKey;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestLatestFileSliceCacheForPartition {

  private static final String PARTITION1 = "partition1";
  private static final String FILEID1 = "fileId1";
  private static final String FILEID2 = "fileId2";
  private static final String FILEID3 = "fileId3";
  private static final String FILEID4 = "fileId4";

  private TableFileSystemView.SliceView sliceView;

  @BeforeEach
  void setUp() {
    sliceView = mock(TableFileSystemView.SliceView.class);
  }

  @Test
  void testLatestFileSliceCache() {
    String instantTime1 = "000111";
    String instantTime2 = "000222";
    FileSlice fileSlice1 = new FileSlice(PARTITION1, instantTime1, FILEID1);
    FileSlice fileSlice2 = new FileSlice(PARTITION1, instantTime1, FILEID2);
    List<FileSlice> fileSlicesInstant1 = Arrays.asList(fileSlice1, fileSlice2);

    when(sliceView.getLatestMergedFileSlicesBeforeOrOn(PARTITION1, instantTime1))
        .thenReturn(fileSlicesInstant1.stream());

    Cache<CacheKey, List<FileSlice>> cache =
        LatestFileSliceCacheForPartition.getCache(sliceView, CacheKey.of(instantTime1, PARTITION1), 50, 10);

    for (int i = 0; i < 3; i++) {
      assertEquals(fileSlicesInstant1,
          cache.get(CacheKey.of(instantTime1, PARTITION1), new FailingLoader()));
    }

    // Verify sliceView was only called once during cache initialization
    verify(sliceView, times(1)).getLatestMergedFileSlicesBeforeOrOn(PARTITION1, instantTime1);

    // Now simulate a new instant time with different file slices
    FileSlice fileSlice3 = new FileSlice(PARTITION1, instantTime2, FILEID3);
    FileSlice fileSlice4 = new FileSlice(PARTITION1, instantTime2, FILEID4);
    List<FileSlice> fileSlicesInstant2 = Arrays.asList(fileSlice3, fileSlice4);

    when(sliceView.getLatestMergedFileSlicesBeforeOrOn(PARTITION1, instantTime2))
        .thenReturn(fileSlicesInstant2.stream());

    cache = LatestFileSliceCacheForPartition.getCache(sliceView, CacheKey.of(instantTime2, PARTITION1), 50, 10);

    // Verify repeated lookups for the same partition at new instant use cached value
    for (int i = 0; i < 3; i++) {
      assertEquals(fileSlicesInstant2,
          cache.get(CacheKey.of(instantTime2, PARTITION1), new FailingLoader()));
    }

    verify(sliceView, times(1)).getLatestMergedFileSlicesBeforeOrOn(PARTITION1, instantTime2);
  }

  @Test
  void testCachedFileSlicesShouldBeSortedByFileId() {
    String instantTime = "000333";
    FileSlice fileSliceA = new FileSlice(PARTITION1, instantTime, "fileId_A");
    FileSlice fileSliceB = new FileSlice(PARTITION1, instantTime, "fileId_B");
    FileSlice fileSliceC = new FileSlice(PARTITION1, instantTime, "fileId_C");

    // Mock returns file slices in unsorted order (simulating stream iteration order)
    List<FileSlice> unsortedFileSlices = Arrays.asList(fileSliceC, fileSliceA, fileSliceB);
    when(sliceView.getLatestMergedFileSlicesBeforeOrOn(PARTITION1, instantTime))
        .thenReturn(unsortedFileSlices.stream());

    Cache<CacheKey, List<FileSlice>> cache =
        LatestFileSliceCacheForPartition.getCache(sliceView, CacheKey.of(instantTime, PARTITION1), 50, 10);
    List<FileSlice> cachedFileSlices = cache.get(CacheKey.of(instantTime, PARTITION1), new FailingLoader());

    // Expected: file slices should be sorted by fileId (A, B, C)
    List<FileSlice> expectedSortedSlices = unsortedFileSlices.stream()
        .sorted(Comparator.comparing(FileSlice::getFileId))
        .collect(Collectors.toList());

    assertEquals("fileId_A", expectedSortedSlices.get(0).getFileId());
    assertEquals("fileId_B", expectedSortedSlices.get(1).getFileId());
    assertEquals("fileId_C", expectedSortedSlices.get(2).getFileId());
    assertEquals(expectedSortedSlices, cachedFileSlices,
        "Cached file slices should be sorted by fileId to ensure consistent positional indexing");
  }

  static class FailingLoader implements Function<CacheKey, List<FileSlice>> {
    @Override
    public List<FileSlice> apply(CacheKey cacheKey) {
      throw new HoodieException("All entries in cache should have already been populated. Unreachable code.");
    }
  }
}
