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

package org.apache.hudi.io;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.MetadataPartitionType;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestLatestFileSliceCache {

  private static String RLI_PARTITION_PATH = MetadataPartitionType.RECORD_INDEX.getPartitionPath();
  private static final String FILEID1 = "fileId1";
  private static final String FILEID2 = "fileId2";
  private static final String FILEID3 = "fileId3";
  private static final String FILEID4 = "fileId4";
  private FileSlice fileSliceP1FileId1;
  private FileSlice fileSliceP1FileId2;
  private FileSlice fileSliceP1FileId3;
  private FileSlice fileSliceP1FileId4;
  private final List<FileSlice> latestFileSlices = new ArrayList<>();

  private TableFileSystemView.SliceView sliceView;

  @BeforeEach
  public void setUp() {
    sliceView = mock(TableFileSystemView.SliceView.class);
    fileSliceP1FileId1 = new FileSlice(RLI_PARTITION_PATH, "000111", FILEID1);
    fileSliceP1FileId2 = new FileSlice(RLI_PARTITION_PATH, "000111", FILEID2);
    fileSliceP1FileId3 = new FileSlice(RLI_PARTITION_PATH, "000111", FILEID3);
    fileSliceP1FileId4 = new FileSlice(RLI_PARTITION_PATH, "000111", FILEID4);
    latestFileSlices.add(fileSliceP1FileId1);
    latestFileSlices.add(fileSliceP1FileId2);
    latestFileSlices.add(fileSliceP1FileId3);
    latestFileSlices.add(fileSliceP1FileId4);
    when(sliceView.getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, "000111")).thenReturn(latestFileSlices.stream());
  }

  @Test
  public void testLatestFileSliceCache() {
    Cache<Triple<String, String, String>, Option<FileSlice>> latestFileSliceCache = LatestFileSliceCache.getCache(sliceView, "000111", 50, 10);

    for (int i = 0; i < 3; i++) {
      assertEquals(Option.of(fileSliceP1FileId1), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID1, "000111"), new FailingLoader()));
      assertEquals(Option.of(fileSliceP1FileId2), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID2, "000111"), new FailingLoader()));
      assertEquals(Option.of(fileSliceP1FileId3), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID3, "000111"), new FailingLoader()));
      assertEquals(Option.of(fileSliceP1FileId4), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID4, "000111"), new FailingLoader()));
    }

    verify(sliceView, times(1)).getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, "000111");

    when(sliceView.getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, "000222")).thenReturn(latestFileSlices.stream());
    latestFileSliceCache = LatestFileSliceCache.getCache(sliceView, "000222", 50, 10);
    // if instant time changes, cached entry will be ignored
    for (int i = 0; i < 3; i++) {
      assertEquals(Option.of(fileSliceP1FileId1), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID1, "000222"), new FailingLoader()));
      assertEquals(Option.of(fileSliceP1FileId2), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID2, "000222"), new FailingLoader()));
      assertEquals(Option.of(fileSliceP1FileId3), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID3, "000222"), new FailingLoader()));
      assertEquals(Option.of(fileSliceP1FileId4), latestFileSliceCache.get(Triple.of(RLI_PARTITION_PATH, FILEID4, "000222"), new FailingLoader()));
    }
    verify(sliceView, times(1)).getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, "000222");
  }

  static class FailingLoader implements Function<Triple<String, String, String>, Option<FileSlice>> {
    @Override
    public Option<FileSlice> apply(Triple<String, String, String> stringStringStringTriple) {
      throw new HoodieException("All entries in cache should have already been populated. Unreachable code.");
    }
  }
}
