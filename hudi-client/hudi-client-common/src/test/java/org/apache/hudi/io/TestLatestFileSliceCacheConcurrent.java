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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestLatestFileSliceCacheConcurrent {

  private static final String RLI_PARTITION_PATH = MetadataPartitionType.RECORD_INDEX.getPartitionPath();
  private static final String INSTANT = "000111";

  private TableFileSystemView.SliceView sliceView;
  private List<FileSlice> latestFileSlices;

  @BeforeEach
  void setUp() throws Exception {
    resetLatestFileSliceCacheStatics();
    sliceView = mock(TableFileSystemView.SliceView.class);
  }

  @AfterEach
  void tearDown() throws Exception {
    resetLatestFileSliceCacheStatics();
  }

  @Test
  void concurrentGetCache_sameInstant_populatesOnce() throws Exception {
    int threads = 16;
    List<String> fileIds = new ArrayList<>();
    latestFileSlices = new ArrayList<>(16);
    for (int i = 0; i < threads; i++) {
      latestFileSlices.add(new FileSlice(RLI_PARTITION_PATH, INSTANT, "fileid" + i));
      fileIds.add("fileid" + i);
    }

    CountDownLatch startGun = new CountDownLatch(1);
    CountDownLatch inMock = new CountDownLatch(threads); // wait until all threads reach contention point
    CountDownLatch releaseMock = new CountDownLatch(1);      // block population until all are ready
    AtomicInteger counter = new AtomicInteger();

    // Make sliceView call block until everyone is contending.
    when(sliceView.getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, INSTANT))
        .thenAnswer((Answer<Stream<FileSlice>>) invocation -> {
          counter.incrementAndGet();
          return latestFileSlices.stream();
        });

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<Option<FileSlice>>> futures = new ArrayList<>();

    for (int i = 0; i < threads; i++) {
      int finalI = i;
      futures.add(pool.submit(() -> {
        startGun.await();
        Cache<org.apache.hudi.common.util.collection.Triple<String, String, String>, Option<FileSlice>> cache = LatestFileSliceCache.getCache(sliceView, INSTANT, 50, 10);
        inMock.countDown();
        return cache.get(Triple.of(RLI_PARTITION_PATH, fileIds.get(finalI), INSTANT), new FailingLoader());
      }));
    }

    startGun.countDown();

    // Ensure everyone reached the mocked call (or attempted to)
    assertTrue(inMock.await(5, TimeUnit.SECONDS), "Not all threads reached contention point");
    releaseMock.countDown();

    // All threads should return a non-null cache reference
    Option<FileSlice> first = futures.get(0).get(5, TimeUnit.SECONDS);
    assertNotNull(first);
    List<FileSlice> actualFileSlices = new ArrayList<>();

    for (Future<Option<FileSlice>> f : futures) {
      Option<FileSlice> fileSliceOpt = f.get(5, TimeUnit.SECONDS);
      assertTrue(fileSliceOpt != null && fileSliceOpt.isPresent());
      actualFileSlices.add(fileSliceOpt.get());
    }

    // Critically: population should happen once
    verify(sliceView, times(1)).getLatestMergedFileSlicesBeforeOrOn(RLI_PARTITION_PATH, INSTANT);

    // validate all file slices have been fetched
    Collections.sort(latestFileSlices, new Comparator<FileSlice>() {
      @Override
      public int compare(FileSlice o1, FileSlice o2) {
        return o1.getFileId().compareTo(o2.getFileId());
      }
    });

    Collections.sort(actualFileSlices, new Comparator<FileSlice>() {
      @Override
      public int compare(FileSlice o1, FileSlice o2) {
        return o1.getFileId().compareTo(o2.getFileId());
      }
    });

    assertEquals(actualFileSlices, latestFileSlices);
    pool.shutdownNow();
  }

  static class FailingLoader implements Function<Triple<String, String, String>, Option<FileSlice>> {
    @Override
    public Option<FileSlice> apply(Triple<String, String, String> k) {
      throw new HoodieException("Loader should not be invoked in this test.");
    }
  }

  // Reflection reset if you cannot add resetForTest() in prod code
  private static void resetLatestFileSliceCacheStatics() throws Exception {
    Class<?> clazz = LatestFileSliceCache.class;

    Field cacheField = clazz.getDeclaredField("LATEST_FILE_SLICE_CACHE");
    cacheField.setAccessible(true);
    cacheField.set(null, null);

    Field instantField = clazz.getDeclaredField("INSTANT_TIME_CACHED");
    instantField.setAccessible(true);
    instantField.set(null, null);
  }
}