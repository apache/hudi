/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.hfile;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the shared caches held by {@link HFileReaderCacheManager}.
 */
public class TestHFileReaderCacheManager {

  private static final int CONTENT_SIZE = 100;
  private static final HFileContext CONTEXT = HFileContext.builder().build();

  @BeforeEach
  @AfterEach
  void resetCaches() {
    HFileReaderCacheManager.reset();
  }

  @Test
  void testLoadOnOpenHeapSizeCountsSharedBackingArrayOnce() {
    byte[] region = createLoadOnOpenRegion();
    HFileTrailerAndLoadOnOpenBlocks blocks = createTrailerAndLoadOnOpenBlocks(region);

    assertEquals(region.length, blocks.heapSize(),
        "Three blocks sliced from one array must weigh the array once");
  }

  @Test
  void testLoadOnOpenCacheEvictsByWeight() throws Exception {
    long entryWeight = createTrailerAndLoadOnOpenBlocks(createLoadOnOpenRegion()).heapSize();
    long maxWeightBytes = 2L * entryWeight + (entryWeight / 2);
    HFileReaderCacheManager manager =
        HFileReaderCacheManager.getInstance(1_000_000, maxWeightBytes, 1_000_000, 30, true);

    for (String file : new String[] {"file-a", "file-b", "file-c"}) {
      manager.getOrLoadTrailerAndLoadOnOpenBlocks(file, () -> createTrailerAndLoadOnOpenBlocks(createLoadOnOpenRegion()));
    }
    manager.cleanUp();

    assertTrue(manager.getTrailerAndLoadOnOpenCacheSize() <= 2,
        "Load-on-open cache must evict by weight, not count. size=" + manager.getTrailerAndLoadOnOpenCacheSize());
  }

  @Test
  void testLoadOnOpenCacheKeepsCountBoundWithoutMaxWeight() throws Exception {
    HFileReaderCacheManager manager = HFileReaderCacheManager.getInstance(10, 0L, 2, 30, true);

    for (String file : new String[] {"file-a", "file-b", "file-c"}) {
      manager.getOrLoadTrailerAndLoadOnOpenBlocks(file, () -> createTrailerAndLoadOnOpenBlocks(createLoadOnOpenRegion()));
    }
    manager.cleanUp();

    assertTrue(manager.getTrailerAndLoadOnOpenCacheSize() <= 2,
        "Load-on-open cache must honor the entry count bound. size=" + manager.getTrailerAndLoadOnOpenCacheSize());
  }

  @Test
  void testStatsReportBothCaches() throws Exception {
    assertEquals("cache=inactive", HFileReaderCacheManager.globalStatsString());
    HFileReaderCacheManager manager = HFileReaderCacheManager.getInstance(10, 0L, 10, 30, true);
    HFileTrailerAndLoadOnOpenBlocks blocks = createTrailerAndLoadOnOpenBlocks(createLoadOnOpenRegion());

    assertSame(blocks, manager.getOrLoadTrailerAndLoadOnOpenBlocks("file-a", () -> blocks));
    assertSame(blocks, manager.getOrLoadTrailerAndLoadOnOpenBlocks("file-a", () -> blocks));

    // the stats format must not follow the JVM's default locale
    Locale defaultLocale = Locale.getDefault();
    Locale.setDefault(Locale.GERMANY);
    String stats;
    try {
      stats = manager.getStats();
    } finally {
      Locale.setDefault(defaultLocale);
    }
    assertTrue(stats.contains("Block Cache [blocks=0"), stats);
    assertTrue(stats.contains("Trailer And Load-On-Open Cache [files=1 hitRate=0.500 hits=1 misses=1 evictions=0]"), stats);
    assertEquals(stats, HFileReaderCacheManager.globalStatsString());
    assertTrue(manager.containsTrailerAndLoadOnOpenBlocks("file-a"));
    assertFalse(manager.containsTrailerAndLoadOnOpenBlocks("file-b"));
  }

  @Test
  void testExplicitConfigReplacesDefaultConfiguredManager() throws Exception {
    HFileReaderCacheManager defaultManager = HFileReaderCacheManager.getInstance(10, 0L, 10, 30, false);
    defaultManager.getOrLoadTrailerAndLoadOnOpenBlocks("file-a", () -> createTrailerAndLoadOnOpenBlocks(createLoadOnOpenRegion()));
    assertFalse(defaultManager.isExplicitlyConfigured());
    assertSame(defaultManager, HFileReaderCacheManager.getInstance(20, 0L, 10, 30, false),
        "A differing default-configured request must not replace the manager");

    HFileReaderCacheManager explicitManager = HFileReaderCacheManager.getInstance(20, 0L, 10, 30, true);
    assertNotSame(defaultManager, explicitManager, "An explicit config must replace a default-configured manager");
    assertTrue(explicitManager.isExplicitlyConfigured());
    assertFalse(explicitManager.containsTrailerAndLoadOnOpenBlocks("file-a"), "The replaced manager's entries are dropped");

    assertSame(explicitManager, HFileReaderCacheManager.getInstance(30, 0L, 10, 30, true),
        "A later differing explicit config is ignored");
    assertSame(explicitManager, HFileReaderCacheManager.getInstance(10, 0L, 10, 30, false),
        "A default-configured request returns the explicit manager");
    assertSame(explicitManager, HFileReaderCacheManager.getInstanceIfInitialized().get());
  }

  @Test
  void testExplicitRequestWithSameConfigPinsDefaultConfiguredManager() {
    HFileReaderCacheManager manager = HFileReaderCacheManager.getInstance(10, 0L, 10, 30, false);
    assertSame(manager, HFileReaderCacheManager.getInstance(10, 0L, 10, 30, true));
    assertTrue(manager.isExplicitlyConfigured());
    assertSame(manager, HFileReaderCacheManager.getInstance(20, 0L, 10, 30, true));
  }

  private static HFileTrailerAndLoadOnOpenBlocks createTrailerAndLoadOnOpenBlocks(byte[] region) {
    int blockSize = HFileBlock.HFILEBLOCK_HEADER_SIZE + CONTENT_SIZE;
    return new HFileTrailerAndLoadOnOpenBlocks(
        new HFileTrailer(3, 0),
        new HFileRootIndexBlock(CONTEXT, region, 0),
        new HFileRootIndexBlock(CONTEXT, region, blockSize),
        new HFileFileInfoBlock(CONTEXT, region, 2 * blockSize));
  }

  /**
   * Lays out two root index blocks and a file info block back to back in one array, the way
   * the load-on-open region is read from the file.
   */
  private static byte[] createLoadOnOpenRegion() {
    int blockSize = HFileBlock.HFILEBLOCK_HEADER_SIZE + CONTENT_SIZE;
    ByteBuffer buffer = ByteBuffer.allocate(3 * blockSize);
    for (HFileBlockType blockType : new HFileBlockType[] {
        HFileBlockType.ROOT_INDEX, HFileBlockType.ROOT_INDEX, HFileBlockType.FILE_INFO}) {
      buffer.put(blockType.getMagic());
      buffer.putInt(CONTENT_SIZE);
      buffer.putInt(CONTENT_SIZE);
      buffer.putLong(0L);
      buffer.put(ChecksumType.CRC32C.getCode());
      buffer.putInt(16384);
      buffer.putInt(blockSize);
      for (int i = 0; i < CONTENT_SIZE; i++) {
        buffer.put((byte) (i % 256));
      }
    }
    return buffer.array();
  }
}
