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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HFile block caching functionality.
 */
public class TestHFileBlockCache {

  @Test
  public void testBlockCacheBasicOperations() {
    HFileBlockCache cache = new HFileBlockCache(2, 30, TimeUnit.MINUTES);
    assertEquals(0, cache.size());

    // Test cache key
    HFileBlockCache.BlockCacheKey key1 = new HFileBlockCache.BlockCacheKey(null, 100, 64);
    HFileBlockCache.BlockCacheKey key2 = new HFileBlockCache.BlockCacheKey(null, 200, 64);
    HFileBlockCache.BlockCacheKey key3 = new HFileBlockCache.BlockCacheKey(null, 300, 64);

    assertNotEquals(key1, key2);
    assertEquals(new HFileBlockCache.BlockCacheKey(null, 100, 64), key1);

    // Create test blocks using mock implementation with valid HFile block data
    HFileContext context = HFileContext.builder()
        .checksumType(ChecksumType.CRC32C)
        .blockSize(1024)
        .build();

    // Create valid HFile block data with proper header
    byte[] validBlockData = createValidHFileBlockData();
    MockHFileDataBlock block1 = new MockHFileDataBlock(context, validBlockData, 0);
    MockHFileDataBlock block2 = new MockHFileDataBlock(context, validBlockData, 0);
    MockHFileDataBlock block3 = new MockHFileDataBlock(context, validBlockData, 0);

    // Test put and get
    cache.putBlock(key1, block1);
    assertEquals(1, cache.size());
    assertEquals(block1, cache.getBlock(key1));
    assertNull(cache.getBlock(key2));

    // Test cache limit (LFU eviction)
    cache.putBlock(key2, block2);
    assertEquals(2, cache.size());

    cache.putBlock(key3, block3);

    // Caffeine has lazy cleanup, force cache cleanup to ensure eviction has happened
    cache.cleanUp();

    // Test that key1 was evicted (least frequently used)
    HFileBlock result1 = cache.getBlock(key1);
    HFileBlock result2 = cache.getBlock(key2);
    HFileBlock result3 = cache.getBlock(key3);

    // key1 should be evicted (LFU) - it was the least frequently used
    assertNull(result1);
    assertSame(block2, result2);
    assertSame(block3, result3);

    // Verify final cache state - should contain at most 2 items
    assertTrue(cache.size() <= 2, "Final cache size should not exceed maximum: " + cache.size());

    // Test clear
    cache.clear();
    assertEquals(0, cache.size());
    assertNull(cache.getBlock(key2));
    assertNull(cache.getBlock(key3));
  }

  @Test
  public void testGetOrComputeWithMissAndMultipleBlocks() throws Exception {
    HFileBlockCache cache = new HFileBlockCache(10, 30, TimeUnit.MINUTES);
    AtomicInteger loaderExecutionCount = new AtomicInteger(0);

    // 0. Define keys and blocks for the test
    HFileBlockCache.BlockCacheKey keyToCompute = new HFileBlockCache.BlockCacheKey("file-A", 1024, 128);
    HFileBlockCache.BlockCacheKey preExistingKey = new HFileBlockCache.BlockCacheKey("file-B", 2048, 256);

    HFileContext context = HFileContext.builder().build();
    byte[] validBlockData = createValidHFileBlockData();
    MockHFileDataBlock blockToCompute = new MockHFileDataBlock(context, validBlockData, 0);
    MockHFileDataBlock preExistingBlock = new MockHFileDataBlock(context, validBlockData, 0);

    // 1. Add a pre-existing block to ensure the cache is not empty
    cache.putBlock(preExistingKey, preExistingBlock);
    assertEquals(1, cache.size());

    // 2. Verify a cache miss for the key we are about to compute
    assertNull(cache.getBlock(keyToCompute), "Key should not be in the cache initially.");

    // 3. Define the loader which increments a counter on execution
    Callable<HFileBlock> loader = () -> {
      loaderExecutionCount.incrementAndGet();
      return blockToCompute;
    };

    // 4. First call to getOrCompute: should execute the loader
    HFileBlock firstResult = cache.getOrCompute(keyToCompute, loader);
    assertEquals(1, loaderExecutionCount.get(), "Loader should be called once on first access.");
    assertEquals(2, cache.size(), "Cache size should be 2 after computing the new block.");
    assertSame(blockToCompute, firstResult, "The newly computed block should be returned.");

    // 5. Second call: should return the cached instance without executing the loader
    HFileBlock secondResult = cache.getOrCompute(keyToCompute, loader);
    assertEquals(1, loaderExecutionCount.get(), "Loader should NOT be called again for a cached key.");
    assertSame(firstResult, secondResult, "Repeated calls should return the exact same cached object instance.");

    // 6. Final check: ensure the pre-existing block is still accessible
    assertSame(preExistingBlock, cache.getBlock(preExistingKey), "Pre-existing block should remain untouched.");
  }

  @Test
  public void testGetOrComputeSurfacesLoaderIOExceptionAndCachesNothing() throws Exception {
    HFileBlockCache cache = new HFileBlockCache(10, 30, TimeUnit.MINUTES);
    HFileBlockCache.BlockCacheKey key = new HFileBlockCache.BlockCacheKey("file-A", 1024, 128);
    IOException loadFailure = new IOException("block read failed");
    AtomicInteger loaderExecutionCount = new AtomicInteger(0);

    IOException thrown = assertThrows(IOException.class, () -> cache.getOrCompute(key, () -> {
      loaderExecutionCount.incrementAndGet();
      throw loadFailure;
    }));
    assertSame(loadFailure, thrown, "The loader's IOException should surface unwrapped.");
    assertEquals(0, cache.size(), "A failed load should leave nothing in the cache.");

    IllegalStateException unchecked = assertThrows(IllegalStateException.class, () -> cache.getOrCompute(key, () -> {
      throw new IllegalStateException("unchecked");
    }));
    assertEquals("unchecked", unchecked.getMessage());

    MockHFileDataBlock block = new MockHFileDataBlock(HFileContext.builder().build(), createValidHFileBlockData(), 0);
    assertSame(block, cache.getOrCompute(key, () -> {
      loaderExecutionCount.incrementAndGet();
      return block;
    }));
    assertEquals(2, loaderExecutionCount.get(), "The key should be loaded again after the failed attempt.");
    assertEquals(1, cache.size());
  }

  @Test
  public void testByteWeightedEviction() {
    HFileContext context = HFileContext.builder().build();
    byte[] validBlockData = createValidHFileBlockData();
    int blockWeight = new MockHFileDataBlock(context, validBlockData, 0).heapSize();
    assertTrue(blockWeight > 0, "heapSize must be positive to weigh blocks");

    // Room for two blocks and a half: the third insert must evict by weight even though the
    // count bound would hold a million blocks.
    long maxWeightBytes = 2L * blockWeight + (blockWeight / 2);
    HFileBlockCache cache = new HFileBlockCache(1_000_000, maxWeightBytes, 30, TimeUnit.MINUTES);

    cache.putBlock(new HFileBlockCache.BlockCacheKey("f", 100, 64), new MockHFileDataBlock(context, validBlockData, 0));
    cache.putBlock(new HFileBlockCache.BlockCacheKey("f", 200, 64), new MockHFileDataBlock(context, validBlockData, 0));
    cache.putBlock(new HFileBlockCache.BlockCacheKey("f", 300, 64), new MockHFileDataBlock(context, validBlockData, 0));
    cache.cleanUp();

    assertTrue(cache.size() <= 2, "Byte-weighted cache must evict by weight, not count. size=" + cache.size());
  }

  @Test
  public void testStatsStringReportsHitsAndMisses() throws Exception {
    HFileContext context = HFileContext.builder().build();
    MockHFileDataBlock block = new MockHFileDataBlock(context, createValidHFileBlockData(), 0);
    HFileBlockCache cache = new HFileBlockCache(10, 4L * 1024 * 1024, 30, TimeUnit.MINUTES);

    HFileBlockCache.BlockCacheKey key = new HFileBlockCache.BlockCacheKey("f", 1024, 128);
    cache.getOrCompute(key, () -> block);
    cache.getOrCompute(key, () -> block);

    String stats = cache.statsString();
    assertTrue(stats.contains("blocks=1"), "expected one block in: " + stats);
    assertTrue(stats.contains("hits=1"), "expected one hit in: " + stats);
    assertTrue(stats.contains("misses=1"), "expected one miss in: " + stats);
  }

  @Test
  public void testHeapSizeWeighsBlockSpanNotBackingArray() {
    HFileContext context = HFileContext.builder().build();
    byte[] block = createValidHFileBlockData();
    int expectedSpan = block.length;

    // The same block embedded at an offset inside a much larger shared array, as a block sliced
    // from a load-on-open buffer is.
    int pad = 400;
    byte[] backing = new byte[pad + block.length + 128];
    System.arraycopy(block, 0, backing, pad, block.length);
    MockHFileDataBlock sliced = new MockHFileDataBlock(context, backing, pad);

    assertEquals(expectedSpan, sliced.heapSize(),
        "heapSize must weigh the block span, not the shared backing array length");
  }

  /**
   * Creates a valid HFile block data with proper header structure for testing. This mimics the structure expected by HFileBlock constructor.
   */
  private static byte[] createValidHFileBlockData() {
    final int headerSize = HFileBlock.HFILEBLOCK_HEADER_SIZE;
    final int dataSize = 100;
    final int totalSize = headerSize + dataSize;

    ByteBuffer buffer = ByteBuffer.allocate(totalSize);

    // Write HFile block header
    buffer.put(HFileBlockType.DATA.getMagic()); // 8 bytes block magic
    buffer.putInt(dataSize); // onDiskSizeWithoutHeader (4 bytes)
    buffer.putInt(dataSize); // uncompressedSizeWithoutHeader (4 bytes) 
    buffer.putLong(0L); // prevBlockOffset (8 bytes)
    buffer.put(ChecksumType.CRC32C.getCode()); // checksum type (1 byte)
    buffer.putInt(16384); // bytesPerChecksum (4 bytes) - valid non-zero value
    buffer.putInt(totalSize); // onDiskDataSizeWithHeader (4 bytes)

    // Fill with dummy data
    for (int i = 0; i < dataSize; i++) {
      buffer.put((byte) (i % 256));
    }

    return buffer.array();
  }

  /**
   * Mock implementation of HFileDataBlock for testing purposes. Extends HFileDataBlock to provide access to protected constructor.
   */
  private static class MockHFileDataBlock extends HFileDataBlock {

    public MockHFileDataBlock(HFileContext context, byte[] byteBuff, int startOffsetInBuff) {
      super(context, byteBuff, startOffsetInBuff);
    }
  }
}
