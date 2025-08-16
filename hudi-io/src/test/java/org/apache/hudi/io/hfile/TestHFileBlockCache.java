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

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HFile block caching functionality.
 */
public class TestHFileBlockCache {

  @Test
  public void testBlockCacheBasicOperations() {
    HFileBlockCache cache = new HFileBlockCache(2);
    assertEquals(0, cache.size());

    // Test cache key
    HFileBlockCache.BlockCacheKey key1 = new HFileBlockCache.BlockCacheKey(100, 64);
    HFileBlockCache.BlockCacheKey key2 = new HFileBlockCache.BlockCacheKey(200, 64);
    HFileBlockCache.BlockCacheKey key3 = new HFileBlockCache.BlockCacheKey(300, 64);

    assertNotEquals(key1, key2);
    assertEquals(new HFileBlockCache.BlockCacheKey(100, 64), key1);

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
    assertEquals(block2, result2);
    assertEquals(block3, result3);

    // Verify final cache state - should contain at most 2 items
    assertTrue(cache.size() <= 2, "Final cache size should not exceed maximum: " + cache.size());

    // Test clear
    cache.clear();
    assertEquals(0, cache.size());
    assertNull(cache.getBlock(key2));
    assertNull(cache.getBlock(key3));
  }

  @Test
  public void testHFileReaderConfig() {
    HFileReaderConfig defaultConfig = new HFileReaderConfig();
    assertEquals(HFileReaderConfig.DEFAULT_BLOCK_CACHE_SIZE, defaultConfig.getBlockCacheSize());
    assertEquals(HFileReaderConfig.DEFAULT_CACHE_TTL_MINUTES, defaultConfig.getCacheTtlMinutes());

    HFileReaderConfig customConfig = new HFileReaderConfig(50, 60);
    assertEquals(50, customConfig.getBlockCacheSize());
    assertEquals(60, customConfig.getCacheTtlMinutes());
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