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

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.compress.CompressionCodec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.hudi.io.hfile.HFileBlock.HFILEBLOCK_HEADER_SIZE;
import static org.apache.hudi.io.hfile.HFileBlock.Header.BYTES_PER_CHECKSUM_INDEX;
import static org.apache.hudi.io.hfile.HFileBlock.Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX;
import static org.apache.hudi.io.hfile.HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX;
import static org.apache.hudi.io.hfile.HFileBlock.Header.UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHfileBlock {
  private static final HFileContext CONTEXT = HFileContext.builder().build();

  @Test
  void testParseDataBlock() throws IOException {
    HFileBlock block = HFileBlock.parse(CONTEXT, serializedDataBlock(), 0);
    assertEquals(HFileBlockType.DATA, block.getBlockType());
  }

  @Test
  void testParseRejectsTruncatedHeader() throws IOException {
    byte[] bytes = serializedDataBlock();
    // A header-only buffer describing an empty block is the smallest parseable input
    byte[] headerOnly = Arrays.copyOf(bytes, HFILEBLOCK_HEADER_SIZE);
    ByteBuffer.wrap(headerOnly).putInt(ON_DISK_SIZE_WITHOUT_HEADER_INDEX, 0)
        .putInt(UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX, 0)
        .putInt(ON_DISK_DATA_SIZE_WITH_HEADER_INDEX, HFILEBLOCK_HEADER_SIZE);
    assertEquals(HFileBlockType.DATA, HFileBlock.parse(CONTEXT, headerOnly, 0).getBlockType());
    assertTruncatedHeader(Arrays.copyOf(bytes, HFILEBLOCK_HEADER_SIZE - 1), 0);
    assertTruncatedHeader(bytes, bytes.length - HFILEBLOCK_HEADER_SIZE + 1);
  }

  @Test
  void testParseRejectsNegativeSize() throws IOException {
    for (int headerIndex : new int[] {ON_DISK_SIZE_WITHOUT_HEADER_INDEX, UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX}) {
      byte[] bytes = serializedDataBlock();
      ByteBuffer.wrap(bytes).putInt(headerIndex, -1);
      assertCorruptHeader(bytes);
    }
  }

  @Test
  void testParseRejectsOnDiskSizeBeyondBuffer() throws IOException {
    byte[] bytes = serializedDataBlock();
    ByteBuffer.wrap(bytes).putInt(ON_DISK_SIZE_WITHOUT_HEADER_INDEX, bytes.length);
    assertCorruptHeader(bytes);
  }

  @Test
  void testParseRejectsZeroBytesPerChecksum() throws IOException {
    byte[] bytes = serializedDataBlock();
    ByteBuffer.wrap(bytes).putInt(BYTES_PER_CHECKSUM_INDEX, 0);
    assertCorruptHeader(bytes);
  }

  @Test
  void testParseRejectsUncompressedSizeBeyondOnDiskSizeWithoutCompression() throws IOException {
    byte[] bytes = serializedDataBlock();
    int onDiskSize = ByteBuffer.wrap(bytes).getInt(ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    ByteBuffer.wrap(bytes).putInt(UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX, onDiskSize + 1);
    assertCorruptHeader(bytes);
  }

  @Test
  void testParseRejectsOnDiskDataSizeOutsideBlock() throws IOException {
    HFileContext gzipContext = HFileContext.builder().compressionCodec(CompressionCodec.GZIP).build();
    byte[] bytes = serializedDataBlock(gzipContext);
    int onDiskSizeWithHeader =
        HFILEBLOCK_HEADER_SIZE + ByteBuffer.wrap(bytes).getInt(ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    for (int atBound : new int[] {HFILEBLOCK_HEADER_SIZE, onDiskSizeWithHeader}) {
      byte[] copy = bytes.clone();
      ByteBuffer.wrap(copy).putInt(ON_DISK_DATA_SIZE_WITH_HEADER_INDEX, atBound);
      assertEquals(HFileBlockType.DATA, HFileBlock.parse(gzipContext, copy, 0).getBlockType());
    }
    for (int outOfBound : new int[] {HFILEBLOCK_HEADER_SIZE - 1, onDiskSizeWithHeader + 1}) {
      byte[] copy = bytes.clone();
      ByteBuffer.wrap(copy).putInt(ON_DISK_DATA_SIZE_WITH_HEADER_INDEX, outOfBound);
      assertCorruptHeader(gzipContext, copy);
    }
  }

  @Test
  void testParseRejectsUncompressedSizeTooLargeForOneArray() throws IOException {
    HFileContext gzipContext = HFileContext.builder().compressionCodec(CompressionCodec.GZIP).build();
    byte[] bytes = serializedDataBlock(gzipContext);
    ByteBuffer.wrap(bytes).putInt(UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX, Integer.MAX_VALUE - 10);
    assertCorruptHeader(gzipContext, bytes);
  }

  private static byte[] serializedDataBlock() throws IOException {
    return serializedDataBlock(CONTEXT);
  }

  private static byte[] serializedDataBlock(HFileContext context) throws IOException {
    HFileDataBlock block = HFileDataBlock.createDataBlockToWrite(context, -1L);
    block.add("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
    ByteBuffer buffer = block.serialize();
    return Arrays.copyOf(buffer.array(), buffer.limit());
  }

  private static void assertCorruptHeader(byte[] bytes) {
    assertCorruptHeader(CONTEXT, bytes);
  }

  private static void assertCorruptHeader(HFileContext context, byte[] bytes) {
    HoodieIOException e = assertThrows(HoodieIOException.class,
        () -> HFileBlock.parse(context, bytes, 0));
    assertTrue(e.getMessage().startsWith("Corrupt DATA block header at buffer offset 0:"), e.getMessage());
  }

  private static void assertTruncatedHeader(byte[] bytes, int startOffset) {
    HoodieIOException e = assertThrows(HoodieIOException.class,
        () -> HFileBlock.parse(CONTEXT, bytes, startOffset));
    assertEquals(String.format("Truncated HFile block header at buffer offset %d: %d bytes remain, %d needed",
        startOffset, bytes.length - startOffset, HFILEBLOCK_HEADER_SIZE), e.getMessage());
  }

  @Test
  void testNumChecksumChunksZeroBytes() {
    Assertions.assertEquals(0, HFileBlock.numChecksumChunks(0L, 512));
  }

  @Test
  void testNumChecksumChunksExactDivision() {
    Assertions.assertEquals(2, HFileBlock.numChecksumChunks(1024L, 512));
  }

  @Test
  void testNumChecksumChunksWithRemainder() {
    Assertions.assertEquals(3, HFileBlock.numChecksumChunks(1200L, 512));
  }

  @Test
  void testNumChecksumChunksSingleChunk() {
    Assertions.assertEquals(1, HFileBlock.numChecksumChunks(200L, 512));
  }
}
