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

import org.apache.hudi.io.compress.CompressionCodec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestHfileBlock {
  @ParameterizedTest
  @EnumSource(value = CompressionCodec.class, names = {"NONE", "GZIP", "ZSTD"})
  void testBlockRoundTripWithChecksums(CompressionCodec codec) throws IOException {
    HFileContext context = HFileContext.builder().compressionCodec(codec).build();
    for (int size : new int[] {0, 16384 - HFileBlock.HFILEBLOCK_HEADER_SIZE, 65536}) {
      byte[] content = new byte[size];
      new Random(42).nextBytes(content);
      ByteBuffer serialized = HFileMetaBlock.createMetaBlockToWrite(
          context, new KeyValueEntry(new byte[] {1}, content)).serialize();
      // Parse a block embedded at a nonzero offset, followed by unrelated bytes.
      byte[] bytes = new byte[serialized.remaining() + 20];
      Arrays.fill(bytes, (byte) 0x7f);
      serialized.get(bytes, 7, serialized.remaining());
      HFileMetaBlock block = (HFileMetaBlock) HFileBlock.parse(context, bytes, 7);
      assertEquals(HFileBlock.numChecksumBytes(block.onDiskDataSizeWithHeader, 16384), block.sizeCheckSum);
      if (codec == CompressionCodec.NONE && size == 16384 - HFileBlock.HFILEBLOCK_HEADER_SIZE) {
        // Checksums must not add an extra chunk when the data ends exactly at the boundary.
        assertEquals(4, block.sizeCheckSum);
      }
      block.unpack();
      ByteBuffer decoded = block.readContent();
      byte[] actual = new byte[decoded.remaining()];
      decoded.get(actual);
      assertArrayEquals(content, actual);
      block.unpack();
    }
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

  @Test
  void testNumChecksumChunksOverflowThrows() {
    long numBytes = ((long) Integer.MAX_VALUE / HFileBlock.CHECKSUM_SIZE + 1)
        * 1024; // force too many chunks
    assertThrows(IllegalArgumentException.class,
        () -> HFileBlock.numChecksumChunks(numBytes, 1024));
  }
}
