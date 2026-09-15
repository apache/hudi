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
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Validates the exact on-disk bytes the root index block writer emits for each entry. An entry is
 * {@code [8-byte offset][4-byte size][varint keyLen][2-byte rowLen][row][1-byte cfLen=0]
 * [8-byte ts=LATEST][1-byte type=Put]}: the block-index "first key" is a full HBase KeyValue key,
 * byte-identical to the data block's key, so an HBase reader can point-look-up the block index.
 */
class TestHFileRootIndexBlock {
  private static final long LATEST_TIMESTAMP = Long.MAX_VALUE;
  private static final byte KEY_TYPE_PUT = (byte) 4;
  // Column-family length (1) + timestamp (8) + key type (1) + the 2-byte row-length prefix.
  private static final int KEY_SUFFIX_AND_PREFIX_LENGTH = 12;

  @Test
  void writesFullHBaseKeyValueKeyPerEntry() throws IOException {
    HFileRootIndexBlock block =
        HFileRootIndexBlock.createRootIndexBlockToWrite(HFileContext.builder().build());
    // Two entries of different key lengths, offsets, and sizes.
    block.add(utf8("key1"), 0L, 100);
    block.add(utf8("key0002"), 100L, 250);

    ByteBuffer buf = block.getUncompressedBlockDataToWrite();
    assertIndexEntry(buf, "key1", 0L, 100);
    assertIndexEntry(buf, "key0002", 100L, 250);
    assertEquals(0, buf.remaining(), "unexpected trailing bytes after the last entry");
  }

  private static void assertIndexEntry(ByteBuffer buf, String key, long offset, int size) {
    int rowLength = utf8(key).length;
    assertEquals(offset, buf.getLong(), "offset for " + key);
    assertEquals(size, buf.getInt(), "size for " + key);
    // The key length is a Hadoop WritableUtils VarInt; it is a single byte here because
    // rowLength + 12 < 128 for these keys (multi-byte VarInt is covered by long-key read tests).
    assertEquals(rowLength + KEY_SUFFIX_AND_PREFIX_LENGTH, buf.get() & 0xff,
        "varint key length for " + key);
    assertEquals((short) rowLength, buf.getShort(), "row length for " + key);
    assertEquals(key, readString(buf, rowLength), "row for " + key);
    assertEquals((byte) 0, buf.get(), "column-family length for " + key);
    assertEquals(LATEST_TIMESTAMP, buf.getLong(), "timestamp for " + key);
    assertEquals(KEY_TYPE_PUT, buf.get(), "key type for " + key);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static String readString(ByteBuffer buf, int length) {
    byte[] bytes = new byte[length];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
