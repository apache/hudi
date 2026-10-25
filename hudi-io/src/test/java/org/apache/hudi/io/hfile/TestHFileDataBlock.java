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
 * Validates the exact on-disk bytes the data block writer emits for each record. A data entry is a
 * full HBase KeyValue: {@code [4-byte keyLen][4-byte valueLen][2-byte rowLen][row][1-byte cfLen=0]
 * [8-byte ts=LATEST][1-byte type=Put][value][1-byte MVCC=0]}. An HBase reader relies on this exact
 * framing, so the test asserts every field rather than a single opaque blob.
 */
class TestHFileDataBlock {
  private static final long LATEST_TIMESTAMP = Long.MAX_VALUE;
  private static final byte KEY_TYPE_PUT = (byte) 4;
  // Column-family length (1) + timestamp (8) + key type (1) + the 2-byte row-length prefix.
  private static final int KEY_SUFFIX_AND_PREFIX_LENGTH = 12;

  @Test
  void writesFullHBaseKeyValuePerRecord() throws IOException {
    HFileDataBlock block =
        HFileDataBlock.createDataBlockToWrite(HFileContext.builder().build(), -1L);
    // Two records of different key/value lengths to verify the framing repeats correctly.
    block.add(utf8("key1"), utf8("value1"));
    block.add(utf8("k22"), utf8("v2"));

    ByteBuffer buf = block.getUncompressedBlockDataToWrite();
    assertDataEntry(buf, "key1", "value1");
    assertDataEntry(buf, "k22", "v2");
    assertEquals(0, buf.remaining(), "unexpected trailing bytes after the last record");
  }

  private static void assertDataEntry(ByteBuffer buf, String key, String value) {
    int rowLength = utf8(key).length;
    int valueLength = utf8(value).length;
    assertEquals(rowLength + KEY_SUFFIX_AND_PREFIX_LENGTH, buf.getInt(), "key length for " + key);
    assertEquals(valueLength, buf.getInt(), "value length for " + key);
    assertEquals((short) rowLength, buf.getShort(), "row length for " + key);
    assertEquals(key, readString(buf, rowLength), "row for " + key);
    assertEquals((byte) 0, buf.get(), "column-family length for " + key);
    assertEquals(LATEST_TIMESTAMP, buf.getLong(), "timestamp for " + key);
    assertEquals(KEY_TYPE_PUT, buf.get(), "key type for " + key);
    assertEquals(value, readString(buf, valueLength), "value for " + key);
    assertEquals((byte) 0, buf.get(), "MVCC version for " + key);
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
