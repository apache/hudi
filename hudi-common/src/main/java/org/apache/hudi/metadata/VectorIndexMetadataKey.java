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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Binary-sortable key contract for records in the vector-index metadata partition.
 *
 * <p>Hudi metadata record keys are strings today, so the fixed binary prefix is represented as
 * ISO-8859-1 characters. That preserves the unsigned byte value of each component and keeps the
 * HFile key bytes lexicographically sortable by family, generation, cluster, shard, and block.
 */
public final class VectorIndexMetadataKey {

  public static final int FAMILY_ACTIVE_MANIFEST = 0x00;
  public static final int FAMILY_MANIFEST = 0x01;
  public static final int FAMILY_QUANTIZER = 0x02;
  public static final int FAMILY_CENTROIDS = 0x03;
  public static final int FAMILY_CLUSTER_STATS = 0x04;
  public static final int FAMILY_SOURCE_INSTANT_MARKER = 0x05;
  public static final int FAMILY_POSTING = 0x10;

  public static final long MAX_PACKED_BLOCK_ID = 0xFFFDFFFFL;
  public static final long FIRST_RESERVED_BLOCK_ID = 0xFFFE0000L;
  public static final long LAST_RESERVED_BLOCK_ID = 0xFFFEFFFFL;
  public static final long DELTA_BLOCK_ID = 0xFFFFFFFFL;

  private VectorIndexMetadataKey() {
  }

  public static String activeManifest() {
    return encode(ByteBuffer.allocate(1).put((byte) FAMILY_ACTIVE_MANIFEST));
  }

  public static String manifest(int generation) {
    return encode(putUnsignedInt(ByteBuffer.allocate(5).put((byte) FAMILY_MANIFEST), Integer.toUnsignedLong(generation)));
  }

  public static String quantizer(int generation, int chunk) {
    ByteBuffer buffer = ByteBuffer.allocate(7).order(ByteOrder.BIG_ENDIAN);
    buffer.put((byte) FAMILY_QUANTIZER);
    putUnsignedInt(buffer, Integer.toUnsignedLong(generation));
    putUnsignedShort(buffer, chunk);
    return encode(buffer);
  }

  public static String centroids(int generation, int chunk) {
    ByteBuffer buffer = ByteBuffer.allocate(7).order(ByteOrder.BIG_ENDIAN);
    buffer.put((byte) FAMILY_CENTROIDS);
    putUnsignedInt(buffer, Integer.toUnsignedLong(generation));
    putUnsignedShort(buffer, chunk);
    return encode(buffer);
  }

  public static String clusterStats(int generation, int clusterId) {
    ByteBuffer buffer = ByteBuffer.allocate(9).order(ByteOrder.BIG_ENDIAN);
    buffer.put((byte) FAMILY_CLUSTER_STATS);
    putUnsignedInt(buffer, Integer.toUnsignedLong(generation));
    putUnsignedInt(buffer, Integer.toUnsignedLong(clusterId));
    return encode(buffer);
  }

  public static String sourceInstantMarker(int generation, String sourceInstant) {
    byte[] instantBytes = sourceInstant.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(5 + instantBytes.length).order(ByteOrder.BIG_ENDIAN);
    buffer.put((byte) FAMILY_SOURCE_INSTANT_MARKER);
    putUnsignedInt(buffer, Integer.toUnsignedLong(generation));
    buffer.put(instantBytes);
    return encode(buffer);
  }

  public static String sourceInstantMarkerPrefix(int generation) {
    ByteBuffer buffer = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN);
    buffer.put((byte) FAMILY_SOURCE_INSTANT_MARKER);
    putUnsignedInt(buffer, Integer.toUnsignedLong(generation));
    return encode(buffer);
  }

  public static String postingBlock(int generation, int clusterId, int shard, long blockId) {
    checkUnsignedInt(blockId, "blockId");
    ByteBuffer buffer = postingPrefixBuffer(generation, clusterId, shard, 4);
    putUnsignedInt(buffer, blockId);
    return encode(buffer);
  }

  public static String postingDelta(int generation, int clusterId, int shard, String recordKey) {
    byte[] recordKeyBytes = recordKey.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = postingPrefixBuffer(generation, clusterId, shard, 4 + recordKeyBytes.length);
    putUnsignedInt(buffer, DELTA_BLOCK_ID);
    buffer.put(recordKeyBytes);
    return encode(buffer);
  }

  public static String postingPrefix(int generation, int clusterId, int shard) {
    return encode(postingPrefixBuffer(generation, clusterId, shard, 0));
  }

  public static String exclusiveEnd(String prefix) {
    byte[] bytes = decode(prefix);
    for (int i = bytes.length - 1; i >= 0; i--) {
      int value = Byte.toUnsignedInt(bytes[i]);
      if (value != 0xFF) {
        bytes[i] = (byte) (value + 1);
        return encode(Arrays.copyOf(bytes, i + 1));
      }
    }
    return null;
  }

  public static byte[] decode(String key) {
    return key.getBytes(StandardCharsets.ISO_8859_1);
  }

  public static int postingClusterId(String key) {
    byte[] bytes = decode(key);
    if (bytes.length < 9 || Byte.toUnsignedInt(bytes[0]) != FAMILY_POSTING) {
      return -1;
    }
    return readInt(bytes, 5);
  }

  public static int postingShard(String key) {
    byte[] bytes = decode(key);
    if (bytes.length < 11 || Byte.toUnsignedInt(bytes[0]) != FAMILY_POSTING) {
      return -1;
    }
    return (Byte.toUnsignedInt(bytes[9]) << 8) | Byte.toUnsignedInt(bytes[10]);
  }

  public static long postingBlockId(String key) {
    byte[] bytes = decode(key);
    if (bytes.length < 15 || Byte.toUnsignedInt(bytes[0]) != FAMILY_POSTING) {
      return -1L;
    }
    return Integer.toUnsignedLong(readInt(bytes, 11));
  }

  public static String postingRecordKey(String key) {
    byte[] bytes = decode(key);
    if (bytes.length <= 15 || Byte.toUnsignedInt(bytes[0]) != FAMILY_POSTING
        || Integer.toUnsignedLong(readInt(bytes, 11)) != DELTA_BLOCK_ID) {
      return null;
    }
    return new String(bytes, 15, bytes.length - 15, StandardCharsets.UTF_8);
  }

  static int compareUnsigned(String left, String right) {
    byte[] leftBytes = decode(left);
    byte[] rightBytes = decode(right);
    int length = Math.min(leftBytes.length, rightBytes.length);
    for (int i = 0; i < length; i++) {
      int comparison = Integer.compare(Byte.toUnsignedInt(leftBytes[i]), Byte.toUnsignedInt(rightBytes[i]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(leftBytes.length, rightBytes.length);
  }

  private static int readInt(byte[] bytes, int offset) {
    return (Byte.toUnsignedInt(bytes[offset]) << 24)
        | (Byte.toUnsignedInt(bytes[offset + 1]) << 16)
        | (Byte.toUnsignedInt(bytes[offset + 2]) << 8)
        | Byte.toUnsignedInt(bytes[offset + 3]);
  }

  private static ByteBuffer postingPrefixBuffer(int generation, int clusterId, int shard, int suffixBytes) {
    ByteBuffer buffer = ByteBuffer.allocate(11 + suffixBytes).order(ByteOrder.BIG_ENDIAN);
    buffer.put((byte) FAMILY_POSTING);
    putUnsignedInt(buffer, Integer.toUnsignedLong(generation));
    putUnsignedInt(buffer, Integer.toUnsignedLong(clusterId));
    putUnsignedShort(buffer, shard);
    return buffer;
  }

  private static ByteBuffer putUnsignedInt(ByteBuffer buffer, long value) {
    checkUnsignedInt(value, "value");
    buffer.putInt((int) value);
    return buffer;
  }

  private static void putUnsignedShort(ByteBuffer buffer, int value) {
    if (value < 0 || value > 0xFFFF) {
      throw new IllegalArgumentException("value must fit in unsigned short: " + value);
    }
    buffer.putShort((short) value);
  }

  private static void checkUnsignedInt(long value, String name) {
    if (value < 0 || value > 0xFFFFFFFFL) {
      throw new IllegalArgumentException(name + " must fit in unsigned int: " + value);
    }
  }

  private static String encode(ByteBuffer buffer) {
    return new String(buffer.array(), StandardCharsets.ISO_8859_1);
  }

  private static String encode(byte[] bytes) {
    return new String(bytes, StandardCharsets.ISO_8859_1);
  }
}
