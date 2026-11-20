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

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;

import lombok.Value;
import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * A sample entry of a metadata table index, laid out and compressed the way the index HFiles store their entries, so
 * that the size the entries take can be measured before the index is written.
 */
@Value
class HoodieIndexEntrySample {

  // Uncompressed size of the data blocks of the index HFiles, each compressed on its own.
  static final int HFILE_BLOCK_SIZE_BYTES = 1024 * 1024;

  // Type of the key of an HFile key-value that puts a value.
  private static final byte KEY_TYPE_PUT = 4;

  // Orders entries as the HFiles do, by the unsigned bytes of their keys.
  static final Comparator<HoodieIndexEntrySample> HFILE_KEY_ORDER = (left, right) -> {
    byte[] a = left.getKey();
    byte[] b = right.getKey();
    for (int i = 0; i < Math.min(a.length, b.length); i++) {
      int cmp = Integer.compare(a[i] & 0xff, b[i] & 0xff);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(a.length, b.length);
  };

  byte[] key;
  byte[] value;

  /**
   * Serializes the value of an index record the way the index HFile writer stores it, without the key.
   */
  static byte[] serializeValue(HoodieRecord<HoodieMetadataPayload> record) {
    try {
      IndexedRecord value = record.getData().getInsertValue(HoodieMetadataRecord.getClassSchema()).get();
      value.put(value.getSchema().getField(HoodieMetadataPayload.KEY_FIELD_NAME).pos(), "");
      return HoodieAvroUtils.avroToBytes(value);
    } catch (IOException e) {
      throw new HoodieMetadataException("Failed to serialize a sample index value", e);
    }
  }

  /**
   * Compresses the entries, in the given order, or only their keys, in GZ blocks of {@link #HFILE_BLOCK_SIZE_BYTES}
   * uncompressed entry bytes, and returns the total compressed size. An entry is laid out as an HFile key-value: the key
   * and value lengths, then the row length, row, family length, timestamp and type of the key, then the value.
   */
  static long compressedSize(List<HoodieIndexEntrySample> entries, boolean wholeEntries) {
    ByteArrayOutputStream block = new ByteArrayOutputStream();
    DataOutputStream blockOut = new DataOutputStream(block);
    long compressedBytes = 0;
    int blockEntryBytes = 0;
    try {
      for (HoodieIndexEntrySample entry : entries) {
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        int keyLength = Short.BYTES + key.length + Byte.BYTES + Long.BYTES + Byte.BYTES;
        if (wholeEntries) {
          blockOut.writeInt(keyLength);
          blockOut.writeInt(value.length);
          blockOut.writeShort(key.length);
          blockOut.write(key);
          blockOut.writeByte(0);
          blockOut.writeLong(0L);
          blockOut.writeByte(KEY_TYPE_PUT);
          blockOut.write(value);
        } else {
          blockOut.write(key);
        }
        blockEntryBytes += Integer.BYTES * 2 + keyLength + value.length;
        if (blockEntryBytes >= HFILE_BLOCK_SIZE_BYTES) {
          compressedBytes += gzipSize(block);
          block.reset();
          blockEntryBytes = 0;
        }
      }
      return compressedBytes + (block.size() > 0 ? gzipSize(block) : 0);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to compress sample index entries", e);
    }
  }

  private static long gzipSize(ByteArrayOutputStream block) throws IOException {
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
      block.writeTo(gzip);
    }
    return compressed.size();
  }
}
