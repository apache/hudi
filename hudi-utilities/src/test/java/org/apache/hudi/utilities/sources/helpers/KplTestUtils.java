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

package org.apache.hudi.utilities.sources.helpers;

import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Test-side encoder for the KPL aggregated record format (magic prefix, AggregatedRecord protobuf
 * payload, trailing MD5 digest), independent of the {@link KinesisDeaggregator} decoder under test.
 */
public final class KplTestUtils {

  public static final byte[] KPL_MAGIC = new byte[] {(byte) 0xF3, (byte) 0x89, (byte) 0x9A, (byte) 0xC2};

  private KplTestUtils() {
  }

  /**
   * Encodes one nested {@code Record} message: partition key index (field 1), optional explicit
   * hash key index (field 2), payload (field 3) and an optional Tag (field 4).
   */
  public static byte[] encodeSubRecord(long pkIndex, Long ehkIndex, byte[] data, String tagKeyOrNull)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    CodedOutputStream stream = CodedOutputStream.newInstance(out);
    stream.writeUInt64(1, pkIndex);
    if (ehkIndex != null) {
      stream.writeUInt64(2, ehkIndex);
    }
    stream.writeByteArray(3, data);
    if (tagKeyOrNull != null) {
      ByteArrayOutputStream tagOut = new ByteArrayOutputStream();
      CodedOutputStream tagStream = CodedOutputStream.newInstance(tagOut);
      tagStream.writeString(1, tagKeyOrNull);
      tagStream.flush();
      stream.writeByteArray(4, tagOut.toByteArray());
    }
    stream.flush();
    return out.toByteArray();
  }

  /**
   * Encodes the {@code AggregatedRecord} message: partition key table (field 1), explicit hash key
   * table (field 2) and the encoded sub-records (field 3).
   */
  public static byte[] encodeAggregatedRecord(List<String> pkTable, List<String> ehkTable,
      List<byte[]> subRecords) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    CodedOutputStream stream = CodedOutputStream.newInstance(out);
    for (String pk : pkTable) {
      stream.writeString(1, pk);
    }
    for (String ehk : ehkTable) {
      stream.writeString(2, ehk);
    }
    for (byte[] subRecord : subRecords) {
      stream.writeByteArray(3, subRecord);
    }
    stream.flush();
    return out.toByteArray();
  }

  /** Wraps a payload in the KPL frame: magic prefix, payload, trailing MD5 digest of the payload. */
  public static byte[] frame(byte[] payload) throws NoSuchAlgorithmException {
    byte[] digest = MessageDigest.getInstance("MD5").digest(payload);
    return ByteBuffer.allocate(KPL_MAGIC.length + payload.length + digest.length)
        .put(KPL_MAGIC).put(payload).put(digest).array();
  }

  /** Decodes a lower-case hex string, used for frozen byte-for-byte fixtures. */
  public static byte[] hexToBytes(String hex) {
    byte[] bytes = new byte[hex.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
    }
    return bytes;
  }
}
