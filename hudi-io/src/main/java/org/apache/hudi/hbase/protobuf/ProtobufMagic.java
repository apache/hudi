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

package org.apache.hudi.hbase.protobuf;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Protobufs utility.
 */
@InterfaceAudience.Private
public class ProtobufMagic {

  private ProtobufMagic() {
  }

  /**
   * Magic we put ahead of a serialized protobuf message.
   * For example, all znode content is protobuf messages with the below magic
   * for preamble.
   */
  public static final byte [] PB_MAGIC = new byte [] {'P', 'B', 'U', 'F'};

  /**
   * @param bytes Bytes to check.
   * @return True if passed <code>bytes</code> has {@link #PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes) {
    if (bytes == null) return false;
    return isPBMagicPrefix(bytes, 0, bytes.length);
  }

  /*
   * Copied from Bytes.java to here
   * hbase-common now depends on hbase-protocol
   * Referencing Bytes.java directly would create circular dependency
   */
  private static int compareTo(byte[] buffer1, int offset1, int length1,
                               byte[] buffer2, int offset2, int length2) {
    // Short circuit equal case
    if (buffer1 == buffer2 &&
        offset1 == offset2 &&
        length1 == length2) {
      return 0;
    }
    // Bring WritableComparator code local
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  /**
   * @param bytes Bytes to check.
   * @param offset offset to start at
   * @param len length to use
   * @return True if passed <code>bytes</code> has {@link #PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes, int offset, int len) {
    if (bytes == null || len < PB_MAGIC.length) return false;
    return compareTo(PB_MAGIC, 0, PB_MAGIC.length, bytes, offset, PB_MAGIC.length) == 0;
  }

  /**
   * @return Length of {@link #PB_MAGIC}
   */
  public static int lengthOfPBMagic() {
    return PB_MAGIC.length;
  }
}
