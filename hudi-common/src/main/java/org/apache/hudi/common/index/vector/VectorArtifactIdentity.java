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

package org.apache.hudi.common.index.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical identities for immutable vector-index generation artifacts. */
public final class VectorArtifactIdentity {

  public static final int ROUTING_VERSION = 1;
  public static final int ROTATION_VERSION = 1;

  private VectorArtifactIdentity() {
  }

  public static String routingDigest(
      float[][] coarseCentroids,
      float[][] leafCentroids,
      int[] leafOffsets,
      float expandRatio) {
    MessageDigest digest = sha256();
    updateInt(digest, ROUTING_VERSION);
    updateInt(digest, Float.floatToRawIntBits(expandRatio));
    updateMatrix(digest, coarseCentroids);
    updateMatrix(digest, leafCentroids);
    updateInt(digest, leafOffsets.length);
    for (int offset : leafOffsets) {
      updateInt(digest, offset);
    }
    return toHex(digest.digest());
  }

  public static String rotationDigest(int dimension, long seed, float[][] rotation) {
    MessageDigest digest = sha256();
    updateInt(digest, ROTATION_VERSION);
    updateInt(digest, dimension);
    updateLong(digest, seed);
    for (float[] row : rotation) {
      for (float value : row) {
        updateInt(digest, Float.floatToRawIntBits(value));
      }
    }
    return toHex(digest.digest());
  }

  private static void updateMatrix(MessageDigest digest, float[][] matrix) {
    updateInt(digest, matrix.length);
    updateInt(digest, matrix.length == 0 ? 0 : matrix[0].length);
    for (float[] row : matrix) {
      for (float value : row) {
        updateInt(digest, Float.floatToRawIntBits(value));
      }
    }
  }

  private static void updateInt(MessageDigest digest, int value) {
    digest.update(ByteBuffer.allocate(Integer.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN).putInt(value).array());
  }

  private static void updateLong(MessageDigest digest, long value) {
    digest.update(ByteBuffer.allocate(Long.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is required by the Java runtime", exception);
    }
  }

  private static String toHex(byte[] digest) {
    final char[] digits = "0123456789abcdef".toCharArray();
    char[] hex = new char[digest.length * 2];
    for (int index = 0; index < digest.length; index++) {
      int value = digest[index] & 0xff;
      hex[index * 2] = digits[value >>> 4];
      hex[index * 2 + 1] = digits[value & 0x0f];
    }
    return new String(hex);
  }
}
