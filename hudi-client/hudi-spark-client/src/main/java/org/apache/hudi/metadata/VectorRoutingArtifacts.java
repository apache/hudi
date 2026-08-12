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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.spark.index.vector.TwoLevelKMeansBootstrap$;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Serialization and validation for generation-scoped two-level routing artifacts. */
public final class VectorRoutingArtifacts {

  public static final int ROUTING_VERSION = 1;

  private VectorRoutingArtifacts() {
  }

  public static String digest(
      float[][] coarseCentroids,
      float[][] leafCentroids,
      int[] leafOffsets,
      float expandRatio) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateInt(digest, ROUTING_VERSION);
      updateInt(digest, Float.floatToRawIntBits(expandRatio));
      updateMatrix(digest, coarseCentroids);
      updateMatrix(digest, leafCentroids);
      updateInt(digest, leafOffsets.length);
      for (int offset : leafOffsets) {
        updateInt(digest, offset);
      }
      return toHex(digest.digest());
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is required by the Java runtime", exception);
    }
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

  static ByteBuffer serializeFloatMatrix(float[][] values) {
    int columns = values.length == 0 ? 0 : values[0].length;
    ByteBuffer buffer = ByteBuffer.allocate(values.length * columns * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (float[] row : values) {
      if (row.length != columns) {
        throw new IllegalArgumentException("Routing centroid rows must have a consistent dimension");
      }
      for (float value : row) {
        if (!Float.isFinite(value)) {
          throw new IllegalArgumentException("Routing centroids must contain only finite values");
        }
        buffer.putFloat(value);
      }
    }
    buffer.flip();
    return buffer;
  }

  static ByteBuffer serializeIntArray(int[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int value : values) {
      buffer.putInt(value);
    }
    buffer.flip();
    return buffer;
  }

  public static Object restore(
      int routingVersion,
      ByteBuffer coarseCentroidBytes,
      ByteBuffer leafOffsetBytes,
      float routingExpandRatio,
      int dimension,
      float[][] leafCentroids) {
    if (routingVersion != ROUTING_VERSION
        || !Float.isFinite(routingExpandRatio)
        || routingExpandRatio < 1.0f) {
      throw new HoodieMetadataException("ACTIVE vector generation has unsupported routing geometry");
    }
    float[][] coarseCentroids = decodeFloatMatrix(
        coarseCentroidBytes, dimension, "routing coarse centroids");
    int[] leafOffsets = decodeIntArray(leafOffsetBytes, "routing leaf offsets");
    try {
      return TwoLevelKMeansBootstrap$.MODULE$.restoreModelForJava(
          coarseCentroids, leafCentroids, leafOffsets);
    } catch (IllegalArgumentException exception) {
      throw new HoodieMetadataException("ACTIVE vector generation has invalid routing artifacts", exception);
    }
  }

  private static float[][] decodeFloatMatrix(ByteBuffer source, int columns, String name) {
    ByteBuffer values = source.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    int rowBytes = columns * Float.BYTES;
    if (columns <= 0 || values.remaining() == 0 || values.remaining() % rowBytes != 0) {
      throw new HoodieMetadataException("Invalid " + name + " payload size");
    }
    float[][] result = new float[values.remaining() / rowBytes][columns];
    for (int row = 0; row < result.length; row++) {
      for (int column = 0; column < columns; column++) {
        float value = values.getFloat();
        if (!Float.isFinite(value)) {
          throw new HoodieMetadataException(name + " contains a non-finite value");
        }
        result[row][column] = value;
      }
    }
    return result;
  }

  private static int[] decodeIntArray(ByteBuffer source, String name) {
    ByteBuffer values = source.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    if (values.remaining() == 0 || values.remaining() % Integer.BYTES != 0) {
      throw new HoodieMetadataException("Invalid " + name + " payload size");
    }
    int[] result = new int[values.remaining() / Integer.BYTES];
    for (int index = 0; index < result.length; index++) {
      result[index] = values.getInt();
    }
    return result;
  }
}
