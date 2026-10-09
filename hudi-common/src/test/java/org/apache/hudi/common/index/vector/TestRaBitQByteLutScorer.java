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

import org.apache.hudi.avro.model.HoodieVectorIndexPostingBlock;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Scalar-to-byte-LUT identity tests for persisted posting-plane ordering. */
class TestRaBitQByteLutScorer {

  private static final int DIMENSION = 13;
  private static final int CODE_ROW_BYTES = Long.BYTES;
  private static final float[] QUERY = {
      0.2f, -0.7f, 1.1f, 0.3f, -0.4f, 0.9f, -1.3f,
      0.6f, 0.8f, -0.2f, 0.5f, -0.1f, 0.4f
  };

  @Test
  void lutMatchesScalarForAllSupportedBitWidthsAndPersistedPlanes() {
    for (int bits : new int[] {1, 2, 4, 8}) {
      RaBitQEncoder encoder = new RaBitQEncoder(DIMENSION, bits, 17L, false);
      QuantizedVector encoded = encoder.encodeResidual(vector(), center());
      byte[] signRow = padded(encoded.code);
      byte[] extendedPlanes = toMsbFirstPlanes(encoded.extendedCode, bits - 1);

      HoodieVectorIndexPostingBlock block = new PostingBlockBuilder(CODE_ROW_BYTES, bits - 1)
          .addRow("first", new byte[CODE_ROW_BYTES], new byte[(bits - 1) * CODE_ROW_BYTES],
              0f, 0f, 0f, 0f, 0f, 0f, "fg", "001", "p", 0)
          .addRow("target", signRow, extendedPlanes,
              0f, 0f, 0f, 0f, 0f, 0f, "fg", "001", "p", 1)
          .build();
      PostingBlockView view = new PostingBlockView(block);
      float querySum = sum(QUERY);
      RaBitQByteLutScorer scorer =
          RaBitQByteLutScorer.forQuery(QUERY, querySum, DIMENSION, CODE_ROW_BYTES);

      int signOffset = view.signPlaneOffset(1);
      double signDot = scorer.planeDot(view.signPlaneBuffer(), signOffset);
      float expectedPass1 = RaBitQEncoder.dotPackedBinary(encoded.code, QUERY, DIMENSION)
          - 0.5f * querySum;
      assertEquals(expectedPass1, scorer.pass1FromDot(signDot), 1e-5f, "pass 1, bits=" + bits);

      float expectedPass2 = RaBitQEncoder.multibitDotTerm(
          QUERY, querySum, encoded.code, encoded.extendedCode, DIMENSION, bits);
      assertEquals(expectedPass2,
          scorer.pass2(signDot, view, view.exPlanesBuffer(), 1, bits - 1, bits),
          5e-5f,
          "pass 2, bits=" + bits);
    }
  }

  @Test
  void pass2RejectsMismatchedPostingLayoutAndVectorIndex() {
    PostingBlockView oneBitView = new PostingBlockView(
        new PostingBlockBuilder(CODE_ROW_BYTES, 0)
            .addRow("row", new byte[CODE_ROW_BYTES], new byte[0],
                0f, 0f, 0f, 0f, 0f, 0f, "fg", "001", "p", 0)
            .build());
    RaBitQByteLutScorer scorer =
        RaBitQByteLutScorer.forQuery(QUERY, sum(QUERY), DIMENSION, CODE_ROW_BYTES);
    assertThrows(IllegalArgumentException.class,
        () -> scorer.pass2(0, oneBitView, oneBitView.exPlanesBuffer(), 1, 0, 1));

    PostingBlockView widerView = new PostingBlockView(
        new PostingBlockBuilder(2 * CODE_ROW_BYTES, 0)
            .addRow("row", new byte[2 * CODE_ROW_BYTES], new byte[0],
                0f, 0f, 0f, 0f, 0f, 0f, "fg", "001", "p", 0)
            .build());
    assertThrows(IllegalArgumentException.class,
        () -> scorer.pass2(0, widerView, widerView.exPlanesBuffer(), 0, 0, 1));
  }

  @Test
  void planeDotSupportsNonzeroBufferOffsetsAndRejectsTruncation() {
    byte[] code = {(byte) 0b01010101, (byte) 0b00010101};
    byte[] row = padded(code);
    ByteBuffer buffer = ByteBuffer.allocate(3 + CODE_ROW_BYTES);
    buffer.position(3);
    buffer.put(row);
    RaBitQByteLutScorer scorer =
        RaBitQByteLutScorer.forQuery(QUERY, sum(QUERY), DIMENSION, CODE_ROW_BYTES);

    assertEquals(RaBitQEncoder.dotPackedBinary(code, QUERY, DIMENSION),
        scorer.planeDot(buffer, 3), 1e-6);
    assertThrows(IllegalArgumentException.class, () -> scorer.planeDot(buffer, 4));
  }

  private static byte[] toMsbFirstPlanes(byte[] packedLevels, int exBits) {
    byte[] planes = new byte[exBits * CODE_ROW_BYTES];
    for (int dimension = 0; dimension < DIMENSION; dimension++) {
      int level = unpack(packedLevels, dimension * exBits, exBits);
      for (int plane = 0; plane < exBits; plane++) {
        int sourceBit = exBits - 1 - plane;
        if ((level & (1 << sourceBit)) != 0) {
          planes[plane * CODE_ROW_BYTES + (dimension >> 3)] |= (byte) (1 << (dimension & 7));
        }
      }
    }
    return planes;
  }

  private static int unpack(byte[] packed, int bitOffset, int bitCount) {
    int value = 0;
    for (int bit = 0; bit < bitCount; bit++) {
      int absoluteBit = bitOffset + bit;
      if ((packed[absoluteBit >> 3] & (1 << (absoluteBit & 7))) != 0) {
        value |= 1 << bit;
      }
    }
    return value;
  }

  private static byte[] padded(byte[] code) {
    return Arrays.copyOf(code, CODE_ROW_BYTES);
  }

  private static float sum(float[] values) {
    float result = 0f;
    for (float value : values) {
      result += value;
    }
    return result;
  }

  private static float[] vector() {
    return new float[] {1f, 3f, -2f, 4f, 0.5f, -1f, 2f, 0.1f, -0.4f, 1.7f, 3.2f, -2.1f, 0.8f};
  }

  private static float[] center() {
    return new float[] {0.2f, 2f, -1f, 3f, 0f, -0.2f, 1f, 0f, -0.1f, 1f, 2f, -1f, 0.2f};
  }
}
