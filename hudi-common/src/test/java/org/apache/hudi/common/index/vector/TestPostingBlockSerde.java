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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.avro.model.HoodieVectorIndexPostingBlock;
import org.apache.hudi.common.index.vector.PostingBlockView.RowLocator;
import org.apache.hudi.common.index.vector.PostingBlockView.ScalarFactor;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestPostingBlockSerde {

  @Test
  void testBuilderAndViewExposeDeterministicSectionOffsets() {
    PostingBlockBuilder builder = new PostingBlockBuilder(16, 2);
    builder.addRow(
        "rk-1",
        bytes(0, 16),
        bytes(100, 32),
        1.0f,
        2.0f,
        0.1f,
        3.0f,
        4.0f,
        5.0f,
        "fg-1",
        "001",
        "p=1",
        7L);
    builder.addRow(
        "rk-22",
        bytes(16, 16),
        bytes(132, 32),
        11.0f,
        12.0f,
        0.2f,
        13.0f,
        14.0f,
        15.0f,
        "fg-1",
        "001",
        "p=1",
        9L);
    builder.addRow(
        "rk-333",
        bytes(32, 16),
        bytes(164, 32),
        21.0f,
        22.0f,
        0.3f,
        23.0f,
        24.0f,
        25.0f,
        "fg-2",
        "002",
        "p=2",
        0xFFFFFFFFL);

    HoodieVectorIndexPostingBlock block = builder.build();
    PostingBlockView view = new PostingBlockView(block);

    assertEquals(PostingBlockBuilder.BLOCK_FORMAT_VERSION, block.getBlockFormatVersion());
    assertEquals(3, view.numVectors());
    assertEquals(16, view.codeRowBytes());
    assertEquals(2, view.numExPlanes());
    assertEquals(48, block.getSignPlane().remaining());
    assertEquals(96, block.getExPlanes().remaining());
    assertEquals(72, block.getScalarFactors().remaining());
    assertEquals(24, block.getRowLocators().remaining());
    assertEquals(16, block.getRecordKeyOffsets().remaining());

    assertEquals(0, view.signPlaneOffset(0));
    assertEquals(16, view.signPlaneOffset(1));
    assertEquals(32, view.signPlaneOffset(2));
    assertEquals(0, view.exPlaneOffset(0, 0));
    assertEquals(16, view.exPlaneOffset(0, 1));
    assertEquals(64, view.exPlaneOffset(2, 0));
    assertArrayEquals(bytes(16, 16), read(view.signPlaneRow(1)));
    assertArrayEquals(bytes(180, 16), read(view.exPlaneRow(2, 1)));

    assertEquals(0, view.scalarFactorOffset(ScalarFactor.F_ADD_1, 0));
    assertEquals(12, view.scalarFactorOffset(ScalarFactor.F_RESCALE_1, 0));
    assertEquals(24, view.scalarFactorOffset(ScalarFactor.ERR_1, 0));
    assertEquals(60, view.scalarFactorOffset(ScalarFactor.RESIDUAL_NORM, 0));
    assertEquals(1.0f, view.scalarFactor(ScalarFactor.F_ADD_1, 0), 0.0f);
    assertEquals(12.0f, view.scalarFactor(ScalarFactor.F_RESCALE_1, 1), 0.0f);
    assertEquals(0.3f, view.scalarFactor(ScalarFactor.ERR_1, 2), 0.0f);
    assertEquals(25.0f, view.scalarFactor(ScalarFactor.RESIDUAL_NORM, 2), 0.0f);

    RowLocator first = view.rowLocator(0);
    assertEquals(0, first.getFileGroupDictIndex());
    assertEquals(0, first.getInstantTimeDictIndex());
    assertEquals(7L, first.getRowPosition());
    assertEquals("fg-1", first.getFileGroupId());
    assertEquals("001", first.getInstantTime());
    assertEquals("p=1", first.getPartitionPath());

    RowLocator third = view.rowLocator(2);
    assertEquals(1, third.getFileGroupDictIndex());
    assertEquals(1, third.getInstantTimeDictIndex());
    assertEquals(0xFFFFFFFFL, third.getRowPosition());
    assertEquals("fg-2", third.getFileGroupId());
    assertEquals("002", third.getInstantTime());
    assertEquals("p=2", third.getPartitionPath());

    assertEquals("rk-1", view.recordKey(0));
    assertEquals("rk-22", view.recordKey(1));
    assertEquals("rk-333", view.recordKey(2));
    assertEquals(2, view.fileGroupDict().size());
    assertEquals(2, view.partitionDict().size());
  }

  @Test
  void testVectorsPerBlockDerivesFromDimPadded() {
    assertEquals(1024, PostingBlockBuilder.deriveVectorsPerBlock(524288, 768, 4, 36));
    assertEquals(512, PostingBlockBuilder.deriveVectorsPerBlock(524288, 1024, 4, 36));
    assertEquals(512, PostingBlockBuilder.deriveVectorsPerBlock(524288, 1536, 4, 36));
    assertEquals(256, PostingBlockBuilder.deriveVectorsPerBlock(524288, 2048, 4, 36));
  }

  @Test
  void testExtendedPlanesRoundTripToLevelPackedCode() {
    int dimension = 17;
    int bits = 4;
    int exBits = bits - 1;
    int codeRowBytes = ((dimension + 63) / 64) * Long.BYTES;
    RaBitQEncoder encoder = new RaBitQEncoder(dimension, bits, 31L, false);
    float[] vector = new float[] {
        1.0f, -2.0f, 3.0f, 4.5f, -1.5f, 0.25f, 2.25f, -3.5f, 1.75f,
        -0.5f, 0.75f, 2.5f, -2.25f, 3.25f, 4.0f, -4.5f, 0.5f
    };
    float[] centroid = new float[] {
        0.25f, -1.0f, 1.5f, 2.0f, -0.5f, 0.0f, 1.0f, -1.5f, 0.75f,
        -0.25f, 0.25f, 1.25f, -1.0f, 1.5f, 2.0f, -2.0f, 0.25f
    };
    QuantizedVector quantized = encoder.encodeResidual(vector, centroid);
    byte[] exPlanes = VectorIndexBootstrapUtils.splitExPlanes(quantized.extendedCode, exBits, dimension, codeRowBytes);

    PostingBlockBuilder builder = new PostingBlockBuilder(codeRowBytes, exBits);
    builder.addRow(
        "rk-round-trip",
        VectorIndexBootstrapUtils.padToRow(quantized.code, codeRowBytes),
        exPlanes,
        quantized.additiveFactor1,
        quantized.rescaleFactor1,
        quantized.error1,
        quantized.additiveFactor,
        quantized.rescaleFactor,
        quantized.scalar,
        "fg-1",
        "001",
        "p=1",
        7L);

    PostingBlockView view = new PostingBlockView(builder.build());
    byte[] repacked = VectorIndexMdtSearchUtils.repackExtendedLevels(
        view, 0, dimension, new byte[quantized.extendedCode.length]);
    assertArrayEquals(quantized.extendedCode, repacked);

    float[] query = new float[] {
        -0.5f, 1.0f, 1.25f, -2.0f, 0.75f, -1.25f, 3.0f, 2.25f, -0.75f,
        0.5f, -2.5f, 1.5f, 2.0f, -3.25f, 0.25f, 1.75f, -1.0f
    };
    RaBitQQueryState queryState = encoder.encodeQueryForL2(query);
    float directDotTerm = RaBitQEncoder.multibitDotTerm(
        queryState.getRotatedQuery(), queryState.getQuerySum(), quantized.code, quantized.extendedCode, dimension, bits);
    float blockDotTerm = RaBitQEncoder.multibitDotTerm(
        queryState.getRotatedQuery(), queryState.getQuerySum(), read(view.signPlaneRow(0)), repacked, dimension, bits);
    assertEquals(directDotTerm, blockDotTerm, 0.0f);
  }

  @Test
  void testBuilderRejectsInvalidRows() {
    PostingBlockBuilder builder = new PostingBlockBuilder(16, 1);
    assertThrows(IllegalArgumentException.class, () -> builder.addRow(
        "rk",
        bytes(0, 8),
        bytes(0, 16),
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        "fg",
        "001",
        "p=1",
        0L));

    builder.addRow(
        "rk-1",
        bytes(0, 16),
        bytes(0, 16),
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        "fg",
        "001",
        "p=1",
        0L);
    assertThrows(IllegalArgumentException.class, () -> builder.addRow(
        "rk-2",
        bytes(0, 16),
        bytes(0, 16),
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        0.0f,
        "fg",
        "001",
        "p=2",
        1L));
  }

  @Test
  void testBuilderAcceptsNeutralRowsWithZeroRescale() {
    PostingBlockBuilder builder = new PostingBlockBuilder(16, 1);
    builder.addRow(
        "rk",
        bytes(0, 16),
        bytes(0, 16),
        0.0f,
        0.0f,
        0.0f,
        1.0f,
        0.0f,
        2.0f,
        "fg",
        "001",
        "p=1",
        0L);
    assertEquals(1, new PostingBlockView(builder.build()).numVectors());
  }

  private static byte[] bytes(int start, int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (start + i);
    }
    return bytes;
  }

  private static byte[] read(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
