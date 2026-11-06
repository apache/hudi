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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Zero-copy section view over a vector posting block payload.
 */
public final class PostingBlockView {

  private final HoodieVectorIndexPostingBlock block;
  private final int numVectors;
  private final int codeRowBytes;
  private final int numExPlanes;
  private final ByteBuffer signPlane;
  private final ByteBuffer exPlanes;
  private final ByteBuffer scalarFactors;
  private final int scalarFactorCount;
  private final ByteBuffer rowLocators;
  private final ByteBuffer recordKeyOffsets;
  private final ByteBuffer recordKeyBytes;

  public PostingBlockView(HoodieVectorIndexPostingBlock block) {
    checkArgument(block != null, "posting block must not be null");
    this.block = block;
    this.numVectors = block.getNumVectors();
    this.codeRowBytes = block.getCodeRowBytes();
    this.signPlane = littleEndian(block.getSignPlane());
    this.exPlanes = littleEndian(block.getExPlanes());
    this.scalarFactors = littleEndian(block.getScalarFactors());
    this.rowLocators = littleEndian(block.getRowLocators());
    this.recordKeyOffsets = littleEndian(block.getRecordKeyOffsets());
    this.recordKeyBytes = littleEndian(block.getRecordKeyBytes());

    checkArgument(block.getBlockFormatVersion() == PostingBlockBuilder.BLOCK_FORMAT_VERSION,
        "Unsupported posting block format version: " + block.getBlockFormatVersion());
    checkArgument(numVectors >= 0, "numVectors must be non-negative");
    checkArgument(codeRowBytes > 0 && codeRowBytes % Long.BYTES == 0,
        "codeRowBytes must be positive and long-aligned");
    checkArgument(signPlane.remaining() == numVectors * codeRowBytes,
        "signPlane length does not match numVectors * codeRowBytes");
    checkArgument(exPlanes.remaining() % Math.max(1, numVectors * codeRowBytes) == 0,
        "exPlanes length is not row aligned");
    this.numExPlanes = numVectors == 0 ? 0 : exPlanes.remaining() / (numVectors * codeRowBytes);
    int scalarArrayBytes = numVectors * Float.BYTES;
    checkArgument(numVectors == 0 || scalarFactors.remaining() % scalarArrayBytes == 0,
        "scalarFactors length must be a whole number of float arrays");
    this.scalarFactorCount = numVectors == 0
        ? PostingBlockBuilder.SCALAR_FACTOR_COUNT
        : scalarFactors.remaining() / scalarArrayBytes;
    checkArgument(scalarFactorCount == PostingBlockBuilder.SCALAR_FACTOR_COUNT
            || scalarFactorCount == PostingBlockBuilder.SCALAR_FACTOR_COUNT_WITH_VECTOR_NORM,
        "scalarFactors length must contain six or seven float arrays");
    checkArgument(rowLocators.remaining() == numVectors * PostingBlockBuilder.ROW_LOCATOR_BYTES,
        "rowLocators length must use 8-byte locator stride");
    checkArgument(recordKeyOffsets.remaining() == (numVectors + 1) * Integer.BYTES,
        "recordKeyOffsets length must be (numVectors + 1) * 4");
  }

  public int numVectors() {
    return numVectors;
  }

  public int codeRowBytes() {
    return codeRowBytes;
  }

  public int numExPlanes() {
    return numExPlanes;
  }

  public boolean hasVectorNorm() {
    return scalarFactorCount == PostingBlockBuilder.SCALAR_FACTOR_COUNT_WITH_VECTOR_NORM;
  }

  public int signPlaneOffset(int vectorIndex) {
    checkVectorIndex(vectorIndex);
    return vectorIndex * codeRowBytes;
  }

  public int exPlaneOffset(int vectorIndex, int exPlaneIndex) {
    checkVectorIndex(vectorIndex);
    checkArgument(exPlaneIndex >= 0 && exPlaneIndex < numExPlanes,
        "exPlaneIndex out of bounds: " + exPlaneIndex);
    return vectorIndex * numExPlanes * codeRowBytes + exPlaneIndex * codeRowBytes;
  }

  public ByteBuffer signPlaneRow(int vectorIndex) {
    return slice(signPlane, signPlaneOffset(vectorIndex), codeRowBytes);
  }

  /**
   * Returns a duplicate of the whole sign-plane section. Hot scan loops should
   * use absolute {@code get(offset)} reads from this buffer instead of slicing
   * per vector.
   */
  public ByteBuffer signPlaneBuffer() {
    return signPlane.duplicate().order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Returns a duplicate of the whole extended-plane section for survivor-only
   * repacking.
   */
  public ByteBuffer exPlanesBuffer() {
    return exPlanes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
  }

  public ByteBuffer exPlaneRow(int vectorIndex, int exPlaneIndex) {
    return slice(exPlanes, exPlaneOffset(vectorIndex, exPlaneIndex), codeRowBytes);
  }

  public float scalarFactor(ScalarFactor factor, int vectorIndex) {
    checkVectorIndex(vectorIndex);
    checkScalarFactor(factor);
    int offset = scalarFactorOffset(factor, vectorIndex);
    return scalarFactors.getFloat(offset);
  }

  public float vectorNormOrNaN(int vectorIndex) {
    return hasVectorNorm() ? scalarFactor(ScalarFactor.VECTOR_NORM, vectorIndex) : Float.NaN;
  }

  public int scalarFactorOffset(ScalarFactor factor, int vectorIndex) {
    checkVectorIndex(vectorIndex);
    checkScalarFactor(factor);
    return factor.ordinal() * numVectors * Float.BYTES + vectorIndex * Float.BYTES;
  }

  public RowLocator rowLocator(int vectorIndex) {
    checkVectorIndex(vectorIndex);
    int offset = vectorIndex * PostingBlockBuilder.ROW_LOCATOR_BYTES;
    int fileGroupIdx = Short.toUnsignedInt(rowLocators.getShort(offset));
    int instantIdx = Short.toUnsignedInt(rowLocators.getShort(offset + Short.BYTES));
    long rowPosition = Integer.toUnsignedLong(rowLocators.getInt(offset + 2 * Short.BYTES));
    return new RowLocator(
        fileGroupIdx,
        instantIdx,
        rowPosition,
        block.getFileGroupDict().get(fileGroupIdx),
        block.getInstantTimeDict().get(instantIdx),
        partitionForFileGroup(fileGroupIdx));
  }

  public String recordKey(int vectorIndex) {
    checkVectorIndex(vectorIndex);
    int start = recordKeyOffsets.getInt(vectorIndex * Integer.BYTES);
    int end = recordKeyOffsets.getInt((vectorIndex + 1) * Integer.BYTES);
    checkArgument(start >= 0 && end >= start && end <= recordKeyBytes.remaining(),
        "Invalid record key offset range");
    ByteBuffer keySlice = slice(recordKeyBytes, start, end - start);
    byte[] bytes = new byte[keySlice.remaining()];
    keySlice.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public List<String> fileGroupDict() {
    return block.getFileGroupDict();
  }

  public List<String> instantTimeDict() {
    return block.getInstantTimeDict();
  }

  public List<String> partitionDict() {
    return block.getPartitionDict();
  }

  private String partitionForFileGroup(int fileGroupIdx) {
    return block.getPartitionDict().isEmpty() ? "" : block.getPartitionDict().get(fileGroupIdx);
  }

  private void checkVectorIndex(int vectorIndex) {
    checkArgument(vectorIndex >= 0 && vectorIndex < numVectors, "vectorIndex out of bounds: " + vectorIndex);
  }

  private void checkScalarFactor(ScalarFactor factor) {
    checkArgument(factor.ordinal() < scalarFactorCount,
        "Scalar factor is absent from this block layout: " + factor);
  }

  private static ByteBuffer littleEndian(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.order(ByteOrder.LITTLE_ENDIAN);
    return duplicate;
  }

  private static ByteBuffer slice(ByteBuffer buffer, int offset, int length) {
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.position(offset);
    duplicate.limit(offset + length);
    ByteBuffer slice = duplicate.slice();
    slice.order(ByteOrder.LITTLE_ENDIAN);
    return slice;
  }

  public enum ScalarFactor {
    F_ADD_1,
    F_RESCALE_1,
    ERR_1,
    F_ADD_EX,
    F_RESCALE_EX,
    RESIDUAL_NORM,
    VECTOR_NORM
  }

  public static final class RowLocator {
    private final int fileGroupDictIndex;
    private final int instantTimeDictIndex;
    private final long rowPosition;
    private final String fileGroupId;
    private final String instantTime;
    private final String partitionPath;

    private RowLocator(int fileGroupDictIndex,
                       int instantTimeDictIndex,
                       long rowPosition,
                       String fileGroupId,
                       String instantTime,
                       String partitionPath) {
      this.fileGroupDictIndex = fileGroupDictIndex;
      this.instantTimeDictIndex = instantTimeDictIndex;
      this.rowPosition = rowPosition;
      this.fileGroupId = fileGroupId;
      this.instantTime = instantTime;
      this.partitionPath = partitionPath;
    }

    public int getFileGroupDictIndex() {
      return fileGroupDictIndex;
    }

    public int getInstantTimeDictIndex() {
      return instantTimeDictIndex;
    }

    public long getRowPosition() {
      return rowPosition;
    }

    public String getFileGroupId() {
      return fileGroupId;
    }

    public String getInstantTime() {
      return instantTime;
    }

    public String getPartitionPath() {
      return partitionPath;
    }
  }
}
