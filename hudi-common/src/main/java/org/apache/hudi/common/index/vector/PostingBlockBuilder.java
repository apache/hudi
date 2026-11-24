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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Streaming-friendly builder for one immutable vector posting block.
 */
public final class PostingBlockBuilder {

  public static final int BLOCK_FORMAT_VERSION = 1;
  public static final int SCALAR_FACTOR_COUNT = 6;
  public static final int SCALAR_FACTOR_COUNT_WITH_VECTOR_NORM = 7;
  public static final int ROW_LOCATOR_BYTES = 8;

  private final int codeRowBytes;
  private final int numExPlanes;
  private final boolean includeVectorNorm;
  private final List<Row> rows = new ArrayList<>();
  private final Map<String, Integer> fileGroupDict = new LinkedHashMap<>();
  private final Map<String, Integer> instantTimeDict = new LinkedHashMap<>();
  private final List<String> partitionDict = new ArrayList<>();

  public PostingBlockBuilder(int codeRowBytes, int numExPlanes) {
    this(codeRowBytes, numExPlanes, false);
  }

  public PostingBlockBuilder(int codeRowBytes, int numExPlanes, boolean includeVectorNorm) {
    checkArgument(codeRowBytes > 0 && codeRowBytes % Long.BYTES == 0,
        "codeRowBytes must be positive and long-aligned: " + codeRowBytes);
    checkArgument(numExPlanes >= 0, "numExPlanes must be non-negative: " + numExPlanes);
    this.codeRowBytes = codeRowBytes;
    this.numExPlanes = numExPlanes;
    this.includeVectorNorm = includeVectorNorm;
  }

  public PostingBlockBuilder addRow(String recordKey,
                                    byte[] signPlane,
                                    byte[] exPlanes,
                                    float fAdd1,
                                    float fRescale1,
                                    float err1,
                                    float fAddEx,
                                    float fRescaleEx,
                                    float residualNorm,
                                    String fileGroupId,
                                    String instantTime,
                                    String partitionPath,
                                    long rowPosition) {
    return addRow(
        recordKey,
        signPlane,
        exPlanes,
        fAdd1,
        fRescale1,
        err1,
        fAddEx,
        fRescaleEx,
        residualNorm,
        null,
        fileGroupId,
        instantTime,
        partitionPath,
        rowPosition);
  }

  public PostingBlockBuilder addRow(String recordKey,
                                    byte[] signPlane,
                                    byte[] exPlanes,
                                    float fAdd1,
                                    float fRescale1,
                                    float err1,
                                    float fAddEx,
                                    float fRescaleEx,
                                    float residualNorm,
                                    Float vectorNorm,
                                    String fileGroupId,
                                    String instantTime,
                                    String partitionPath,
                                    long rowPosition) {
    checkArgument(recordKey != null, "recordKey must not be null");
    checkArgument(signPlane != null && signPlane.length == codeRowBytes,
        "signPlane must be exactly codeRowBytes");
    checkArgument(exPlanes != null && exPlanes.length == numExPlanes * codeRowBytes,
        "exPlanes must be numExPlanes * codeRowBytes");
    checkArgument(rowPosition >= 0 && rowPosition <= 0xFFFFFFFFL,
        "rowPosition must fit in unsigned int: " + rowPosition);
    checkArgument(includeVectorNorm == (vectorNorm != null),
        "vectorNorm presence must match block scalar layout");

    int fileGroupIdx = fileGroupDictionaryIndex(fileGroupId, partitionPath);
    int instantIdx = dictionaryIndex(instantTimeDict, instantTime);
    rows.add(new Row(
        recordKey,
        signPlane.clone(),
        exPlanes.clone(),
        fAdd1,
        fRescale1,
        err1,
        fAddEx,
        fRescaleEx,
        residualNorm,
        vectorNorm == null ? 0.0f : vectorNorm,
        fileGroupIdx,
        instantIdx,
        rowPosition));
    return this;
  }

  public HoodieVectorIndexPostingBlock build() {
    int numVectors = rows.size();
    ByteBuffer signPlane = allocateLittleEndian(numVectors * codeRowBytes);
    ByteBuffer exPlanes = allocateLittleEndian(numVectors * numExPlanes * codeRowBytes);
    ByteBuffer scalarFactors = allocateLittleEndian(numVectors * scalarFactorCount() * Float.BYTES);
    ByteBuffer rowLocators = allocateLittleEndian(numVectors * ROW_LOCATOR_BYTES);
    ByteBuffer recordKeyOffsets = allocateLittleEndian((numVectors + 1) * Integer.BYTES);
    ByteBuffer recordKeyBytes = allocateLittleEndian(totalRecordKeyBytes());

    for (Row row : rows) {
      signPlane.put(row.signPlane);
      exPlanes.put(row.exPlanes);
    }

    writeScalarArray(scalarFactors, ScalarFactor.F_ADD_1);
    writeScalarArray(scalarFactors, ScalarFactor.F_RESCALE_1);
    writeScalarArray(scalarFactors, ScalarFactor.ERR_1);
    writeScalarArray(scalarFactors, ScalarFactor.F_ADD_EX);
    writeScalarArray(scalarFactors, ScalarFactor.F_RESCALE_EX);
    writeScalarArray(scalarFactors, ScalarFactor.RESIDUAL_NORM);
    if (includeVectorNorm) {
      writeScalarArray(scalarFactors, ScalarFactor.VECTOR_NORM);
    }

    int currentKeyOffset = 0;
    recordKeyOffsets.putInt(currentKeyOffset);
    for (Row row : rows) {
      rowLocators.putShort((short) row.fileGroupIdx);
      rowLocators.putShort((short) row.instantIdx);
      rowLocators.putInt((int) row.rowPosition);

      byte[] keyBytes = row.recordKey.getBytes(StandardCharsets.UTF_8);
      recordKeyBytes.put(keyBytes);
      currentKeyOffset += keyBytes.length;
      recordKeyOffsets.putInt(currentKeyOffset);
    }

    return new HoodieVectorIndexPostingBlock(
        BLOCK_FORMAT_VERSION,
        numVectors,
        codeRowBytes,
        flip(signPlane),
        flip(exPlanes),
        flip(scalarFactors),
        flip(rowLocators),
        new ArrayList<>(fileGroupDict.keySet()),
        new ArrayList<>(instantTimeDict.keySet()),
        new ArrayList<>(partitionDict),
        flip(recordKeyOffsets),
        flip(recordKeyBytes));
  }

  public int rowCount() {
    return rows.size();
  }

  public void reset() {
    rows.clear();
    fileGroupDict.clear();
    instantTimeDict.clear();
    partitionDict.clear();
  }

  public static int deriveVectorsPerBlock(int targetBlockBytes, int dimPadded, int bitsTotal, int avgKeyLen) {
    return deriveVectorsPerBlock(targetBlockBytes, dimPadded, bitsTotal, avgKeyLen, false);
  }

  public static int deriveVectorsPerBlock(int targetBlockBytes, int dimPadded, int bitsTotal, int avgKeyLen, boolean includeVectorNorm) {
    checkArgument(targetBlockBytes > 0, "targetBlockBytes must be positive");
    checkArgument(dimPadded > 0, "dimPadded must be positive");
    checkArgument(bitsTotal > 0, "bitsTotal must be positive");
    int codeRowBytes = ((dimPadded + 63) / 64) * Long.BYTES;
    int scalarFactorCount = includeVectorNorm ? SCALAR_FACTOR_COUNT_WITH_VECTOR_NORM : SCALAR_FACTOR_COUNT;
    int perVectorBytes = bitsTotal * codeRowBytes + scalarFactorCount * Float.BYTES
        + ROW_LOCATOR_BYTES + Math.max(0, avgKeyLen) + Integer.BYTES;
    int raw = targetBlockBytes / perVectorBytes;
    int prevPowerOfTwo = Integer.highestOneBit(Math.max(1, raw));
    return Math.max(256, Math.min(4096, prevPowerOfTwo));
  }

  private void writeScalarArray(ByteBuffer scalarFactors, ScalarFactor factor) {
    for (Row row : rows) {
      scalarFactors.putFloat(row.scalar(factor));
    }
  }

  private int scalarFactorCount() {
    return includeVectorNorm ? SCALAR_FACTOR_COUNT_WITH_VECTOR_NORM : SCALAR_FACTOR_COUNT;
  }

  private int totalRecordKeyBytes() {
    int total = 0;
    for (Row row : rows) {
      total += row.recordKey.getBytes(StandardCharsets.UTF_8).length;
    }
    return total;
  }

  private static int dictionaryIndex(Map<String, Integer> dictionary, String value) {
    checkArgument(value != null, "dictionary value must not be null");
    Integer existing = dictionary.get(value);
    if (existing != null) {
      return existing;
    }
    int index = dictionary.size();
    checkArgument(index <= 0xFFFF, "block-local dictionary cannot exceed unsigned short cardinality");
    dictionary.put(value, index);
    return index;
  }

  private int fileGroupDictionaryIndex(String fileGroupId, String partitionPath) {
    checkArgument(fileGroupId != null, "fileGroupId must not be null");
    checkArgument(partitionPath != null, "partitionPath must not be null");
    Integer existing = fileGroupDict.get(fileGroupId);
    if (existing != null) {
      checkArgument(partitionDict.get(existing).equals(partitionPath),
          "fileGroupId cannot map to multiple partition paths in one block: " + fileGroupId);
      return existing;
    }
    int index = fileGroupDict.size();
    checkArgument(index <= 0xFFFF, "block-local dictionary cannot exceed unsigned short cardinality");
    fileGroupDict.put(fileGroupId, index);
    partitionDict.add(partitionPath);
    return index;
  }

  private static ByteBuffer allocateLittleEndian(int size) {
    return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer flip(ByteBuffer buffer) {
    buffer.flip();
    return buffer;
  }

  private enum ScalarFactor {
    F_ADD_1,
    F_RESCALE_1,
    ERR_1,
    F_ADD_EX,
    F_RESCALE_EX,
    RESIDUAL_NORM,
    VECTOR_NORM
  }

  private static final class Row {
    private final String recordKey;
    private final byte[] signPlane;
    private final byte[] exPlanes;
    private final float fAdd1;
    private final float fRescale1;
    private final float err1;
    private final float fAddEx;
    private final float fRescaleEx;
    private final float residualNorm;
    private final float vectorNorm;
    private final int fileGroupIdx;
    private final int instantIdx;
    private final long rowPosition;

    private Row(String recordKey,
                byte[] signPlane,
                byte[] exPlanes,
                float fAdd1,
                float fRescale1,
                float err1,
                float fAddEx,
                float fRescaleEx,
                float residualNorm,
                float vectorNorm,
                int fileGroupIdx,
                int instantIdx,
                long rowPosition) {
      this.recordKey = recordKey;
      this.signPlane = signPlane;
      this.exPlanes = exPlanes;
      this.fAdd1 = fAdd1;
      this.fRescale1 = fRescale1;
      this.err1 = err1;
      this.fAddEx = fAddEx;
      this.fRescaleEx = fRescaleEx;
      this.residualNorm = residualNorm;
      this.vectorNorm = vectorNorm;
      this.fileGroupIdx = fileGroupIdx;
      this.instantIdx = instantIdx;
      this.rowPosition = rowPosition;
    }

    private float scalar(ScalarFactor factor) {
      switch (factor) {
        case F_ADD_1:
          return fAdd1;
        case F_RESCALE_1:
          return fRescale1;
        case ERR_1:
          return err1;
        case F_ADD_EX:
          return fAddEx;
        case F_RESCALE_EX:
          return fRescaleEx;
        case RESIDUAL_NORM:
          return residualNorm;
        case VECTOR_NORM:
          return vectorNorm;
        default:
          throw new IllegalArgumentException("Unknown scalar factor: " + factor);
      }
    }
  }
}
