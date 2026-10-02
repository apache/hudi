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
import org.apache.hudi.avro.model.HoodieVectorIndexPostingDelta;
import org.apache.hudi.avro.model.HoodieVectorIndexTombstone;
import org.apache.hudi.metadata.VectorIndexMetadataKey;

import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

final class VectorPostingSerde {

  private VectorPostingSerde() {
  }

  static boolean isPostingBlockInfo(Object info) {
    return info instanceof HoodieVectorIndexPostingBlock || hasAvroName(info, "HoodieVectorIndexPostingBlock");
  }

  static boolean isPostingDeltaInfo(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta || hasAvroName(info, "HoodieVectorIndexPostingDelta");
  }

  static boolean isPostingTombstoneInfo(Object info) {
    return info instanceof HoodieVectorIndexTombstone || hasAvroName(info, "HoodieVectorIndexTombstone");
  }

  static boolean hasAvroName(Object info, String name) {
    return info instanceof GenericRecord && name.equals(((GenericRecord) info).getSchema().getName());
  }

  static HoodieVectorIndexPostingBlock asPostingBlock(Object info) {
    if (info instanceof HoodieVectorIndexPostingBlock) {
      return (HoodieVectorIndexPostingBlock) info;
    }
    GenericRecord record = (GenericRecord) info;
    return new HoodieVectorIndexPostingBlock(
        intField(record, "blockFormatVersion"),
        intField(record, "numVectors"),
        intField(record, "codeRowBytes"),
        byteBufferField(record, "signPlane"),
        byteBufferField(record, "exPlanes"),
        byteBufferField(record, "scalarFactors"),
        byteBufferField(record, "rowLocators"),
        stringListField(record, "fileGroupDict"),
        stringListField(record, "instantTimeDict"),
        stringListField(record, "partitionDict"),
        byteBufferField(record, "recordKeyOffsets"),
        byteBufferField(record, "recordKeyBytes"));
  }

  static String getDeltaRecordKey(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getRecordKey().toString()
        : stringField((GenericRecord) info, "recordKey");
  }

  static ByteBuffer getDeltaBinaryCode(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getBinaryCode()
        : byteBufferField((GenericRecord) info, "binaryCode");
  }

  static float getDeltaResidualNorm(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getResidualNorm()
        : floatField((GenericRecord) info, "residualNorm");
  }

  static float getDeltaFAddEx(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getFAddEx()
        : floatField((GenericRecord) info, "fAddEx");
  }

  static float getDeltaFRescaleEx(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getFRescaleEx()
        : floatField((GenericRecord) info, "fRescaleEx");
  }

  static Float getDeltaVectorNormOrNull(Object info) {
    Object vectorNorm = info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getVectorNorm()
        : ((GenericRecord) info).get("vectorNorm");
    return vectorNorm == null ? null : ((Number) vectorNorm).floatValue();
  }

  static float getDeltaVectorNormOrNaN(Object info) {
    Float vectorNorm = getDeltaVectorNormOrNull(info);
    return vectorNorm == null ? Float.NaN : vectorNorm;
  }

  static String getDeltaFileGroupId(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getFileGroupId().toString()
        : stringField((GenericRecord) info, "fileGroupId");
  }

  static String getDeltaPartitionPath(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getPartitionPath().toString()
        : stringField((GenericRecord) info, "partitionPath");
  }

  static String getDeltaBaseInstantTime(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getBaseInstantTime().toString()
        : stringField((GenericRecord) info, "baseInstantTime");
  }

  static long getDeltaRowPosition(Object info) {
    return info instanceof HoodieVectorIndexPostingDelta
        ? ((HoodieVectorIndexPostingDelta) info).getRowPosition()
        : longField((GenericRecord) info, "rowPosition");
  }

  static int[] parsePostingKey(String metadataRecordKey) {
    return new int[] {
        VectorIndexMetadataKey.postingClusterId(metadataRecordKey),
        VectorIndexMetadataKey.postingShard(metadataRecordKey)
    };
  }

  static float[] subtract(float[] left, float[] right) {
    if (left.length != right.length) {
      throw new IllegalArgumentException("Vector length mismatch: " + left.length + " != " + right.length);
    }
    float[] residual = new float[left.length];
    for (int i = 0; i < left.length; i++) {
      residual[i] = left[i] - right[i];
    }
    return residual;
  }

  static byte[] packExtendedLevels(PostingBlockView view, int vectorIndex) {
    int exBits = view.numExPlanes();
    if (exBits <= 0) {
      return new byte[0];
    }
    int rowBits = view.codeRowBytes() * Byte.SIZE;
    byte[] packed = new byte[(rowBits * exBits + 7) / 8];
    byte[][] planes = new byte[exBits][];
    for (int plane = 0; plane < exBits; plane++) {
      planes[plane] = copyBuffer(view.exPlaneRow(vectorIndex, plane));
    }
    int bitOffset = 0;
    for (int dim = 0; dim < rowBits; dim++) {
      for (int bit = 0; bit < exBits; bit++) {
        int plane = exBits - 1 - bit;
        if ((planes[plane][dim >> 3] & (1 << (dim & 7))) != 0) {
          int absoluteBit = bitOffset + bit;
          packed[absoluteBit >> 3] |= (byte) (1 << (absoluteBit & 7));
        }
      }
      bitOffset += exBits;
    }
    return packed;
  }

  static byte[] copyBuffer(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }

  static ByteBuffer byteBufferField(GenericRecord record, String field) {
    return ((ByteBuffer) record.get(field)).duplicate();
  }

  static List<String> stringListField(GenericRecord record, String field) {
    List<String> values = new ArrayList<>();
    for (Object value : (Collection<?>) record.get(field)) {
      values.add(value.toString());
    }
    return values;
  }

  static String stringField(GenericRecord record, String field) {
    Object value = record.get(field);
    return value == null ? "" : value.toString();
  }

  static int intField(GenericRecord record, String field) {
    return ((Number) record.get(field)).intValue();
  }

  static long longField(GenericRecord record, String field) {
    return ((Number) record.get(field)).longValue();
  }

  static float floatField(GenericRecord record, String field) {
    return ((Number) record.get(field)).floatValue();
  }
}
