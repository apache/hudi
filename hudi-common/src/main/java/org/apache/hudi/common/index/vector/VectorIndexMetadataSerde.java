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

import org.apache.hudi.avro.model.HoodieVectorIndexCentroids;
import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexQuantizer;
import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/** Avro conversion and binary decoding for vector index metadata artifacts. */
final class VectorIndexMetadataSerde {

  private VectorIndexMetadataSerde() {
  }

  static HoodieVectorIndexManifest asManifest(Object info) {
    if (info instanceof HoodieVectorIndexManifest) {
      return (HoodieVectorIndexManifest) info;
    }
    GenericRecord record = (GenericRecord) info;
    return new HoodieVectorIndexManifest(
        intField(record, "indexVersion"),
        stringField(record, "generationId"),
        stringField(record, "state"),
        intField(record, "dim"),
        intField(record, "dimPadded"),
        intField(record, "codeRowBytes"),
        intField(record, "bitsTotal"),
        intField(record, "numExPlanes"),
        intField(record, "numClusters"),
        intField(record, "shardCount"),
        intField(record, "fileGroupCount"),
        stringField(record, "metric"),
        booleanField(record, "assumeNormalized"),
        booleanField(record, "residualEncoding"),
        stringField(record, "vectorColumn"),
        intField(record, "targetBlockBytes"),
        intField(record, "vectorsPerBlock"),
        intField(record, "blockFormatVersion"),
        intField(record, "factorVersion"),
        doubleField(record, "kappa"),
        doubleField(record, "gMin"),
        doubleField(record, "eps1Max"),
        doubleField(record, "epsNRel"),
        intField(record, "centroidChunkCount"),
        nullableStringField(record, "centroidChecksum"),
        intField(record, "splitLimit"),
        intField(record, "mergeFloor"),
        nullableStringField(record, "bootstrapInstant"),
        nullableStringField(record, "verifiedFrontier"),
        longField(record, "createdTs"));
  }

  static HoodieVectorIndexCentroids asCentroids(Object info) {
    if (info instanceof HoodieVectorIndexCentroids) {
      return (HoodieVectorIndexCentroids) info;
    }
    GenericRecord record = (GenericRecord) info;
    return new HoodieVectorIndexCentroids(
        byteBufferField(record, "clusterIds"),
        byteBufferField(record, "centroidBytes"),
        byteBufferField(record, "clusterRadii"));
  }

  static HoodieVectorIndexQuantizer asQuantizer(Object info) {
    if (info instanceof HoodieVectorIndexQuantizer) {
      return (HoodieVectorIndexQuantizer) info;
    }
    GenericRecord record = (GenericRecord) info;
    Object rotationBytes = record.get("rotationBytes");
    return new HoodieVectorIndexQuantizer(
        stringField(record, "quantizerType"),
        longField(record, "randomSeed"),
        rotationBytes == null ? null : (ByteBuffer) rotationBytes);
  }

  static float[][] deserializeCentroids(ByteBuffer bytes, HoodieSchema.Vector vectorSchema) {
    return deserializeCentroids(bytes, vectorSchema.getDimension(), vectorSchema.getDimension(), -1);
  }

  static float[][] deserializeCentroids(ByteBuffer bytes, HoodieVectorIndexManifest manifest) {
    return deserializeCentroids(bytes, manifest.getDim(), manifest.getDimPadded(), manifest.getNumClusters());
  }

  static String manifestState(Object info) {
    return info instanceof HoodieVectorIndexManifest
        ? ((HoodieVectorIndexManifest) info).getState()
        : stringField((GenericRecord) info, "state");
  }

  static ByteBuffer centroidBytes(Object info) {
    return info instanceof HoodieVectorIndexCentroids
        ? ((HoodieVectorIndexCentroids) info).getCentroidBytes()
        : (ByteBuffer) ((GenericRecord) info).get("centroidBytes");
  }

  static long quantizerSeed(Object info) {
    return info instanceof HoodieVectorIndexQuantizer
        ? ((HoodieVectorIndexQuantizer) info).getRandomSeed()
        : longField((GenericRecord) info, "randomSeed");
  }

  static long clusterLiveCount(Object info) {
    return info instanceof org.apache.hudi.avro.model.HoodieVectorIndexClusterStats
        ? ((org.apache.hudi.avro.model.HoodieVectorIndexClusterStats) info).getLiveCount()
        : longField((GenericRecord) info, "liveCount");
  }

  static int intField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).intValue();
  }

  private static float[][] deserializeCentroids(
      ByteBuffer bytes, int dimension, int paddedDimension, int expectedCentroidCount) {
    ByteBuffer buffer = bytes.duplicate().order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    int bytesPerCentroid = paddedDimension * Float.BYTES;
    if (bytesPerCentroid == 0) {
      return new float[0][];
    }
    if (expectedCentroidCount >= 0) {
      int expectedBytes = expectedCentroidCount * bytesPerCentroid;
      if (buffer.remaining() != expectedBytes) {
        throw new IllegalArgumentException(
            "Centroid payload size mismatch: expected " + expectedBytes + " bytes for "
                + expectedCentroidCount + " centroids with padded dimension " + paddedDimension
                + ", got " + buffer.remaining());
      }
    } else if (buffer.remaining() % bytesPerCentroid != 0) {
      throw new IllegalArgumentException(
          "Centroid payload has trailing bytes: remaining=" + buffer.remaining()
              + ", bytesPerCentroid=" + bytesPerCentroid);
    }
    int centroidCount = expectedCentroidCount >= 0
        ? expectedCentroidCount : buffer.remaining() / bytesPerCentroid;
    float[][] result = new float[centroidCount][dimension];
    for (int i = 0; i < centroidCount; i++) {
      for (int j = 0; j < dimension; j++) {
        result[i][j] = buffer.getFloat();
      }
      for (int j = dimension; j < paddedDimension; j++) {
        buffer.getFloat();
      }
    }
    return result;
  }

  private static String stringField(GenericRecord record, String field) {
    return requiredField(record, field).toString();
  }

  private static String nullableStringField(GenericRecord record, String field) {
    Object value = record.get(field);
    return value == null ? null : value.toString();
  }

  private static ByteBuffer byteBufferField(GenericRecord record, String field) {
    return (ByteBuffer) requiredField(record, field);
  }

  private static float floatField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).floatValue();
  }

  private static double doubleField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).doubleValue();
  }

  private static long longField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).longValue();
  }

  private static boolean booleanField(GenericRecord record, String field) {
    return (Boolean) requiredField(record, field);
  }

  private static Object requiredField(GenericRecord record, String field) {
    Object value = record.get(field);
    if (value == null) {
      throw new IllegalArgumentException(
          "Required vector metadata field '" + field + "' is missing from " + record.getSchema().getName());
    }
    return value;
  }
}
