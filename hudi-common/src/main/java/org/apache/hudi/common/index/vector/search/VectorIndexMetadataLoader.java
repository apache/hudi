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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.avro.model.HoodieVectorIndexActiveManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexCentroids;
import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexQuantizer;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorIndexMetadataKey;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Loads and validates the singleton pointer and immutable artifacts for its active generation. */
public final class VectorIndexMetadataLoader {

  private VectorIndexMetadataLoader() {
  }

  public static LoadedVectorIndexMetadata load(HoodieTableMetadata metadataTable,
                                               String indexPartition,
                                               HoodieSchema.Vector vectorSchema) {
    HoodieVectorIndexActiveManifest pointer = requireSingle(
        records(metadataTable, indexPartition, VectorIndexMetadataKey.activeManifest()),
        HoodieVectorIndexActiveManifest.class, "active manifest");
    if (pointer.getActiveGeneration() == null) {
      throw new IllegalStateException("Vector index does not have an active generation");
    }
    int generation = pointer.getActiveGeneration();
    HoodieVectorIndexManifest manifest = requireSingle(
        records(metadataTable, indexPartition, VectorIndexMetadataKey.manifest(generation)),
        HoodieVectorIndexManifest.class, "generation manifest");
    if (!"ACTIVE".contentEquals(manifest.getState())) {
      throw new IllegalStateException(
          "Active pointer references generation in state " + manifest.getState());
    }
    if (!manifest.getResidualEncoding()) {
      throw new IllegalStateException("Reader requires residual-encoded vector postings");
    }

    if (vectorSchema.getDimension() != manifest.getDim()) {
      throw new IllegalStateException(
          "Vector schema dimension does not match active generation manifest");
    }
    List<HoodieRecord<HoodieMetadataPayload>> centroidRecords = records(
        metadataTable, indexPartition, VectorIndexMetadataKey.centroidsPrefix(generation));
    float[][] centroids = decodeCentroids(
        centroidRecords, manifest.getNumClusters(), manifest.getDim(), vectorSchema.getVectorElementType());
    HoodieVectorIndexQuantizer quantizer = requireSingle(
        records(metadataTable, indexPartition, VectorIndexMetadataKey.quantizerPrefix(generation)),
        HoodieVectorIndexQuantizer.class, "quantizer");

    Map<Integer, Integer> shardCounts = new HashMap<>();
    for (int clusterId = 0; clusterId < manifest.getNumClusters(); clusterId++) {
      shardCounts.put(clusterId, manifest.getShardCount());
    }
    VectorIndexSnapshot snapshot = new VectorIndexSnapshot(
        generation,
        manifest.getFactorVersion(),
        manifest.getBlockFormatVersion(),
        quantizer.getQuantizerType().toString(),
        "rabitq-" + manifest.getBitsTotal(),
        manifest.getLastContiguousSourceInstant() == null
            ? null : manifest.getLastContiguousSourceInstant().toString());
    return new LoadedVectorIndexMetadata(
        snapshot, centroids, shardCounts, manifest.getDim(), manifest.getBitsTotal(),
        quantizer.getRandomSeed(), manifest.getAssumeNormalized(),
        parseMetric(manifest.getMetric().toString()), manifest.getVectorColumn().toString());
  }

  private static List<HoodieRecord<HoodieMetadataPayload>> records(
      HoodieTableMetadata metadataTable, String partition, String prefix) {
    RawKey rawKey = () -> prefix;
    return metadataTable.getRecordsByKeyPrefixes(
        HoodieListData.eager(Collections.singletonList(rawKey)), partition, true).collectAsList();
  }

  private static <T> T requireSingle(List<HoodieRecord<HoodieMetadataPayload>> records,
                                     Class<T> type,
                                     String artifactName) {
    List<T> matches = new ArrayList<>();
    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Option<Object> metadata = record.getData().getVectorIndexMetadata();
      if (metadata.isPresent() && type.isInstance(metadata.get())) {
        matches.add(type.cast(metadata.get()));
      }
    }
    if (matches.size() != 1) {
      throw new IllegalStateException(
          "Expected exactly one " + artifactName + ", found " + matches.size());
    }
    return matches.get(0);
  }

  private static float[][] decodeCentroids(
      List<HoodieRecord<HoodieMetadataPayload>> records,
      int clusterCount,
      int dimension,
      HoodieSchema.Vector.VectorElementType elementType) {
    float[][] result = new float[clusterCount][];
    int decoded = 0;
    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Option<Object> metadata = record.getData().getVectorIndexMetadata();
      if (!metadata.isPresent() || !(metadata.get() instanceof HoodieVectorIndexCentroids)) {
        continue;
      }
      HoodieVectorIndexCentroids chunk = (HoodieVectorIndexCentroids) metadata.get();
      ByteBuffer ids = chunk.getClusterIds().duplicate().order(ByteOrder.LITTLE_ENDIAN);
      ByteBuffer values = chunk.getCentroidBytes().duplicate().order(ByteOrder.LITTLE_ENDIAN);
      if (ids.remaining() % Integer.BYTES != 0) {
        throw new IllegalStateException("Centroid cluster-id payload is misaligned");
      }
      int rows = ids.remaining() / Integer.BYTES;
      int elementBytes = elementType.getElementSize();
      if (values.remaining() != rows * dimension * elementBytes) {
        throw new IllegalStateException("Centroid value payload has an invalid length");
      }
      for (int row = 0; row < rows; row++) {
        int clusterId = ids.getInt();
        if (clusterId < 0 || clusterId >= clusterCount || result[clusterId] != null) {
          throw new IllegalStateException("Invalid or duplicate centroid cluster " + clusterId);
        }
        float[] centroid = new float[dimension];
        for (int dim = 0; dim < dimension; dim++) {
          centroid[dim] = readElement(values, elementType);
        }
        result[clusterId] = centroid;
        decoded++;
      }
    }
    if (decoded != clusterCount) {
      throw new IllegalStateException(
          "Incomplete centroid artifacts: expected=" + clusterCount + ", actual=" + decoded);
    }
    return result;
  }

  private static float readElement(
      ByteBuffer values, HoodieSchema.Vector.VectorElementType elementType) {
    switch (elementType) {
      case DOUBLE:
        return (float) values.getDouble();
      case INT8:
        return values.get();
      case FLOAT:
        return values.getFloat();
      default:
        throw new IllegalStateException("Unsupported vector element type: " + elementType);
    }
  }

  private static VectorDistanceMetric parseMetric(String metric) {
    switch (metric.toUpperCase()) {
      case "L2":
        return VectorDistanceMetric.L2;
      case "DOT":
      case "DOT_PRODUCT":
        return VectorDistanceMetric.DOT_PRODUCT;
      case "COSINE":
        return VectorDistanceMetric.COSINE;
      default:
        throw new IllegalStateException("Unsupported vector index metric: " + metric);
    }
  }
}
