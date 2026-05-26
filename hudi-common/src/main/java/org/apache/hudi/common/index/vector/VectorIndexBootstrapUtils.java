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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Shared Phase 1 bootstrap helper for vector-index metadata generation.
 *
 * <p>This is intentionally engine-agnostic so Spark can reuse it while tests can
 * validate the clustering and MDT-record generation logic without spinning up a
 * full table/index bootstrap flow.
 */
public final class VectorIndexBootstrapUtils {

  private VectorIndexBootstrapUtils() {
  }

  public static BootstrapResult buildBootstrapResult(List<VectorDocument> documents,
                                                     String partitionPath,
                                                     Map<String, String> options,
                                                     HoodieSchema.Vector.VectorElementType elementType) {
    checkArgument(documents != null && !documents.isEmpty(), "Vector index bootstrap requires at least one vector");

    int dimension = VectorIndexOptions.getDimension(options);
    int requestedClusters = Math.max(1, VectorIndexOptions.getNumClusters(options));
    int numClusters = Math.min(requestedClusters, documents.size());
    int maxIter = Math.max(1, VectorIndexOptions.getMaxIter(options));
    VectorDistanceMetric metric = VectorIndexOptions.getMetric(options);

    validateDocuments(documents, dimension);

    double[][] centroids = initializeCentroids(documents, numClusters, dimension);
    int[] assignments = new int[documents.size()];
    for (int i = 0; i < assignments.length; i++) {
      assignments[i] = -1;
    }

    for (int iteration = 0; iteration < maxIter; iteration++) {
      boolean changed = assignToClosestCentroids(documents, centroids, assignments, metric);
      double[][] nextCentroids = recomputeCentroids(documents, assignments, centroids, dimension);
      centroids = nextCentroids;
      if (!changed) {
        break;
      }
    }

    List<HoodieRecord> records = new ArrayList<>(documents.size() + 1);
    records.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(serializeCentroids(centroids, elementType), partitionPath));

    Map<String, Integer> assignmentMap = new LinkedHashMap<>();
    for (int i = 0; i < documents.size(); i++) {
      VectorDocument document = documents.get(i);
      int clusterId = assignments[i];
      assignmentMap.put(document.getRecordKey(), clusterId);
      records.add(HoodieMetadataPayload.createVectorIndexAssignmentRecord(document.getRecordKey(), clusterId, partitionPath));
    }

    return new BootstrapResult(toFloatCentroids(centroids), assignmentMap, records);
  }

  public static List<HoodieRecord> buildBootstrapRecords(List<VectorDocument> documents,
                                                         String partitionPath,
                                                         Map<String, String> options,
                                                         HoodieSchema.Vector.VectorElementType elementType) {
    return buildBootstrapResult(documents, partitionPath, options, elementType).getRecords();
  }

  private static void validateDocuments(List<VectorDocument> documents, int dimension) {
    documents.forEach(document -> {
      checkArgument(document.getRecordKey() != null && !document.getRecordKey().isEmpty(),
          "Vector index bootstrap requires a non-empty record key");
      checkArgument(document.getVector() != null, "Vector index bootstrap requires non-null vectors");
      checkArgument(document.getVector().length == dimension,
          String.format("Vector dimension mismatch for record %s: expected %s but got %s",
              document.getRecordKey(), dimension, document.getVector().length));
    });
  }

  private static double[][] initializeCentroids(List<VectorDocument> documents, int numClusters, int dimension) {
    double[][] centroids = new double[numClusters][dimension];
    for (int clusterId = 0; clusterId < numClusters; clusterId++) {
      int sourceIndex = (int) (((long) clusterId * documents.size()) / numClusters);
      System.arraycopy(documents.get(sourceIndex).getVector(), 0, centroids[clusterId], 0, dimension);
    }
    return centroids;
  }

  private static boolean assignToClosestCentroids(List<VectorDocument> documents,
                                                  double[][] centroids,
                                                  int[] assignments,
                                                  VectorDistanceMetric metric) {
    boolean changed = false;
    for (int i = 0; i < documents.size(); i++) {
      double[] vector = documents.get(i).getVector();
      int bestCluster = findClosestCentroid(vector, centroids, metric);
      if (assignments[i] != bestCluster) {
        assignments[i] = bestCluster;
        changed = true;
      }
    }
    return changed;
  }

  private static double[][] recomputeCentroids(List<VectorDocument> documents,
                                              int[] assignments,
                                              double[][] previousCentroids,
                                              int dimension) {
    double[][] sums = new double[previousCentroids.length][dimension];
    int[] counts = new int[previousCentroids.length];

    for (int i = 0; i < documents.size(); i++) {
      int clusterId = assignments[i];
      counts[clusterId]++;
      double[] vector = documents.get(i).getVector();
      for (int dimensionIndex = 0; dimensionIndex < dimension; dimensionIndex++) {
        sums[clusterId][dimensionIndex] += vector[dimensionIndex];
      }
    }

    double[][] centroids = new double[previousCentroids.length][dimension];
    for (int clusterId = 0; clusterId < previousCentroids.length; clusterId++) {
      if (counts[clusterId] == 0) {
        System.arraycopy(previousCentroids[clusterId], 0, centroids[clusterId], 0, dimension);
      } else {
        for (int dimensionIndex = 0; dimensionIndex < dimension; dimensionIndex++) {
          centroids[clusterId][dimensionIndex] = sums[clusterId][dimensionIndex] / counts[clusterId];
        }
      }
    }

    return centroids;
  }

  public static ByteBuffer serializeCentroids(double[][] centroids, HoodieSchema.Vector.VectorElementType elementType) {
    int dimension = centroids[0].length;
    ByteBuffer byteBuffer = ByteBuffer.allocate(centroids.length * dimension * elementType.getElementSize())
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (double[] centroid : centroids) {
      for (double value : centroid) {
        switch (elementType) {
          case FLOAT:
            byteBuffer.putFloat((float) value);
            break;
          case DOUBLE:
            byteBuffer.putDouble(value);
            break;
          case INT8:
            byteBuffer.put((byte) Math.round(value));
            break;
          default:
            throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
        }
      }
    }
    byteBuffer.flip();
    return byteBuffer;
  }

  public static int findClosestCentroid(double[] vector, double[][] centroids, VectorDistanceMetric metric) {
    checkArgument(centroids != null && centroids.length > 0, "At least one centroid is required");
    int bestCluster = 0;
    double bestDistance = compute(metric, vector, centroids[0]);
    for (int clusterId = 1; clusterId < centroids.length; clusterId++) {
      double distance = compute(metric, vector, centroids[clusterId]);
      if (distance < bestDistance) {
        bestDistance = distance;
        bestCluster = clusterId;
      }
    }
    return bestCluster;
  }

  private static double compute(VectorDistanceMetric metric, double[] a, double[] b) {
    checkArgument(a.length == b.length, "Vector dimension mismatch: " + a.length + " vs " + b.length);
    switch (metric) {
      case COSINE:
        double dot = 0d;
        double normA = 0d;
        double normB = 0d;
        for (int i = 0; i < a.length; i++) {
          dot += a[i] * b[i];
          normA += a[i] * a[i];
          normB += b[i] * b[i];
        }
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom == 0d ? 1d : 1d - dot / denom;
      case L2:
        double sum = 0d;
        for (int i = 0; i < a.length; i++) {
          double delta = a[i] - b[i];
          sum += delta * delta;
        }
        return Math.sqrt(sum);
      case DOT_PRODUCT:
        double product = 0d;
        for (int i = 0; i < a.length; i++) {
          product += a[i] * b[i];
        }
        return -product;
      default:
        throw new IllegalArgumentException("Unsupported vector metric: " + metric);
    }
  }

  private static float[][] toFloatCentroids(double[][] centroids) {
    float[][] converted = new float[centroids.length][];
    for (int i = 0; i < centroids.length; i++) {
      converted[i] = new float[centroids[i].length];
      for (int j = 0; j < centroids[i].length; j++) {
        converted[i][j] = (float) centroids[i][j];
      }
    }
    return converted;
  }

  public static final class VectorDocument {
    private final String recordKey;
    private final double[] vector;

    public VectorDocument(String recordKey, double[] vector) {
      this.recordKey = recordKey;
      this.vector = vector;
    }

    public String getRecordKey() {
      return recordKey;
    }

    public double[] getVector() {
      return vector;
    }
  }

  public static final class BootstrapResult {
    private final float[][] centroids;
    private final Map<String, Integer> assignments;
    private final List<HoodieRecord> records;

    BootstrapResult(float[][] centroids, Map<String, Integer> assignments, List<HoodieRecord> records) {
      this.centroids = centroids;
      this.assignments = Collections.unmodifiableMap(new LinkedHashMap<>(assignments));
      this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public float[][] getCentroids() {
      return centroids;
    }

    public Map<String, Integer> getAssignments() {
      return assignments;
    }

    public List<HoodieRecord> getRecords() {
      return records;
    }
  }
}
