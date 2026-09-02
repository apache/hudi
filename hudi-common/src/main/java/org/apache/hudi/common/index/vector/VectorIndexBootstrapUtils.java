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

import org.apache.hudi.common.schema.HoodieSchema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/** Engine-neutral helpers shared by vector-index bootstrap implementations. */
public final class VectorIndexBootstrapUtils {

  private VectorIndexBootstrapUtils() {
  }

  /** Trains deterministic KMeans centroids from an already bounded sample. */
  public static double[][] trainCentroids(
      List<double[]> sampleVectors,
      int dimension,
      int requestedClusters,
      int maxIterations,
      VectorDistanceMetric metric) {
    checkArgument(sampleVectors != null && !sampleVectors.isEmpty(),
        "At least one sample vector is required for centroid training");
    int numClusters = Math.min(Math.max(1, requestedClusters), sampleVectors.size());
    for (int i = 0; i < sampleVectors.size(); i++) {
      checkArgument(sampleVectors.get(i).length == dimension,
          String.format("Sample vector %d has dimension %d, expected %d",
              i, sampleVectors.get(i).length, dimension));
    }

    double[][] centroids = new double[numClusters][dimension];
    for (int i = 0; i < numClusters; i++) {
      centroids[i] = sampleVectors.get(i).clone();
    }
    int[] assignments = new int[sampleVectors.size()];
    Arrays.fill(assignments, -1);
    for (int iteration = 0; iteration < Math.max(1, maxIterations); iteration++) {
      boolean changed = false;
      for (int row = 0; row < sampleVectors.size(); row++) {
        int closest = closestCentroid(sampleVectors.get(row), centroids, metric);
        changed |= assignments[row] != closest;
        assignments[row] = closest;
      }
      centroids = recompute(sampleVectors, assignments, centroids, dimension);
      if (!changed) {
        break;
      }
    }
    return centroids;
  }

  /** Serializes centroids in the vector column's physical element format. */
  public static ByteBuffer serializeCentroids(
      double[][] centroids, HoodieSchema.Vector.VectorElementType elementType) {
    int dimension = centroids.length == 0 ? 0 : centroids[0].length;
    int elementBytes = elementType.getElementSize();
    ByteBuffer buffer = ByteBuffer.allocate(centroids.length * dimension * elementBytes)
        .order(ByteOrder.LITTLE_ENDIAN);
    for (double[] centroid : centroids) {
      for (double value : centroid) {
        if (elementType == HoodieSchema.Vector.VectorElementType.DOUBLE) {
          buffer.putDouble(value);
        } else if (elementType == HoodieSchema.Vector.VectorElementType.INT8) {
          buffer.put((byte) Math.round(value));
        } else {
          buffer.putFloat((float) value);
        }
      }
    }
    buffer.flip();
    return buffer;
  }

  /** Pads one packed sign-code row to the manifest's fixed row width. */
  public static byte[] padToRow(byte[] source, int rowBytes) {
    checkArgument(source.length <= rowBytes, "Packed code exceeds fixed row width");
    return Arrays.copyOf(source, rowBytes);
  }

  /** Transposes packed per-dimension lower-bit codes into fixed-width posting-block planes. */
  public static byte[] splitExPlanes(
      byte[] extendedCode, int planeCount, int dimension, int rowBytes) {
    if (planeCount == 0) {
      return new byte[0];
    }
    checkArgument(extendedCode != null, "Extended RaBitQ planes are required");
    int expectedBytes = (dimension * planeCount + 7) / 8;
    checkArgument(extendedCode.length == expectedBytes,
        "Extended RaBitQ code size does not match dimension and bit width");
    byte[] planes = new byte[planeCount * rowBytes];
    for (int vectorOffset = 0; vectorOffset < dimension; vectorOffset++) {
      for (int plane = 0; plane < planeCount; plane++) {
        int sourceBit = vectorOffset * planeCount + plane;
        if ((extendedCode[sourceBit >> 3] & (1 << (sourceBit & 7))) != 0) {
          int targetPlane = planeCount - 1 - plane;
          int targetBit = targetPlane * rowBytes * 8 + vectorOffset;
          planes[targetBit >> 3] |= (byte) (1 << (targetBit & 7));
        }
      }
    }
    return planes;
  }

  private static int closestCentroid(
      double[] vector, double[][] centroids, VectorDistanceMetric metric) {
    int closest = 0;
    double best = Double.POSITIVE_INFINITY;
    for (int cluster = 0; cluster < centroids.length; cluster++) {
      double distance = distance(vector, centroids[cluster], metric);
      if (distance < best) {
        best = distance;
        closest = cluster;
      }
    }
    return closest;
  }

  private static double distance(
      double[] left, double[] right, VectorDistanceMetric metric) {
    double dot = 0.0;
    double leftNorm = 0.0;
    double rightNorm = 0.0;
    double squaredDistance = 0.0;
    for (int i = 0; i < left.length; i++) {
      double delta = left[i] - right[i];
      squaredDistance += delta * delta;
      dot += left[i] * right[i];
      leftNorm += left[i] * left[i];
      rightNorm += right[i] * right[i];
    }
    if (metric == VectorDistanceMetric.COSINE) {
      return leftNorm == 0.0 || rightNorm == 0.0
          ? 1.0 : 1.0 - dot / Math.sqrt(leftNorm * rightNorm);
    }
    return metric == VectorDistanceMetric.DOT_PRODUCT ? -dot : squaredDistance;
  }

  private static double[][] recompute(
      List<double[]> vectors, int[] assignments, double[][] previous, int dimension) {
    double[][] next = new double[previous.length][dimension];
    int[] counts = new int[previous.length];
    for (int row = 0; row < vectors.size(); row++) {
      int cluster = assignments[row];
      counts[cluster]++;
      for (int d = 0; d < dimension; d++) {
        next[cluster][d] += vectors.get(row)[d];
      }
    }
    for (int cluster = 0; cluster < next.length; cluster++) {
      if (counts[cluster] == 0) {
        next[cluster] = previous[cluster].clone();
      } else {
        for (int d = 0; d < dimension; d++) {
          next[cluster][d] /= counts[cluster];
        }
      }
    }
    return next;
  }
}
