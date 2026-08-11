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

package org.apache.hudi.metadata;

import org.apache.hudi.common.index.vector.QuantizedVector;
import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.spark.index.vector.TwoLevelKMeansBootstrap$;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import scala.Tuple2;

/** Applies the secondary-index-style classifier to touched vector file groups. */
public final class SparkVectorIndexUpdater {

  private SparkVectorIndexUpdater() {
  }

  public static HoodieJavaRDD<HoodieRecord> update(
      JavaRDD<FileGroupRows> fileGroups,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType,
      int generation,
      String instantTime,
      String indexPartition) {
    JavaRDD<Change> changes = fileGroups.flatMap(fileGroup ->
        classify(fileGroup, artifacts, vectorType).iterator());
    JavaRDD<HoodieRecord> postings = changes.flatMap(change ->
        change.postingRecords(generation, instantTime, indexPartition).iterator());
    JavaRDD<HoodieRecord> stats = changes
        .flatMapToPair(change -> change.statDeltas().iterator())
        .reduceByKey(ClusterDelta::plus)
        .map(entry -> HoodieMetadataPayload.createVectorIndexClusterStatsRecord(
            generation,
            entry._1,
            entry._2.liveCount,
            entry._2.deltaCount,
            entry._2.tombstoneCount,
            indexPartition));
    return HoodieJavaRDD.of(postings.union(stats));
  }

  @VisibleForTesting
  static List<HoodieRecord> classifyPostingRecords(
      FileGroupRows fileGroup,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType,
      int generation,
      String instantTime,
      String indexPartition) {
    List<HoodieRecord> records = new ArrayList<>();
    for (Change change : classify(fileGroup, artifacts, vectorType)) {
      records.addAll(change.postingRecords(generation, instantTime, indexPartition));
    }
    return records;
  }

  private static List<Change> classify(
      FileGroupRows fileGroup,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    Set<String> recordKeys = new TreeSet<>(fileGroup.previousRows.keySet());
    recordKeys.addAll(fileGroup.currentRows.keySet());
    List<Change> changes = new ArrayList<>();
    for (String recordKey : recordKeys) {
      SparkVectorIndexBootstrap.VectorRow previous = fileGroup.previousRows.get(recordKey);
      SparkVectorIndexBootstrap.VectorRow current = fileGroup.currentRows.get(recordKey);
      if (previous == null) {
        EncodedRow encoded = tryEncode(current, artifacts, vectorType);
        if (encoded != null) {
          changes.add(new Change(null, encoded, ChangeType.INSERT));
        }
      } else if (current == null) {
        EncodedRow routed = tryRoute(previous, artifacts, vectorType);
        if (routed != null) {
          changes.add(new Change(routed, null, ChangeType.DELETE));
        }
      } else if (!Arrays.equals(previous.vectorBytes, current.vectorBytes)) {
        EncodedRow routed = tryRoute(previous, artifacts, vectorType);
        EncodedRow encoded = tryEncode(current, artifacts, vectorType);
        if (routed != null || encoded != null) {
          changes.add(new Change(routed, encoded, ChangeType.VECTOR_UPDATE));
        }
      } else if (!sameLocator(previous, current)) {
        EncodedRow encoded = tryEncode(current, artifacts, vectorType);
        if (encoded != null) {
          changes.add(new Change(null, encoded, ChangeType.LOCATOR_UPDATE));
        }
      }
    }
    return changes;
  }

  private static EncodedRow tryEncode(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    try {
      return encode(row, artifacts, vectorType);
    } catch (IllegalArgumentException exception) {
      return null;
    }
  }

  private static EncodedRow tryRoute(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    try {
      return route(row, artifacts, vectorType);
    } catch (IllegalArgumentException exception) {
      return null;
    }
  }

  private static EncodedRow encode(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    EncodedRow routed = route(row, artifacts, vectorType);
    float[] vector = SparkVectorIndexBootstrap.toFloatArrayFromBytes(
        row.vectorBytes, artifacts.dimension, vectorType);
    QuantizedVector quantized = new RaBitQEncoder(
        artifacts.dimension, artifacts.bits, artifacts.seed, artifacts.assumeNormalized)
        .encodeResidual(vector, artifacts.residualEncoding ? artifacts.centroids[routed.clusterId] : null);
    return new EncodedRow(row, routed.clusterId, routed.shardId, quantized);
  }

  private static EncodedRow route(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    float[] vector = SparkVectorIndexBootstrap.toFloatArrayFromBytes(
        row.vectorBytes, artifacts.dimension, vectorType);
    int clusterId = TwoLevelKMeansBootstrap$.MODULE$.assignOneForJava(
        artifacts.routingModel, vector, artifacts.routingExpandRatio);
    int shardId = SparkVectorIndexBootstrap.computeShardId(
        row.recordKey, artifacts.shardCount);
    return new EncodedRow(row, clusterId, shardId, null);
  }

  private static boolean sameLocator(
      SparkVectorIndexBootstrap.VectorRow left,
      SparkVectorIndexBootstrap.VectorRow right) {
    return left.rowPosition == right.rowPosition
        && left.partitionPath.equals(right.partitionPath)
        && left.fileId.equals(right.fileId)
        && left.baseInstantTime.equals(right.baseInstantTime);
  }

  /** Previous and current merged rows for one touched file group. */
  public static final class FileGroupRows implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, SparkVectorIndexBootstrap.VectorRow> previousRows;
    private final Map<String, SparkVectorIndexBootstrap.VectorRow> currentRows;

    public FileGroupRows(
        Map<String, SparkVectorIndexBootstrap.VectorRow> previousRows,
        Map<String, SparkVectorIndexBootstrap.VectorRow> currentRows) {
      this.previousRows = previousRows;
      this.currentRows = currentRows;
    }
  }

  /** Immutable ACTIVE-generation artifacts required by update executors. */
  public static final class Artifacts implements Serializable {
    private static final long serialVersionUID = 1L;
    private final float[][] centroids;
    private final Object routingModel;
    private final float routingExpandRatio;
    private final int shardCount;
    private final VectorDistanceMetric metric;
    private final int dimension;
    private final int bits;
    private final long seed;
    private final boolean assumeNormalized;
    private final boolean residualEncoding;

    public Artifacts(
        float[][] centroids,
        Object routingModel,
        float routingExpandRatio,
        int shardCount,
        VectorDistanceMetric metric,
        int dimension,
        int bits,
        long seed,
        boolean assumeNormalized,
        boolean residualEncoding) {
      this.centroids = centroids;
      this.routingModel = routingModel;
      this.routingExpandRatio = routingExpandRatio;
      this.shardCount = shardCount;
      this.metric = metric;
      this.dimension = dimension;
      this.bits = bits;
      this.seed = seed;
      this.assumeNormalized = assumeNormalized;
      this.residualEncoding = residualEncoding;
    }
  }

  private enum ChangeType {
    INSERT,
    DELETE,
    VECTOR_UPDATE,
    LOCATOR_UPDATE
  }

  private static final class Change implements Serializable {
    private static final long serialVersionUID = 1L;
    private final EncodedRow previous;
    private final EncodedRow current;
    private final ChangeType type;

    private Change(EncodedRow previous, EncodedRow current, ChangeType type) {
      this.previous = previous;
      this.current = current;
      this.type = type;
    }

    private List<HoodieRecord> postingRecords(
        int generation, String instantTime, String indexPartition) {
      List<HoodieRecord> records = new ArrayList<>(2);
      if (previous != null
          && (current == null
          || previous.clusterId != current.clusterId
          || previous.shardId != current.shardId)) {
        records.add(HoodieMetadataPayload.createVectorIndexPostingDeleteRecord(
            generation,
            previous.row.recordKey,
            previous.clusterId,
            previous.shardId,
            instantTime,
            indexPartition));
      }
      if (current != null) {
        QuantizedVector vector = current.quantized;
        records.add(HoodieMetadataPayload.createVectorIndexPostingRecord(
            generation,
            current.row.recordKey,
            current.clusterId,
            current.shardId,
            current.row.fileId,
            current.row.partitionPath,
            current.row.baseInstantTime,
            vector.getCode(),
            vector.getExtendedCode(),
            vector.getScalar(),
            vector.getAdditiveFactor(),
            vector.getRescaleFactor(),
            vector.getAdditiveFactor1(),
            vector.getRescaleFactor1(),
            vector.getError1(),
            vector.getVectorNorm(),
            current.row.rowPosition,
            0L,
            indexPartition));
      }
      return records;
    }

    private List<Tuple2<Integer, ClusterDelta>> statDeltas() {
      if (type == ChangeType.INSERT) {
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(1, 1, 0)));
      }
      if (type == ChangeType.DELETE) {
        return Collections.singletonList(
            new Tuple2<>(previous.clusterId, new ClusterDelta(-1, 0, 1)));
      }
      if (type == ChangeType.LOCATOR_UPDATE) {
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(0, 1, 0)));
      }
      if (previous == null) {
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(1, 1, 0)));
      }
      if (current == null) {
        return Collections.singletonList(
            new Tuple2<>(previous.clusterId, new ClusterDelta(-1, 0, 1)));
      }
      if (previous.clusterId == current.clusterId) {
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(0, 1, 0)));
      }
      return Arrays.asList(
          new Tuple2<>(previous.clusterId, new ClusterDelta(-1, 0, 1)),
          new Tuple2<>(current.clusterId, new ClusterDelta(1, 1, 0)));
    }
  }

  private static final class EncodedRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private final SparkVectorIndexBootstrap.VectorRow row;
    private final int clusterId;
    private final int shardId;
    private final QuantizedVector quantized;

    private EncodedRow(
        SparkVectorIndexBootstrap.VectorRow row,
        int clusterId,
        int shardId,
        QuantizedVector quantized) {
      this.row = row;
      this.clusterId = clusterId;
      this.shardId = shardId;
      this.quantized = quantized;
    }
  }

  private static final class ClusterDelta implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long liveCount;
    private final long deltaCount;
    private final long tombstoneCount;

    private ClusterDelta(long liveCount, long deltaCount, long tombstoneCount) {
      this.liveCount = liveCount;
      this.deltaCount = deltaCount;
      this.tombstoneCount = tombstoneCount;
    }

    private ClusterDelta plus(ClusterDelta other) {
      return new ClusterDelta(
          liveCount + other.liveCount,
          deltaCount + other.deltaCount,
          tombstoneCount + other.tombstoneCount);
    }
  }
}
