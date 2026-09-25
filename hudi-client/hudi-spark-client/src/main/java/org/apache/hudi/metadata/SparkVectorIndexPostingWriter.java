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

import org.apache.hudi.common.index.vector.PostingBlockBuilder;
import org.apache.hudi.common.index.vector.QuantizedVector;
import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.index.vector.VectorIndexBootstrapUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.SparkVectorIndexBootstrap.VectorRow;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/** Encodes assigned vector rows and writes sorted MDT posting blocks. */
final class SparkVectorIndexPostingWriter {

  private SparkVectorIndexPostingWriter() {
  }

  static JavaRDD<HoodieRecord> buildPostingRecords(
      JavaPairRDD<Integer, VectorRow> assignedRows,
      Broadcast<Integer> dimension,
      Broadcast<HoodieSchema.Vector.VectorElementType> vectorType,
      Broadcast<float[][]> centroids,
      Broadcast<Map<Integer, Integer>> shardCounts,
      long quantizerSeed,
      int rabitqBits,
      boolean assumeNormalized,
      boolean residualEncoding,
      VectorDistanceMetric metric,
      int vectorsPerBlock,
      int generation,
      String indexName) {
    boolean includeVectorNorm = metric == VectorDistanceMetric.COSINE && !assumeNormalized;
    JavaPairRDD<ClusterShardSortKey, EncodedPostingRow> encodedRows = assignedRows.mapToPair(entry -> {
      RaBitQEncoder encoder = new RaBitQEncoder(
          dimension.value(), rabitqBits, quantizerSeed, assumeNormalized);
      int clusterId = entry._1;
      VectorRow row = entry._2;
      int shardId = computeShardId(
          row.recordKey, shardCounts.value().getOrDefault(clusterId, 1));
      if (row.rowPosition < 0) {
        throw new IllegalStateException(
            "Vector index bootstrap requires file-absolute rowPosition for record "
                + row.recordKey
                + "; enable parquet row-index extraction before packed MDT block emission");
      }

      float[] vector = SparkVectorIndexBootstrap.toFloatArrayFromBytes(
          row.vectorBytes, dimension.value(), vectorType.value());
      float[] center = residualEncoding ? centroids.value()[clusterId] : null;
      QuantizedVector quantized = rabitqBits > 1 || residualEncoding
          ? encoder.encodeResidual(vector, center)
          : encoder.encode(vector);
      int codeRowBytes = ((dimension.value() + 63) / 64) * Long.BYTES;
      EncodedPostingRow encoded = new EncodedPostingRow(
          row,
          VectorIndexBootstrapUtils.padToRow(quantized.getCode(), codeRowBytes),
          VectorIndexBootstrapUtils.splitExPlanes(
              quantized.getExtendedCode(), Math.max(0, rabitqBits - 1),
              dimension.value(), codeRowBytes),
          quantized,
          includeVectorNorm);
      return new Tuple2<>(
          new ClusterShardSortKey(
              clusterId, shardId, row.fileId, row.rowPosition, row.recordKey),
          encoded);
    });

    int shufflePartitions = Math.max(1, encodedRows.getNumPartitions());
    return encodedRows
        .repartitionAndSortWithinPartitions(new ClusterShardPartitioner(shufflePartitions))
        .mapPartitions(iterator -> buildBlocks(
            iterator, dimension.value(), rabitqBits, includeVectorNorm,
            vectorsPerBlock, generation, indexName));
  }

  private static java.util.Iterator<HoodieRecord> buildBlocks(
      java.util.Iterator<Tuple2<ClusterShardSortKey, EncodedPostingRow>> iterator,
      int dimension,
      int rabitqBits,
      boolean includeVectorNorm,
      int vectorsPerBlock,
      int generation,
      String indexName) {
    List<HoodieRecord> records = new ArrayList<>();
    ClusterShardSortKey currentKey = null;
    PostingBlockBuilder builder = null;
    int blockId = 0;
    int rowsInBlock = 0;
    while (iterator.hasNext()) {
      Tuple2<ClusterShardSortKey, EncodedPostingRow> entry = iterator.next();
      ClusterShardSortKey key = entry._1;
      EncodedPostingRow row = entry._2;
      if (currentKey == null || !currentKey.sameClusterShard(key)) {
        if (builder != null && rowsInBlock > 0) {
          records.add(createPostingBlockRecord(
              generation, currentKey, blockId, builder, indexName));
        }
        currentKey = key;
        builder = newPostingBlockBuilder(dimension, rabitqBits, includeVectorNorm);
        blockId = 0;
        rowsInBlock = 0;
      }

      row.addTo(builder);
      rowsInBlock++;
      if (rowsInBlock == vectorsPerBlock) {
        records.add(createPostingBlockRecord(
            generation, currentKey, blockId++, builder, indexName));
        builder = newPostingBlockBuilder(dimension, rabitqBits, includeVectorNorm);
        rowsInBlock = 0;
      }
    }
    if (builder != null && rowsInBlock > 0) {
      records.add(createPostingBlockRecord(
          generation, currentKey, blockId, builder, indexName));
    }
    return records.iterator();
  }

  private static PostingBlockBuilder newPostingBlockBuilder(
      int dimension, int rabitqBits, boolean includeVectorNorm) {
    return new PostingBlockBuilder(
        ((dimension + 63) / 64) * Long.BYTES,
        Math.max(0, rabitqBits - 1),
        includeVectorNorm);
  }

  private static HoodieRecord createPostingBlockRecord(
      int generation,
      ClusterShardSortKey key,
      int blockId,
      PostingBlockBuilder builder,
      String indexName) {
    return HoodieMetadataPayload.createVectorIndexPostingBlockRecord(
        generation, key.clusterId, key.shardId, blockId, builder.build(), indexName);
  }

  static int computeShardId(String recordKey, int shardCount) {
    int hash = recordKey.hashCode();
    hash ^= hash >>> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >>> 13;
    return Math.floorMod(hash, Math.max(1, shardCount));
  }

  private static final class ClusterShardSortKey
      implements Comparable<ClusterShardSortKey>, Serializable {
    private static final long serialVersionUID = 1L;

    private final int clusterId;
    private final int shardId;
    private final String fileGroupId;
    private final long rowPosition;
    private final String recordKey;

    private ClusterShardSortKey(
        int clusterId, int shardId, String fileGroupId, long rowPosition, String recordKey) {
      this.clusterId = clusterId;
      this.shardId = shardId;
      this.fileGroupId = fileGroupId == null ? "" : fileGroupId;
      this.rowPosition = rowPosition;
      this.recordKey = recordKey == null ? "" : recordKey;
    }

    private boolean sameClusterShard(ClusterShardSortKey other) {
      return other != null && clusterId == other.clusterId && shardId == other.shardId;
    }

    @Override
    public int compareTo(ClusterShardSortKey other) {
      int comparison = Integer.compare(clusterId, other.clusterId);
      if (comparison != 0) {
        return comparison;
      }
      comparison = Integer.compare(shardId, other.shardId);
      if (comparison != 0) {
        return comparison;
      }
      comparison = fileGroupId.compareTo(other.fileGroupId);
      if (comparison != 0) {
        return comparison;
      }
      comparison = Long.compare(rowPosition, other.rowPosition);
      return comparison != 0 ? comparison : recordKey.compareTo(other.recordKey);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ClusterShardSortKey)) {
        return false;
      }
      ClusterShardSortKey that = (ClusterShardSortKey) other;
      return clusterId == that.clusterId
          && shardId == that.shardId
          && rowPosition == that.rowPosition
          && fileGroupId.equals(that.fileGroupId)
          && recordKey.equals(that.recordKey);
    }

    @Override
    public int hashCode() {
      int result = clusterId;
      result = 31 * result + shardId;
      result = 31 * result + fileGroupId.hashCode();
      result = 31 * result + Long.hashCode(rowPosition);
      return 31 * result + recordKey.hashCode();
    }
  }

  private static final class ClusterShardPartitioner extends Partitioner {
    private static final long serialVersionUID = 1L;

    private final int numPartitions;

    private ClusterShardPartitioner(int numPartitions) {
      this.numPartitions = Math.max(1, numPartitions);
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
      ClusterShardSortKey sortKey = (ClusterShardSortKey) key;
      int hash = 31 * sortKey.clusterId + sortKey.shardId;
      hash ^= hash >>> 16;
      return Math.floorMod(hash, numPartitions);
    }
  }

  private static final class EncodedPostingRow implements Serializable {
    private static final long serialVersionUID = 1L;

    private final VectorRow source;
    private final byte[] signPlane;
    private final byte[] exPlanes;
    private final float fAdd1;
    private final float fRescale1;
    private final float err1;
    private final float fAddEx;
    private final float fRescaleEx;
    private final float residualNorm;
    private final Float vectorNorm;

    private EncodedPostingRow(
        VectorRow source,
        byte[] signPlane,
        byte[] exPlanes,
        QuantizedVector quantized,
        boolean includeVectorNorm) {
      this.source = source;
      this.signPlane = signPlane;
      this.exPlanes = exPlanes;
      this.fAdd1 = valueOrZero(quantized.getAdditiveFactor1());
      this.fRescale1 = valueOrZero(quantized.getRescaleFactor1());
      this.err1 = valueOrZero(quantized.getError1());
      this.fAddEx = valueOrZero(quantized.getAdditiveFactor());
      this.fRescaleEx = valueOrZero(quantized.getRescaleFactor());
      this.residualNorm = quantized.getScalar();
      this.vectorNorm = includeVectorNorm ? quantized.getVectorNorm() : null;
    }

    private void addTo(PostingBlockBuilder builder) {
      builder.addRow(
          source.recordKey,
          signPlane,
          exPlanes,
          fAdd1,
          fRescale1,
          err1,
          fAddEx,
          fRescaleEx,
          residualNorm,
          vectorNorm,
          source.fileId,
          source.baseInstantTime,
          source.partitionPath,
          source.rowPosition);
    }

    private static float valueOrZero(Float value) {
      return value == null ? 0.0f : value;
    }
  }
}
