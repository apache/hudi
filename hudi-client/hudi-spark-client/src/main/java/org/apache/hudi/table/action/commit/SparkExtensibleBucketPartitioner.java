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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.index.bucket.HoodieExtensibleBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import scala.Tuple2;

import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE;
import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE_TABLE;

public class SparkExtensibleBucketPartitioner<T> extends SparkHoodiePartitioner<T> {

  private final String indexKeyFiled;
  private final List<String> partitionPaths;
  private final TreeMap<Integer/*RDD partition id offset*/, ExtensibleBucketIdentifier/*partitionPath*/> rddPartitionToIdentifier;
  private final Map<String/*partition path*/, Pair<VersionedPartitionStartOffset/*latest committed*/, Option<VersionedPartitionStartOffset>/*optional uncommitted*/>> bucketVersionedLayout;
  private Map<String/*partition path*/, Set<String/*bucket file id*/>> updatePartitionPathToFileIds;
  private int totalBucketNum;

  private final boolean isOverwrite;
  private final boolean isNonBlockingConcurrencyControl;

  static class VersionedPartitionStartOffset implements Serializable {
    Short bucketVersion;
    Integer bucketNum;
    Integer partitionStartOffset;

    public VersionedPartitionStartOffset(short bucketVersion, int bucketNum, int partitionStartOffset) {
      this.bucketVersion = bucketVersion;
      this.bucketNum = bucketNum;
      this.partitionStartOffset = partitionStartOffset;
    }
  }

  public SparkExtensibleBucketPartitioner(WorkloadProfile profile,
                                          HoodieEngineContext context,
                                          HoodieTable table,
                                          HoodieWriteConfig config) {
    super(profile, table);
    if (!(table.getIndex() instanceof HoodieExtensibleBucketIndex)) {
      throw new HoodieException("HoodieExtensibleBucketIndex is required for SparkExtensibleBucketPartitioner, but got "
          + table.getIndex().getClass().getName());
    }
    this.indexKeyFiled = config.getBucketIndexHashField();
    this.partitionPaths = new ArrayList<>(profile.getPartitionPaths());
    this.bucketVersionedLayout = new HashMap<>();
    this.rddPartitionToIdentifier = new TreeMap<>();

    Map<String, Pair<ExtensibleBucketIdentifier, Option<ExtensibleBucketIdentifier>>> partitionToVersionedLayout =
        ExtensibleBucketIndexUtils.fetchLatestCommittedExtensibleBucketIdentifierWithUncommitted(table, new HashSet<>(partitionPaths));
    partitionToVersionedLayout.forEach((partition, pair) -> {
      ExtensibleBucketIdentifier committedIdentifier = pair.getKey();
      Option<ExtensibleBucketIdentifier> uncommittedIdentifier = pair.getValue();
      VersionedPartitionStartOffset committed = new VersionedPartitionStartOffset(committedIdentifier.getBucketVersion(), committedIdentifier.getBucketNum(), totalBucketNum);
      rddPartitionToIdentifier.put(totalBucketNum, committedIdentifier);
      totalBucketNum += committedIdentifier.getBucketNum();
      Option<VersionedPartitionStartOffset> uncommitted = uncommittedIdentifier.map(identifier -> {
        VersionedPartitionStartOffset uncommittedStartOffset = new VersionedPartitionStartOffset(identifier.getBucketVersion(), identifier.getBucketNum(), totalBucketNum);
        rddPartitionToIdentifier.put(totalBucketNum, identifier);
        totalBucketNum += identifier.getBucketNum();
        return uncommittedStartOffset;
      });
      bucketVersionedLayout.put(partition, Pair.of(committed, uncommitted));
    });
    WriteOperationType operationType = profile.getOperationType();
    this.isOverwrite = INSERT_OVERWRITE.equals(operationType) || INSERT_OVERWRITE_TABLE.equals(operationType);
    // TODO: consider overwrite scenario
    this.isNonBlockingConcurrencyControl = config.isNonBlockingConcurrencyControl();
    assignUpdates(profile);
  }

  private void assignUpdates(WorkloadProfile profile) {
    updatePartitionPathToFileIds = new HashMap<>();
    profile.getInputPartitionPathStatMap().forEach((partitionPath, stat) -> {
      updatePartitionPathToFileIds.putIfAbsent(partitionPath, new HashSet<>());
      stat.getUpdateLocationToCount().forEach((fileId, instantAndCount) -> {
        if (instantAndCount.getLeft() != null) {
          // for insert to a not exist bucket, index will tag with logical fileId and null instant calculated by index's bucket id and version
          // so even if the fileId in location present, we should ignore the case that instant is null
          updatePartitionPathToFileIds.get(partitionPath).add(fileId);
        }
      });
    });
  }

  @Override
  public BucketInfo getBucketInfo(int bucketNumber) {
    Map.Entry<Integer, ExtensibleBucketIdentifier> integerExtensibleBucketIdentifierEntry = rddPartitionToIdentifier.floorEntry(bucketNumber);
    int bucketStartOffset = integerExtensibleBucketIdentifierEntry.getKey();
    ExtensibleBucketIdentifier identifier = integerExtensibleBucketIdentifierEntry.getValue();
    int bucketId = bucketNumber - bucketStartOffset;
    if (isOverwrite) {
      throw new HoodieNotSupportedException("Overwrite is not supported for SparkExtensibleBucketPartitioner");
    }
    short bucketVersion = identifier.getBucketVersion();
    String fileIdPrefix = ExtensibleBucketIdentifier.newExtensibleBucketFileIdFixedSuffix(bucketId, bucketVersion);
    String partitionPath = identifier.getPartitionPath();
    Option<String> fileIdOption = Option.fromJavaOptional(updatePartitionPathToFileIds
        .getOrDefault(partitionPath, Collections.emptySet()).stream()
        .filter(e -> e.startsWith(fileIdPrefix))
        .findFirst());
    if (fileIdOption.isPresent()) {
      return new BucketInfo(BucketType.UPDATE, fileIdOption.get(), partitionPath);
    } else {
      // Always write into log file instead of base file if using NB-CC
      BucketType bucketType = isNonBlockingConcurrencyControl ? BucketType.UPDATE : BucketType.INSERT;
      return new BucketInfo(bucketType, fileIdPrefix, partitionPath);
    }
  }

  @Override
  public int getPartition(Object key) {
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    String partitionPath = keyLocation._1.getPartitionPath();
    Option<HoodieRecordLocation> location = keyLocation._2;
    // location is present and not logical location happens in update scenario
    // 1. For record writing to a bucket without pending bucket-resizing
    //    a). current bucket exist, update to the bucket, tagged with committed bucket version.
    //    b). current bucket not exist, insert to the bucket with current bucket layout, tagged with committed bucket version but is logical location.
    // 2. For record writing to a bucket during bucket-resizing
    //    record will be dual written to both committed bucket and uncommitted bucket, record-old to committed bucket, record-new to uncommitted bucket
    //    a). current bucket exist, update to the bucket
    //       i). record-old tag with committed bucket version.
    //       ii). record-new tag with uncommitted bucket version.
    //    b). current bucket not exist, insert to the bucket
    //       i). record-old tag with committed bucket version but is logical location.
    //       ii). record-new tag with uncommitted bucket version.


    // tagged scenario
    // for record in above cases:
    // --------- 1.a
    // --------- 2.a.i
    // --------- 2.a.ii
    // --------- 2.b.ii
    if (location.isPresent() && !location.get().isLogicalLocation()) {
      String fileId = location.get().getFileId();
      int bucketId = ExtensibleBucketIdentifier.bucketIdFromFileId(fileId);
      short bucketVersion = ExtensibleBucketIdentifier.bucketVersionFromFileId(fileId);
      Pair<VersionedPartitionStartOffset, Option<VersionedPartitionStartOffset>> versionedPartitionStartOffsetOptionPair = bucketVersionedLayout.get(partitionPath);
      VersionedPartitionStartOffset committed = versionedPartitionStartOffsetOptionPair.getKey();
      if (bucketVersion == committed.bucketVersion) {
        return committed.partitionStartOffset + bucketId;
      }
      if (versionedPartitionStartOffsetOptionPair.getValue().isPresent()) {
        VersionedPartitionStartOffset uncommitted = versionedPartitionStartOffsetOptionPair.getValue().get();
        if (bucketVersion == uncommitted.bucketVersion) {
          return uncommitted.partitionStartOffset + bucketId;
        }
      }
      // unexpected bucket version
      throw new HoodieException("Unexpected bucket version " + bucketVersion + " for file id " + fileId + "versioned layout " + versionedPartitionStartOffsetOptionPair);
    }
    // tagged with logical location scenario
    // for record in above cases:
    // --------- 1.b
    // --------- 2.b.i
    Pair<VersionedPartitionStartOffset, Option<VersionedPartitionStartOffset>> versionedPartitionStartOffsetOptionPair = bucketVersionedLayout.get(partitionPath);
    VersionedPartitionStartOffset committed = versionedPartitionStartOffsetOptionPair.getKey();
    return committed.partitionStartOffset + BucketIdentifier.getBucketId(keyLocation._1, indexKeyFiled, committed.bucketNum);
  }

  @Override
  public int numPartitions() {
    return totalBucketNum;
  }
}
