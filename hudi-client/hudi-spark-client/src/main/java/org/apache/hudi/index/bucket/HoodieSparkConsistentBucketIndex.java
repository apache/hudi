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

package org.apache.hudi.index.bucket;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Consistent hashing bucket index implementation, with auto-adjust bucket number.
 * NOTE: bucket resizing is triggered by clustering.
 */
public class HoodieSparkConsistentBucketIndex extends HoodieBucketIndex {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkConsistentBucketIndex.class);

  private Map<String, ConsistentBucketIdentifier> partitionToIdentifier;

  public HoodieSparkConsistentBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    return writeStatuses;
  }

  /**
   * Do nothing.
   * A failed write may create a hashing metadata for a partition. In this case, we still do nothing when rolling back
   * the failed write. Because the hashing metadata created by a write must have 00000000000000 timestamp and can be viewed
   * as the initialization of a partition rather than as a part of the failed write.
   * @param instantTime
   * @return
   */
  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  /**
   * Initialize bucket metadata for each partition
   * @param table
   * @param partitions partitions that need to be initialized
   */
  @Override
  protected void initialize(HoodieTable table, List<String> partitions) {
    partitionToIdentifier = new HashMap(partitions.size() + partitions.size() / 3);

    // TODO maybe parallel
    partitions.stream().forEach(p -> {
      HoodieConsistentHashingMetadata metadata = loadOrCreateMetadata(table, p);
      ConsistentBucketIdentifier identifier = new ConsistentBucketIdentifier(metadata);
      partitionToIdentifier.put(p, identifier);
    });
  }

  /**
   * Get bucket location for given key and partition
   *
   * @param key
   * @param partitionPath
   * @return
   */
  @Override
  protected HoodieRecordLocation getBucket(HoodieKey key, String partitionPath) {
    ConsistentHashingNode node = partitionToIdentifier.get(partitionPath).getBucket(key, indexKeyFields);
    if (node.getFileIdPfx() != null && !node.getFileIdPfx().isEmpty()) {
      /**
       * Dynamic Bucket Index doesn't need the instant time of the latest file group.
       * We add suffix 0 here to the file uuid, following the naming convention.
       */
      return new HoodieRecordLocation(null, FSUtils.createNewFileId(node.getFileIdPfx(), 0));
    }

    LOG.error("Consistent hashing node has no file group, partition: " + partitionPath + ", meta: "
        + partitionToIdentifier.get(partitionPath).getMetadata().getFilename() + ", record_key: " + key.toString());
    throw new HoodieIndexException("Failed to getBucket as hashing node has no file group");
  }

  /**
   * Load hashing metadata of the given partition, if it is not existed, create a new one (also persist it into storage)
   *
   * @param table     hoodie table
   * @param partition table partition
   * @return Consistent hashing metadata
   */
  public HoodieConsistentHashingMetadata loadOrCreateMetadata(HoodieTable table, String partition) {
    int retry = 3;
    // TODO maybe use ConsistencyGuard to do the retry thing
    // retry to allow concurrent creation of metadata (only one attempt can succeed)
    while (retry-- > 0) {
      HoodieConsistentHashingMetadata metadata = loadMetadata(table, partition);
      if (metadata == null) {
        metadata = new HoodieConsistentHashingMetadata(partition, numBuckets);
        if (saveMetadata(table, metadata, false)) {
          return metadata;
        }
      } else {
        return metadata;
      }
    }
    throw new HoodieIndexException("Failed to load or create metadata, partition: " + partition);
  }

  /**
   * Load hashing metadata of the given partition, if it is not existed, return null
   *
   * @param table     hoodie table
   * @param partition table partition
   * @return Consistent hashing metadata or null if it does not exist
   */
  public static HoodieConsistentHashingMetadata loadMetadata(HoodieTable table, String partition) {
    Path metadataPath = FSUtils.getPartitionPath(table.getMetaClient().getHashingMetadataPath(), partition);

    try {
      if (!table.getMetaClient().getFs().exists(metadataPath)) {
        return null;
      }
      FileStatus[] metaFiles = table.getMetaClient().getFs().listStatus(metadataPath);
      HoodieTimeline completedCommits = table.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants();
      Predicate<FileStatus> metaFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        return filename.contains(HoodieConsistentHashingMetadata.HASHING_METADATA_FILE_SUFFIX)
            && (completedCommits.containsInstant(HoodieConsistentHashingMetadata.getTimestampFromFile(filename))
            || HoodieConsistentHashingMetadata.getTimestampFromFile(filename).equals(HoodieTimeline.INIT_INSTANT_TS));
      };

      FileStatus metaFile = Arrays.stream(metaFiles).filter(metaFilePredicate)
          .max(Comparator.comparing(a -> a.getPath().getName())).orElse(null);

      if (metaFile != null) {
        byte[] content = FileIOUtils.readAsByteArray(table.getMetaClient().getFs().open(metaFile.getPath()));
        return HoodieConsistentHashingMetadata.fromBytes(content);
      }

      return null;
    } catch (IOException e) {
      LOG.warn("Error when loading hashing metadata, partition: " + partition + ", error meg: " + e.getMessage());
      throw new HoodieIndexException("Error while loading hashing metadata, " + e.getMessage());
    }
  }

  /**
   * Save metadata into storage
   * @param table
   * @param metadata
   * @param overwrite
   * @return
   */
  private static boolean saveMetadata(HoodieTable table, HoodieConsistentHashingMetadata metadata, boolean overwrite) {
    FSDataOutputStream fsOut = null;
    HoodieWrapperFileSystem fs = table.getMetaClient().getFs();
    Path dir = FSUtils.getPartitionPath(table.getMetaClient().getHashingMetadataPath(), metadata.getPartitionPath());
    Path fullPath = new Path(dir, metadata.getFilename());
    try {
      byte[] bytes = metadata.toBytes();
      fsOut = fs.create(fullPath, overwrite);
      fsOut.write(bytes);
      fsOut.close();
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      LOG.warn("Failed to update bucket metadata: " + metadata + ", error: " + e.getMessage());
    } finally {
      try {
        if (fsOut != null) {
          fsOut.close();
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close file: " + fullPath.toString(), e);
      }
    }
    return false;
  }
}
