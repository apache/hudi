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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIndexException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/**
 * Helper class to manage consistent bucket index metadata.
 */
public class ConsistentBucketIndexUtils {

  private static final Logger LOG = LogManager.getLogger(ConsistentBucketIndexUtils.class);

  /**
   * Load hashing metadata of the given partition, if it is not existed, create a new one (also persist it into storage)
   * NOTE: When creating a new hashing metadata, the content will always be the same for the same partition. It means when
   * multiple writer are trying to initialize metadata for the same partition, no lock or synchronization is necessary as they
   * are creating the file with the same content;
   *
   * @param metaClient  hoodie meta client
   * @param partition   table partition
   * @param numBuckets  default bucket number
   * @return Consistent hashing metadata
   */
  public static HoodieConsistentHashingMetadata loadOrCreateMetadata(HoodieTableMetaClient metaClient, String partition, int numBuckets) {
    Option<HoodieConsistentHashingMetadata> metadataOption = loadMetadata(metaClient, partition);
    if (metadataOption.isPresent()) {
      return metadataOption.get();
    }

    LOG.info("Failed to load metadata, try to create one. Partition: " + partition);

    // There is no metadata, so try to create a new one and save it.
    HoodieConsistentHashingMetadata metadata = new HoodieConsistentHashingMetadata(partition, numBuckets);
    if (saveMetadata(metaClient, metadata)) {
      return metadata;
    }

    LOG.info("Failed to create metadata (May caused by concurrent metadata creation. Try load again. Partition:" + partition);
    // The creation failed, so try load metadata again. Concurrent creation of metadata should have succeeded.
    // Note: the consistent problem of cloud storage is handled internal in the HoodieWrapperFileSystem, i.e., ConsistentGuard
    metadataOption = loadMetadata(metaClient, partition);
    ValidationUtils.checkState(metadataOption.isPresent(), "Failed to load or create metadata, partition: " + partition);
    return metadataOption.get();
  }


  /**
   * Load hashing metadata of the given partition, if it is not existed, return Option.empty
   *
   * @param metaClient  hoodie meta client
   * @param partition   table partition
   * @return Consistent hashing metadata or Option.empty if it does not exist
   */
  public static Option<HoodieConsistentHashingMetadata> loadMetadata(HoodieTableMetaClient metaClient, String partition) {
    Path metadataPath = FSUtils.getPartitionPath(metaClient.getHashingMetadataPath(), partition);

    try {
      HoodieWrapperFileSystem fs = metaClient.getFs();
      if (!fs.exists(metadataPath)) {
        return Option.empty();
      }
      FileStatus[] metaFiles = fs.listStatus(metadataPath);
      final HoodieTimeline completedCommits = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      Predicate<FileStatus> metaFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        if (!filename.endsWith(HoodieConsistentHashingMetadata.HASHING_METADATA_FILE_SUFFIX)) {
          return false;
        }
        String timestamp = HoodieConsistentHashingMetadata.getTimestampFromFile(filename);
        return completedCommits.containsInstant(timestamp) || timestamp.equals(HoodieTimeline.INIT_INSTANT_TS);
      };

      // Get a valid hashing metadata with the largest (latest) timestamp
      FileStatus metaFile = Arrays.stream(metaFiles).filter(metaFilePredicate)
          .max(Comparator.comparing(a -> a.getPath().getName())).orElse(null);

      if (metaFile == null) {
        return Option.empty();
      }

      byte[] content = FileIOUtils.readAsByteArray(fs.open(metaFile.getPath()));
      return Option.of(HoodieConsistentHashingMetadata.fromBytes(content));
    } catch (IOException e) {
      LOG.error("Error when loading hashing metadata, partition: " + partition, e);
      throw new HoodieIndexException("Error while loading hashing metadata, partition: " + partition, e);
    }
  }

  /**
   * Save metadata into storage
   *
   * @param metaClient  hoodie meta client
   * @param metadata    hashing metadata to be saved
   * @return true if the metadata is saved successfully
   */
  public static boolean saveMetadata(HoodieTableMetaClient metaClient, HoodieConsistentHashingMetadata metadata) {
    HoodieWrapperFileSystem fs = metaClient.getFs();
    Path dir = FSUtils.getPartitionPath(metaClient.getHashingMetadataPath(), metadata.getPartitionPath());
    Path fullPath = new Path(dir, metadata.getFilename() + ".tmp." + Math.abs(ThreadLocalRandom.current().nextInt()));
    try (FSDataOutputStream fsOut = fs.create(fullPath, false)) {
      byte[] bytes = metadata.toBytes();
      fsOut.write(bytes);
      fsOut.flush();
    } catch (IOException e) {
      LOG.warn("Failed to save bucket to tmp path, metadata: " + metadata, e);
      return false;
    }

    try {
      // Atomic renaming to ensure only one can succeed creating the initial hashing metadata
      boolean success = fs.rename(fullPath, new Path(dir, metadata.getFilename()));
      if (success) {
        return true;
      }
    } catch (FileAlreadyExistsException e) {
      LOG.info("Failed to save bucket metadata: " + metadata + " as it has been created by others");
    } catch (IOException e) {
      LOG.warn("Failed to save bucket metadata: " + metadata, e);
    }

    // Cleanup tmp files if necessary
    try {
      if (fs.exists(fullPath)) {
        fs.delete(fullPath);
      }
    } catch (IOException ex) {
      LOG.warn("Error trying to clean up temporary metadata: " + metadata, ex);
    }
    return false;
  }
}
