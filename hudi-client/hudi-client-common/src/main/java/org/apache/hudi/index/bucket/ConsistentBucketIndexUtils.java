/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASHING_METADATA_COMMIT_FILE_SUFFIX;
import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASHING_METADATA_FILE_SUFFIX;
import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.getTimestampFromFile;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Utilities class for consistent bucket index metadata management.
 */
public class ConsistentBucketIndexUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketIndexUtils.class);

  /**
   * Loads hashing metadata of the given partition, if it does not exist, creates a new one (also persist it into storage).
   *
   * <p>NOTE: When creating a new hashing metadata, the content will always be the same for the same partition.
   * It means when multiple writer are trying to initialize metadata for the same partition,
   * no lock or synchronization is necessary as they are creating the file with the same content.
   *
   * @param table      Hoodie table
   * @param partition  Table partition
   * @param numBuckets Default bucket number
   * @return Consistent hashing metadata
   */
  public static HoodieConsistentHashingMetadata loadOrCreateMetadata(HoodieTable table, String partition, int numBuckets) {
    Option<HoodieConsistentHashingMetadata> metadataOption = loadMetadata(table, partition);
    if (metadataOption.isPresent()) {
      return metadataOption.get();
    }

    LOG.info("Failed to load metadata, try to create one. Partition: {}.", partition);

    // There is no metadata, so try to create a new one and save it.
    HoodieConsistentHashingMetadata metadata = new HoodieConsistentHashingMetadata(partition, numBuckets);
    if (saveMetadata(table, metadata)) {
      return metadata;
    }

    // The creation failed, so try load metadata again. Concurrent creation of metadata should have succeeded.
    // Note: the consistent problem of cloud storage is handled internal in the HoodieWrapperFileSystem, i.e., ConsistentGuard
    metadataOption = loadMetadata(table, partition);
    ValidationUtils.checkState(metadataOption.isPresent(), "Failed to load or create metadata, partition: " + partition);
    return metadataOption.get();
  }

  /**
   * Loads latest committed hashing metadata of the given partition, if it does not exist, returns empty.
   *
   * @param table     Hoodie table
   * @param partition Table partition
   * @return Consistent hashing metadata or empty if it does not exist
   */
  public static Option<HoodieConsistentHashingMetadata> loadMetadata(HoodieTable table, String partition) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    StoragePath metadataPath = FSUtils.constructAbsolutePath(metaClient.getHashingMetadataPath(), partition);
    try {
      Predicate<StoragePathInfo> hashingMetaCommitFilePredicate = pathInfo -> {
        String filename = pathInfo.getPath().getName();
        return filename.endsWith(HoodieConsistentHashingMetadata.HASHING_METADATA_COMMIT_FILE_SUFFIX);
      };
      Predicate<StoragePathInfo> hashingMetadataFilePredicate = pathInfo -> {
        String filename = pathInfo.getPath().getName();
        return filename.endsWith(HASHING_METADATA_FILE_SUFFIX);
      };
      final List<StoragePathInfo> metaFiles = metaClient.getStorage().listDirectEntries(metadataPath);

      final TreeMap<String/*instantTime*/, Pair<StoragePathInfo/*hash metadata file path*/, Boolean/*commited*/>> versionedHashMetadataFiles = metaFiles.stream()
          .filter(hashingMetadataFilePredicate)
          .map(metaFile -> {
            String instantTime = HoodieConsistentHashingMetadata.getTimestampFromFile(metaFile.getPath().getName());
            return Pair.of(instantTime, Pair.of(metaFile, false));
          })
          .sorted(Collections.reverseOrder())
          .collect(Collectors.toMap(Pair::getLeft, Pair::getRight, (a, b) -> a, TreeMap::new));

      metaFiles.stream().filter(hashingMetaCommitFilePredicate)
          .forEach(commitFile -> {
            String instantTime = HoodieConsistentHashingMetadata.getTimestampFromFile(commitFile.getPath().getName());
            if (!versionedHashMetadataFiles.containsKey(instantTime)) {
              // unexpect that the commit file exists but the corresponding metadata file does not
              LOG.error("Commit file {} exists but the corresponding metadata file does not", commitFile.getPath().getName());
              throw new HoodieIndexException("Commit file: " + commitFile.getPath().getName() + " exists but the corresponding metadata file does not");
            }
            versionedHashMetadataFiles.computeIfPresent(instantTime, (k, v) -> Pair.of(v.getLeft(), true));
          });

      Option<Pair<String/*instant*/, StoragePathInfo/*hash metadata file path*/>> latestCommittedMetaFile = Option.fromJavaOptional(versionedHashMetadataFiles.entrySet()
          .stream()
          .filter(entry -> entry.getValue().getRight())
          .map(entry -> Pair.of(entry.getKey(), entry.getValue().getLeft()))
          .findFirst());

      final List<Pair<String/*instant*/, StoragePathInfo/*hash metadata file path*/>> uncommittedMetaFilesAfterLatestCommited = latestCommittedMetaFile
          .map(pair -> versionedHashMetadataFiles.tailMap(pair.getLeft()))
          .orElse(versionedHashMetadataFiles.tailMap(HoodieTimeline.INIT_INSTANT_TS, true))
          .entrySet()
          .stream()
          .map(entry -> Pair.of(entry.getKey(), entry.getValue().getLeft()))
          .sorted(Comparator.comparing(Pair::getLeft))
          .collect(Collectors.toList());

      if (uncommittedMetaFilesAfterLatestCommited.isEmpty()) {
        // all metadata files are committed, pick the latest committed file's hash metadata
        return latestCommittedMetaFile.map(pair -> loadMetadataFromGivenFile(table, pair.getRight())).orElse(Option.empty());
      }

      // find that there are uncommitted metadata files after the latest committed metadata file, we need to resolve the inconsistency
      HoodieTimeline completedCommits = metaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants();
      // fix from the latest committed metadata file (exclusive) to the latest uncommitted metadata file (inclusive)
      for (Pair<String, StoragePathInfo> pair : uncommittedMetaFilesAfterLatestCommited) {
        String instantTime = pair.getLeft();
        StoragePathInfo hashMetadataPath = pair.getRight();
        /** check if the metadata file can be committed
         * 1. the action corresponding to the file has already been committed on the timeline
         * 2. the file is the first metadata file of the partition whose instant equals to {@link HoodieTimeline#INIT_INSTANT_TS}
         */
        if (completedCommits.containsInstant(instantTime) || instantTime.equals(HoodieTimeline.INIT_INSTANT_TS)) {
          try {
            createCommitMarker(table, hashMetadataPath.getPath(), metadataPath);
          } catch (IOException e) {
            throw new HoodieIOException("Exception while creating marker file for hash metadata file: " + hashMetadataPath.getPath().getName() + " in partition " + partition, e);
          }
          // update the latest committed metadata file
          latestCommittedMetaFile = Option.of(pair);
        } else if (recommitMetadataFile(table, hashMetadataPath, partition)) {
          // the un-initial hash metadata file exist but there is no corresponding commit file, and no corresponding completed commit on the active timeline
          // recommit it to fix the inconsistency
          // update the latest committed metadata file
          latestCommittedMetaFile = Option.of(pair);
        }
      }
      // after fixing the inconsistency, return the latest committed metadata file
      return latestCommittedMetaFile.map(pair -> loadMetadataFromGivenFile(table, pair.getRight())).orElse(Option.empty());
    } catch (FileNotFoundException e) {
      return Option.empty();
    } catch (IOException e) {
      LOG.error("Error when loading hashing metadata, partition: " + partition, e);
      throw new HoodieIndexException("Error while loading hashing metadata", e);
    }
  }

  /**
   * Saves the metadata into storage
   *
   * @param table    Hoodie table
   * @param metadata Hashing metadata to be saved
   * @return true if the metadata is saved successfully
   */
  public static boolean saveMetadata(HoodieTable table, HoodieConsistentHashingMetadata metadata) {
    HoodieStorage storage = table.getStorage();
    StoragePath dir = FSUtils.constructAbsolutePath(
        table.getMetaClient().getHashingMetadataPath(), metadata.getPartitionPath());
    StoragePath fullPath = new StoragePath(dir, metadata.getFilename());
    try {
      if (storage.exists(fullPath)) {
        // the file has been created by other tasks
        return true;
      }
      storage.createImmutableFileInPath(fullPath, Option.of(metadata.toBytes()), true);
      return true;
    } catch (IOException e1) {
      // ignore the exception and check the file existence
      try {
        if (storage.exists(fullPath)) {
          return true;
        }
      } catch (IOException e2) {
        // ignore the exception and return false
        LOG.warn("Failed to check the existence of bucket metadata file: " + fullPath, e2);
      }
      LOG.warn("Failed to update bucket metadata: " + metadata, e1);
      return false;
    }
  }

  /***
   * Creates commit marker corresponding to hashing metadata file after post commit clustering operation.
   *
   * @param table         Hoodie table
   * @param path          File for which commit marker should be created
   * @param metadataPath  Consistent-Bucket metadata path the file belongs to
   * @throws IOException
   */
  private static void createCommitMarker(HoodieTable table, StoragePath path, StoragePath metadataPath) throws IOException {
    HoodieStorage storage = table.getStorage();
    StoragePath fullPath = new StoragePath(metadataPath,
        getTimestampFromFile(path.getName()) + HASHING_METADATA_COMMIT_FILE_SUFFIX);
    if (storage.exists(fullPath)) {
      return;
    }
    // prevent exception from race condition. We are ok with the file being created in another thread, so we should
    // check for the marker after catching the exception and we don't need to fail if the file exists
    try {
      FileIOUtils.createFileInPath(storage, fullPath, Option.of(getUTF8Bytes(StringUtils.EMPTY_STRING)));
      LOG.info("Created commit marker: {} for metadata file: {}", fullPath.getName(), path.getName());
    } catch (HoodieIOException e) {
      if (!storage.exists(fullPath)) {
        throw e;
      }
      LOG.warn("Failed to create marker but " + fullPath + " exists", e);
    }
  }

  /***
   * Loads consistent hashing metadata of table from the given meta file
   *
   * @param table    Hoodie table
   * @param metaFile Hashing metadata file
   * @return HoodieConsistentHashingMetadata object
   */
  private static Option<HoodieConsistentHashingMetadata> loadMetadataFromGivenFile(HoodieTable table, StoragePathInfo metaFile) {
    if (metaFile == null) {
      return Option.empty();
    }
    try (InputStream is = table.getStorage().open(metaFile.getPath())) {
      byte[] content = FileIOUtils.readAsByteArray(is);
      return Option.of(HoodieConsistentHashingMetadata.fromBytes(content));
    } catch (FileNotFoundException e) {
      return Option.empty();
    } catch (IOException e) {
      LOG.error("Error when loading hashing metadata, for path: " + metaFile.getPath().getName(), e);
      throw new HoodieIndexException("Error while loading hashing metadata", e);
    }
  }

  /***
   * COMMIT MARKER RECOVERY JOB.
   *
   * <p>If particular hashing metadata file doesn't have commit marker then there could be a case where clustering is done but post commit marker
   * creation operation failed. In this case this method will check file group id from consistent hashing metadata against storage base file group ids.
   * if one of the file group matches then we can conclude that this is the latest metadata file.
   *
   * <p>Note : we will end up calling this method if there is no marker file and no replace commit on active timeline, if replace commit is not present on
   * active timeline that means old file group id's before clustering operation got cleaned and only new file group id's of current clustering operation
   * are present on the disk.
   *
   * @param table     Hoodie table
   * @param metaFile  Metadata file on which sync check needs to be performed
   * @param partition Partition metadata file belongs to
   * @return true if hashing metadata file is latest else false
   */
  private static boolean recommitMetadataFile(HoodieTable table, StoragePathInfo metaFile, String partition) {
    StoragePath metadataPath = FSUtils.constructAbsolutePath(table.getMetaClient().getHashingMetadataPath(), partition);
    String timestamp = getTimestampFromFile(metaFile.getPath().getName());
    if (table.getPendingCommitsTimeline().containsInstant(timestamp)) {
      return false;
    }
    Option<HoodieConsistentHashingMetadata> hoodieConsistentHashingMetadataOption = loadMetadataFromGivenFile(table, metaFile);
    if (!hoodieConsistentHashingMetadataOption.isPresent()) {
      return false;
    }
    HoodieConsistentHashingMetadata hoodieConsistentHashingMetadata = hoodieConsistentHashingMetadataOption.get();

    Predicate<String> hoodieFileGroupIdPredicate = hoodieBaseFile ->
        hoodieConsistentHashingMetadata.getNodes()
            .stream()
            .anyMatch(node -> node.getFileIdPrefix().equals(hoodieBaseFile));
    if (table.getBaseFileOnlyView().getLatestBaseFiles(partition)
        .map(fileIdPrefix -> FSUtils.getFileIdPfxFromFileId(fileIdPrefix.getFileId())).anyMatch(hoodieFileGroupIdPredicate)) {
      try {
        createCommitMarker(table, metaFile.getPath(), metadataPath);
        LOG.info("Recommit metadata file: {} in partition: {}", metaFile.getPath().getName(), partition);
        return true;
      } catch (IOException e) {
        throw new HoodieIOException("Exception while creating marker file " + metaFile.getPath().getName() + " for partition " + partition, e);
      }
    }
    return false;
  }

  /**
   * Initialize fileIdPfx for each data partition. Specifically, the following fields is constructed:
   * - fileIdPfxList: the Nth element corresponds to the Nth data partition, indicating its fileIdPfx
   * - partitionToFileIdPfxIdxMap (return value): (table partition) -> (fileIdPfx -> idx) mapping
   *
   * @param partitionToIdentifier Mapping from table partition to bucket identifier
   */
  public static Map<String, Map<String, Integer>> generatePartitionToFileIdPfxIdxMap(Map<String, ConsistentBucketIdentifier> partitionToIdentifier) {
    Map<String, Map<String, Integer>> partitionToFileIdPfxIdxMap = new HashMap(partitionToIdentifier.size() * 2);
    int count = 0;
    for (ConsistentBucketIdentifier identifier : partitionToIdentifier.values()) {
      Map<String, Integer> fileIdPfxToIdx = new HashMap();
      for (ConsistentHashingNode node : identifier.getNodes()) {
        fileIdPfxToIdx.put(node.getFileIdPrefix(), count++);
      }
      partitionToFileIdPfxIdxMap.put(identifier.getMetadata().getPartitionPath(), fileIdPfxToIdx);
    }
    return partitionToFileIdPfxIdxMap;
  }
}
