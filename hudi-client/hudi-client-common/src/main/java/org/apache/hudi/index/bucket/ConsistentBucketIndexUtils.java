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
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASHING_METADATA_COMMIT_FILE_SUFFIX;
import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASHING_METADATA_FILE_SUFFIX;
import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.getTimestampFromFile;

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
   *
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
    if (saveMetadata(table, metadata, false)) {
      return metadata;
    }

    // The creation failed, so try load metadata again. Concurrent creation of metadata should have succeeded.
    // Note: the consistent problem of cloud storage is handled internal in the HoodieWrapperFileSystem, i.e., ConsistentGuard
    metadataOption = loadMetadata(table, partition);
    ValidationUtils.checkState(metadataOption.isPresent(), "Failed to load or create metadata, partition: " + partition);
    return metadataOption.get();
  }

  /**
   * Loads hashing metadata of the given partition, if it does not exist, returns empty.
   *
   * @param table     Hoodie table
   * @param partition Table partition
   * @return Consistent hashing metadata or empty if it does not exist
   */
  public static Option<HoodieConsistentHashingMetadata> loadMetadata(HoodieTable table, String partition) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    Path metadataPath = FSUtils.getPartitionPath(metaClient.getHashingMetadataPath(), partition);
    Path partitionPath = FSUtils.getPartitionPath(metaClient.getBasePathV2(), partition);
    try {
      Predicate<FileStatus> hashingMetaCommitFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        return filename.contains(HoodieConsistentHashingMetadata.HASHING_METADATA_COMMIT_FILE_SUFFIX);
      };
      Predicate<FileStatus> hashingMetadataFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        return filename.contains(HASHING_METADATA_FILE_SUFFIX);
      };
      final FileStatus[] metaFiles = metaClient.getFs().listStatus(metadataPath);
      final TreeSet<String> commitMetaTss = Arrays.stream(metaFiles).filter(hashingMetaCommitFilePredicate)
          .map(commitFile -> HoodieConsistentHashingMetadata.getTimestampFromFile(commitFile.getPath().getName()))
          .sorted()
          .collect(Collectors.toCollection(TreeSet::new));
      final FileStatus[] hashingMetaFiles = Arrays.stream(metaFiles).filter(hashingMetadataFilePredicate)
          .sorted(Comparator.comparing(f -> f.getPath().getName()))
          .toArray(FileStatus[]::new);
      // max committed metadata file
      final String maxCommitMetaFileTs = commitMetaTss.isEmpty() ? null : commitMetaTss.last();
      // max updated metadata file
      FileStatus maxMetadataFile = hashingMetaFiles.length > 0 ? hashingMetaFiles[hashingMetaFiles.length - 1] : null;
      // If single file present in metadata and if its default file return it
      if (maxMetadataFile != null && HoodieConsistentHashingMetadata.getTimestampFromFile(maxMetadataFile.getPath().getName()).equals(HoodieTimeline.INIT_INSTANT_TS)) {
        return loadMetadataFromGivenFile(table, maxMetadataFile);
      }
      // if max updated metadata file and committed metadata file are same then return
      if (maxCommitMetaFileTs != null && maxMetadataFile != null
          && maxCommitMetaFileTs.equals(HoodieConsistentHashingMetadata.getTimestampFromFile(maxMetadataFile.getPath().getName()))) {
        return loadMetadataFromGivenFile(table, maxMetadataFile);
      }
      HoodieTimeline completedCommits = metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

      // fix the in-consistency between un-committed and committed hashing metadata files.
      List<FileStatus> fixed = new ArrayList<>();
      Arrays.stream(hashingMetaFiles).forEach(hashingMetaFile -> {
        Path path = hashingMetaFile.getPath();
        String timestamp = HoodieConsistentHashingMetadata.getTimestampFromFile(path.getName());
        if (maxCommitMetaFileTs != null && timestamp.compareTo(maxCommitMetaFileTs) <= 0) {
          // only fix the metadata with greater timestamp than max committed timestamp
          return;
        }
        boolean isRehashingCommitted = completedCommits.containsInstant(timestamp) || timestamp.equals(HoodieTimeline.INIT_INSTANT_TS);
        if (isRehashingCommitted) {
          if (!commitMetaTss.contains(timestamp)) {
            try {
              createCommitMarker(table, path, partitionPath);
            } catch (IOException e) {
              throw new HoodieIOException("Exception while creating marker file " + path.getName() + " for partition " + partition, e);
            }
          }
          fixed.add(hashingMetaFile);
        } else if (recommitMetadataFile(table, hashingMetaFile, partition)) {
          fixed.add(hashingMetaFile);
        }
      });

      return fixed.isEmpty() ? Option.empty() : loadMetadataFromGivenFile(table, fixed.get(fixed.size() - 1));
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
   * @param table     Hoodie table
   * @param metadata  Hashing metadata to be saved
   * @param overwrite Whether to overwrite existing metadata
   * @return true if the metadata is saved successfully
   */
  public static boolean saveMetadata(HoodieTable table, HoodieConsistentHashingMetadata metadata, boolean overwrite) {
    HoodieWrapperFileSystem fs = table.getMetaClient().getFs();
    Path dir = FSUtils.getPartitionPath(table.getMetaClient().getHashingMetadataPath(), metadata.getPartitionPath());
    Path fullPath = new Path(dir, metadata.getFilename());
    try (FSDataOutputStream fsOut = fs.create(fullPath, overwrite)) {
      byte[] bytes = metadata.toBytes();
      fsOut.write(bytes);
      fsOut.close();
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to update bucket metadata: " + metadata, e);
    }
    return false;
  }

  /***
   * Creates commit marker corresponding to hashing metadata file after post commit clustering operation.
   *
   * @param table         Hoodie table
   * @param fileStatus    File for which commit marker should be created
   * @param partitionPath Partition path the file belongs to
   * @throws IOException
   */
  private static void createCommitMarker(HoodieTable table, Path fileStatus, Path partitionPath) throws IOException {
    HoodieWrapperFileSystem fs = table.getMetaClient().getFs();
    Path fullPath = new Path(partitionPath, getTimestampFromFile(fileStatus.getName()) + HASHING_METADATA_COMMIT_FILE_SUFFIX);
    if (fs.exists(fullPath)) {
      return;
    }
    FileIOUtils.createFileInPath(fs, fullPath, Option.of(StringUtils.EMPTY_STRING.getBytes()));
  }

  /***
   * Loads consistent hashing metadata of table from the given meta file
   *
   * @param table    Hoodie table
   * @param metaFile Hashing metadata file
   * @return HoodieConsistentHashingMetadata object
   */
  private static Option<HoodieConsistentHashingMetadata> loadMetadataFromGivenFile(HoodieTable table, FileStatus metaFile) {
    try {
      if (metaFile == null) {
        return Option.empty();
      }
      byte[] content = FileIOUtils.readAsByteArray(table.getMetaClient().getFs().open(metaFile.getPath()));
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
  private static boolean recommitMetadataFile(HoodieTable table, FileStatus metaFile, String partition) {
    Path partitionPath = FSUtils.getPartitionPath(table.getMetaClient().getBasePathV2(), partition);
    String timestamp = getTimestampFromFile(metaFile.getPath().getName());
    if (table.getPendingCommitTimeline().containsInstant(timestamp)) {
      return false;
    }
    Option<HoodieConsistentHashingMetadata> hoodieConsistentHashingMetadataOption = loadMetadataFromGivenFile(table, metaFile);
    if (!hoodieConsistentHashingMetadataOption.isPresent()) {
      return false;
    }
    HoodieConsistentHashingMetadata hoodieConsistentHashingMetadata = hoodieConsistentHashingMetadataOption.get();

    Predicate<String> hoodieFileGroupIdPredicate = hoodieBaseFile -> hoodieConsistentHashingMetadata.getNodes().stream().anyMatch(node -> node.getFileIdPrefix().equals(hoodieBaseFile));
    if (table.getBaseFileOnlyView().getLatestBaseFiles(partition)
        .map(fileIdPrefix -> FSUtils.getFileIdPfxFromFileId(fileIdPrefix.getFileId())).anyMatch(hoodieFileGroupIdPredicate)) {
      try {
        createCommitMarker(table, metaFile.getPath(), partitionPath);
        return true;
      } catch (IOException e) {
        throw new HoodieIOException("Exception while creating marker file " + metaFile.getPath().getName() + " for partition " + partition, e);
      }
    }
    return false;
  }
}
