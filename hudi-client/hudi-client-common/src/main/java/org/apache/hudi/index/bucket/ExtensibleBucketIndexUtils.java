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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ExtensibleBucketResizingOperation;
import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.BaseExtensibleBucketClusteringPlanStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Most of the utility methods copied from {@link HoodieConsistentBucketIndex}, TODO: Refactor to common class
 */
public class ExtensibleBucketIndexUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensibleBucketIndexUtils.class);

  public static final String METADATA_MARK_AS_BUCKET_RESIZING = "bucket.resizing.mark";
  public static final String METADATA_PARTITION_PATH = "bucket.resizing.partition";
  public static final String METADATA_NEW_BUCKET_NUMBER = "bucket.resizing.new.bucket.number";
  public static final String METADATA_NEW_BUCKET_VERSION = "bucket.resizing.new.bucket.version";
  public static final String METADATA_PREV_BUCKET_NUMBER = "bucket.resizing.prev.bucket.number";
  public static final String METADATA_BUCKET_RESIZING_OP = "bucket.resizing.op";

  public static ExtensibleBucketIdentifier loadExtensibleBucketIdentifierWithExistLocation(final HoodieTable table, final String partition) {
    HoodieExtensibleBucketMetadata metadata = loadOrCreateMetadata(table, partition);

    Map<Integer/*bucket id*/, HoodieRecordLocation> bucketIdToFileIdMapping = new HashMap<>();

    HoodieIndexUtils.getLatestFileSlicesForPartition(partition, table).stream().filter(slice -> {
      String fileId = slice.getFileId();
      // only interested in file slices that are of the same version as the metadata
      return ExtensibleBucketIdentifier.extensibleBucketIdFromFileId(fileId).getBucketVersion() == metadata.getBucketVersion();
    }).forEach(slice -> {
      String fileId = slice.getFileId();
      String instant = slice.getBaseInstantTime();

      int bucketId = ExtensibleBucketIdentifier.extensibleBucketIdFromFileId(fileId).getBucketId();
      if (!bucketIdToFileIdMapping.containsKey(bucketId)) {
        bucketIdToFileIdMapping.put(bucketId, new HoodieRecordLocation(instant, fileId));
      } else {
        LOG.error("Duplicate bucketId found for fileId: " + fileId + ", existing: " + bucketIdToFileIdMapping.get(bucketId));
        throw new HoodieIndexException("Duplicate bucketId found for fileId: " + fileId);
      }
    });

    return new ExtensibleBucketIdentifier(metadata, false, bucketIdToFileIdMapping);
  }

  /**
   * Load or create metadata for the given partition.
   */
  public static HoodieExtensibleBucketMetadata loadOrCreateMetadata(HoodieTable table, String partition) {
    int bucketNum = table.getMetaClient().getTableConfig().getInitialBucketNumberForNewPartition();
    return loadOrCreateMetadata(table, partition, bucketNum);
  }

  /**
   * Load or create metadata for the given partition using the given bucket number.
   * @param table Hoodie table
   * @param partition Partition path
   * @param bucketNum Initial bucket number
   */
  public static HoodieExtensibleBucketMetadata loadOrCreateMetadata(HoodieTable table, String partition, int bucketNum) {
    Option<HoodieExtensibleBucketMetadata> metadataOpt = loadMetadata(table, partition);
    if (metadataOpt.isPresent()) {
      return metadataOpt.get();
    }

    LOG.info("Failed to load extensible bucket metadata for partition " + partition + ". Creating new metadata");
    HoodieExtensibleBucketMetadata metadata = HoodieExtensibleBucketMetadata.initialVersionMetadata(partition, bucketNum);
    if (saveMetadata(table, metadata, false)) {
      return metadata;
    }

    // The creation failed, so try load metadata again. Concurrent creation of metadata should have succeeded.
    // Note: the consistent problem of cloud storage is handled internal in the HoodieWrapperFileSystem, i.e., ConsistentGuard
    metadataOpt = loadMetadata(table, partition);
    ValidationUtils.checkState(metadataOpt.isPresent(), "Failed to load or create metadata, partition: " + partition);
    return metadataOpt.get();
  }

  public static boolean saveMetadata(HoodieTable table, HoodieExtensibleBucketMetadata metadata, boolean overwrite) {
    HoodieStorage storage = table.getStorage();
    StoragePath dir = FSUtils.constructAbsolutePath(
        table.getMetaClient().getExtensibleBucketMetadataPath(), metadata.getPartitionPath());
    StoragePath fullPath = new StoragePath(dir, metadata.getFilename());
    try (OutputStream out = storage.create(fullPath, overwrite)) {
      byte[] bytes = metadata.toBytes();
      out.write(bytes);
      out.close();
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to update extensible bucket metadata: " + metadata, e);
    }
    return false;
  }

  public static Option<HoodieExtensibleBucketMetadata> loadMetadata(HoodieTable table, String partition) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    StoragePath metadataPath = FSUtils.constructAbsolutePath(metaClient.getExtensibleBucketMetadataPath(), partition);
    try {
      Predicate<StoragePathInfo> hashingMetaCommitFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        return filename.contains(HoodieExtensibleBucketMetadata.BUCKET_METADATA_COMMIT_FILE_SUFFIX);
      };
      Predicate<StoragePathInfo> hashingMetadataFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        return filename.contains(HoodieExtensibleBucketMetadata.BUCKET_METADATA_FILE_SUFFIX);
      };
      final List<StoragePathInfo> metaFiles = metaClient.getStorage().listDirectEntries(metadataPath);
      final TreeSet<String> commitMetaTss = metaFiles.stream().filter(hashingMetaCommitFilePredicate)
          .map(commitFile -> HoodieExtensibleBucketMetadata.getInstantFromFile(commitFile.getPath().getName()))
          .sorted()
          .collect(Collectors.toCollection(TreeSet::new));
      final List<StoragePathInfo> hashingMetaFiles = metaFiles.stream().filter(hashingMetadataFilePredicate)
          .sorted(Comparator.comparing(f -> f.getPath().getName()))
          .collect(Collectors.toList());
      // max committed metadata file
      final String maxCommitMetaFileTs = commitMetaTss.isEmpty() ? null : commitMetaTss.last();
      // max updated metadata file
      StoragePathInfo maxMetadataFile = hashingMetaFiles.size() > 0 ? hashingMetaFiles.get(hashingMetaFiles.size() - 1) : null;
      // If single file present in metadata and if its default file return it
      if (maxMetadataFile != null && HoodieExtensibleBucketMetadata.getInstantFromFile(maxMetadataFile.getPath().getName()).equals(HoodieTimeline.INIT_INSTANT_TS)) {
        return loadMetadataFromGivenFile(table, maxMetadataFile);
      }
      // if max updated metadata file and committed metadata file are same then return
      if (maxCommitMetaFileTs != null && maxMetadataFile != null
          && maxCommitMetaFileTs.equals(HoodieExtensibleBucketMetadata.getInstantFromFile(maxMetadataFile.getPath().getName()))) {
        return loadMetadataFromGivenFile(table, maxMetadataFile);
      }
      HoodieTimeline completedCommits = metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

      // fix the in-consistency between un-committed and committed hashing metadata files.
      List<StoragePathInfo> fixed = new ArrayList<>();
      hashingMetaFiles.forEach(hashingMetaFile -> {
        StoragePath path = hashingMetaFile.getPath();
        String timestamp = HoodieExtensibleBucketMetadata.getInstantFromFile(path.getName());
        if (maxCommitMetaFileTs != null && timestamp.compareTo(maxCommitMetaFileTs) <= 0) {
          // only fix the metadata with greater timestamp than max committed timestamp
          return;
        }
        boolean isResizingCommitted = completedCommits.containsInstant(timestamp) || timestamp.equals(HoodieTimeline.INIT_INSTANT_TS);
        if (isResizingCommitted) {
          if (!commitMetaTss.contains(timestamp)) {
            try {
              createCommitMarker(table, path, metadataPath);
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

  /***
   * Creates commit marker corresponding to hashing metadata file after post commit clustering operation.
   *
   * @param table         Hoodie table
   * @param fileStatus    File for which commit marker should be created
   * @param metadataPartitionPath Extensible Bucket Metadata partition path, format : {basePath}/.hoodie/.bucket_index/extensible_bucket_metadata/{partition}
   * @throws IOException
   */
  private static void createCommitMarker(HoodieTable table, StoragePath fileStatus, StoragePath metadataPartitionPath) throws IOException {
    HoodieStorage storage = table.getStorage();
    StoragePath fullPath = new StoragePath(metadataPartitionPath,
        HoodieExtensibleBucketMetadata.getInstantFromFile(fileStatus.getName()) + HoodieExtensibleBucketMetadata.BUCKET_METADATA_COMMIT_FILE_SUFFIX);
    if (storage.exists(fullPath)) {
      return;
    }
    FileIOUtils.createFileInPath(storage, fullPath, Option.of(StringUtils.EMPTY_STRING.getBytes()));
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
    StoragePath metadataPath = FSUtils.constructAbsolutePath(table.getMetaClient().getExtensibleBucketMetadataPath(), partition);
    String timestamp = HoodieExtensibleBucketMetadata.getInstantFromFile(metaFile.getPath().getName());
    if (table.getPendingCommitsTimeline().containsInstant(timestamp)) {
      return false;
    }
    Option<HoodieExtensibleBucketMetadata> extensibleBucketMetadataOption = loadMetadataFromGivenFile(table, metaFile);
    if (extensibleBucketMetadataOption.isEmpty()) {
      return false;
    }
    HoodieExtensibleBucketMetadata extensibleBucketMetadata = extensibleBucketMetadataOption.get();

    Predicate<String> hoodieFileGroupIdPredicate =
        fileIdPrefix -> new ExtensibleBucketIdentifier(extensibleBucketMetadata).generateFileIdPrefixForAllBuckets().anyMatch(prefix -> prefix.equals(fileIdPrefix));
    if (table.getBaseFileOnlyView().getLatestBaseFiles(partition)
        .map(baseFile -> FSUtils.getFileIdPfxFromFileId(baseFile.getFileId())).anyMatch(hoodieFileGroupIdPredicate)) {
      try {
        createCommitMarker(table, metaFile.getPath(), metadataPath);
        return true;
      } catch (IOException e) {
        throw new HoodieIOException("Exception while creating marker file " + metaFile.getPath().getName() + " for partition " + partition, e);
      }
    }
    return false;
  }

  /**
   * Loads extensible bucket metadata of table from the given meta file
   *
   * @param table    Hoodie table
   * @param metaFile Hashing metadata file
   * @return HoodieConsistentHashingMetadata object
   */
  private static Option<HoodieExtensibleBucketMetadata> loadMetadataFromGivenFile(HoodieTable table, StoragePathInfo metaFile) {
    if (metaFile == null) {
      return Option.empty();
    }
    try (InputStream is = table.getStorage().open(metaFile.getPath())) {
      byte[] content = FileIOUtils.readAsByteArray(is);
      return Option.of(HoodieExtensibleBucketMetadata.fromBytes(content));
    } catch (FileNotFoundException e) {
      return Option.empty();
    } catch (IOException e) {
      LOG.error("Error when loading extensible bucket metadata, for path: " + metaFile.getPath().getName(), e);
      throw new HoodieIndexException("Error while loading extensible bucket metadata", e);
    }
  }

  public static Map<String/*extra metadata key*/, String/*extra metadata value*/> constructExtensibleExtraMetadata(String partition, HoodieExtensibleBucketMetadata currentMetadata,
                                                                                                                   int newBucketNum) {
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(METADATA_MARK_AS_BUCKET_RESIZING, "true");
    extraMetadata.put(METADATA_PARTITION_PATH, partition);
    extraMetadata.put(METADATA_NEW_BUCKET_NUMBER, String.valueOf(newBucketNum));
    extraMetadata.put(METADATA_NEW_BUCKET_VERSION, String.valueOf(currentMetadata.getBucketVersion() + 1));
    extraMetadata.put(METADATA_PREV_BUCKET_NUMBER, String.valueOf(currentMetadata.getBucketNum()));
    ExtensibleBucketResizingOperation op = newBucketNum > currentMetadata.getBucketNum() ? ExtensibleBucketResizingOperation.SPLIT : ExtensibleBucketResizingOperation.MERGE;
    extraMetadata.put(METADATA_BUCKET_RESIZING_OP, op.name());
    return extraMetadata;
  }

  public static HoodieExtensibleBucketMetadata deconstructExtensibleExtraMetadata(Map<String, String> extraMetadata, String instantTime) {
    return new HoodieExtensibleBucketMetadata(
        Short.parseShort(extraMetadata.get(METADATA_NEW_BUCKET_VERSION)),
        extraMetadata.get(METADATA_PARTITION_PATH),
        instantTime,
        Integer.parseInt(extraMetadata.get(METADATA_NEW_BUCKET_NUMBER)),
        Integer.parseInt(extraMetadata.get(METADATA_PREV_BUCKET_NUMBER)),
        Collections.emptyMap());
  }

  public static Map<String/*partition*/, ExtensibleBucketIdentifier/*bucket layout, maybe uncommitted*/> fetchLatestUncommittedExtensibleBucketIdentifier(HoodieTable table, Set<String> partitions) {
    // fetch from timeline
    Map<String, ExtensibleBucketIdentifier> pendingIdentifier = table.getActiveTimeline().reload().filterPendingReplaceOrClusteringTimeline().getInstantsAsStream()
        .map(instant -> ClusteringUtils.getClusteringPlan(table.getMetaClient(), instant))
        .filter(o -> o.isPresent())
        .map(Option::get)
        .filter(planPair -> {
          HoodieClusteringPlan plan = planPair.getRight();
          return plan.getExtraMetadata().getOrDefault(BaseExtensibleBucketClusteringPlanStrategy.CLUSTERING_PLAN_TYPE_KEY, "empty")
              .equals(BaseExtensibleBucketClusteringPlanStrategy.BUCKET_RESIZING_PLAN);
        }).flatMap(planPair -> {
          String instantTime = planPair.getKey().requestedTime();
          return planPair.getValue().getInputGroups()
              .stream()
              .filter(group -> partitions.contains(group.getExtraMetadata().get(METADATA_PARTITION_PATH)))
              .map(group -> {
                HoodieExtensibleBucketMetadata metadata = deconstructExtensibleExtraMetadata(group.getExtraMetadata(), instantTime);
                return Pair.of(metadata.getPartitionPath(), new ExtensibleBucketIdentifier(metadata, true));
              });
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // fetch from extensible-bucket metadata path
    partitions.stream().filter(partition -> !pendingIdentifier.containsKey(partition)).forEach(partition -> {
      HoodieExtensibleBucketMetadata metadata = loadOrCreateMetadata(table, partition);
      pendingIdentifier.put(partition, new ExtensibleBucketIdentifier(metadata));
    });
    return pendingIdentifier;
  }

  public static Map<String/*partition*/, Pair<ExtensibleBucketIdentifier/*latest committed layout*/, Option<ExtensibleBucketIdentifier>/*optional uncommitted layout*/>>
      fetchLatestCommittedExtensibleBucketIdentifierWithUncommitted(HoodieTable table, Set<String> partitions) {
    // fetch from timeline
    Map<String, ExtensibleBucketIdentifier> pendingIdentifier = table.getActiveTimeline().reload().filterPendingReplaceOrClusteringTimeline().getInstantsAsStream()
        .map(instant -> ClusteringUtils.getClusteringPlan(table.getMetaClient(), instant))
        .filter(o -> o.isPresent())
        .map(Option::get)
        .filter(planPair -> {
          HoodieClusteringPlan plan = planPair.getRight();
          return plan.getExtraMetadata().getOrDefault(BaseExtensibleBucketClusteringPlanStrategy.CLUSTERING_PLAN_TYPE_KEY, "empty")
              .equals(BaseExtensibleBucketClusteringPlanStrategy.BUCKET_RESIZING_PLAN);
        }).flatMap(planPair -> {
          String instantTime = planPair.getKey().requestedTime();
          return planPair.getValue().getInputGroups()
              .stream()
              .filter(group -> partitions.contains(group.getExtraMetadata().get(METADATA_PARTITION_PATH)))
              .map(group -> {
                HoodieExtensibleBucketMetadata metadata = deconstructExtensibleExtraMetadata(group.getExtraMetadata(), instantTime);
                return Pair.of(metadata.getPartitionPath(), new ExtensibleBucketIdentifier(metadata, true));
              });
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // fetch from extensible-bucket metadata path
    return partitions.stream().map(partition -> {
      HoodieExtensibleBucketMetadata metadata = loadOrCreateMetadata(table, partition);
      return Pair.of(partition, Pair.of(new ExtensibleBucketIdentifier(metadata), Option.ofNullable(pendingIdentifier.get(partition))));
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Get tagged record for the passed in {@link HoodieRecord}.
   *
   * @param record   instance of {@link HoodieRecord} for which tagging is requested
   * @param location {@link HoodieRecordLocation} for the passed in {@link HoodieRecord}
   * @return the tagged {@link HoodieRecord}
   */
  public static <R> HoodieRecord<R> tagAsDualWriteRecordIfNeeded(HoodieRecord<R> record, Option<HoodieRecordLocation> location) {
    if (location.isPresent()) {
      // When you have a record in multiple files in the same partition, then <row key, record> collection
      // will have 2 entries with the same exact in memory copy of the HoodieRecord and the 2
      // separate filenames that the record is found in. This will result in setting
      // currentLocation 2 times and it will fail the second time. So creating a new in memory
      // copy of the hoodie record.
      HoodieRecord<R> newRecord = record.newInstance();
      newRecord.unseal();
      newRecord.setCurrentLocation(location.get());
      // mark it as a dual write record
      newRecord.setDualWriteRecord(true);
      newRecord.seal();
      return newRecord;
    } else {
      return record;
    }
  }

}
