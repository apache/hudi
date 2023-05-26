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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
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
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASHING_METADATA_COMMIT_FILE_SUFFIX;
import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASHING_METADATA_FILE_SUFFIX;
import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.getTimestampFromFile;

/**
 * Consistent hashing bucket index implementation, with auto-adjust bucket number.
 * NOTE: bucket resizing is triggered by clustering.
 */
public class HoodieSparkConsistentBucketIndex extends HoodieBucketIndex {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkConsistentBucketIndex.class);

  public HoodieSparkConsistentBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
                                                HoodieEngineContext context,
                                                HoodieTable hoodieTable)
      throws HoodieIndexException {
    throw new HoodieIndexException("Consistent hashing index does not support update location without the instant parameter");
  }

  /**
   * Persist hashing metadata to storage. Only clustering operations will modify the metadata.
   * For example, splitting & merging buckets, or just sorting and producing a new bucket.
   */
  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
                                                HoodieEngineContext context,
                                                HoodieTable hoodieTable,
                                                String instantTime)
      throws HoodieIndexException {
    HoodieInstant instant = hoodieTable.getMetaClient().getActiveTimeline().findInstantsAfterOrEquals(instantTime, 1).firstInstant().get();
    ValidationUtils.checkState(instant.getTimestamp().equals(instantTime), "Cannot get the same instant, instantTime: " + instantTime);
    if (!instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      return writeStatuses;
    }

    // Double-check if it is a clustering operation by trying to obtain the clustering plan
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlanPair =
        ClusteringUtils.getClusteringPlan(hoodieTable.getMetaClient(), HoodieTimeline.getReplaceCommitRequestedInstant(instantTime));
    if (!instantPlanPair.isPresent()) {
      return writeStatuses;
    }

    HoodieClusteringPlan plan = instantPlanPair.get().getRight();
    HoodieJavaRDD.getJavaRDD(context.parallelize(plan.getInputGroups().stream().map(HoodieClusteringGroup::getExtraMetadata).collect(Collectors.toList())))
        .mapToPair(m -> new Tuple2<>(m.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_PARTITION_KEY), m)
    ).groupByKey().foreach((input) -> {
      // Process each partition
      String partition = input._1();
      List<ConsistentHashingNode> childNodes = new ArrayList<>();
      int seqNo = 0;
      for (Map<String, String> m: input._2()) {
        String nodesJson = m.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY);
        childNodes.addAll(ConsistentHashingNode.fromJsonString(nodesJson));
        seqNo = Integer.parseInt(m.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_SEQUENCE_NUMBER_KEY));
      }

      Option<HoodieConsistentHashingMetadata> metadataOption = loadMetadata(hoodieTable, partition);
      ValidationUtils.checkState(metadataOption.isPresent(), "Failed to load metadata for partition: " + partition);
      HoodieConsistentHashingMetadata meta = metadataOption.get();
      ValidationUtils.checkState(meta.getSeqNo() == seqNo,
          "Non serialized update to hashing metadata, old seq: " + meta.getSeqNo() + ", new seq: " + seqNo);

      // Get new metadata and save
      meta.setChildrenNodes(childNodes);
      List<ConsistentHashingNode> newNodes = (new ConsistentBucketIdentifier(meta)).getNodes().stream()
          .map(n -> new ConsistentHashingNode(n.getValue(), n.getFileIdPrefix(), ConsistentHashingNode.NodeTag.NORMAL))
          .collect(Collectors.toList());
      HoodieConsistentHashingMetadata newMeta = new HoodieConsistentHashingMetadata(meta.getVersion(), meta.getPartitionPath(),
          instantTime, meta.getNumBuckets(), seqNo + 1, newNodes);
      // Overwrite to tolerate re-run of clustering operation
      saveMetadata(hoodieTable, newMeta, true);
    });

    return writeStatuses;
  }

  /**
   * Do nothing.
   * A failed write may create a hashing metadata for a partition. In this case, we still do nothing when rolling back
   * the failed write. Because the hashing metadata created by a writer must have 00000000000000 timestamp and can be viewed
   * as the initialization of a partition rather than as a part of the failed write.
   */
  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  protected BucketIndexLocationMapper getLocationMapper(HoodieTable table, List<String> partitionPath) {
    return new ConsistentBucketIndexLocationMapper(table, partitionPath);
  }

  /**
   * Load hashing metadata of the given partition, if it is not existed, create a new one (also persist it into storage)
   *
   * @param table     hoodie table
   * @param partition table partition
   * @return Consistent hashing metadata
   */
  public HoodieConsistentHashingMetadata loadOrCreateMetadata(HoodieTable table, String partition) {
    Option<HoodieConsistentHashingMetadata> metadataOption = loadMetadata(table, partition);
    if (metadataOption.isPresent()) {
      return metadataOption.get();
    }

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
   * Load hashing metadata of the given partition, if it is not existed, return null
   *
   * @param table     hoodie table
   * @param partition table partition
   * @return Consistent hashing metadata or null if it does not exist
   */
  public static Option<HoodieConsistentHashingMetadata> loadMetadata(HoodieTable table, String partition) {
    Path metadataPath = FSUtils.getPartitionPath(table.getMetaClient().getHashingMetadataPath(), partition);
    Path partitionPath = FSUtils.getPartitionPath(table.getMetaClient().getBasePathV2(), partition);
    try {
      Predicate<FileStatus> hashingMetaCommitFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        return filename.contains(HoodieConsistentHashingMetadata.HASHING_METADATA_COMMIT_FILE_SUFFIX);
      };
      Predicate<FileStatus> hashingMetadataFilePredicate = fileStatus -> {
        String filename = fileStatus.getPath().getName();
        return filename.contains(HASHING_METADATA_FILE_SUFFIX);
      };
      final FileStatus[] metaFiles = table.getMetaClient().getFs().listStatus(metadataPath);
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
      HoodieTimeline completedCommits = table.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants();

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
   * Save metadata into storage
   *
   * @param table hoodie table
   * @param metadata hashing metadata to be saved
   * @param overwrite whether to overwrite existing metadata
   * @return true if the metadata is saved successfully
   */
  private static boolean saveMetadata(HoodieTable table, HoodieConsistentHashingMetadata metadata, boolean overwrite) {
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

  public class ConsistentBucketIndexLocationMapper implements BucketIndexLocationMapper {

    /**
     * Mapping from partitionPath -> bucket identifier
     */
    private final Map<String, ConsistentBucketIdentifier> partitionToIdentifier;

    public ConsistentBucketIndexLocationMapper(HoodieTable table, List<String> partitions) {
      // TODO maybe parallel
      partitionToIdentifier = partitions.stream().collect(Collectors.toMap(p -> p, p -> {
        HoodieConsistentHashingMetadata metadata = loadOrCreateMetadata(table, p);
        return new ConsistentBucketIdentifier(metadata);
      }));
    }

    @Override
    public Option<HoodieRecordLocation> getRecordLocation(HoodieKey key) {
      String partitionPath = key.getPartitionPath();
      ConsistentHashingNode node = partitionToIdentifier.get(partitionPath).getBucket(key, indexKeyFields);
      if (!StringUtils.isNullOrEmpty(node.getFileIdPrefix())) {
        /**
         * Dynamic Bucket Index doesn't need the instant time of the latest file group.
         * We add suffix 0 here to the file uuid, following the naming convention, i.e., fileId = [uuid]_[numWrites]
         */
        return Option.of(new HoodieRecordLocation(null, FSUtils.createNewFileId(node.getFileIdPrefix(), 0)));
      }

      LOG.error("Consistent hashing node has no file group, partition: " + partitionPath + ", meta: "
              + partitionToIdentifier.get(partitionPath).getMetadata().getFilename() + ", record_key: " + key.toString());
      throw new HoodieIndexException("Failed to getBucket as hashing node has no file group");
    }
  }

  /***
   * Creates commit marker corresponding to hashing metadata file after post commit clustering operation.
   * @param table hoodie table
   * @param fileStatus file for which commit marker should be created
   * @param partitionPath partition path the file belongs to
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
   * @param table hoodie table
   * @param metaFile hashing metadata file
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
   * If particular hashing metadta file doesn't have commit marker then there could be a case where clustering is done but post commit marker
   * creation operation failed. In this case this method will check file group id from consistent hashing metadata against storage base file group ids.
   * if one of the file group matches then we can conclude that this is the latest metadata file.
   * Note : we will end up calling this method if there is no marker file and no replace commit on active timeline, if replace commit is not present on
   * active timeline that means old file group id's before clustering operation got cleaned and only new file group id's  of current clustering operation
   * are present on the disk.
   * @param table hoodie table
   * @param metaFile metadata file on which sync check needs to be performed
   * @param partition partition metadata file belongs to
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
