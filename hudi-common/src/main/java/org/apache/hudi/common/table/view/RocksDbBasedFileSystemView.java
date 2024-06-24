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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapBaseFileMapping;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RocksDBSchemaHelper;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.RocksDBDAO;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A file-system view implementation on top of embedded Rocks DB store. For each table : 3 column Family is added for
 * storing (1) File-Slices and Data Files for View lookups (2) Pending compaction operations (3) Partitions tracked
 *
 * Fine-grained retrieval API to fetch latest file-slice and data-file which are common operations for
 * ingestion/compaction are supported.
 *
 * TODO: vb The current implementation works in embedded server mode where each restarts blows away the view stores. To
 * support view-state preservation across restarts, Hoodie timeline also needs to be stored inorder to detect changes to
 * timeline across restarts.
 */
public class RocksDbBasedFileSystemView extends IncrementalTimelineSyncFileSystemView {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDbBasedFileSystemView.class);

  private final FileSystemViewStorageConfig config;

  private final RocksDBSchemaHelper schemaHelper;

  private RocksDBDAO rocksDB;

  private boolean closed = false;

  public RocksDbBasedFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileSystemViewStorageConfig config) {
    super(config.isIncrementalTimelineSyncEnabled());
    this.config = config;
    this.schemaHelper = new RocksDBSchemaHelper(metaClient);
    this.rocksDB = new RocksDBDAO(metaClient.getBasePath().toString(), config.getRocksdbBasePath());
    init(metaClient, visibleActiveTimeline);
  }

  public RocksDbBasedFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
                                    List<StoragePathInfo> pathInfoList, FileSystemViewStorageConfig config) {
    this(metaClient, visibleActiveTimeline, config);
    addFilesToView(pathInfoList);
  }

  @Override
  protected void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    schemaHelper.getAllColumnFamilies().forEach(rocksDB::addColumnFamily);
    super.init(metaClient, visibleActiveTimeline);
    LOG.info("Created ROCKSDB based file-system view at " + config.getRocksdbBasePath());
  }

  @Override
  protected boolean isPendingCompactionScheduledForFileId(HoodieFileGroupId fgId) {
    return getPendingCompactionOperationWithInstant(fgId).isPresent();
  }

  @Override
  protected void resetPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch -> {
      operations.forEach(opPair ->
          rocksDB.putInBatch(batch, schemaHelper.getColFamilyForPendingCompaction(),
              schemaHelper.getKeyForPendingCompactionLookup(opPair.getValue().getFileGroupId()), opPair)
      );
      LOG.info("Initializing pending compaction operations. Count=" + batch.count());
    });
  }

  @Override
  protected void addPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch ->
        operations.forEach(opInstantPair -> {
          ValidationUtils.checkArgument(!isPendingCompactionScheduledForFileId(opInstantPair.getValue().getFileGroupId()),
              "Duplicate FileGroupId found in pending compaction operations. FgId :"
                  + opInstantPair.getValue().getFileGroupId());
          rocksDB.putInBatch(batch, schemaHelper.getColFamilyForPendingCompaction(),
              schemaHelper.getKeyForPendingCompactionLookup(opInstantPair.getValue().getFileGroupId()), opInstantPair);
        })
    );
  }

  @Override
  void removePendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch ->
        operations.forEach(opInstantPair -> {
          ValidationUtils.checkArgument(
              getPendingCompactionOperationWithInstant(opInstantPair.getValue().getFileGroupId()) != null,
              "Trying to remove a FileGroupId which is not found in pending compaction operations. FgId :"
                  + opInstantPair.getValue().getFileGroupId());
          rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForPendingCompaction(),
              schemaHelper.getKeyForPendingCompactionLookup(opInstantPair.getValue().getFileGroupId()));
        })
    );
  }

  @Override
  protected boolean isPendingLogCompactionScheduledForFileId(HoodieFileGroupId fgId) {
    return getPendingLogCompactionOperationWithInstant(fgId).isPresent();
  }

  @Override
  protected void resetPendingLogCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch -> {
      operations.forEach(opPair ->
          rocksDB.putInBatch(batch, schemaHelper.getColFamilyForPendingLogCompaction(),
              schemaHelper.getKeyForPendingLogCompactionLookup(opPair.getValue().getFileGroupId()), opPair)
      );
      LOG.info("Initializing pending Log compaction operations. Count=" + batch.count());
    });
  }

  @Override
  protected void addPendingLogCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch ->
        operations.forEach(opInstantPair -> {
          ValidationUtils.checkArgument(!isPendingLogCompactionScheduledForFileId(opInstantPair.getValue().getFileGroupId()),
              "Duplicate FileGroupId found in pending log compaction operations. FgId :"
                  + opInstantPair.getValue().getFileGroupId());
          rocksDB.putInBatch(batch, schemaHelper.getColFamilyForPendingLogCompaction(),
              schemaHelper.getKeyForPendingLogCompactionLookup(opInstantPair.getValue().getFileGroupId()), opInstantPair);
        })
    );
  }

  @Override
  void removePendingLogCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch ->
        operations.forEach(opInstantPair -> {
          ValidationUtils.checkArgument(
              getPendingLogCompactionOperationWithInstant(opInstantPair.getValue().getFileGroupId()) != null,
              "Trying to remove a FileGroupId which is not found in pending Log compaction operations. FgId :"
                  + opInstantPair.getValue().getFileGroupId());
          rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForPendingLogCompaction(),
              schemaHelper.getKeyForPendingLogCompactionLookup(opInstantPair.getValue().getFileGroupId()));
        })
    );
  }

  @Override
  protected boolean isPendingClusteringScheduledForFileId(HoodieFileGroupId fgId) {
    return getPendingClusteringInstant(fgId).isPresent();
  }

  @Override
  protected Option<HoodieInstant> getPendingClusteringInstant(HoodieFileGroupId fgId) {
    String lookupKey = schemaHelper.getKeyForFileGroupsInPendingClustering(fgId);
    HoodieInstant pendingClusteringInstant =
        rocksDB.get(schemaHelper.getColFamilyForFileGroupsInPendingClustering(), lookupKey);
    return Option.ofNullable(pendingClusteringInstant);
  }

  @Override
  public Stream<Pair<HoodieFileGroupId, HoodieInstant>> fetchFileGroupsInPendingClustering() {
    return rocksDB.<Pair<HoodieFileGroupId, HoodieInstant>>prefixSearch(schemaHelper.getColFamilyForFileGroupsInPendingClustering(), "")
        .map(Pair::getValue);
  }

  @Override
  void resetFileGroupsInPendingClustering(Map<HoodieFileGroupId, HoodieInstant> fgIdToInstantMap) {
    LOG.info("Resetting file groups in pending clustering to ROCKSDB based file-system view at "
        + config.getRocksdbBasePath() + ", Total file-groups=" + fgIdToInstantMap.size());

    // Delete all replaced file groups
    rocksDB.prefixDelete(schemaHelper.getColFamilyForFileGroupsInPendingClustering(), "part=");
    // Now add new entries
    addFileGroupsInPendingClustering(fgIdToInstantMap.entrySet().stream().map(entry -> Pair.of(entry.getKey(), entry.getValue())));
    LOG.info("Resetting replacedFileGroups to ROCKSDB based file-system view complete");
  }

  @Override
  void addFileGroupsInPendingClustering(Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroups) {
    rocksDB.writeBatch(batch ->
        fileGroups.forEach(fgIdToClusterInstant -> {
          ValidationUtils.checkArgument(!isPendingClusteringScheduledForFileId(fgIdToClusterInstant.getLeft()),
              "Duplicate FileGroupId found in pending clustering operations. FgId :"
                  + fgIdToClusterInstant.getLeft());

          rocksDB.putInBatch(batch, schemaHelper.getColFamilyForFileGroupsInPendingClustering(),
              schemaHelper.getKeyForFileGroupsInPendingClustering(fgIdToClusterInstant.getKey()), fgIdToClusterInstant);
        })
    );
  }

  @Override
  void removeFileGroupsInPendingClustering(Stream<Pair<HoodieFileGroupId, HoodieInstant>> fileGroups) {
    rocksDB.writeBatch(batch ->
        fileGroups.forEach(fgToPendingClusteringInstant -> {
          ValidationUtils.checkArgument(
              !isPendingClusteringScheduledForFileId(fgToPendingClusteringInstant.getLeft()),
              "Trying to remove a FileGroupId which is not found in pending clustering operations. FgId :"
                  + fgToPendingClusteringInstant.getLeft());
          rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForFileGroupsInPendingClustering(),
              schemaHelper.getKeyForFileGroupsInPendingClustering(fgToPendingClusteringInstant.getLeft()));
        })
    );
  }

  @Override
  protected void resetViewState() {
    LOG.info("Deleting all rocksdb data associated with table filesystem view");
    rocksDB.close();
    rocksDB = new RocksDBDAO(metaClient.getBasePath().toString(), config.getRocksdbBasePath());
    schemaHelper.getAllColumnFamilies().forEach(rocksDB::addColumnFamily);
  }

  @Override
  protected Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(HoodieFileGroupId fgId) {
    String lookupKey = schemaHelper.getKeyForPendingCompactionLookup(fgId);
    Pair<String, CompactionOperation> instantOperationPair =
        rocksDB.get(schemaHelper.getColFamilyForPendingCompaction(), lookupKey);
    return Option.ofNullable(instantOperationPair);
  }

  @Override
  protected Option<Pair<String, CompactionOperation>> getPendingLogCompactionOperationWithInstant(HoodieFileGroupId fgId) {
    String lookupKey = schemaHelper.getKeyForPendingLogCompactionLookup(fgId);
    Pair<String, CompactionOperation> instantOperationPair =
        rocksDB.get(schemaHelper.getColFamilyForPendingLogCompaction(), lookupKey);
    return Option.ofNullable(instantOperationPair);
  }

  @Override
  protected boolean isPartitionAvailableInStore(String partitionPath) {
    String lookupKey = schemaHelper.getKeyForPartitionLookup(partitionPath);
    Serializable obj = rocksDB.get(schemaHelper.getColFamilyForStoredPartitions(), lookupKey);
    return obj != null;
  }

  @Override
  protected void storePartitionView(String partitionPath, List<HoodieFileGroup> fileGroups) {
    LOG.info("Resetting and adding new partition (" + partitionPath + ") to ROCKSDB based file-system view at "
        + config.getRocksdbBasePath() + ", Total file-groups=" + fileGroups.size());

    String lookupKey = schemaHelper.getKeyForPartitionLookup(partitionPath);
    rocksDB.delete(schemaHelper.getColFamilyForStoredPartitions(), lookupKey);

    // First delete partition views
    rocksDB.prefixDelete(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForSliceViewByPartition(partitionPath));
    rocksDB.prefixDelete(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForDataFileViewByPartition(partitionPath));

    // Now add them
    fileGroups.forEach(fg ->
        rocksDB.writeBatch(batch ->
            fg.getAllFileSlicesIncludingInflight().forEach(fs -> {
              rocksDB.putInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForSliceView(fg, fs), fs);
              fs.getBaseFile().ifPresent(df ->
                  rocksDB.putInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForDataFileView(fg, fs), df)
              );
            })
        )
    );

    // record that partition is loaded.
    rocksDB.put(schemaHelper.getColFamilyForStoredPartitions(), lookupKey, Boolean.TRUE);
    LOG.info("Finished adding new partition (" + partitionPath + ") to ROCKSDB based file-system view at "
        + config.getRocksdbBasePath() + ", Total file-groups=" + fileGroups.size());
  }

  @Override
  /*
   * This is overridden to incrementally apply file-slices to rocks DB
   */
  protected void applyDeltaFileSlicesToPartitionView(String partition, List<HoodieFileGroup> deltaFileGroups,
      DeltaApplyMode mode) {
    rocksDB.writeBatch(batch ->
        deltaFileGroups.forEach(fg ->
            fg.getAllRawFileSlices().map(fs -> {
              FileSlice oldSlice = getFileSlice(partition, fs.getFileId(), fs.getBaseInstantTime());
              if (null == oldSlice) {
                return fs;
              } else {
                // First remove the file-slice
                LOG.info("Removing old Slice in DB. FS=" + oldSlice);
                rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForSliceView(fg, oldSlice));
                rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForDataFileView(fg, oldSlice));

                Map<String, HoodieLogFile> logFiles = oldSlice.getLogFiles()
                    .map(lf -> Pair.of(FSUtils.getPathWithoutSchemeAndAuthority(lf.getPath()).toString(), lf))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                Map<String, HoodieLogFile> deltaLogFiles =
                    fs.getLogFiles().map(lf -> Pair.of(FSUtils.getPathWithoutSchemeAndAuthority(lf.getPath()).toString(), lf))
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

                switch (mode) {
                  case ADD: {
                    FileSlice newFileSlice = new FileSlice(oldSlice.getFileGroupId(), oldSlice.getBaseInstantTime());
                    oldSlice.getBaseFile().ifPresent(newFileSlice::setBaseFile);
                    fs.getBaseFile().ifPresent(newFileSlice::setBaseFile);
                    Map<String, HoodieLogFile> newLogFiles = new HashMap<>(logFiles);
                    deltaLogFiles.entrySet().stream().filter(e -> !logFiles.containsKey(e.getKey()))
                        .forEach(p -> newLogFiles.put(p.getKey(), p.getValue()));
                    newLogFiles.values().forEach(newFileSlice::addLogFile);
                    LOG.info("Adding back new File Slice after add FS=" + newFileSlice);
                    return newFileSlice;
                  }
                  case REMOVE: {
                    LOG.info("Removing old File Slice =" + fs);
                    FileSlice newFileSlice = new FileSlice(oldSlice.getFileGroupId(), oldSlice.getBaseInstantTime());
                    fs.getBaseFile().orElseGet(() -> {
                      oldSlice.getBaseFile().ifPresent(newFileSlice::setBaseFile);
                      return null;
                    });

                    deltaLogFiles.keySet().forEach(logFiles::remove);
                    // Add remaining log files back
                    logFiles.values().forEach(newFileSlice::addLogFile);
                    if (newFileSlice.getBaseFile().isPresent() || (newFileSlice.getLogFiles().count() > 0)) {
                      LOG.info("Adding back new file-slice after remove FS=" + newFileSlice);
                      return newFileSlice;
                    }
                    return null;
                  }
                  default:
                    throw new IllegalStateException("Unknown diff apply mode=" + mode);
                }
              }
            }).filter(Objects::nonNull).forEach(fs -> {
              rocksDB.putInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForSliceView(fg, fs), fs);
              fs.getBaseFile().ifPresent(df ->
                  rocksDB.putInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForDataFileView(fg, fs), df)
              );
            })
        )
    );
  }

  @Override
  Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations() {
    return rocksDB.<Pair<String, CompactionOperation>>prefixSearch(schemaHelper.getColFamilyForPendingCompaction(), "")
        .map(Pair::getValue);
  }

  @Override
  Stream<Pair<String, CompactionOperation>> fetchPendingLogCompactionOperations() {
    return rocksDB.<Pair<String, CompactionOperation>>prefixSearch(schemaHelper.getColFamilyForPendingLogCompaction(), "")
        .map(Pair::getValue);
  }

  @Override
  Stream<HoodieBaseFile> fetchAllBaseFiles(String partitionPath) {
    return rocksDB.<HoodieBaseFile>prefixSearch(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForDataFileViewByPartition(partitionPath)).map(Pair::getValue);
  }

  @Override
  protected boolean isBootstrapBaseFilePresentForFileId(HoodieFileGroupId fgId) {
    return getBootstrapBaseFile(fgId).isPresent();
  }

  @Override
  void resetBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream) {
    rocksDB.writeBatch(batch -> {
      bootstrapBaseFileStream.forEach(externalBaseFile -> {
        rocksDB.putInBatch(batch, schemaHelper.getColFamilyForBootstrapBaseFile(),
            schemaHelper.getKeyForBootstrapBaseFile(externalBaseFile.getFileGroupId()), externalBaseFile);
      });
      LOG.info("Initializing external data file mapping. Count=" + batch.count());
    });
  }

  @Override
  void addBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream) {
    rocksDB.writeBatch(batch -> {
      bootstrapBaseFileStream.forEach(externalBaseFile -> {
        ValidationUtils.checkArgument(!isBootstrapBaseFilePresentForFileId(externalBaseFile.getFileGroupId()),
            "Duplicate FileGroupId found in external data file. FgId :" + externalBaseFile.getFileGroupId());
        rocksDB.putInBatch(batch, schemaHelper.getColFamilyForBootstrapBaseFile(),
            schemaHelper.getKeyForBootstrapBaseFile(externalBaseFile.getFileGroupId()), externalBaseFile);
      });
    });
  }

  @Override
  void removeBootstrapBaseFileMapping(Stream<BootstrapBaseFileMapping> bootstrapBaseFileStream) {
    rocksDB.writeBatch(batch -> {
      bootstrapBaseFileStream.forEach(externalBaseFile -> {
        ValidationUtils.checkArgument(
            getBootstrapBaseFile(externalBaseFile.getFileGroupId()) != null,
            "Trying to remove a FileGroupId which is not found in external data file mapping. FgId :"
                + externalBaseFile.getFileGroupId());
        rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForBootstrapBaseFile(),
            schemaHelper.getKeyForBootstrapBaseFile(externalBaseFile.getFileGroupId()));
      });
    });
  }

  @Override
  protected Option<BootstrapBaseFileMapping> getBootstrapBaseFile(HoodieFileGroupId fileGroupId) {
    String lookupKey = schemaHelper.getKeyForBootstrapBaseFile(fileGroupId);
    BootstrapBaseFileMapping externalBaseFile =
        rocksDB.get(schemaHelper.getColFamilyForBootstrapBaseFile(), lookupKey);
    return Option.ofNullable(externalBaseFile);
  }

  @Override
  Stream<BootstrapBaseFileMapping> fetchBootstrapBaseFiles() {
    return rocksDB.<BootstrapBaseFileMapping>prefixSearch(schemaHelper.getColFamilyForBootstrapBaseFile(), "")
        .map(Pair::getValue);
  }

  @Override
  Stream<HoodieFileGroup> fetchAllStoredFileGroups(String partitionPath) {
    return getFileGroups(rocksDB.<FileSlice>prefixSearch(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForSliceViewByPartition(partitionPath)).map(Pair::getValue));
  }

  @Override
  Stream<HoodieFileGroup> fetchAllStoredFileGroups() {
    return getFileGroups(
        rocksDB.<FileSlice>prefixSearch(schemaHelper.getColFamilyForView(), schemaHelper.getPrefixForSliceView())
            .map(Pair::getValue));
  }

  @Override
  public Option<FileSlice> fetchLatestFileSlice(String partitionPath, String fileId) {
    // Retries only file-slices of the file and filters for the latest
    return Option.ofNullable(rocksDB
        .<FileSlice>prefixSearch(schemaHelper.getColFamilyForView(),
            schemaHelper.getPrefixForSliceViewByPartitionFile(partitionPath, fileId))
        .map(Pair::getValue).reduce(null,
            (x, y) -> ((x == null) ? y
                : (y == null) ? null
                    : HoodieTimeline.compareTimestamps(x.getBaseInstantTime(), HoodieTimeline.GREATER_THAN, y.getBaseInstantTime()
            ) ? x : y)));
  }

  @Override
  protected Option<HoodieBaseFile> fetchLatestBaseFile(String partitionPath, String fileId) {
    // Retries only file-slices of the file and filters for the latest
    return Option
        .ofNullable(rocksDB
            .<HoodieBaseFile>prefixSearch(schemaHelper.getColFamilyForView(),
                schemaHelper.getPrefixForDataFileViewByPartitionFile(partitionPath, fileId))
            .map(Pair::getValue).reduce(null,
                (x, y) -> ((x == null) ? y
                    : (y == null) ? null
                        : HoodieTimeline.compareTimestamps(x.getCommitTime(), HoodieTimeline.GREATER_THAN, y.getCommitTime())
                            ? x
                            : y)));
  }

  @Override
  Option<HoodieFileGroup> fetchHoodieFileGroup(String partitionPath, String fileId) {
    return Option.fromJavaOptional(getFileGroups(rocksDB.<FileSlice>prefixSearch(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForSliceViewByPartitionFile(partitionPath, fileId)).map(Pair::getValue)).findFirst());
  }

  @Override
  protected void resetReplacedFileGroups(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups) {
    LOG.info("Resetting replacedFileGroups to ROCKSDB based file-system view at "
        + config.getRocksdbBasePath() + ", Total file-groups=" + replacedFileGroups.size());

    // Delete all replaced file groups
    rocksDB.prefixDelete(schemaHelper.getColFamilyForReplacedFileGroups(), "part=");
    // Now add new entries
    addReplacedFileGroups(replacedFileGroups);
    LOG.info("Resetting replacedFileGroups to ROCKSDB based file-system view complete");
  }

  @Override
  protected void addReplacedFileGroups(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups) {
    Map<String, List<Map.Entry<HoodieFileGroupId, HoodieInstant>>> partitionToReplacedFileGroups =
        replacedFileGroups.entrySet().stream().collect(Collectors.groupingBy(e -> e.getKey().getPartitionPath()));
    partitionToReplacedFileGroups.entrySet().stream().forEach(partitionToReplacedFileGroupsEntry -> {
      String partitionPath = partitionToReplacedFileGroupsEntry.getKey();
      List<Map.Entry<HoodieFileGroupId, HoodieInstant>> replacedFileGroupsInPartition = partitionToReplacedFileGroupsEntry.getValue();

      // Now add them
      rocksDB.writeBatch(batch ->
          replacedFileGroupsInPartition.stream().forEach(fgToReplacedInstant -> {
            rocksDB.putInBatch(batch, schemaHelper.getColFamilyForReplacedFileGroups(),
                schemaHelper.getKeyForReplacedFileGroup(fgToReplacedInstant.getKey()), fgToReplacedInstant.getValue());
          })
      );

      LOG.info("Finished adding replaced file groups to  partition (" + partitionPath + ") to ROCKSDB based view at "
          + config.getRocksdbBasePath() + ", Total file-groups=" + partitionToReplacedFileGroupsEntry.getValue().size());
    });
  }

  @Override
  protected void removeReplacedFileIdsAtInstants(Set<String> instants) {
    //TODO can we make this more efficient by storing reverse mapping (Instant -> FileGroupId) as well?
    Stream<String> keysToDelete = rocksDB.<HoodieInstant>prefixSearch(schemaHelper.getColFamilyForReplacedFileGroups(), "")
        .filter(entry -> instants.contains(entry.getValue().getTimestamp()))
        .map(Pair::getKey);

    rocksDB.writeBatch(batch ->
        keysToDelete.forEach(key -> rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForReplacedFileGroups(), key))
    );
  }

  @Override
  protected boolean hasReplacedFilesInPartition(String partitionPath) {
    return rocksDB.<HoodieInstant>prefixSearch(schemaHelper.getColFamilyForReplacedFileGroups(), schemaHelper.getPrefixForReplacedFileGroup(partitionPath))
        .findAny().isPresent();
  }

  @Override
  protected Option<HoodieInstant> getReplaceInstant(final HoodieFileGroupId fileGroupId) {
    String lookupKey = schemaHelper.getKeyForReplacedFileGroup(fileGroupId);
    HoodieInstant replacedInstant =
        rocksDB.get(schemaHelper.getColFamilyForReplacedFileGroups(), lookupKey);
    return Option.ofNullable(replacedInstant);
  }

  private Stream<HoodieFileGroup> getFileGroups(Stream<FileSlice> sliceStream) {
    return sliceStream.map(s -> Pair.of(Pair.of(s.getPartitionPath(), s.getFileId()), s))
        .collect(Collectors.groupingBy(Pair::getKey)).entrySet().stream().map(slicePair -> {
          HoodieFileGroup fg = new HoodieFileGroup(slicePair.getKey().getKey(), slicePair.getKey().getValue(),
              getVisibleCommitsAndCompactionTimeline());
          slicePair.getValue().forEach(e -> fg.addFileSlice(e.getValue()));
          return fg;
        });
  }

  private FileSlice getFileSlice(String partitionPath, String fileId, String instantTime) {
    String key = schemaHelper.getKeyForSliceView(partitionPath, fileId, instantTime);
    return rocksDB.get(schemaHelper.getColFamilyForView(), key);
  }

  @Override
  public void close() {
    LOG.info("Closing Rocksdb !!");
    closed = true;
    rocksDB.close();
    LOG.info("Closed Rocksdb !!");
  }

  @Override
  boolean isClosed() {
    return closed;
  }
}
