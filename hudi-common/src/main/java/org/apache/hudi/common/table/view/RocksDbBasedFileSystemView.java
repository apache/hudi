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

import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RocksDBDAO;
import org.apache.hudi.common.util.RocksDBSchemaHelper;
import org.apache.hudi.common.util.collection.Pair;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A file-system view implementation on top of embedded Rocks DB store. For each DataSet : 3 column Family is added for
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
    this.rocksDB = new RocksDBDAO(metaClient.getBasePath(), config.getRocksdbBasePath());
    init(metaClient, visibleActiveTimeline);
  }

  public RocksDbBasedFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileStatus[] fileStatuses, FileSystemViewStorageConfig config) {
    this(metaClient, visibleActiveTimeline, config);
    addFilesToView(fileStatuses);
  }

  @Override
  protected void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    schemaHelper.getAllColumnFamilies().stream().forEach(rocksDB::addColumnFamily);
    super.init(metaClient, visibleActiveTimeline);
    LOG.info("Created ROCKSDB based file-system view at {}", config.getRocksdbBasePath());
  }

  @Override
  protected boolean isPendingCompactionScheduledForFileId(HoodieFileGroupId fgId) {
    return getPendingCompactionOperationWithInstant(fgId).isPresent();
  }

  @Override
  protected void resetPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch -> {
      operations.forEach(opPair -> {
        rocksDB.putInBatch(batch, schemaHelper.getColFamilyForPendingCompaction(),
            schemaHelper.getKeyForPendingCompactionLookup(opPair.getValue().getFileGroupId()), opPair);
      });
      LOG.info("Initializing pending compaction operations. Count={}", batch.count());
    });
  }

  @Override
  protected void addPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch -> {
      operations.forEach(opInstantPair -> {
        Preconditions.checkArgument(!isPendingCompactionScheduledForFileId(opInstantPair.getValue().getFileGroupId()),
            "Duplicate FileGroupId found in pending compaction operations. FgId :"
                + opInstantPair.getValue().getFileGroupId());
        rocksDB.putInBatch(batch, schemaHelper.getColFamilyForPendingCompaction(),
            schemaHelper.getKeyForPendingCompactionLookup(opInstantPair.getValue().getFileGroupId()), opInstantPair);
      });
    });
  }

  @Override
  void removePendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations) {
    rocksDB.writeBatch(batch -> {
      operations.forEach(opInstantPair -> {
        Preconditions.checkArgument(
            getPendingCompactionOperationWithInstant(opInstantPair.getValue().getFileGroupId()) != null,
            "Trying to remove a FileGroupId which is not found in pending compaction operations. FgId :"
                + opInstantPair.getValue().getFileGroupId());
        rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForPendingCompaction(),
            schemaHelper.getKeyForPendingCompactionLookup(opInstantPair.getValue().getFileGroupId()));
      });
    });
  }

  @Override
  protected void resetViewState() {
    LOG.info("Deleting all rocksdb data associated with dataset filesystem view");
    rocksDB.close();
    rocksDB = new RocksDBDAO(metaClient.getBasePath(), config.getRocksdbBasePath());
  }

  @Override
  protected Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(HoodieFileGroupId fgId) {
    String lookupKey = schemaHelper.getKeyForPendingCompactionLookup(fgId);
    Pair<String, CompactionOperation> instantOperationPair =
        rocksDB.get(schemaHelper.getColFamilyForPendingCompaction(), lookupKey);
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
    LOG.info("Resetting and adding new partition ({}) to ROCKSDB based file-system view at {}, Total file-groups={}", partitionPath, config.getRocksdbBasePath(), fileGroups.size());

    String lookupKey = schemaHelper.getKeyForPartitionLookup(partitionPath);
    rocksDB.delete(schemaHelper.getColFamilyForStoredPartitions(), lookupKey);

    // First delete partition views
    rocksDB.prefixDelete(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForSliceViewByPartition(partitionPath));
    rocksDB.prefixDelete(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForDataFileViewByPartition(partitionPath));

    // Now add them
    fileGroups.stream().forEach(fg -> {
      rocksDB.writeBatch(batch -> {
        fg.getAllFileSlicesIncludingInflight().forEach(fs -> {
          rocksDB.putInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForSliceView(fg, fs), fs);
          fs.getDataFile().ifPresent(df -> {
            rocksDB.putInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForDataFileView(fg, fs),
                df);
          });
        });
      });
    });

    // record that partition is loaded.
    rocksDB.put(schemaHelper.getColFamilyForStoredPartitions(), lookupKey, Boolean.TRUE);
    LOG.info("Finished adding new partition ({}) to ROCKSDB based file-system view at {}, Total file-groups={}", partitionPath, config.getRocksdbBasePath(), fileGroups.size());
  }

  @Override
  /**
   * This is overridden to incrementally apply file-slices to rocks DB
   */
  protected void applyDeltaFileSlicesToPartitionView(String partition, List<HoodieFileGroup> deltaFileGroups,
      DeltaApplyMode mode) {
    rocksDB.writeBatch(batch -> {
      deltaFileGroups.stream().forEach(fg -> {
        fg.getAllRawFileSlices().map(fs -> {
          FileSlice oldSlice = getFileSlice(partition, fs.getFileId(), fs.getBaseInstantTime());
          if (null == oldSlice) {
            return fs;
          } else {
            // First remove the file-slice
            LOG.info("Removing old Slice in DB. FS={}", oldSlice);
            rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForView(),
                schemaHelper.getKeyForSliceView(fg, oldSlice));
            rocksDB.deleteInBatch(batch, schemaHelper.getColFamilyForView(),
                schemaHelper.getKeyForDataFileView(fg, oldSlice));

            Map<String, HoodieLogFile> logFiles = oldSlice.getLogFiles()
                .map(lf -> Pair.of(Path.getPathWithoutSchemeAndAuthority(lf.getPath()).toString(), lf))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            Map<String, HoodieLogFile> deltaLogFiles =
                fs.getLogFiles().map(lf -> Pair.of(Path.getPathWithoutSchemeAndAuthority(lf.getPath()).toString(), lf))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            switch (mode) {
              case ADD: {
                FileSlice newFileSlice = new FileSlice(oldSlice.getFileGroupId(), oldSlice.getBaseInstantTime());
                oldSlice.getDataFile().ifPresent(df -> newFileSlice.setDataFile(df));
                fs.getDataFile().ifPresent(df -> newFileSlice.setDataFile(df));
                Map<String, HoodieLogFile> newLogFiles = new HashMap<>(logFiles);
                deltaLogFiles.entrySet().stream().filter(e -> !logFiles.containsKey(e.getKey()))
                    .forEach(p -> newLogFiles.put(p.getKey(), p.getValue()));
                newLogFiles.values().stream().forEach(lf -> newFileSlice.addLogFile(lf));
                LOG.info("Adding back new File Slice after add FS={}", newFileSlice);
                return newFileSlice;
              }
              case REMOVE: {
                LOG.info("Removing old File Slice ={}", fs);
                FileSlice newFileSlice = new FileSlice(oldSlice.getFileGroupId(), oldSlice.getBaseInstantTime());
                fs.getDataFile().orElseGet(() -> {
                  oldSlice.getDataFile().ifPresent(df -> newFileSlice.setDataFile(df));
                  return null;
                });

                deltaLogFiles.keySet().stream().forEach(p -> logFiles.remove(p));
                // Add remaining log files back
                logFiles.values().stream().forEach(lf -> newFileSlice.addLogFile(lf));
                if (newFileSlice.getDataFile().isPresent() || (newFileSlice.getLogFiles().count() > 0)) {
                  LOG.info("Adding back new file-slice after remove FS={}", newFileSlice);
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
          fs.getDataFile().ifPresent(df -> {
            rocksDB.putInBatch(batch, schemaHelper.getColFamilyForView(), schemaHelper.getKeyForDataFileView(fg, fs),
                df);
          });
        });
      });
    });
  }

  @Override
  Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations() {
    return rocksDB.<Pair<String, CompactionOperation>>prefixSearch(schemaHelper.getColFamilyForPendingCompaction(), "")
        .map(Pair::getValue);
  }

  @Override
  Stream<HoodieDataFile> fetchAllDataFiles(String partitionPath) {
    return rocksDB.<HoodieDataFile>prefixSearch(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForDataFileViewByPartition(partitionPath)).map(Pair::getValue);
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
  protected Option<FileSlice> fetchLatestFileSlice(String partitionPath, String fileId) {
    // Retries only file-slices of the file and filters for the latest
    return Option.ofNullable(rocksDB
        .<FileSlice>prefixSearch(schemaHelper.getColFamilyForView(),
            schemaHelper.getPrefixForSliceViewByPartitionFile(partitionPath, fileId))
        .map(Pair::getValue).reduce(null,
            (x, y) -> ((x == null) ? y
                : (y == null) ? null
                    : HoodieTimeline.compareTimestamps(x.getBaseInstantTime(), y.getBaseInstantTime(),
                        HoodieTimeline.GREATER) ? x : y)));
  }

  @Override
  protected Option<HoodieDataFile> fetchLatestDataFile(String partitionPath, String fileId) {
    // Retries only file-slices of the file and filters for the latest
    return Option
        .ofNullable(rocksDB
            .<HoodieDataFile>prefixSearch(schemaHelper.getColFamilyForView(),
                schemaHelper.getPrefixForDataFileViewByPartitionFile(partitionPath, fileId))
            .map(Pair::getValue).reduce(null,
                (x, y) -> ((x == null) ? y
                    : (y == null) ? null
                        : HoodieTimeline.compareTimestamps(x.getCommitTime(), y.getCommitTime(), HoodieTimeline.GREATER)
                            ? x
                            : y)));
  }

  @Override
  Option<HoodieFileGroup> fetchHoodieFileGroup(String partitionPath, String fileId) {
    return Option.fromJavaOptional(getFileGroups(rocksDB.<FileSlice>prefixSearch(schemaHelper.getColFamilyForView(),
        schemaHelper.getPrefixForSliceViewByPartitionFile(partitionPath, fileId)).map(Pair::getValue)).findFirst());
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
    return rocksDB.<FileSlice>get(schemaHelper.getColFamilyForView(), key);
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
