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

import org.apache.hadoop.fs.FileStatus;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.BootstrapBaseFileMapping;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Table FileSystemView implementation where view is stored in spillable disk using fixed memory.
 */
public class SpillableMapBasedFileSystemView extends HoodieTableFileSystemView {

  private static final Logger LOG = LogManager.getLogger(SpillableMapBasedFileSystemView.class);

  private final long maxMemoryForFileGroupMap;
  private final long maxMemoryForPendingCompaction;
  private final long maxMemoryForBootstrapBaseFile;
  private final long maxMemoryForReplaceFileGroups;
  private final long maxMemoryForClusteringFileGroups;
  private final String baseStoreDir;
  private final ExternalSpillableMap.DiskMapType diskMapType;
  private final boolean isBitCaskDiskMapCompressionEnabled;

  public SpillableMapBasedFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileSystemViewStorageConfig config, HoodieCommonConfig commonConfig) {
    super(config.isIncrementalTimelineSyncEnabled());
    this.maxMemoryForFileGroupMap = config.getMaxMemoryForFileGroupMap();
    this.maxMemoryForPendingCompaction = config.getMaxMemoryForPendingCompaction();
    this.maxMemoryForBootstrapBaseFile = config.getMaxMemoryForBootstrapBaseFile();
    this.maxMemoryForReplaceFileGroups = config.getMaxMemoryForReplacedFileGroups();
    this.maxMemoryForClusteringFileGroups = config.getMaxMemoryForPendingClusteringFileGroups();
    this.baseStoreDir = config.getSpillableDir();
    diskMapType = commonConfig.getSpillableDiskMapType();
    isBitCaskDiskMapCompressionEnabled = commonConfig.isBitCaskDiskMapCompressionEnabled();
    init(metaClient, visibleActiveTimeline);
  }

  public SpillableMapBasedFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileStatus[] fileStatuses, FileSystemViewStorageConfig config, HoodieCommonConfig commonConfig) {
    this(metaClient, visibleActiveTimeline, config, commonConfig);
    addFilesToView(fileStatuses);
  }

  @Override
  protected Map<String, List<HoodieFileGroup>> createPartitionToFileGroups() {
    try {
      LOG.info("Creating Partition To File groups map using external spillable Map. Max Mem=" + maxMemoryForFileGroupMap
          + ", BaseDir=" + baseStoreDir);
      new File(baseStoreDir).mkdirs();
      return (Map<String, List<HoodieFileGroup>>) (new ExternalSpillableMap<>(maxMemoryForFileGroupMap, baseStoreDir,
          new DefaultSizeEstimator(), new DefaultSizeEstimator<>(),
          diskMapType, isBitCaskDiskMapCompressionEnabled));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Map<HoodieFileGroupId, Pair<String, CompactionOperation>> createFileIdToPendingCompactionMap(
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> fgIdToPendingCompaction) {
    try {
      LOG.info("Creating Pending Compaction map using external spillable Map. Max Mem=" + maxMemoryForPendingCompaction
          + ", BaseDir=" + baseStoreDir);
      new File(baseStoreDir).mkdirs();
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> pendingMap = new ExternalSpillableMap<>(
          maxMemoryForPendingCompaction, baseStoreDir, new DefaultSizeEstimator(), new DefaultSizeEstimator<>(),
          diskMapType, isBitCaskDiskMapCompressionEnabled);
      pendingMap.putAll(fgIdToPendingCompaction);
      return pendingMap;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Map<HoodieFileGroupId, BootstrapBaseFileMapping> createFileIdToBootstrapBaseFileMap(
      Map<HoodieFileGroupId, BootstrapBaseFileMapping> fileGroupIdBootstrapBaseFileMap) {
    try {
      LOG.info("Creating bootstrap base File Map using external spillable Map. Max Mem=" + maxMemoryForBootstrapBaseFile
          + ", BaseDir=" + baseStoreDir);
      new File(baseStoreDir).mkdirs();
      Map<HoodieFileGroupId, BootstrapBaseFileMapping> pendingMap = new ExternalSpillableMap<>(
          maxMemoryForBootstrapBaseFile, baseStoreDir, new DefaultSizeEstimator(), new DefaultSizeEstimator<>(),
          diskMapType, isBitCaskDiskMapCompressionEnabled);
      pendingMap.putAll(fileGroupIdBootstrapBaseFileMap);
      return pendingMap;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Map<HoodieFileGroupId, HoodieInstant> createFileIdToReplaceInstantMap(final Map<HoodieFileGroupId, HoodieInstant> replacedFileGroups) {
    try {
      LOG.info("Creating file group id to replace instant map using external spillable Map. Max Mem=" + maxMemoryForReplaceFileGroups
          + ", BaseDir=" + baseStoreDir);
      new File(baseStoreDir).mkdirs();
      Map<HoodieFileGroupId, HoodieInstant> pendingMap = new ExternalSpillableMap<>(
          maxMemoryForReplaceFileGroups, baseStoreDir, new DefaultSizeEstimator(), new DefaultSizeEstimator<>(),
          diskMapType, isBitCaskDiskMapCompressionEnabled);
      pendingMap.putAll(replacedFileGroups);
      return pendingMap;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Map<HoodieFileGroupId, HoodieInstant> createFileIdToPendingClusteringMap(final Map<HoodieFileGroupId, HoodieInstant> fileGroupsInClustering) {
    try {
      LOG.info("Creating file group id to clustering instant map using external spillable Map. Max Mem=" + maxMemoryForClusteringFileGroups
          + ", BaseDir=" + baseStoreDir);
      new File(baseStoreDir).mkdirs();
      Map<HoodieFileGroupId, HoodieInstant> pendingMap = new ExternalSpillableMap<>(
          maxMemoryForClusteringFileGroups, baseStoreDir, new DefaultSizeEstimator(), new DefaultSizeEstimator<>(),
          diskMapType, isBitCaskDiskMapCompressionEnabled);
      pendingMap.putAll(fileGroupsInClustering);
      return pendingMap;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<HoodieFileGroup> getAllFileGroups() {
    return ((ExternalSpillableMap) partitionToFileGroupsMap).valueStream()
        .flatMap(fg -> ((List<HoodieFileGroup>) fg).stream());
  }

  @Override
  Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations() {
    return ((ExternalSpillableMap) fgIdToPendingCompaction).valueStream();
  }

  @Override
  Stream<BootstrapBaseFileMapping> fetchBootstrapBaseFiles() {
    return ((ExternalSpillableMap) fgIdToBootstrapBaseFile).valueStream();
  }

  @Override
  public Stream<HoodieFileGroup> fetchAllStoredFileGroups() {
    return ((ExternalSpillableMap) partitionToFileGroupsMap).valueStream().flatMap(fg -> ((List<HoodieFileGroup>) fg).stream());
  }

  @Override
  protected void removeReplacedFileIdsAtInstants(Set<String> instants) {
    //TODO should we make this more efficient by having reverse mapping of instant to file group id?
    Stream<HoodieFileGroupId> fileIdsToRemove = fgIdToReplaceInstants.entrySet().stream().map(entry -> {
      if (instants.contains(entry.getValue().getTimestamp())) {
        return Option.of(entry.getKey());
      } else {
        return Option.ofNullable((HoodieFileGroupId) null);
      }
    }).filter(Option::isPresent).map(Option::get);

    fileIdsToRemove.forEach(fileGroupId -> fgIdToReplaceInstants.remove(fileGroupId));
  }
}
