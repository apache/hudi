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
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Table FileSystemView implementation where view is stored in spillable disk using fixed memory.
 */
public class SpillableMapBasedFileSystemView extends HoodieTableFileSystemView {

  private static final Logger LOG = LoggerFactory.getLogger(SpillableMapBasedFileSystemView.class);

  private final long maxMemoryForFileGroupMap;
  private final long maxMemoryForPendingCompaction;
  private final String baseStoreDir;

  public SpillableMapBasedFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileSystemViewStorageConfig config) {
    super(config.isIncrementalTimelineSyncEnabled());
    this.maxMemoryForFileGroupMap = config.getMaxMemoryForFileGroupMap();
    this.maxMemoryForPendingCompaction = config.getMaxMemoryForPendingCompaction();
    this.baseStoreDir = config.getBaseStoreDir();
    init(metaClient, visibleActiveTimeline);
  }

  public SpillableMapBasedFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
      FileStatus[] fileStatuses, FileSystemViewStorageConfig config) {
    this(metaClient, visibleActiveTimeline, config);
    addFilesToView(fileStatuses);
  }

  @Override
  protected Map<String, List<HoodieFileGroup>> createPartitionToFileGroups() {
    try {
      LOG.info("Creating Partition To File groups map using external spillable Map.  Max Mem={}, BaseDir={}", maxMemoryForPendingCompaction, baseStoreDir);
      new File(baseStoreDir).mkdirs();
      return (Map<String, List<HoodieFileGroup>>) (new ExternalSpillableMap<>(maxMemoryForFileGroupMap, baseStoreDir,
          new DefaultSizeEstimator(), new DefaultSizeEstimator<>()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Map<HoodieFileGroupId, Pair<String, CompactionOperation>> createFileIdToPendingCompactionMap(
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> fgIdToPendingCompaction) {
    try {
      LOG.info("Creating Pending Compaction map using external spillable Map. Max Mem={}, BaseDir={}", maxMemoryForPendingCompaction, baseStoreDir);
      new File(baseStoreDir).mkdirs();
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> pendingMap = new ExternalSpillableMap<>(
          maxMemoryForPendingCompaction, baseStoreDir, new DefaultSizeEstimator(), new DefaultSizeEstimator<>());
      pendingMap.putAll(fgIdToPendingCompaction);
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
  public Stream<HoodieFileGroup> fetchAllStoredFileGroups() {
    return ((ExternalSpillableMap) partitionToFileGroupsMap).valueStream().flatMap(fg -> {
      return ((List<HoodieFileGroup>) fg).stream();
    });
  }
}
