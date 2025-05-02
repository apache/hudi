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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

/**
 * Interface for initializing and updating a type of metadata or index
 * in the metadata table
 * <p>
 * When a new type of index is added to MetadataPartitionType, an
 * implementation of the {@link Indexer} interface is required, and it
 * must be added to {@link IndexerFactory}.
 */
public interface Indexer {
  /**
   * Generates records for initializing the index.
   *
   * @param partitionInfoList          list of directory information
   * @param partitionToFilesMap        map of partition to files
   * @param lazyPartitionFileSliceList lazily-evaluated list of file slices for the indexer
   *                                   that needs it
   * @param createInstantTime          instant time of the data table that the metadata table
   *                                   is initialized on
   * @param instantTimeForPartition    instant time for initializing the metadata table partition
   * @return a list of {@link InitialIndexPartitionData}, which each data item
   * representing the records to initialize a particular partition (note that
   * one index type can correspond to one or multiple partitions in the metadata
   * table). An empty list returned indicates that the metadata partition does
   * not need to be initialized.
   * @throws IOException upon IO error
   */
  List<InitialIndexPartitionData> initialize(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      Lazy<List<Pair<String, FileSlice>>> lazyPartitionFileSliceList,
      String createInstantTime,
      String instantTimeForPartition) throws IOException;

  /**
   * Updates the table config of the data table to reflect the state of the index
   */
  default void updateTableConfig() {
    // No index-specific table config update by default
  }

  static List<Pair<String, FileSlice>> getPartitionFileSlicePairs(
      HoodieTableMetaClient dataTableMetaClient,
      HoodieTableMetadata metadata,
      HoodieTableFileSystemView fsView)
      throws IOException {
    String latestInstant = dataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
        .map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
    // TODO(yihua): originally this uses try-catch on fsView so it's not reused.
    // now we need to rely on outside caller to pass fsView and close the fsView.
    // also see if we can reuse fsView across indexes.
    // Collect the list of latest file slices present in each partition
    List<String> partitions = metadata.getAllPartitionPaths();
    fsView.loadAllPartitions();
    List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
    partitions.forEach(partition -> fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latestInstant)
        .forEach(fs -> partitionFileSlicePairs.add(Pair.of(partition, fs))));
    return partitionFileSlicePairs;
  }

  static List<Tuple3<String, String, Boolean>> fetchPartitionFileInfoTriplets(
      Map<String, Map<String, Long>> partitionToAppendedFiles) {
    // Total number of files which are added or deleted
    final int totalFiles = partitionToAppendedFiles.values().stream().mapToInt(Map::size).sum();
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList = new ArrayList<>(totalFiles);
    partitionToAppendedFiles.entrySet().stream()
        .flatMap(
            entry -> entry.getValue().keySet().stream().map(addedFile -> Tuple3.of(entry.getKey(), addedFile, false)))
        .collect(Collectors.toCollection(() -> partitionFileFlagTupleList));
    return partitionFileFlagTupleList;
  }

  class IndexPartitionData {
    private final String partitionName;
    private final HoodieData<HoodieRecord> records;

    private IndexPartitionData(String partitionName, HoodieData<HoodieRecord> records) {
      this.partitionName = partitionName;
      this.records = records;
    }

    public static IndexPartitionData of(String partitionName, HoodieData<HoodieRecord> records) {
      return new IndexPartitionData(partitionName, records);
    }

    public String partitionName() {
      return partitionName;
    }

    public HoodieData<HoodieRecord> records() {
      return records;
    }
  }

  class InitialIndexPartitionData {
    private final int numFileGroup;
    private final IndexPartitionData partitionedRecords;

    private InitialIndexPartitionData(int numFileGroup,
                                      String partitionName,
                                      HoodieData<HoodieRecord> records) {
      this.numFileGroup = numFileGroup;
      this.partitionedRecords = IndexPartitionData.of(partitionName, records);
    }

    public static InitialIndexPartitionData of(int numFileGroup,
                                               String partitionName,
                                               HoodieData<HoodieRecord> records) {
      ValidationUtils.checkArgument(numFileGroup > 0,
          "The number of file groups of the index data should be positive");
      return new InitialIndexPartitionData(numFileGroup, partitionName, records);
    }

    public int numFileGroup() {
      return numFileGroup;
    }

    public String partitionName() {
      return partitionedRecords.partitionName();
    }

    public HoodieData<HoodieRecord> records() {
      return partitionedRecords.records();
    }
  }
}
