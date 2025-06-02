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

import org.apache.hudi.common.model.FileSliceAndPartition;
import org.apache.hudi.metadata.model.IndexPartitionInitialization;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.common.model.FileAndPartitionFlag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
   * @param dataTableInstantTime                   instant time of the data table that the metadata table
   *                                               is initialized on
   * @param partitionIdToAllFilesMap               map of partition to files
   * @param lazyLatestMergedPartitionFileSliceList lazily-evaluated list of file slices for the indexer
   *                                               that needs it
   * @return a list of {@link IndexPartitionInitialization}, which each data item
   * representing the records to initialize a particular partition (note that
   * one index type can correspond to one or multiple partitions in the metadata
   * table). An empty list returned indicates that the metadata partition does
   * not need to be initialized.
   * @throws IOException upon IO error
   */
  List<IndexPartitionInitialization> initialize(
      String dataTableInstantTime,
      Map<String, Map<String, Long>> partitionIdToAllFilesMap,
      Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException;

  /**
   * Updates the table config of the data table to reflect the state of the index
   */
  default void updateTableConfig() {
    // No index-specific table config update by default
  }

  static List<FileAndPartitionFlag> fetchPartitionFileInfoTriplets(
      Map<String, Map<String, Long>> partitionToAppendedFiles) {
    // Total number of files which are added or deleted
    final int totalFiles = partitionToAppendedFiles.values().stream().mapToInt(Map::size).sum();
    final List<FileAndPartitionFlag> partitionFileFlagList = new ArrayList<>(totalFiles);
    partitionToAppendedFiles.entrySet().stream()
        .flatMap(
            entry -> entry.getValue().keySet().stream().map(addedFile -> 
                FileAndPartitionFlag.of(entry.getKey(), addedFile, false)))
        .collect(Collectors.toCollection(() -> partitionFileFlagList));
    return partitionFileFlagList;
  }
}
