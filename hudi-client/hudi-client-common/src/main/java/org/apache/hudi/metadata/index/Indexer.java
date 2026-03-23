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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.util.Lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface to building records for initializing and updating a type of metadata or index
 * in the metadata table.
 * <p>
 * When a new type of index is added to MetadataPartitionType, an
 * implementation of the {@link Indexer} interface is required, and it
 * must be added to {@link IndexerFactory}.
 */
public interface Indexer {
  /**
   * Generates records for initializing the index.
   *
   * @param dataTableInstantTime    instant time of the data table that the metadata table is initialized on
   * @param instantTimeForPartition instant time used for initializing a specific metadata partition
   * @param partitionToAllFilesMap  map of partition to files
   * @param lazyPartitionFileSlices lazily evaluated list of file slices for the indexer that needs it
   * @return zero or more {@link IndexPartitionInitialization} entries to be initialized.
   * Returning an empty list means no metadata partition needs initialization in this invocation.
   * @throws IOException upon IO error
   */
  List<IndexPartitionInitialization> buildInitialization(
      String dataTableInstantTime,
      String instantTimeForPartition,
      Map<String, List<FileInfo>> partitionToAllFilesMap,
      Lazy<List<FileSliceAndPartition>> lazyPartitionFileSlices) throws IOException;

  /**
   * Generates records for updating the index based on the commit metadata.
   *
   * @param instantTime        instant time of the commit being applied to the index
   * @param tableMetadata      metadata table accessor used by indexers that need table lookups
   * @param lazyFileSystemView lazily-evaluated file system view used by indexers that require file slices
   * @param commitMetadata     commit metadata containing write stats for the update
   * @return zero or more {@link IndexPartitionAndRecords} entries to be committed to metadata partitions.
   * Returning an empty list means no index updates are required for this commit.
   */
  List<IndexPartitionAndRecords> buildUpdate(
      String instantTime,
      HoodieBackedTableMetadata tableMetadata,
      Lazy<HoodieTableFileSystemView> lazyFileSystemView,
      HoodieCommitMetadata commitMetadata);

  /**
   * Generates records for cleaning index entries based on the clean metadata.
   *
   * @param instantTime   instant time of the clean action being applied to the index
   * @param cleanMetadata clean metadata describing files removed from the data table
   * @return zero or more {@link IndexPartitionAndRecords} entries to be committed to metadata partitions.
   * Returning an empty list means no index cleanup is required for this clean action.
   */
  List<IndexPartitionAndRecords> buildClean(
      String instantTime,
      HoodieCleanMetadata cleanMetadata);

  /**
   * Generates records for restoring index entries based on the restore metadata.
   * <p>
   * Implementations can emit records for files added back, files deleted, and partition deletions.
   * The default implementation is a no-op and returns an empty list.
   *
   * @param instantTime       instant time of the restore action being applied to the index
   * @param deletedPartitions data table partitions deleted during restore
   * @param filesAdded        files that need to be re-added to index state, grouped by partition
   * @param filesDeleted      files that need to be removed from index state, grouped by partition
   * @return zero or more {@link IndexPartitionAndRecords} entries to be committed to metadata partitions.
   * Returning an empty list means no index restore updates are required.
   */
  List<IndexPartitionAndRecords> buildRestore(
      String instantTime,
      List<String> deletedPartitions,
      Map<String, List<FileInfo>> filesAdded,
      Map<String, List<String>> filesDeleted);

  /**
   * Hook invoked after the bootstrap bulk commit for an index partition succeeds.
   * Implementations can use this to perform index-specific follow-up work.
   *
   * @param metadataMetaClient     metadata table meta client used during initialization
   * @param records                records committed during index partition initialization
   * @param fileGroupCount         number of file groups created for the index partition
   * @param relativePartitionPath  metadata table relative partition path being initialized
   */
  void postInitialization(
      HoodieTableMetaClient metadataMetaClient,
      HoodieData<HoodieRecord> records,
      int fileGroupCount,
      String relativePartitionPath);
}
