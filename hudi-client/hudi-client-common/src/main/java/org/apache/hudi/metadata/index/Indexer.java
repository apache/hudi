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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.metadata.model.FileInfo;
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
   * @param dataTableInstantTime                   instant time of the data table that the metadata table is initialized on
   * @param instantTimeForPartition                instant time used for initializing a specific metadata partition
   * @param partitionToAllFilesMap                 map of partition to files
   * @param lazyLatestMergedPartitionFileSliceList lazily-evaluated list of file slices for the indexer that needs it
   * @return zero or more {@link IndexPartitionInitialization} entries to be initialized.
   * Returning an empty list means no metadata partition needs initialization in this invocation.
   * @throws IOException upon IO error
   */
  List<IndexPartitionInitialization> buildInitialization(
      String dataTableInstantTime,
      String instantTimeForPartition,
      Map<String, List<FileInfo>> partitionToAllFilesMap,
      Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException;

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
