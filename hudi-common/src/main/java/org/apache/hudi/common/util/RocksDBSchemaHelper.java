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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.Arrays;
import java.util.List;

/**
 * Helper class to generate Key and column names for rocksdb based view
 *
 * For RocksDB, 3 colFamilies are used for storing file-system view for each table. (a) View (b) Partitions Cached (c)
 * Pending Compactions
 *
 *
 * View : Key : Store both slice and Data file stored. Slice : Key =
 * "type=slice,part=<PartitionPath>,id=<FileId>,instant=<Timestamp>" Value = Serialized FileSlice Data File : Key =
 * "type=df,part=<PartitionPath>,id=<FileId>,instant=<Timestamp>" Value = Serialized DataFile
 *
 * Partitions : Key = "part=<PartitionPath>" Value = Boolean
 *
 * Pending Compactions Key = "part=<PartitionPath>,id=<FileId>" Value = Pair<CompactionTime, CompactionOperation>
 */
public class RocksDBSchemaHelper {

  private final String colFamilyForView;
  private final String colFamilyForPendingCompaction;
  private final String colFamilyForPendingLogCompaction;
  private final String colFamilyForBootstrapBaseFile;
  private final String colFamilyForStoredPartitions;
  private final String colFamilyForReplacedFileGroups;
  private final String colFamilyForPendingClusteringFileGroups;

  public RocksDBSchemaHelper(HoodieTableMetaClient metaClient) {
    this.colFamilyForBootstrapBaseFile = "hudi_bootstrap_basefile_" + metaClient.getBasePath().replace("/", "_");
    this.colFamilyForPendingCompaction = "hudi_pending_compaction_" + metaClient.getBasePath().replace("/", "_");
    this.colFamilyForPendingLogCompaction = "hudi_pending_log_compaction_" + metaClient.getBasePath().replace("/", "_");
    this.colFamilyForStoredPartitions = "hudi_partitions_" + metaClient.getBasePath().replace("/", "_");
    this.colFamilyForView = "hudi_view_" + metaClient.getBasePath().replace("/", "_");
    this.colFamilyForReplacedFileGroups = "hudi_replaced_fg" + metaClient.getBasePath().replace("/", "_");
    this.colFamilyForPendingClusteringFileGroups = "hudi_pending_clustering_fg" + metaClient.getBasePath().replace("/", "_");
  }

  public List<String> getAllColumnFamilies() {
    return Arrays.asList(getColFamilyForView(), getColFamilyForPendingCompaction(), getColFamilyForPendingLogCompaction(),
        getColFamilyForBootstrapBaseFile(), getColFamilyForStoredPartitions(), getColFamilyForReplacedFileGroups(),
        getColFamilyForFileGroupsInPendingClustering());
  }

  public String getKeyForPartitionLookup(String partition) {
    return String.format("part=%s", partition);
  }

  public String getKeyForPendingCompactionLookup(HoodieFileGroupId fgId) {
    return getPartitionFileIdBasedLookup(fgId);
  }

  public String getKeyForPendingLogCompactionLookup(HoodieFileGroupId fgId) {
    return getPartitionFileIdBasedLookup(fgId);
  }

  public String getKeyForBootstrapBaseFile(HoodieFileGroupId fgId) {
    return getPartitionFileIdBasedLookup(fgId);
  }

  public String getKeyForReplacedFileGroup(HoodieFileGroupId fgId) {
    return getPartitionFileIdBasedLookup(fgId);
  }

  public String getKeyForFileGroupsInPendingClustering(HoodieFileGroupId fgId) {
    return getPartitionFileIdBasedLookup(fgId);
  }

  public String getKeyForSliceView(HoodieFileGroup fileGroup, FileSlice slice) {
    return getKeyForSliceView(fileGroup.getPartitionPath(), fileGroup.getFileGroupId().getFileId(),
        slice.getBaseInstantTime());
  }

  public String getKeyForSliceView(String partitionPath, String fileId, String instantTime) {
    return String.format("type=slice,part=%s,id=%s,instant=%s", partitionPath, fileId, instantTime);
  }

  public String getPrefixForSliceViewByPartitionFile(String partitionPath, String fileId) {
    return String.format("type=slice,part=%s,id=%s,instant=", partitionPath, fileId);
  }

  public String getPrefixForDataFileViewByPartitionFile(String partitionPath, String fileId) {
    return String.format("type=df,part=%s,id=%s,instant=", partitionPath, fileId);
  }

  public String getKeyForDataFileView(HoodieFileGroup fileGroup, FileSlice slice) {
    return String.format("type=df,part=%s,id=%s,instant=%s", fileGroup.getPartitionPath(),
        fileGroup.getFileGroupId().getFileId(), slice.getBaseInstantTime());
  }

  public String getPrefixForSliceViewByPartition(String partitionPath) {
    return String.format("type=slice,part=%s,id=", partitionPath);
  }

  public String getPrefixForSliceView() {
    return "type=slice,part=";
  }

  public String getPrefixForDataFileViewByPartition(String partitionPath) {
    return String.format("type=df,part=%s,id=", partitionPath);
  }

  private String getPartitionFileIdBasedLookup(HoodieFileGroupId fgId) {
    return String.format("part=%s,id=%s", fgId.getPartitionPath(), fgId.getFileId());
  }

  public String getColFamilyForView() {
    return colFamilyForView;
  }

  public String getColFamilyForPendingCompaction() {
    return colFamilyForPendingCompaction;
  }

  public String getColFamilyForPendingLogCompaction() {
    return colFamilyForPendingLogCompaction;
  }

  public String getColFamilyForBootstrapBaseFile() {
    return colFamilyForBootstrapBaseFile;
  }

  public String getColFamilyForStoredPartitions() {
    return colFamilyForStoredPartitions;
  }

  public String getColFamilyForReplacedFileGroups() {
    return colFamilyForReplacedFileGroups;
  }

  public String getColFamilyForFileGroupsInPendingClustering() {
    return colFamilyForPendingClusteringFileGroups;
  }
}
