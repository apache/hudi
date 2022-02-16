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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Create PreCommitFileSystemView by only filtering instants that are of interest. 
 * For example, we want to exclude
 * other inflight instants. This is achieved by combining
 * 1) FileSystemView with completed commits
 * 2) Using list of files written/replaced by inflight instant that we are validating
 *
 */
public class HoodieTablePreCommitFileSystemView {
  
  private Map<String, List<String>> partitionToReplaceFileIds;
  private List<HoodieWriteStat> filesWritten;
  private String preCommitInstantTime;
  private SyncableFileSystemView completedCommitsFileSystemView;
  private HoodieTableMetaClient tableMetaClient;

  /**
   * Create a file system view for the inflight commit that we are validating.
   */
  public HoodieTablePreCommitFileSystemView(HoodieTableMetaClient metaClient,
                                            SyncableFileSystemView completedCommitsFileSystemView,
                                            List<HoodieWriteStat> filesWritten,
                                            Map<String, List<String>> partitionToReplaceFileIds,
                                            String instantTime) {
    this.completedCommitsFileSystemView = completedCommitsFileSystemView;
    this.filesWritten = filesWritten;
    this.partitionToReplaceFileIds = partitionToReplaceFileIds;
    this.preCommitInstantTime = instantTime;
    this.tableMetaClient = metaClient;
  }
  
  /**
   * Combine committed base files + new files created/replaced for given partition.
   */
  public final Stream<HoodieBaseFile> getLatestBaseFiles(String partitionStr) {
    // get fileIds replaced by current inflight commit
    List<String> replacedFileIdsForPartition = partitionToReplaceFileIds.getOrDefault(partitionStr, Collections.emptyList());
    
    // get new files written by current inflight commit
    Map<String, HoodieBaseFile> newFilesWrittenForPartition = filesWritten.stream()
        .filter(file -> partitionStr.equals(file.getPartitionPath()))
        .collect(Collectors.toMap(HoodieWriteStat::getFileId, writeStat -> 
            new HoodieBaseFile(new Path(tableMetaClient.getBasePath(), writeStat.getPath()).toString())));

    Stream<HoodieBaseFile> committedBaseFiles = this.completedCommitsFileSystemView.getLatestBaseFiles(partitionStr);
    Map<String, HoodieBaseFile> allFileIds = committedBaseFiles
            // Remove files replaced by current inflight commit
            .filter(baseFile -> !replacedFileIdsForPartition.contains(baseFile.getFileId()))
            .collect(Collectors.toMap(HoodieBaseFile::getFileId, baseFile -> baseFile));

    allFileIds.putAll(newFilesWrittenForPartition);
    return allFileIds.values().stream();
  }
}
