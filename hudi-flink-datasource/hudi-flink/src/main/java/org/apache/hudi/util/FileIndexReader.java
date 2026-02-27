/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Abstract base class for creating input splits from a Hudi file index.
 *
 * <p>This class provides common functionality for reading file slices and creating
 * input splits for both base files and merge-on-read scenarios. Subclasses must
 * implement {@link #buildFileIndex()} to provide the file index.
 */
public abstract class FileIndexReader implements Serializable {
  protected transient FileIndex fileIndex;

  /**
   * Retrieves file slices containing only base files (no log files).
   *
   * <p>This method is used for COPY_ON_WRITE tables and READ_OPTIMIZED queries
   * where only the latest base files are needed.
   *
   * @param metaClient the Hudi table meta client
   * @return list of file slices with base files only, or empty list if no partitions found
   */
  public List<FileSlice> getBaseFileOnlyFileSlices(HoodieTableMetaClient metaClient) {
    try (FileIndex fileIndex = getOrBuildFileIndex()) {
      List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
      if (relPartitionPaths.isEmpty()) {
        return Collections.emptyList();
      }
      List<StoragePathInfo> pathInfoList = fileIndex.getFilesInPartitions();
      try (HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
          metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants(), pathInfoList)) {

        List<FileSlice> allFileSlices = relPartitionPaths.stream()
            .flatMap(par -> fsView.getLatestBaseFiles(par)
                .map(baseFile -> new FileSlice(new HoodieFileGroupId(par, baseFile.getFileId()), baseFile.getCommitTime(), baseFile, Collections.emptyList())))
            .collect(Collectors.toList());
        return fileIndex.filterFileSlices(allFileSlices);
      }
    } finally {
      fileIndex = null;
    }
  }

  /**
   * Creates Hoodie source splits from base files only (no log files).
   *
   * <p>This is used for COPY_ON_WRITE tables and READ_OPTIMIZED query mode.
   *
   * @param metaClient the Hudi table meta client
   * @param path the storage path of the table
   * @param mergeType the merge type configuration
   * @return list of Hoodie source splits, or empty list if no file slices found
   */
  public List<HoodieSourceSplit> baseFileOnlyHoodieSourceSplits(HoodieTableMetaClient metaClient, StoragePath path, String mergeType) {
    final List<FileSlice> fileSlices = getBaseFileOnlyFileSlices(metaClient);
    if (fileSlices.isEmpty()) {
      return Collections.emptyList();
    }

    return fileSlices.stream()
        .filter(fileSlice -> fileSlice.getBaseFile().isPresent())
        .map(fileSlice ->
            new HoodieSourceSplit(
                HoodieSourceSplit.SPLIT_ID_GEN.incrementAndGet(),
                fileSlice.getBaseFile().get().getPath(),
                Option.empty(),
                FilePathUtils.toFlinkPath(path).getPath(),
                fileSlice.getPartitionPath(),
                mergeType,
                fileSlice.getLatestInstantTime(),
                fileSlice.getFileId(),
                Option.empty()))
        .collect(Collectors.toList());
  }

  /**
   * Builds Hoodie source splits for the table.
   *
   * <p>This method creates HoodieSourceSplit objects for MERGE_ON_READ tables by combining
   * base files with their corresponding log files from the file system view.
   *
   * @param metaClient the Hudi table meta client
   * @param conf the Flink configuration
   * @return list of Hoodie source splits
   */
  public List<HoodieSourceSplit> buildHoodieSplits(HoodieTableMetaClient metaClient, Configuration conf) {

    Pair<List<FileSlice>, String> result = readFileSlice(metaClient, conf);
    final String mergeType = conf.get(FlinkOptions.MERGE_TYPE);
    final AtomicInteger cnt = new AtomicInteger(0);
    // generates one Hoodie source split for each file group
    return result.getLeft().stream()
        .map(fileSlice -> {
          String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
          Option<List<String>> logPaths = Option.ofNullable(
              fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .collect(Collectors.toList()));
          return new HoodieSourceSplit(
              cnt.getAndAdd(1),
              basePath,
              logPaths,
              metaClient.getBasePath().toString(),
              fileSlice.getPartitionPath(),
              mergeType,
              result.getRight(),
              fileSlice.getFileId(),
              Option.empty());
        })
        .collect(Collectors.toList());
  }

  /**
   * Builds merge-on-read input splits for the table.
   *
   * <p>This method creates input splits for MERGE_ON_READ tables by combining
   * base files with their corresponding log files from the file system view.
   *
   * @param metaClient the Hudi table meta client
   * @param conf the Flink configuration
   * @return list of merge-on-read input splits
   * @throws HoodieException if no files are found for reading
   */
  public List<MergeOnReadInputSplit> buildInputSplits(HoodieTableMetaClient metaClient, Configuration conf) {
    Pair<List<FileSlice>, String> result = readFileSlice(metaClient, conf);
    final String mergeType = conf.get(FlinkOptions.MERGE_TYPE);
    final AtomicInteger cnt = new AtomicInteger(0);
    // generates one input split for each file group
    return result.getLeft().stream()
        .map(fileSlice -> {
          String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
          Option<List<String>> logPaths = Option.ofNullable(
              fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .collect(Collectors.toList()));
          return new MergeOnReadInputSplit(
              cnt.getAndAdd(1),
              basePath,
              logPaths,
              result.getRight(),
              metaClient.getBasePath().toString(),
              StreamerUtil.getMaxCompactionMemoryInBytes(conf),
              mergeType,
              null,
              fileSlice.getFileId());
        })
        .collect(Collectors.toList());
  }

  private Pair<List<FileSlice>, String> readFileSlice(HoodieTableMetaClient metaClient, Configuration conf) {
    try (FileIndex fileIndex = getOrBuildFileIndex()) {
      List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
      if (relPartitionPaths.isEmpty()) {
        return ImmutablePair.of(Collections.emptyList(), "");
      }
      List<StoragePathInfo> pathInfoList = fileIndex.getFilesInPartitions();
      if (pathInfoList.isEmpty()) {
        throw new HoodieException("No files found for reading in user provided path.");
      }

      String latestCommit;
      List<FileSlice> allFileSlices;
      // file-slice after pending compaction-requested instant-time is also considered valid
      try (HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
          metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants(), pathInfoList)) {
        if (!fsView.getLastInstant().isPresent()) {
          return ImmutablePair.of(Collections.emptyList(), "");
        }
        latestCommit = fsView.getLastInstant().get().requestedTime();
        allFileSlices = relPartitionPaths.stream()
            .flatMap(par -> fsView.getLatestMergedFileSlicesBeforeOrOn(par, latestCommit))
            .collect(Collectors.toList());
      }
      return ImmutablePair.of(fileIndex.filterFileSlices(allFileSlices), latestCommit);
    } finally {
      fileIndex = null;
    }
  }

  /**
   * Gets or builds the file index, caching it for reuse.
   *
   * @return the file index
   */
  public FileIndex getOrBuildFileIndex() {
    if (fileIndex == null) {
      fileIndex = buildFileIndex();
    }
    return fileIndex;
  }

  /**
   * Builds the file index internally. Must be implemented by subclasses.
   *
   * @return the file index
   */
  protected abstract FileIndex buildFileIndex();
}
