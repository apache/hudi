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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SpillableMapBasedFileSystemView;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkHoodieBackedMetadataSyncMetadataWriter extends SparkHoodieBackedTableMetadataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieBackedMetadataSyncMetadataWriter.class);
  private final String sourceBasePath;
  private String inflightInstantTimestamp;

  public SparkHoodieBackedMetadataSyncMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig,
                                              HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy, HoodieEngineContext engineContext,
                                              String inflightInstantTimestamp, String sourceBasePath) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, Option.of(inflightInstantTimestamp));
    this.sourceBasePath = sourceBasePath;
    this.inflightInstantTimestamp = inflightInstantTimestamp;
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 String inflightInstantTimestamp) {
    return new SparkHoodieBackedMetadataSyncMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp, writeConfig.getMetadataConfig().getBasePathOverride());
  }

  protected boolean initializeIfNeeded(HoodieTableMetaClient dataMetaClient,
                                       Option<String> inflightInstantTimestamp) throws IOException {
    // Do not initialize the metadata table during metadata sync
    metadataMetaClient = initializeMetaClient();
    this.metadata = new HoodieBackedTableMetadata(engineContext, dataWriteConfig.getMetadataConfig(), dataWriteConfig.getBasePath(), true);
    return true;
  }

  /**
   * Bootstraps the metadata table’s FILES partition up to the specified instant.
   * <p>
   * If no instant is provided, the method exits early. Otherwise, it loads the
   * metadata clients if needed, scans the source table to collect partition/file
   * info, and prepares the initial FILES partition records.
   * <p>
   * If the FILES partition is not yet created, the method initializes its file
   * groups and performs a bulk commit. If it already exists, it simply commits
   * the new records. Table services are executed before committing.
   *
   * @param boostrapUntilInstantOpt the instant up to which bootstrapping should be performed.
   * @throws IOException if any step in initialization or commit fails.
   */
  public void bootstrap(Option<String> boostrapUntilInstantOpt) throws IOException {
    if (!boostrapUntilInstantOpt.isPresent()) {
      return;
    }

    String lastInstantTimestamp = boostrapUntilInstantOpt.get();
    boolean filesPartitionAvailable = dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.FILES);

    // Check and then open the metadata table reader so FILES partition can be read during initialization of other partitions
    // initMetadataReader();
    // Load the metadata table metaclient if required
    if (dataMetaClient == null) {
      dataMetaClient = HoodieTableMetaClient.builder().setConf(engineContext.getHadoopConf().get()).setBasePath(metadataWriteConfig.getBasePath()).build();
    }

    // initialize metadata writer
    List<DirectoryInfo> partitionInfoList = listAllPartitionsFromFilesystem(lastInstantTimestamp, sourceBasePath);

    Pair<Integer, HoodieData<HoodieRecord>> fileGroupCountAndRecordsPair = initializeFilesPartition(lastInstantTimestamp, partitionInfoList);

    try {
      if (!filesPartitionAvailable) {
        initializeFileGroups(metadataMetaClient, MetadataPartitionType.FILES, inflightInstantTimestamp, fileGroupCountAndRecordsPair.getKey());
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to bootstrap table " + sourceBasePath, e);
    }

    HoodieData<HoodieRecord> records = fileGroupCountAndRecordsPair.getValue();
    // perform tables services on metadata table
    performTableServices(Option.of(inflightInstantTimestamp));
    if (!filesPartitionAvailable) {
      bulkCommit(inflightInstantTimestamp, MetadataPartitionType.FILES, records, fileGroupCountAndRecordsPair.getKey());
      dataMetaClient.reloadActiveTimeline();
      dataMetaClient.getTableConfig().setMetadataPartitionState(dataMetaClient, MetadataPartitionType.FILES, true);
    } else {
      commit(inflightInstantTimestamp, Collections.singletonMap(MetadataPartitionType.FILES, fileGroupCountAndRecordsPair.getValue()));
    }
  }

  /**
   * Initializes the FILES metadata partition by generating:
   *  1. A record containing the full list of partitions, and
   *  2. A record for each partition that captures the latest base files
   *     (before or on the given instant) along with their file sizes.
   *
   * This method uses a single file group for the FILES partition. It first
   * parallelizes a record containing all partition identifiers. If there are
   * no partitions, it returns this record directly.
   *
   * When partitions exist, it loads the source table’s active timeline and
   * constructs a spillable file system view to fetch the latest base files
   * for each partition. Each partition is mapped into a file-list record,
   * including commit-time–appended filenames and optional base path overrides.
   *
   * @param lastInstantTimestamp the instant up to which base files should be considered.
   * @param partitionInfoList    list of directory info objects describing each partition.
   * @return a pair containing:
   *         (1) the file group count (always 1), and
   *         (2) HoodieData with the partition list record plus per-partition file records.
   */
  private Pair<Integer, HoodieData<HoodieRecord>> initializeFilesPartition(String lastInstantTimestamp, List<DirectoryInfo> partitionInfoList) {
    // FILES partition uses a single file group
    final int fileGroupCount = 1;

    List<String> partitions = partitionInfoList.stream().map(p -> HoodieTableMetadataUtil.getPartitionIdentifier(p.getRelativePath()))
        .collect(Collectors.toList());
    final int totalDataFilesCount = partitionInfoList.stream().mapToInt(DirectoryInfo::getTotalFiles).sum();
    LOG.info("Committing total {} partitions and {} files to metadata", partitions.size(), totalDataFilesCount);

    // Record which saves the list of all partitions
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(partitions);
    HoodieData<HoodieRecord> allPartitionsRecord = engineContext.parallelize(Collections.singletonList(record), 1);
    if (partitionInfoList.isEmpty()) {
      return Pair.of(fileGroupCount, allPartitionsRecord);
    }

    HoodieTableMetaClient sourceTableMetaClient = HoodieTableMetaClient.builder().setBasePath(sourceBasePath).setConf(hadoopConf.get()).build();

    FileSystemViewStorageConfig.Builder spillableConfBuilder = FileSystemViewStorageConfig.newBuilder();
    spillableConfBuilder.withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).fromProperties(dataWriteConfig.getProps());

    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieTimeline timeline = sourceTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    SpillableMapBasedFileSystemView fileSystemView = new SpillableMapBasedFileSystemView(sourceTableMetaClient, timeline, spillableConfBuilder.build(), commonConfig);

    engineContext.setJobStatus(this.getClass().getSimpleName(), "Creating records for metadata FILES partition");
    boolean enableBasePathOverride = dataWriteConfig.shouldEnableBasePathOverride();

    HoodieData<HoodieRecord> fileListRecords = engineContext.parallelize(partitionInfoList, partitionInfoList.size()).map(partition -> {
      // ToDo : fileSystemView object being shared across executors, might be too large
      Stream<HoodieBaseFile> latestBaseFiles = fileSystemView.getLatestBaseFilesBeforeOrOn(partition.getRelativePath(), lastInstantTimestamp);
      Map<String, Long> fileNameToSizeMap = latestBaseFiles.collect(Collectors.toMap(e ->
          ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(e.getFileName(), inflightInstantTimestamp, partition.getRelativePath()), HoodieBaseFile::getFileLen));

      fileSystemView.close();
      return HoodieMetadataPayload.createPartitionFilesRecord(
          HoodieTableMetadataUtil.getPartitionIdentifier(partition.getRelativePath()), fileNameToSizeMap, Collections.emptyList(),
          enableBasePathOverride, Option.of(dataWriteConfig.getMetadataConfig().getBasePathOverride()));
    });
    ValidationUtils.checkState(fileListRecords.count() == partitions.size());
    return Pair.of(fileGroupCount, allPartitionsRecord.union(fileListRecords));
  }
}
