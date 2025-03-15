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

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieMergedReadHandle;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.IndexBuilder;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.readRecordKeysFromBaseFiles;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;

public class RecordIndexBuilder implements IndexBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(RecordIndexBuilder.class);
  // Average size of a record saved within the record index.
  // Record index has a fixed size schema. This has been calculated based on experiments with default settings
  // for block size (1MB), compression (GZ) and disabling the hudi metadata fields.
  private static final int RECORD_INDEX_AVERAGE_RECORD_SIZE = 48;
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final HoodieTable table;

  public RecordIndexBuilder(HoodieEngineContext engineContext,
                            HoodieWriteConfig dataTableWriteConfig,
                            HoodieTableMetaClient dataTableMetaClient,
                            HoodieTable table) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.table = table;
  }

  @Override
  public Pair<Integer, HoodieData<HoodieRecord>> createRecordsFromExistingFiles(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      HoodieTableFileSystemView fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    // TODO(yihua): could we unify the file listing across indexes?
    // Collect the list of latest base files present in each partition
    List<String> partitions = metadata.getAllPartitionPaths();
    fsView.loadAllPartitions();
    HoodieData<HoodieRecord> records = null;
    if (dataTableMetaClient.getTableConfig().getTableType() == HoodieTableType.COPY_ON_WRITE) {
      // for COW, we can only consider base files to initialize.
      final List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs = new ArrayList<>();
      for (String partition : partitions) {
        partitionBaseFilePairs.addAll(fsView.getLatestBaseFiles(partition)
            .map(basefile -> Pair.of(partition, basefile)).collect(Collectors.toList()));
      }

      LOG.info("Initializing record index from " + partitionBaseFilePairs.size() + " base files in "
          + partitions.size() + " partitions");

      // Collect record keys from the files in parallel
      records = readRecordKeysFromBaseFiles(
          engineContext,
          dataTableWriteConfig,
          partitionBaseFilePairs,
          false,
          dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism(),
          dataTableMetaClient.getBasePath(),
          engineContext.getStorageConf(),
          this.getClass().getSimpleName());
    } else {
      final List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
      String latestCommit = dataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
          .map(instant -> instant.requestedTime()).orElse(SOLO_COMMIT_TIMESTAMP);
      for (String partition : partitions) {
        fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latestCommit)
            .forEach(fs -> partitionFileSlicePairs.add(Pair.of(partition, fs)));
      }

      LOG.info("Initializing record index from " + partitionFileSlicePairs.size() + " file slices in "
          + partitions.size() + " partitions");
      records = readRecordKeysFromFileSliceSnapshot(
          engineContext,
          partitionFileSlicePairs,
          dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism(),
          this.getClass().getSimpleName(),
          dataTableMetaClient,
          dataTableWriteConfig,
          // TODO(yihua): is table instance needed here?
          table);
    }
    records.persist("MEMORY_AND_DISK_SER");
    final long recordCount = records.count();

    // Initialize the file groups
    final int fileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(RECORD_INDEX, recordCount,
        RECORD_INDEX_AVERAGE_RECORD_SIZE, dataTableWriteConfig.getRecordIndexMinFileGroupCount(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupCount(), dataTableWriteConfig.getRecordIndexGrowthFactor(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupSizeBytes());

    LOG.info("Initializing record index with {} mappings and {} file groups.", recordCount, fileGroupCount);
    return Pair.of(fileGroupCount, records);
  }

  /**
   * Fetch record locations from FileSlice snapshot.
   *
   * @param engineContext             context ot use.
   * @param partitionFileSlicePairs   list of pairs of partition and file slice.
   * @param recordIndexMaxParallelism parallelism to use.
   * @param activeModule              active module of interest.
   * @param metaClient                metaclient instance to use.
   * @param dataWriteConfig           write config to use.
   * @param hoodieTable               hoodie table instance of interest.
   * @return
   */
  private static HoodieData<HoodieRecord> readRecordKeysFromFileSliceSnapshot(HoodieEngineContext engineContext,
                                                                              List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                                              int recordIndexMaxParallelism,
                                                                              String activeModule,
                                                                              HoodieTableMetaClient metaClient,
                                                                              HoodieWriteConfig dataWriteConfig,
                                                                              HoodieTable hoodieTable) {
    if (partitionFileSlicePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    Option<String> instantTime = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime);

    engineContext.setJobStatus(activeModule,
        "Record Index: reading record keys from " + partitionFileSlicePairs.size() + " file slices");
    final int parallelism = Math.min(partitionFileSlicePairs.size(), recordIndexMaxParallelism);

    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndFileSlice -> {

      final String partition = partitionAndFileSlice.getKey();
      final FileSlice fileSlice = partitionAndFileSlice.getValue();
      final String fileId = fileSlice.getFileId();
      return new HoodieMergedReadHandle(dataWriteConfig, instantTime, hoodieTable,
          Pair.of(partition, fileSlice.getFileId()),
          Option.of(fileSlice)).getMergedRecords().stream()
          .map(record -> {
            HoodieRecord record1 = (HoodieRecord) record;
            return HoodieMetadataPayload.createRecordIndexUpdate(record1.getRecordKey(), partition, fileId,
                record1.getCurrentLocation().getInstantTime(), 0);
          }).iterator();
    });
  }
}
