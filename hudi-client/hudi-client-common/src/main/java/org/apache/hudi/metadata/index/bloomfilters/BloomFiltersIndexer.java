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

package org.apache.hudi.metadata.index.bloomfilters;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.fetchPartitionFileInfoTriplets;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;

public class BloomFiltersIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(BloomFiltersIndexer.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;

  public BloomFiltersIndexer(HoodieEngineContext engineContext,
                             HoodieWriteConfig dataTableWriteConfig,
                             HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
  }

  @Override
  public List<InitialIndexPartitionData> build(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    HoodieData<HoodieRecord> records = convertFilesToBloomFilterRecords(
        engineContext, Collections.emptyMap(), partitionToFilesMap, createInstantTime, dataTableMetaClient,
        dataTableWriteConfig.getBloomIndexParallelism(), dataTableWriteConfig.getBloomFilterType());

    return Collections.singletonList(InitialIndexPartitionData.of(
        dataTableWriteConfig.getMetadataConfig().getBloomFilterIndexFileGroupCount(),
        BLOOM_FILTERS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionData> update(String instantTime,
                                         HoodieBackedTableMetadata tableMetadata,
                                         Lazy<HoodieTableFileSystemView> lazyFileSystemView, HoodieCommitMetadata commitMetadata) {
    return convertMetadataToBloomFilterRecords(
        engineContext, dataTableWriteConfig, commitMetadata, instantTime,
        dataTableMetaClient, dataTableWriteConfig.getBloomFilterType(),
        dataTableWriteConfig.getBloomIndexParallelism());
  }

  @Override
  public List<IndexPartitionData> clean(String instantTime, HoodieCleanMetadata cleanMetadata) {
    return Collections.singletonList(IndexPartitionData.of(
        BLOOM_FILTERS.getPartitionPath(), convertMetadataToBloomFilterRecords(
            cleanMetadata, engineContext, instantTime, dataTableWriteConfig.getBloomIndexParallelism())));
  }

  /**
   * Convert added and deleted files metadata to bloom filter index records.
   */
  public static HoodieData<HoodieRecord> convertFilesToBloomFilterRecords(HoodieEngineContext engineContext,
                                                                          Map<String, List<String>> partitionToDeletedFiles,
                                                                          Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                          String instantTime,
                                                                          HoodieTableMetaClient dataMetaClient,
                                                                          int bloomIndexParallelism,
                                                                          String bloomFilterType) {
    // Create the tuple (partition, filename, isDeleted) to handle both deletes and appends
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList = fetchPartitionFileInfoTriplets(partitionToDeletedFiles, partitionToAppendedFiles);

    // Create records MDT
    int parallelism = Math.max(Math.min(partitionFileFlagTupleList.size(), bloomIndexParallelism), 1);
    return engineContext.parallelize(partitionFileFlagTupleList, parallelism).flatMap(partitionFileFlagTuple -> {
      final String partitionName = partitionFileFlagTuple.f0;
      final String filename = partitionFileFlagTuple.f1;
      final boolean isDeleted = partitionFileFlagTuple.f2;
      if (!FSUtils.isBaseFile(new StoragePath(filename))) {
        LOG.warn("Ignoring file {} as it is not a base file", filename);
        return Stream.<HoodieRecord>empty().iterator();
      }

      // Read the bloom filter from the base file if the file is being added
      ByteBuffer bloomFilterBuffer = ByteBuffer.allocate(0);
      if (!isDeleted) {
        final String pathWithPartition = partitionName + "/" + filename;
        final StoragePath addedFilePath = new StoragePath(dataMetaClient.getBasePath(), pathWithPartition);
        bloomFilterBuffer = readBloomFilter(dataMetaClient.getStorage(), addedFilePath);

        // If reading the bloom filter failed then do not add a record for this file
        if (bloomFilterBuffer == null) {
          LOG.error("Failed to read bloom filter from {}", addedFilePath);
          return Stream.<HoodieRecord>empty().iterator();
        }
      }

      return Stream.<HoodieRecord>of(HoodieMetadataPayload.createBloomFilterMetadataRecord(
              partitionName, filename, instantTime, bloomFilterType, bloomFilterBuffer, partitionFileFlagTuple.f2))
          .iterator();
    });
  }

  /**
   * Convert commit action metadata to bloom filter records.
   *
   * @param context               - Engine context to use
   * @param hoodieConfig          - Hudi configs
   * @param commitMetadata        - Commit action metadata
   * @param instantTime           - Action instant time
   * @param dataMetaClient        - HoodieTableMetaClient for data
   * @param bloomFilterType       - Type of generated bloom filter records
   * @param bloomIndexParallelism - Parallelism for bloom filter record generation
   * @return HoodieData of metadata table records
   */
  public static List<IndexPartitionData> convertMetadataToBloomFilterRecords(HoodieEngineContext context,
                                                                             HoodieConfig hoodieConfig,
                                                                             HoodieCommitMetadata commitMetadata,
                                                                             String instantTime,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             String bloomFilterType,
                                                                             int bloomIndexParallelism) {
    final List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
    if (allWriteStats.isEmpty()) {
      return Collections.emptyList();
    }

    final int parallelism = Math.max(Math.min(allWriteStats.size(), bloomIndexParallelism), 1);
    HoodieData<HoodieWriteStat> allWriteStatsRDD = context.parallelize(allWriteStats, parallelism);
    return Collections.singletonList(
        IndexPartitionData.of(BLOOM_FILTERS.getPartitionPath(), allWriteStatsRDD.flatMap(hoodieWriteStat -> {
          final String partition = hoodieWriteStat.getPartitionPath();

          // For bloom filter index, delta writes do not change the base file bloom filter entries
          if (hoodieWriteStat instanceof HoodieDeltaWriteStat) {
            return Collections.emptyListIterator();
          }

          String pathWithPartition = hoodieWriteStat.getPath();
          if (pathWithPartition == null) {
            // Empty partition
            LOG.error("Failed to find path in write stat to update metadata table {}", hoodieWriteStat);
            return Collections.emptyListIterator();
          }

          String fileName = FSUtils.getFileName(pathWithPartition, partition);
          if (!FSUtils.isBaseFile(new StoragePath(fileName))) {
            return Collections.emptyListIterator();
          }

          final StoragePath writeFilePath = new StoragePath(dataMetaClient.getBasePath(), pathWithPartition);
          try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(dataMetaClient.getStorage())
              .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(hoodieConfig, writeFilePath)) {
            try {
              final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
              if (fileBloomFilter == null) {
                LOG.error("Failed to read bloom filter for {}", writeFilePath);
                return Collections.emptyListIterator();
              }
              ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(fileBloomFilter.serializeToString()));
              HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
                  partition, fileName, instantTime, bloomFilterType, bloomByteBuffer, false);
              return Collections.singletonList(record).iterator();
            } catch (Exception e) {
              LOG.error("Failed to read bloom filter for {}", writeFilePath);
              return Collections.emptyListIterator();
            }
          } catch (IOException e) {
            LOG.error("Failed to get bloom filter for file: {}, write stat: {}", writeFilePath, hoodieWriteStat);
          }
          return Collections.emptyListIterator();
        })));
  }

  /**
   * Convert clean metadata to bloom filter index records.
   *
   * @param cleanMetadata           - Clean action metadata
   * @param engineContext           - Engine context
   * @param instantTime             - Clean action instant time
   * @param bloomIndexParallelism   - Parallelism for bloom filter record generation
   * @return List of bloom filter index records for the clean metadata
   */
  public static HoodieData<HoodieRecord> convertMetadataToBloomFilterRecords(HoodieCleanMetadata cleanMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             String instantTime,
                                                                             int bloomIndexParallelism) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> {
        final StoragePath deletedFilePath = new StoragePath(entry);
        if (FSUtils.isBaseFile(deletedFilePath)) {
          deleteFileList.add(Pair.of(partition, deletedFilePath.getName()));
        }
      });
    });

    final int parallelism = Math.max(Math.min(deleteFileList.size(), bloomIndexParallelism), 1);
    HoodieData<Pair<String, String>> deleteFileListRDD = engineContext.parallelize(deleteFileList, parallelism);
    return deleteFileListRDD.map(deleteFileInfoPair -> HoodieMetadataPayload.createBloomFilterMetadataRecord(
        deleteFileInfoPair.getLeft(), deleteFileInfoPair.getRight(), instantTime, StringUtils.EMPTY_STRING,
        ByteBuffer.allocate(0), true));
  }

  private static ByteBuffer readBloomFilter(HoodieStorage storage, StoragePath filePath) throws IOException {
    HoodieConfig hoodieConfig = getReaderConfigs(storage.getConf());
    try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(hoodieConfig, filePath)) {
      final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
      if (fileBloomFilter == null) {
        return null;
      }
      return ByteBuffer.wrap(getUTF8Bytes(fileBloomFilter.serializeToString()));
    }
  }
}
