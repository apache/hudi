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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;

/**
 * Implementation of {@link MetadataPartitionType#BLOOM_FILTERS} index
 */
@Slf4j
public class BloomFiltersIndexer extends BaseIndexer {

  public BloomFiltersIndexer(HoodieEngineContext engineContext,
                             HoodieWriteConfig dataTableWriteConfig,
                             HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToBloomFilterRecords(
        engineContext, Collections.emptyMap(), context.allFiles(), context.dataInstantTime(), dataTableMetaClient,
        dataTableWriteConfig.getBloomIndexParallelism(), dataTableWriteConfig.getBloomFilterType());
    final int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getBloomFilterIndexFileGroupCount();
    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, BLOOM_FILTERS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    final HoodieData<HoodieRecord> records = convertMetadataToBloomFilterRecords(
        engineContext, dataTableWriteConfig, context.commitMetadata(), context.instantTime(), dataTableMetaClient,
        dataTableWriteConfig.getBloomFilterType(), dataTableWriteConfig.getBloomIndexParallelism());
    return Collections.singletonList(IndexPartitionAndRecords.of(BLOOM_FILTERS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    final HoodieData<HoodieRecord> records =
        convertMetadataToBloomFilterRecords(context.cleanMetadata(), engineContext, context.instantTime(), dataTableWriteConfig.getBloomIndexParallelism());
    return Collections.singletonList(IndexPartitionAndRecords.of(BLOOM_FILTERS.getPartitionPath(), records));
  }

  /**
   * Convert commit action metadata to bloom filter records.
   *
   * @param context                 - Engine context to use
   * @param hoodieConfig            - Hudi configs
   * @param commitMetadata          - Commit action metadata
   * @param instantTime             - Action instant time
   * @param dataMetaClient          - HoodieTableMetaClient for data
   * @param bloomFilterType         - Type of generated bloom filter records
   * @param bloomIndexParallelism   - Parallelism for bloom filter record generation
   * @return HoodieData of metadata table records
   */
  private static HoodieData<HoodieRecord> convertMetadataToBloomFilterRecords(
      HoodieEngineContext context,
      HoodieWriteConfig hoodieConfig,
      HoodieCommitMetadata commitMetadata,
      String instantTime,
      HoodieTableMetaClient dataMetaClient,
      String bloomFilterType,
      int bloomIndexParallelism) {
    final List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
    if (allWriteStats.isEmpty()) {
      return context.emptyHoodieData();
    }

    final int parallelism = Math.max(Math.min(allWriteStats.size(), bloomIndexParallelism), 1);
    HoodieData<HoodieWriteStat> allWriteStatsRDD = context.parallelize(allWriteStats, parallelism);
    return allWriteStatsRDD.flatMap(hoodieWriteStat -> {
      final String partition = hoodieWriteStat.getPartitionPath();

      // For bloom filter index, delta writes do not change the base file bloom filter entries
      if (hoodieWriteStat instanceof HoodieDeltaWriteStat) {
        return Collections.emptyListIterator();
      }

      String pathWithPartition = hoodieWriteStat.getPath();
      if (pathWithPartition == null) {
        // Empty partition
        log.error("Failed to find path in write stat to update metadata table {}", hoodieWriteStat);
        return Collections.emptyListIterator();
      }

      String fileName = FSUtils.getFileName(pathWithPartition, partition);
      if (!FSUtils.isBaseFile(new StoragePath(fileName))) {
        return Collections.emptyListIterator();
      }

      final StoragePath writeFilePath = new StoragePath(dataMetaClient.getBasePath(), pathWithPartition);
      try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(dataMetaClient.getStorage())
          .getReaderFactory(hoodieConfig.getRecordMerger().getRecordType())
          .getFileReader(hoodieConfig, writeFilePath)) {
        try {
          final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
          if (fileBloomFilter == null) {
            log.error("Failed to read bloom filter for {}", writeFilePath);
            return Collections.emptyListIterator();
          }
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(fileBloomFilter.serializeToString()));
          HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
              partition, fileName, instantTime, bloomFilterType, bloomByteBuffer, false);
          return Collections.singletonList(record).iterator();
        } catch (Exception e) {
          log.error("Failed to read bloom filter for {}", writeFilePath);
          return Collections.emptyListIterator();
        }
      } catch (IOException e) {
        log.error("Failed to get bloom filter for file: {}, write stat: {}", writeFilePath, hoodieWriteStat);
      }
      return Collections.emptyListIterator();
    });
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
  public static HoodieData<HoodieRecord> convertMetadataToBloomFilterRecords(
      HoodieCleanMetadata cleanMetadata,
      HoodieEngineContext engineContext,
      String instantTime,
      int bloomIndexParallelism) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(deletedFilePathStr -> {
        final StoragePath deletedFilePath = new StoragePath(deletedFilePathStr);
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
}
