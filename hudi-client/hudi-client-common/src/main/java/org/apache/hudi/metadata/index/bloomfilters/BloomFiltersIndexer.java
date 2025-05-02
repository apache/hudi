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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;

/**
 * Implementation of {@link MetadataPartitionType#BLOOM_FILTERS} index
 */
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
  public List<InitialIndexPartitionData> initialize(
      String createInstantTime,
      String instantTimeForPartition,
      Map<String, Map<String, Long>> partitionIdToAllFilesMap,
      Lazy<List<Pair<String, FileSlice>>> lazyLatestMergedPartitionFileSliceList) throws IOException {
    String bloomFilterType = dataTableWriteConfig.getBloomFilterType();
    // Create the tuple (partition, filename, isDeleted) to handle both deletes and appends
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList =
        Indexer.fetchPartitionFileInfoTriplets(partitionIdToAllFilesMap);
    int parallelism = Math.max(Math.min(partitionFileFlagTupleList.size(),
        dataTableWriteConfig.getBloomIndexParallelism()), 1);
    // This meta client object has to be local to allow Spark to serialize the object
    // instead of the whole indexer object
    HoodieTableMetaClient metaClient = dataTableMetaClient;
    HoodieData<HoodieRecord> records = engineContext.parallelize(partitionFileFlagTupleList, parallelism)
        .flatMap(partitionFileFlagTuple -> {
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
            final StoragePath addedFilePath = new StoragePath(
                metaClient.getBasePath(), pathWithPartition);
            bloomFilterBuffer = readBloomFilter(metaClient.getStorage(), addedFilePath);

            // If reading the bloom filter failed then do not add a record for this file
            if (bloomFilterBuffer == null) {
              LOG.error("Failed to read bloom filter from {}", addedFilePath);
              return Stream.<HoodieRecord>empty().iterator();
            }
          }

          return Stream.<HoodieRecord>of(HoodieMetadataPayload.createBloomFilterMetadataRecord(
                  partitionName, filename, createInstantTime, bloomFilterType, bloomFilterBuffer, partitionFileFlagTuple.f2))
              .iterator();
        });
    return Collections.singletonList(InitialIndexPartitionData.of(
        dataTableWriteConfig.getMetadataConfig().getBloomFilterIndexFileGroupCount(),
        BLOOM_FILTERS.getPartitionPath(), records));
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
