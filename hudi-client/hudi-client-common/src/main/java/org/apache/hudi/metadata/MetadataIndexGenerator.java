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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;

/**
 * For now this is a placeholder to generate all MDT records in one place.
 * Once https://github.com/apache/hudi/pull/13226 is landed, we will leverage the new abstraction to generate MDT records.
 */
public class MetadataIndexGenerator implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataIndexGenerator.class);

  /**
   * MDT record generation utility. This function is expected to be invoked from a map Partition call, where one spark task will receive
   * one WriteStatus as input and the output contains prepared Metadata table records for all eligible partitions that can operate on one
   * WriteStatus instance only.
   */
  static class PerWriteStatsBasedIndexGenerator implements SerializableFunction<WriteStatus, Iterator<Pair<String, HoodieRecord>>> {
    List<MetadataPartitionType> enabledPartitionTypes;
    HoodieWriteConfig dataWriteConfig;
    StorageConfiguration<?> storageConf;
    String instantTime;

    public PerWriteStatsBasedIndexGenerator(List<MetadataPartitionType> enabledPartitionTypes, HoodieWriteConfig dataWriteConfig, StorageConfiguration<?> storageConf, String instantTime) {
      this.enabledPartitionTypes = enabledPartitionTypes;
      this.dataWriteConfig = dataWriteConfig;
      this.storageConf = storageConf;
      this.instantTime = instantTime;
    }

    @Override
    public Iterator<Pair<String, HoodieRecord>> apply(WriteStatus writeStatus) throws Exception {
      List<Pair<String, HoodieRecord>> allRecords = new ArrayList<>();
      if (enabledPartitionTypes.contains(COLUMN_STATS)) {
        allRecords.addAll(processWriteStatusForColStats(writeStatus));
      }
      if (enabledPartitionTypes.contains(RECORD_INDEX)) {
        allRecords.addAll(processWriteStatusForRLI(writeStatus, dataWriteConfig));
      }
      if (enabledPartitionTypes.contains(BLOOM_FILTERS)) {
        allRecords.addAll(processWriteStatusForBloomFilters(writeStatus, dataWriteConfig));
      }
      // yet to add support for more partitions.
      // bloom filter
      // secondary index
      // expression index.
      return allRecords.iterator();
    }

    private List<Pair<String, HoodieRecord>> processWriteStatusForBloomFilters(WriteStatus writeStatus, HoodieWriteConfig dataWriteConfig) throws IOException {
      HoodieWriteStat hoodieWriteStat = writeStatus.getStat();
      final String partition = hoodieWriteStat.getPartitionPath();

      // For bloom filter index, delta writes do not change the base file bloom filter entries
      if (hoodieWriteStat instanceof HoodieDeltaWriteStat) {
        return Collections.emptyList();
      }

      String pathWithPartition = hoodieWriteStat.getPath();
      if (pathWithPartition == null) {
        // Empty partition
        LOG.error("Failed to find path in write stat to update metadata table {}", hoodieWriteStat);
        return Collections.emptyList();
      }

      String fileName = FSUtils.getFileName(pathWithPartition, partition);
      if (!FSUtils.isBaseFile(new StoragePath(fileName))) {
        return Collections.emptyList();
      }

      final StoragePath writeFilePath = new StoragePath(dataWriteConfig.getBasePath(), pathWithPartition);
      HoodieHadoopStorage storage = new HoodieHadoopStorage(writeFilePath, storageConf);
      try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(dataWriteConfig, writeFilePath)) {
          final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
          if (fileBloomFilter == null) {
            LOG.error("Failed to read bloom filter for {}", writeFilePath);
            return Collections.emptyList();
          }
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(fileBloomFilter.serializeToString()));
          HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
              partition, fileName, instantTime, dataWriteConfig.getBloomFilterType(), bloomByteBuffer, false);
          return Collections.singletonList(Pair.of(BLOOM_FILTERS.getPartitionPath(), record));
      }
    }
  }

  protected HoodieData<HoodieRecord> prepareFilesPartitionRecords(HoodieEngineContext context, HoodieCommitMetadata commitMetadata, String instantTime) {
    return context.parallelize(
        HoodieTableMetadataUtil.convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1);
  }

  protected static List<Pair<String, HoodieRecord>> processWriteStatusForColStats(WriteStatus writeStatus) {
    List<Pair<String, HoodieRecord>> allRecords = new ArrayList<>();
    writeStatus.getStat().getColumnStats().ifPresent(columnRangeMap -> {
      Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = columnRangeMap.values();
      allRecords.addAll(HoodieMetadataPayload.createColumnStatsRecords(writeStatus.getStat().getPartitionPath(), columnRangeMetadataList, false)
          .collect(Collectors.toList()).stream().map(record -> Pair.of(COLUMN_STATS.getPartitionPath(), record)).collect(Collectors.toList()));
    });
    return allRecords;
  }

  protected static List<Pair<String, HoodieRecord>> processWriteStatusForRLI(WriteStatus writeStatus, HoodieWriteConfig dataWriteConfig) {
    List<Pair<String, HoodieRecord>> allRecords = new ArrayList<>();
    for (HoodieRecordDelegate recordDelegate : writeStatus.getWrittenRecordDelegates()) {
      if (!writeStatus.isErrored(recordDelegate.getHoodieKey())) {
        if (recordDelegate.getIgnoreIndexUpdate()) {
          continue;
        }
        HoodieRecord hoodieRecord;
        Option<HoodieRecordLocation> newLocation = recordDelegate.getNewLocation();
        if (newLocation.isPresent()) {
          if (recordDelegate.getCurrentLocation().isPresent()) {
            // This is an update, no need to update index if the location has not changed
            // newLocation should have the same fileID as currentLocation. The instantTimes differ as newLocation's
            // instantTime refers to the current commit which was completed.
            if (!recordDelegate.getCurrentLocation().get().getFileId().equals(newLocation.get().getFileId())) {
              final String msg = String.format("Detected update in location of record with key %s from %s to %s. The fileID should not change.",
                  recordDelegate, recordDelegate.getCurrentLocation().get(), newLocation.get());
              LOG.error(msg);
              throw new HoodieMetadataException(msg);
            }
            // for updates, we can skip updating RLI partition in MDT
          } else {
            // Insert new record case
            hoodieRecord = HoodieMetadataPayload.createRecordIndexUpdate(
                recordDelegate.getRecordKey(), recordDelegate.getPartitionPath(),
                newLocation.get().getFileId(), newLocation.get().getInstantTime(), dataWriteConfig.getWritesFileIdEncoding());
            allRecords.add(Pair.of(RECORD_INDEX.getPartitionPath(), hoodieRecord));
          }
        } else {
          // Delete existing index for a deleted record
          hoodieRecord = HoodieMetadataPayload.createRecordIndexDelete(recordDelegate.getRecordKey());
          allRecords.add(Pair.of(RECORD_INDEX.getPartitionPath(), hoodieRecord));
        }
      }
    }
    return allRecords;
  }

  HoodieData<Pair<String, HoodieRecord>> prepareMDTRecordsGroupedByHudiPartition(HoodieData<WriteStatus> writeStatusHoodieData) {
    HoodieData<WriteStatus> writeStatusPartitionedByHudiPartition = repartitionRecordsByHudiPartition(writeStatusHoodieData, Math.min(writeStatusHoodieData.getNumPartitions(), 10));
    HoodieData<Pair<String, HoodieRecord>> perPartitionRecords = writeStatusPartitionedByHudiPartition.map(WriteStatus::getStat)
        .mapPartitions(new ProcessWriteStatsMapPartitionFunc(), true);
    return perPartitionRecords;
  }

  protected HoodieData<WriteStatus> repartitionRecordsByHudiPartition(HoodieData<WriteStatus> records, int numPartitions) {
    // override.
    return records;
  }

  static class ProcessWriteStatsMapPartitionFunc
      implements SerializableFunction<Iterator<HoodieWriteStat>, Iterator<Pair<String, HoodieRecord>>> {

    @Override
    public Iterator<Pair<String, HoodieRecord>> apply(Iterator<HoodieWriteStat> v1) throws Exception {
      // generate partition stats record when enabled.
      Map<String, List<HoodieWriteStat>> allWriteStats = new HashMap<>();
      List<Pair<String, HoodieRecord>> toReturn = new ArrayList<>();
      return toReturn.iterator();
    }
  }
}


