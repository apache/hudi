
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

import org.apache.hudi.avro.model.HoodieMetadataBloomFilter;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieMetadataException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class BaseTableMetadata implements HoodieTableMetadata {

  private static final Logger LOG = LogManager.getLogger(BaseTableMetadata.class);

  public static final long MAX_MEMORY_SIZE_IN_BYTES = 1024 * 1024 * 1024;
  public static final int BUFFER_SIZE = 10 * 1024 * 1024;

  protected final transient HoodieEngineContext engineContext;
  protected final SerializableConfiguration hadoopConf;
  protected final String dataBasePath;
  protected final HoodieTableMetaClient dataMetaClient;
  protected final Option<HoodieMetadataMetrics> metrics;
  protected final HoodieMetadataConfig metadataConfig;
  // Directory used for Spillable Map when merging records
  protected final String spillableMapDirectory;

  protected boolean isMetadataTableEnabled;
  protected boolean isBloomFilterIndexEnabled = false;
  protected boolean isColumnStatsIndexEnabled = false;

  protected BaseTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                              String dataBasePath, String spillableMapDirectory) {
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(engineContext.getHadoopConf());
    this.dataBasePath = dataBasePath;
    this.dataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(dataBasePath).build();
    this.spillableMapDirectory = spillableMapDirectory;
    this.metadataConfig = metadataConfig;

    this.isMetadataTableEnabled = metadataConfig.enabled();
    if (metadataConfig.enableMetrics()) {
      this.metrics = Option.of(new HoodieMetadataMetrics(Registry.getRegistry("HoodieMetadata")));
    } else {
      this.metrics = Option.empty();
    }
  }

  /**
   * Return the list of partitions in the dataset.
   * <p>
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   * <p>
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   */
  @Override
  public List<String> getAllPartitionPaths() throws IOException {
    if (isMetadataTableEnabled) {
      try {
        return fetchAllPartitionPaths();
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve list of partition from metadata", e);
      }
    }
    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, dataBasePath,
        metadataConfig.shouldAssumeDatePartitioning()).getAllPartitionPaths();
  }

  /**
   * Return the list of files in a partition.
   * <p>
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   * <p>
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param partitionPath The absolute path of the partition to list
   */
  @Override
  public FileStatus[] getAllFilesInPartition(Path partitionPath)
      throws IOException {
    if (isMetadataTableEnabled) {
      try {
        return fetchAllFilesInPartition(partitionPath);
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve files in partition " + partitionPath + " from metadata", e);
      }
    }

    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, dataBasePath, metadataConfig.shouldAssumeDatePartitioning())
        .getAllFilesInPartition(partitionPath);
  }

  @Override
  public Map<String, FileStatus[]> getAllFilesInPartitions(List<String> partitions)
      throws IOException {
    if (isMetadataTableEnabled) {
      try {
        List<Path> partitionPaths = partitions.stream().map(Path::new).collect(Collectors.toList());
        return fetchAllFilesInPartitionPaths(partitionPaths);
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve files in partition from metadata", e);
      }
    }

    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, dataBasePath, metadataConfig.shouldAssumeDatePartitioning())
        .getAllFilesInPartitions(partitions);
  }

  @Override
  public Option<BloomFilter> getBloomFilter(final String partitionName, final String fileName)
      throws HoodieMetadataException {
    if (!isBloomFilterIndexEnabled) {
      LOG.error("Metadata bloom filter index is disabled!");
      return Option.empty();
    }

    final Pair<String, String> partitionFileName = Pair.of(partitionName, fileName);
    Map<Pair<String, String>, BloomFilter> bloomFilters = getBloomFilters(Collections.singletonList(partitionFileName));
    if (bloomFilters.isEmpty()) {
      LOG.error("Meta index: missing bloom filter for partition: " + partitionName + ", file: " + fileName);
      return Option.empty();
    }

    ValidationUtils.checkState(bloomFilters.containsKey(partitionFileName));
    return Option.of(bloomFilters.get(partitionFileName));
  }

  @Override
  public Map<Pair<String, String>, BloomFilter> getBloomFilters(final List<Pair<String, String>> partitionNameFileNameList)
      throws HoodieMetadataException {
    if (!isBloomFilterIndexEnabled) {
      LOG.error("Metadata bloom filter index is disabled!");
      return Collections.emptyMap();
    }
    if (partitionNameFileNameList.isEmpty()) {
      return Collections.emptyMap();
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    Set<String> partitionIDFileIDSortedStrings = new TreeSet<>();
    Map<String, Pair<String, String>> fileToKeyMap = new HashMap<>();
    partitionNameFileNameList.forEach(partitionNameFileNamePair -> {
          final String bloomFilterIndexKey = HoodieMetadataPayload.getBloomFilterIndexKey(
              new PartitionIndexID(partitionNameFileNamePair.getLeft()), new FileIndexID(partitionNameFileNamePair.getRight()));
          partitionIDFileIDSortedStrings.add(bloomFilterIndexKey);
          fileToKeyMap.put(bloomFilterIndexKey, partitionNameFileNamePair);
        }
    );

    List<String> partitionIDFileIDStrings = new ArrayList<>(partitionIDFileIDSortedStrings);
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> hoodieRecordList =
        getRecordsByKeys(partitionIDFileIDStrings, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_BLOOM_FILTERS_METADATA_STR,
        (timer.endTimer() / partitionIDFileIDStrings.size())));

    Map<Pair<String, String>, BloomFilter> partitionFileToBloomFilterMap = new HashMap<>();
    for (final Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry : hoodieRecordList) {
      if (entry.getRight().isPresent()) {
        final Option<HoodieMetadataBloomFilter> bloomFilterMetadata =
            entry.getRight().get().getData().getBloomFilterMetadata();
        if (bloomFilterMetadata.isPresent()) {
          if (!bloomFilterMetadata.get().getIsDeleted()) {
            ValidationUtils.checkState(fileToKeyMap.containsKey(entry.getLeft()));
            final ByteBuffer bloomFilterByteBuffer = bloomFilterMetadata.get().getBloomFilter();
            final String bloomFilterType = bloomFilterMetadata.get().getType();
            final BloomFilter bloomFilter = BloomFilterFactory.fromString(
                StandardCharsets.UTF_8.decode(bloomFilterByteBuffer).toString(), bloomFilterType);
            partitionFileToBloomFilterMap.put(fileToKeyMap.get(entry.getLeft()), bloomFilter);
          }
        } else {
          LOG.error("Meta index bloom filter missing for: " + fileToKeyMap.get(entry.getLeft()));
        }
      }
    }
    return partitionFileToBloomFilterMap;
  }

  @Override
  public Map<Pair<String, String>, HoodieMetadataColumnStats> getColumnStats(final List<Pair<String, String>> partitionNameFileNameList, final String columnName)
      throws HoodieMetadataException {
    if (!isColumnStatsIndexEnabled) {
      LOG.error("Metadata column stats index is disabled!");
      return Collections.emptyMap();
    }

    Map<String, Pair<String, String>> columnStatKeyToFileNameMap = new HashMap<>();
    TreeSet<String> sortedKeys = new TreeSet<>();
    final ColumnIndexID columnIndexID = new ColumnIndexID(columnName);
    for (Pair<String, String> partitionNameFileNamePair : partitionNameFileNameList) {
      final String columnStatsIndexKey = HoodieMetadataPayload.getColumnStatsIndexKey(
          new PartitionIndexID(partitionNameFileNamePair.getLeft()),
          new FileIndexID(partitionNameFileNamePair.getRight()),
          columnIndexID);
      sortedKeys.add(columnStatsIndexKey);
      columnStatKeyToFileNameMap.put(columnStatsIndexKey, partitionNameFileNamePair);
    }

    List<String> columnStatKeys = new ArrayList<>(sortedKeys);
    HoodieTimer timer = new HoodieTimer().startTimer();
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> hoodieRecordList =
        getRecordsByKeys(columnStatKeys, MetadataPartitionType.COLUMN_STATS.getPartitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_COLUMN_STATS_METADATA_STR, timer.endTimer()));

    Map<Pair<String, String>, HoodieMetadataColumnStats> fileToColumnStatMap = new HashMap<>();
    for (final Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry : hoodieRecordList) {
      if (entry.getRight().isPresent()) {
        final Option<HoodieMetadataColumnStats> columnStatMetadata =
            entry.getRight().get().getData().getColumnStatMetadata();
        if (columnStatMetadata.isPresent()) {
          if (!columnStatMetadata.get().getIsDeleted()) {
            ValidationUtils.checkState(columnStatKeyToFileNameMap.containsKey(entry.getLeft()));
            final Pair<String, String> partitionFileNamePair = columnStatKeyToFileNameMap.get(entry.getLeft());
            ValidationUtils.checkState(!fileToColumnStatMap.containsKey(partitionFileNamePair));
            fileToColumnStatMap.put(partitionFileNamePair, columnStatMetadata.get());
          }
        } else {
          LOG.error("Meta index column stats missing for: " + entry.getLeft());
        }
      }
    }
    return fileToColumnStatMap;
  }

  /**
   * Returns a list of all partitions.
   */
  protected List<String> fetchAllPartitionPaths() {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getRecordByKey(RECORDKEY_PARTITION_LIST,
        MetadataPartitionType.FILES.getPartitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_PARTITIONS_STR, timer.endTimer()));

    List<String> partitions = Collections.emptyList();
    if (hoodieRecord.isPresent()) {
      handleSpuriousDeletes(hoodieRecord, "\"all partitions\"");
      partitions = hoodieRecord.get().getData().getFilenames();
      // Partition-less tables have a single empty partition
      if (partitions.contains(NON_PARTITIONED_NAME)) {
        partitions.remove(NON_PARTITIONED_NAME);
        partitions.add("");
      }
    }

    LOG.info("Listed partitions from metadata: #partitions=" + partitions.size());
    return partitions;
  }

  /**
   * Return all the files from the partition.
   *
   * @param partitionPath The absolute path of the partition
   */
  FileStatus[] fetchAllFilesInPartition(Path partitionPath) throws IOException {
    String partitionName = FSUtils.getRelativePartitionPath(new Path(dataBasePath), partitionPath);
    if (partitionName.isEmpty()) {
      partitionName = NON_PARTITIONED_NAME;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getRecordByKey(partitionName,
        MetadataPartitionType.FILES.getPartitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));

    FileStatus[] statuses = {};
    if (hoodieRecord.isPresent()) {
      handleSpuriousDeletes(hoodieRecord, partitionName);
      statuses = hoodieRecord.get().getData().getFileStatuses(hadoopConf.get(), partitionPath);
    }

    LOG.info("Listed file in partition from metadata: partition=" + partitionName + ", #files=" + statuses.length);
    return statuses;
  }

  Map<String, FileStatus[]> fetchAllFilesInPartitionPaths(List<Path> partitionPaths) throws IOException {
    Map<String, Path> partitionInfo = new HashMap<>();
    boolean foundNonPartitionedPath = false;
    for (Path partitionPath: partitionPaths) {
      String partitionName = FSUtils.getRelativePartitionPath(new Path(dataBasePath), partitionPath);
      if (partitionName.isEmpty()) {
        if (partitionInfo.size() > 1) {
          throw new HoodieMetadataException("Found mix of partitioned and non partitioned paths while fetching data from metadata table");
        }
        partitionInfo.put(NON_PARTITIONED_NAME, partitionPath);
        foundNonPartitionedPath = true;
      } else {
        if (foundNonPartitionedPath) {
          throw new HoodieMetadataException("Found mix of partitioned and non partitioned paths while fetching data from metadata table");
        }
        partitionInfo.put(partitionName, partitionPath);
      }
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> partitionsFileStatus =
        getRecordsByKeys(new ArrayList<>(partitionInfo.keySet()), MetadataPartitionType.FILES.getPartitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));
    Map<String, FileStatus[]> result = new HashMap<>();

    for (Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry: partitionsFileStatus) {
      if (entry.getValue().isPresent()) {
        handleSpuriousDeletes(entry.getValue(), entry.getKey());
        result.put(partitionInfo.get(entry.getKey()).toString(), entry.getValue().get().getData().getFileStatuses(hadoopConf.get(), partitionInfo.get(entry.getKey())));
      }
    }

    LOG.info("Listed files in partitions from metadata: partition list =" + Arrays.toString(partitionPaths.toArray()));
    return result;
  }

  /**
   * Handle spurious deletes. Depending on config, throw an exception or log a warn msg.
   * @param hoodieRecord instance of {@link HoodieRecord} of interest.
   * @param partitionName partition name of interest.
   */
  private void handleSpuriousDeletes(Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord, String partitionName) {
    if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
      if (metadataConfig.ignoreSpuriousDeletes()) {
        LOG.warn("Metadata record for " + partitionName + " encountered some files to be deleted which was not added before. "
            + "Ignoring the spurious deletes as the `" + HoodieMetadataConfig.IGNORE_SPURIOUS_DELETES.key() + "` config is set to true");
      } else {
        throw new HoodieMetadataException("Metadata record for " + partitionName + " is inconsistent: "
            + hoodieRecord.get().getData());
      }
    }
  }

  protected abstract Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key, String partitionName);

  public abstract List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> getRecordsByKeys(List<String> key, String partitionName);

  protected HoodieEngineContext getEngineContext() {
    return engineContext != null ? engineContext : new HoodieLocalEngineContext(hadoopConf.get());
  }

  public HoodieMetadataConfig getMetadataConfig() {
    return metadataConfig;
  }

  protected String getLatestDataInstantTime() {
    return dataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
        .map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);
  }
}
