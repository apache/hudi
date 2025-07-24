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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.IDENTITY_ENCODING;

/**
 * Abstract class for implementing common table metadata operations.
 */
public abstract class BaseTableMetadata extends AbstractHoodieTableMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(BaseTableMetadata.class);

  protected final HoodieTableMetaClient dataMetaClient;
  protected final Option<HoodieMetadataMetrics> metrics;
  protected final HoodieMetadataConfig metadataConfig;

  protected boolean isMetadataTableInitialized;
  protected final boolean hiveStylePartitioningEnabled;
  protected final boolean urlEncodePartitioningEnabled;

  protected BaseTableMetadata(HoodieEngineContext engineContext,
                              HoodieStorage storage,
                              HoodieMetadataConfig metadataConfig,
                              String dataBasePath) {
    super(engineContext, storage, dataBasePath);

    this.dataMetaClient = HoodieTableMetaClient.builder()
        .setStorage(storage)
        .setBasePath(dataBasePath)
        .build();

    this.hiveStylePartitioningEnabled = Boolean.parseBoolean(dataMetaClient.getTableConfig().getHiveStylePartitioningEnable());
    this.urlEncodePartitioningEnabled = Boolean.parseBoolean(dataMetaClient.getTableConfig().getUrlEncodePartitioning());
    this.metadataConfig = metadataConfig;
    this.isMetadataTableInitialized = dataMetaClient.getTableConfig().isMetadataTableAvailable();

    if (metadataConfig.isMetricsEnabled()) {
      this.metrics = Option.of(new HoodieMetadataMetrics(HoodieMetricsConfig.newBuilder()
          .fromProperties(metadataConfig.getProps()).withPath(dataBasePath).build(), dataMetaClient.getStorage()));
    } else {
      this.metrics = Option.empty();
    }
  }

  protected HoodieEngineContext getEngineContext() {
    if (engineContext == null) {
      engineContext = new HoodieLocalEngineContext(dataMetaClient.getStorageConf());
    }
    return engineContext;
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
    ValidationUtils.checkArgument(isMetadataTableInitialized);
    try {
      return fetchAllPartitionPaths();
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to retrieve list of partition from metadata", e);
    }
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
  public List<StoragePathInfo> getAllFilesInPartition(StoragePath partitionPath) throws IOException {
    ValidationUtils.checkArgument(isMetadataTableInitialized);
    try {
      return fetchAllFilesInPartition(partitionPath);
    } catch (Exception e) {
      throw new HoodieMetadataException(
          "Failed to retrieve files in partition " + partitionPath + " from metadata", e);
    }
  }

  @Override
  public Map<String, List<StoragePathInfo>> getAllFilesInPartitions(Collection<String> partitions)
      throws IOException {
    ValidationUtils.checkArgument(isMetadataTableInitialized);
    if (partitions.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      List<StoragePath> partitionPaths =
          partitions.stream().map(StoragePath::new).collect(Collectors.toList());
      return fetchAllFilesInPartitionPaths(partitionPaths);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to retrieve files in partition from metadata", e);
    }
  }

  @Override
  public Option<BloomFilter> getBloomFilter(final String partitionName, final String fileName, final String metadataPartitionName) throws HoodieMetadataException {
    if (!dataMetaClient.getTableConfig().getMetadataPartitions().contains(metadataPartitionName)) {
      LOG.error("Metadata bloom filter index is disabled!");
      return Option.empty();
    }

    final Pair<String, String> partitionFileName = Pair.of(partitionName, fileName);
    Map<Pair<String, String>, BloomFilter> bloomFilters = getBloomFilters(Collections.singletonList(partitionFileName), metadataPartitionName);
    if (bloomFilters.isEmpty()) {
      LOG.error("Meta index: missing bloom filter for partition: {}, file: {}", partitionName, fileName);
      return Option.empty();
    }

    ValidationUtils.checkState(bloomFilters.containsKey(partitionFileName));
    return Option.of(bloomFilters.get(partitionFileName));
  }

  @Override
  public Map<Pair<String, String>, BloomFilter> getBloomFilters(final List<Pair<String, String>> partitionNameFileNameList, final String metadataPartitionName)
      throws HoodieMetadataException {
    if (!dataMetaClient.getTableConfig().getMetadataPartitions().contains(metadataPartitionName)) {
      LOG.error("Metadata bloom filter index is disabled!");
      return Collections.emptyMap();
    }
    if (partitionNameFileNameList.isEmpty()) {
      return Collections.emptyMap();
    }

    HoodieTimer timer = HoodieTimer.start();
    Set<String> partitionIDFileIDStrings = new HashSet<>();
    Map<String, Pair<String, String>> fileToKeyMap = new HashMap<>();
    partitionNameFileNameList.forEach(partitionNameFileNamePair -> {
      final String bloomFilterIndexKey = HoodieMetadataPayload.getBloomFilterIndexKey(
          new PartitionIndexID(HoodieTableMetadataUtil.getBloomFilterIndexPartitionIdentifier(partitionNameFileNamePair.getLeft())), new FileIndexID(partitionNameFileNamePair.getRight()));
      partitionIDFileIDStrings.add(bloomFilterIndexKey);
      fileToKeyMap.put(bloomFilterIndexKey, partitionNameFileNamePair);
    });

    List<String> partitionIDFileIDStringsList = new ArrayList<>(partitionIDFileIDStrings);
    Map<String, HoodieRecord<HoodieMetadataPayload>> hoodieRecords =
        HoodieDataUtils.dedupeAndCollectAsMap(
            getRecordsByKeys(HoodieListData.eager(partitionIDFileIDStringsList), metadataPartitionName, IDENTITY_ENCODING));
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_BLOOM_FILTERS_METADATA_STR, timer.endTimer()));
    metrics.ifPresent(m -> m.setMetric(HoodieMetadataMetrics.LOOKUP_BLOOM_FILTERS_FILE_COUNT_STR, partitionIDFileIDStringsList.size()));

    Map<Pair<String, String>, BloomFilter> partitionFileToBloomFilterMap = new HashMap<>(hoodieRecords.size());
    for (final Map.Entry<String, HoodieRecord<HoodieMetadataPayload>> entry : hoodieRecords.entrySet()) {
      final Option<HoodieMetadataBloomFilter> bloomFilterMetadata =
          entry.getValue().getData().getBloomFilterMetadata();
      if (bloomFilterMetadata.isPresent()) {
        if (!bloomFilterMetadata.get().getIsDeleted()) {
          ValidationUtils.checkState(fileToKeyMap.containsKey(entry.getKey()));
          // NOTE: We have to duplicate the [[ByteBuffer]] object here since:
          //        - Reading out [[ByteBuffer]] mutates its state
          //        - [[BloomFilterMetadata]] could be re-used, and hence have to stay immutable
          final ByteBuffer bloomFilterByteBuffer =
              bloomFilterMetadata.get().getBloomFilter().duplicate();
          final String bloomFilterType = bloomFilterMetadata.get().getType();
          final BloomFilter bloomFilter = BloomFilterFactory.fromString(
              StandardCharsets.UTF_8.decode(bloomFilterByteBuffer).toString(), bloomFilterType);
          partitionFileToBloomFilterMap.put(fileToKeyMap.get(entry.getKey()), bloomFilter);
        }
      } else {
        LOG.error("Meta index bloom filter missing for: {}", fileToKeyMap.get(entry.getKey()));
      }
    }
    return partitionFileToBloomFilterMap;
  }

  @Override
  public Map<Pair<String, String>, HoodieMetadataColumnStats> getColumnStats(final List<Pair<String, String>> partitionNameFileNameList, final String columnName)
      throws HoodieMetadataException {
    Map<Pair<String, String>, List<HoodieMetadataColumnStats>> partitionNameFileToColStats = getColumnStats(partitionNameFileNameList, Collections.singletonList(columnName));
    ValidationUtils.checkArgument(partitionNameFileToColStats.isEmpty() || partitionNameFileToColStats.values().stream().anyMatch(stats -> stats.size() == 1));
    return partitionNameFileToColStats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(0)));
  }

  @Override
  public Map<Pair<String, String>, List<HoodieMetadataColumnStats>> getColumnStats(List<Pair<String, String>> partitionNameFileNameList, List<String> columnNames)
      throws HoodieMetadataException {
    if (!dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS)) {
      LOG.error("Metadata column stats index is disabled!");
      return Collections.emptyMap();
    }

    Map<String, Pair<String, String>> columnStatKeyToFileNameMap = computeColStatKeyToFileName(partitionNameFileNameList, columnNames);
    return computeFileToColumnStatsMap(columnStatKeyToFileNameMap);
  }

  /**
   * Returns a list of all partitions.
   */
  protected List<String> fetchAllPartitionPaths() {
    HoodieTimer timer = HoodieTimer.start();
    Option<HoodieRecord<HoodieMetadataPayload>> recordOpt = getRecordByKey(RECORDKEY_PARTITION_LIST,
        MetadataPartitionType.FILES.getPartitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_PARTITIONS_STR, timer.endTimer()));

    List<String> partitions = recordOpt.map(record -> {
      HoodieMetadataPayload metadataPayload = record.getData();
      checkForSpuriousDeletes(metadataPayload, "\"all partitions\"");

      List<String> relativePaths = metadataPayload.getFilenames();
      // Non-partitioned tables have a single empty partition
      if (relativePaths.size() == 1 && relativePaths.get(0).equals(NON_PARTITIONED_NAME)) {
        return Collections.singletonList("");
      } else {
        return relativePaths;
      }
    })
        .orElse(Collections.emptyList());

    LOG.info("Listed partitions from metadata: #partitions={}", partitions.size());
    return partitions;
  }

  /**
   * Return all the files from the partition.
   *
   * @param partitionPath The absolute path of the partition
   */
  List<StoragePathInfo> fetchAllFilesInPartition(StoragePath partitionPath) {
    String relativePartitionPath = FSUtils.getRelativePartitionPath(dataBasePath, partitionPath);
    String recordKey = relativePartitionPath.isEmpty() ? NON_PARTITIONED_NAME : relativePartitionPath;

    HoodieTimer timer = HoodieTimer.start();
    Option<HoodieRecord<HoodieMetadataPayload>> recordOpt = getRecordByKey(recordKey,
        MetadataPartitionType.FILES.getPartitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));

    List<StoragePathInfo> pathInfoList = recordOpt
        .map(record -> {
          HoodieMetadataPayload metadataPayload = record.getData();
          checkForSpuriousDeletes(metadataPayload, recordKey);
          try {
            return metadataPayload.getFileList(dataMetaClient.getStorage(), partitionPath);
          } catch (Exception e) {
            throw new HoodieException("Failed to extract file-pathInfoList from the payload", e);
          }
        })
        .orElseGet(Collections::emptyList);

    LOG.debug("Listed file in partition from metadata: partition={}, #files={}", relativePartitionPath, pathInfoList.size());
    return pathInfoList;
  }

  Map<String, List<StoragePathInfo>> fetchAllFilesInPartitionPaths(List<StoragePath> partitionPaths) {
    Map<String, StoragePath> partitionIdToPathMap =
        partitionPaths.parallelStream()
            .collect(
                Collectors.toMap(partitionPath -> {
                  String partitionId =
                      FSUtils.getRelativePartitionPath(dataBasePath, partitionPath);
                  return partitionId.isEmpty() ? NON_PARTITIONED_NAME : partitionId;
                }, Function.identity())
            );

    HoodieTimer timer = HoodieTimer.start();
    Map<String, HoodieRecord<HoodieMetadataPayload>> partitionIdRecordPairs =
        HoodieDataUtils.dedupeAndCollectAsMap(
            getRecordsByKeys(HoodieListData.eager(new ArrayList<>(partitionIdToPathMap.keySet())),
                MetadataPartitionType.FILES.getPartitionPath(), IDENTITY_ENCODING));
    metrics.ifPresent(
        m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));

    Map<String, List<StoragePathInfo>> partitionPathToFilesMap =
        partitionIdRecordPairs.entrySet().stream()
            .map(e -> {
              final String partitionId = e.getKey();
              StoragePath partitionPath = partitionIdToPathMap.get(partitionId);

              HoodieMetadataPayload metadataPayload = e.getValue().getData();
              checkForSpuriousDeletes(metadataPayload, partitionId);

              List<StoragePathInfo> files = metadataPayload.getFileList(dataMetaClient.getStorage(), partitionPath);
              return Pair.of(partitionPath.toString(), files);
            })
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    LOG.info("Listed files in {} partitions from metadata", partitionPaths.size());

    return partitionPathToFilesMap;
  }

  /**
   * Computes a map from col-stats key to partition and file name pair.
   *
   * @param partitionNameFileNameList - List of partition and file name pair for which bloom filters need to be retrieved.
   * @param columnNames - List of column name for which stats are needed.
   */
  private Map<String, Pair<String, String>> computeColStatKeyToFileName(
      final List<Pair<String, String>> partitionNameFileNameList,
      final List<String> columnNames) {
    Map<String, Pair<String, String>> columnStatKeyToFileNameMap = new HashMap<>();
    for (String columnName : columnNames) {
      final ColumnIndexID columnIndexID = new ColumnIndexID(columnName);
      for (Pair<String, String> partitionNameFileNamePair : partitionNameFileNameList) {
        final String columnStatsIndexKey = HoodieMetadataPayload.getColumnStatsIndexKey(
            new PartitionIndexID(HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier(partitionNameFileNamePair.getLeft())),
            new FileIndexID(partitionNameFileNamePair.getRight()),
            columnIndexID);
        columnStatKeyToFileNameMap.put(columnStatsIndexKey, partitionNameFileNamePair);
      }
    }
    return columnStatKeyToFileNameMap;
  }

  /**
   * Computes the map from partition and file name pair to HoodieMetadataColumnStats record.
   *
   * @param columnStatKeyToFileNameMap - A map from col-stats key to partition and file name pair.
   */
  private Map<Pair<String, String>, List<HoodieMetadataColumnStats>> computeFileToColumnStatsMap(Map<String, Pair<String, String>> columnStatKeyToFileNameMap) {
    List<String> columnStatKeylist = new ArrayList<>(columnStatKeyToFileNameMap.keySet());
    HoodieTimer timer = HoodieTimer.start();
    Map<String, HoodieRecord<HoodieMetadataPayload>> hoodieRecords =
        HoodieDataUtils.dedupeAndCollectAsMap(
            getRecordsByKeys(
                HoodieListData.eager(columnStatKeylist), MetadataPartitionType.COLUMN_STATS.getPartitionPath(), IDENTITY_ENCODING));
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_COLUMN_STATS_METADATA_STR, timer.endTimer()));
    Map<Pair<String, String>, List<HoodieMetadataColumnStats>> fileToColumnStatMap = new HashMap<>();
    for (final Map.Entry<String, HoodieRecord<HoodieMetadataPayload>> entry : hoodieRecords.entrySet()) {
      final Option<HoodieMetadataColumnStats> columnStatMetadata =
          entry.getValue().getData().getColumnStatMetadata();
      if (columnStatMetadata.isPresent() && !columnStatMetadata.get().getIsDeleted()) {
        ValidationUtils.checkState(columnStatKeyToFileNameMap.containsKey(entry.getKey()));
        final Pair<String, String> partitionFileNamePair = columnStatKeyToFileNameMap.get(entry.getKey());
        fileToColumnStatMap.computeIfAbsent(partitionFileNamePair, k -> new ArrayList<>()).add(columnStatMetadata.get());
      } else {
        LOG.error("Meta index column stats missing for {}", entry.getKey());
      }
    }
    return fileToColumnStatMap;
  }

  /**
   * Handle spurious deletes. Depending on config, throw an exception or log a warn msg.
   */
  private void checkForSpuriousDeletes(HoodieMetadataPayload metadataPayload, String partitionName) {
    if (!metadataPayload.getDeletions().isEmpty()) {
      if (metadataConfig.shouldIgnoreSpuriousDeletes()) {
        LOG.warn("Metadata record for " + partitionName + " encountered some files to be deleted which was not added before. "
            + "Ignoring the spurious deletes as the `" + HoodieMetadataConfig.IGNORE_SPURIOUS_DELETES.key() + "` config is set to true");
      } else {
        throw new HoodieMetadataException("Metadata record for " + partitionName + " is inconsistent: "
            + metadataPayload);
      }
    }
  }

  /**
   * Retrieves a single record from the metadata table by its key.
   *
   * @param key The escaped/encoded key to look up in the metadata table
   * @param partitionName The partition name where the record is stored
   * @return Option containing the record if found, empty Option if not found
   */
  protected abstract Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key, String partitionName);

  /**
   * Retrieves a collection of pairs (key -> record) from the metadata table by its keys.
   *
   * @param keys The to look up in the metadata table
   * @param partitionName The partition name where the records are stored
   * @return A collection of pairs (key -> record)
   */
  public abstract HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> getRecordsByKeys(
          HoodieData<String> keys, String partitionName, SerializableFunctionUnchecked<String, String> keyEncodingFn);

  /**
   * Returns a collection of pairs (secondary-key -> set-of-record-keys) for the provided secondary keys.
   *
   * @param keys The unescaped/decoded secondary keys to look up in the metadata table
   * @param partitionName The partition name where the secondary index records are stored
   * @return A collection of pairs where each key is a secondary key and the value is a set of record keys that are indexed by that secondary key
   */
  public abstract HoodiePairData<String, Set<String>> getSecondaryIndexRecords(HoodieData<String> keys, String partitionName);

  public HoodieMetadataConfig getMetadataConfig() {
    return metadataConfig;
  }

  protected StorageConfiguration<?> getStorageConf() {
    return dataMetaClient.getStorageConf();
  }

  protected String getLatestDataInstantTime() {
    return dataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
        .map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
  }

  public boolean isMetadataTableInitialized() {
    return isMetadataTableInitialized;
  }
}
