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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.bloomfilters.BloomFiltersIndexer;
import org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer;
import org.apache.hudi.metadata.index.expression.ExpressionIndexer;
import org.apache.hudi.metadata.index.files.FilesIndexer;
import org.apache.hudi.metadata.index.partitionstats.PartitionStatsIndexer;
import org.apache.hudi.metadata.index.record.PartitionedRecordIndexer;
import org.apache.hudi.metadata.index.record.RecordIndexer;
import org.apache.hudi.metadata.index.secondary.SecondaryIndexer;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Factory for creating {@link Indexer} implementations and resolving enabled indexers
 * based on table and metadata configuration.
 */
@Slf4j
public class IndexerFactory {
  // Record count of each table whose record index is deferred, tagged with the completion time it was taken at, so the
  // metadata writers built during one commit share a single scan of the base files.
  private static final Map<String, Pair<String, Long>> DEFERRED_TABLE_RECORD_COUNTS = new ConcurrentHashMap<>();

  private static Indexer createIndexer(MetadataPartitionType partitionType,
                                       HoodieEngineContext engineContext,
                                       HoodieWriteConfig dataTableWriteConfig,
                                       HoodieTableMetaClient dataTableMetaClient,
                                       EngineIndexerSupport engineIndexerSupport) {
    switch (partitionType) {
      case FILES:
        return new FilesIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient);
      case BLOOM_FILTERS:
        return new BloomFiltersIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient);
      case COLUMN_STATS:
        return new ColumnStatsIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient);
      case RECORD_INDEX:
        return dataTableWriteConfig.isRecordLevelIndexEnabled()
            ? new PartitionedRecordIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient)
            : new RecordIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient);
      case EXPRESSION_INDEX:
        return new ExpressionIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient, engineIndexerSupport);
      case PARTITION_STATS:
        return new PartitionStatsIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient);
      case SECONDARY_INDEX:
        return new SecondaryIndexer(engineContext, dataTableWriteConfig, dataTableMetaClient);
      default:
        throw new HoodieNotSupportedException("Unsupported metadata partition type for indexing: " + partitionType);
    }
  }

  /**
   * Returns the map of metadata partition type to the indexer for the enabled metadata
   * partition types based on the metadata config and table config.
   *
   * @param inflightInstantTimestamp the data table instant the metadata writer is created for, which does not hold back
   *                                 the initialization of record index
   * @param streamingWrites          whether the metadata writer is created for streaming writes, which are not done under
   *                                 the table lock
   */
  public static Map<MetadataPartitionType, Indexer> getEnabledIndexerMap(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient,
      EngineIndexerSupport engineIndexerSupport,
      Option<String> inflightInstantTimestamp,
      boolean streamingWrites) {
    if (!dataTableWriteConfig.getMetadataConfig().isEnabled()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(Arrays.stream(MetadataPartitionType.getValidValues(dataTableMetaClient.getTableConfig().getTableVersion()))
        .filter(partitionType ->
            (partitionType.isMetadataPartitionEnabled(dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient.getTableConfig())
            || partitionType.isMetadataPartitionAvailable(dataTableMetaClient))
            && (partitionType != MetadataPartitionType.RECORD_INDEX || !shouldDeferRecordIndexInit(engineContext, dataTableWriteConfig, dataTableMetaClient, inflightInstantTimestamp, streamingWrites))
        )
        .collect(Collectors.toMap(
            Function.identity(),
            type -> IndexerFactory.createIndexer(type, engineContext, dataTableWriteConfig, dataTableMetaClient, engineIndexerSupport))));
  }

  /**
   * Returns whether the initialization of record index should be deferred. A record index that is already built or
   * being built is never deferred, since leaving it out of the enabled indexers would stop it from receiving updates.
   * Otherwise it is deferred while the table has no completed commit. With {@link HoodieMetadataConfig#DEFER_RLI_INIT_MIN_RECORDS}
   * set, it is also deferred:
   * <ul>
   *   <li>for a metadata writer of streaming writes, which is created outside the table lock, so the index is only
   *   built by a non-streaming metadata writer, which the table lock serializes with other writers when a lock is
   *   configured;</li>
   *   <li>while the latest base files hold fewer records than the threshold;</li>
   *   <li>while another data table instant is pending, since a writer of that instant may have started while the record
   *   index was deferred and would not add its records to an index built in the meantime.</li>
   * </ul>
   * The threshold is not applied with async metadata indexing, because the async indexer needs the record index in the
   * enabled indexers to build it.
   */
  private static boolean shouldDeferRecordIndexInit(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient,
      Option<String> inflightInstantTimestamp,
      boolean streamingWrites) {
    HoodieMetadataConfig metadataConfig = dataTableWriteConfig.getMetadataConfig();
    if (!metadataConfig.shouldDeferRliInitForFreshTable() || isRecordIndexAvailableOrInflight(dataTableMetaClient)) {
      return false;
    }
    if (dataTableMetaClient.getActiveTimeline().filterCompletedInstants().countInstants() == 0) {
      return true;
    }
    long minRecords = metadataConfig.getDeferRliInitMinRecords();
    if (minRecords <= 0 || dataTableWriteConfig.isMetadataAsyncIndex()) {
      return false;
    }
    String basePath = dataTableMetaClient.getBasePath().toString();
    if (streamingWrites) {
      log.debug("Record index for table {} is not built by a metadata writer of streaming writes", basePath);
      return true;
    }
    String latestCompletionTime = dataTableMetaClient.getActiveTimeline().filterCompletedInstants().getLatestCompletionTime().orElse("");
    long recordCount = getOrComputeRecordCount(basePath, latestCompletionTime,
        () -> countRecordsInLatestBaseFiles(engineContext, dataTableMetaClient, metadataConfig));
    if (recordCount < minRecords) {
      log.info("Record index initialization for table {} is deferred: latest base files hold {} records, threshold is {}",
          basePath, recordCount, minRecords);
      return true;
    }
    List<String> otherPendingInstants = getOtherPendingInstants(dataTableMetaClient, inflightInstantTimestamp);
    if (!otherPendingInstants.isEmpty()) {
      log.warn("Record index initialization for table {} is held: latest base files hold {} records, over the threshold of {}, "
          + "but instants {} are pending. The record index is built once no other instant is pending.",
          basePath, recordCount, minRecords, otherPendingInstants);
      return true;
    }
    DEFERRED_TABLE_RECORD_COUNTS.remove(basePath);
    log.info("Record index initialization for table {} is not deferred: latest base files hold {} records, threshold is {}",
        basePath, recordCount, minRecords);
    return false;
  }

  /**
   * Returns the record count of the table at its latest completion time, reusing the count taken by an earlier call at
   * the same completion time.
   */
  @VisibleForTesting
  static long getOrComputeRecordCount(String basePath, String latestCompletionTime, Supplier<Long> recordCounter) {
    Pair<String, Long> cached = DEFERRED_TABLE_RECORD_COUNTS.get(basePath);
    if (cached != null && cached.getLeft().equals(latestCompletionTime)) {
      return cached.getRight();
    }
    long recordCount = recordCounter.get();
    DEFERRED_TABLE_RECORD_COUNTS.put(basePath, Pair.of(latestCompletionTime, recordCount));
    return recordCount;
  }

  /**
   * Returns the pending data table instants other than the given one, leaving out indexing instants as the metadata
   * writer does when it initializes partitions.
   */
  private static List<String> getOtherPendingInstants(HoodieTableMetaClient dataTableMetaClient, Option<String> inflightInstantTimestamp) {
    return dataTableMetaClient.getActiveTimeline().getInstantsAsStream()
        .filter(instant -> !instant.isCompleted() && !HoodieTimeline.INDEXING_ACTION.equals(instant.getAction()))
        .map(HoodieInstant::requestedTime)
        .filter(instantTime -> !inflightInstantTimestamp.isPresent() || !instantTime.equals(inflightInstantTimestamp.get()))
        .collect(Collectors.toList());
  }

  private static boolean isRecordIndexAvailableOrInflight(HoodieTableMetaClient dataTableMetaClient) {
    String recordIndexPartition = MetadataPartitionType.RECORD_INDEX.getPartitionPath();
    return dataTableMetaClient.getTableConfig().getMetadataPartitions().contains(recordIndexPartition)
        || dataTableMetaClient.getTableConfig().getMetadataPartitionsInflight().contains(recordIndexPartition);
  }

  private static long countRecordsInLatestBaseFiles(
      HoodieEngineContext engineContext,
      HoodieTableMetaClient dataTableMetaClient,
      HoodieMetadataConfig metadataConfig) {
    List<StoragePath> baseFilePaths;
    try (HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, dataTableMetaClient, metadataConfig)) {
      fsView.loadAllPartitions();
      baseFilePaths = fsView.getLatestBaseFiles().map(HoodieBaseFile::getStoragePath).collect(Collectors.toList());
    }
    if (baseFilePaths.isEmpty()) {
      return 0L;
    }
    int parallelism = Math.min(baseFilePaths.size(), metadataConfig.getRecordIndexMaxParallelism());
    engineContext.setJobStatus(IndexerFactory.class.getSimpleName(),
        "Record Index: counting records in " + baseFilePaths.size() + " base files to decide whether to initialize");
    return engineContext.parallelize(baseFilePaths, parallelism)
        .map(path -> HoodieIOFactory.getIOFactory(dataTableMetaClient.getStorage()).getFileFormatUtils(path)
            .getRowCount(dataTableMetaClient.getStorage(), path))
        .collectAsList().stream()
        .mapToLong(Long::longValue)
        .sum();
  }
}
