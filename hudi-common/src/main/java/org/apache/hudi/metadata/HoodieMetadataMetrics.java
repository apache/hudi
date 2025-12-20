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

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metrics.HoodieGauge;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.storage.HoodieStorage;

import com.codahale.metrics.MetricRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Metrics for metadata.
 */
@Slf4j
public class HoodieMetadataMetrics implements Serializable {

  // Metric names
  public static final String LOOKUP_PARTITIONS_STR = "lookup_partitions";
  public static final String LOOKUP_FILES_STR = "lookup_files";
  public static final String LOOKUP_BLOOM_FILTERS_METADATA_STR = "lookup_meta_index_bloom_filters";
  public static final String LOOKUP_COLUMN_STATS_METADATA_STR = "lookup_meta_index_column_ranges";
  public static final String LOOKUP_BLOOM_FILTERS_FILE_COUNT_STR = "lookup_meta_index_bloom_filters_file_count";
  public static final String LOOKUP_COLUMN_STATS_FILE_COUNT_STR = "lookup_meta_index_column_ranges_file_count";

  // Time for lookup from record index
  public static final String LOOKUP_RECORD_INDEX_TIME_STR = "lookup_record_index_time";
  // Number of keys looked up in a call
  public static final String LOOKUP_RECORD_INDEX_KEYS_COUNT_STR = "lookup_record_index_key_count";
  // Number of keys found in record index
  public static final String LOOKUP_RECORD_INDEX_KEYS_HITS_COUNT_STR = "lookup_record_index_key_hit_count";
  public static final String SCAN_STR = "scan";
  public static final String BASEFILE_READ_STR = "basefile_read";
  public static final String INITIALIZE_STR = "initialize";
  public static final String REBOOTSTRAP_STR = "rebootstrap_count";
  public static final String BOOTSTRAP_ERR_STR = "bootstrap_error";

  // Stats names
  public static final String STAT_TOTAL_BASE_FILE_SIZE = "totalBaseFileSizeInBytes";
  public static final String STAT_TOTAL_LOG_FILE_SIZE = "totalLogFileSizeInBytes";
  public static final String STAT_COUNT_BASE_FILES = "baseFileCount";
  public static final String STAT_COUNT_LOG_FILES = "logFileCount";
  public static final String STAT_COUNT_PARTITION = "partitionCount";
  public static final String STAT_LAST_COMPACTION_TIMESTAMP = "lastCompactionTimestamp";
  public static final String SKIP_TABLE_SERVICES = "skip_table_services";
  public static final String TABLE_SERVICE_EXECUTION_STATUS = "table_service_execution_status";
  public static final String TABLE_SERVICE_EXECUTION_DURATION = "table_service_execution_duration";
  public static final String ASYNC_INDEXER_CATCHUP_TIME = "async_indexer_catchup_time";

  private final transient MetricRegistry metricsRegistry;
  private final transient Metrics metrics;

  public HoodieMetadataMetrics(HoodieMetricsConfig metricsConfig, HoodieStorage storage) {
    this.metrics = Metrics.getInstance(metricsConfig, storage);
    this.metricsRegistry = metrics.getRegistry();
  }

  public Map<String, String> getStats(boolean detailed, HoodieTableMetaClient metaClient, HoodieTableMetadata metadata, Set<String> metadataPartitions) {
    try {
      HoodieTableFileSystemView fileSystemView =
          HoodieTableFileSystemView.fileListingBasedFileSystemView(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient, metaClient.getActiveTimeline());
      return getStats(fileSystemView, detailed, metadata, metadataPartitions);
    } catch (IOException ioe) {
      throw new HoodieIOException("Unable to get metadata stats.", ioe);
    }
  }

  private Map<String, String> getStats(HoodieTableFileSystemView fsView, boolean detailed, HoodieTableMetadata tableMetadata, Set<String> metadataPartitions)
      throws IOException {
    Map<String, String> stats = new HashMap<>();

    // Total size of the metadata and count of base/log files for enabled partitions
    for (String metadataPartition : metadataPartitions) {
      List<FileSlice> latestSlices = fsView.getLatestFileSlices(metadataPartition).collect(Collectors.toList());

      // Total size of the metadata and count of base/log files
      long totalBaseFileSizeInBytes = 0;
      long totalLogFileSizeInBytes = 0;
      int baseFileCount = 0;
      int logFileCount = 0;

      for (FileSlice slice : latestSlices) {
        if (slice.getBaseFile().isPresent()) {
          totalBaseFileSizeInBytes += slice.getBaseFile().get().getPathInfo().getLength();
          ++baseFileCount;
        }
        Iterator<HoodieLogFile> it = slice.getLogFiles().iterator();
        while (it.hasNext()) {
          totalLogFileSizeInBytes += it.next().getFileSize();
          ++logFileCount;
        }
      }

      stats.put(metadataPartition + "." + STAT_TOTAL_BASE_FILE_SIZE, String.valueOf(totalBaseFileSizeInBytes));
      stats.put(metadataPartition + "." + STAT_TOTAL_LOG_FILE_SIZE, String.valueOf(totalLogFileSizeInBytes));
      stats.put(metadataPartition + "." + STAT_COUNT_BASE_FILES, String.valueOf(baseFileCount));
      stats.put(metadataPartition + "." + STAT_COUNT_LOG_FILES, String.valueOf(logFileCount));
    }

    if (detailed) {
      stats.put(HoodieMetadataMetrics.STAT_COUNT_PARTITION, String.valueOf(tableMetadata.getAllPartitionPaths().size()));
    }

    return stats;
  }

  public void updateMetrics(String action, long durationInMs) {
    if (metricsRegistry == null) {
      return;
    }

    // Update sum of duration and total for count
    String countKey = action + ".count";
    String durationKey = action + ".totalDuration";
    incrementMetric(countKey, 1);
    setMetric(durationKey, durationInMs);
  }

  public void updateSizeMetrics(HoodieTableMetaClient metaClient, HoodieBackedTableMetadata metadata, Set<String> metadataPartitions) {
    Map<String, String> stats = getStats(false, metaClient, metadata, metadataPartitions);
    for (Map.Entry<String, String> e : stats.entrySet()) {
      setMetric(e.getKey(), Long.parseLong(e.getValue()));
    }
  }

  protected void incrementMetric(String action, long value) {
    log.debug("Updating metadata metrics ({}={}) in {}", action, value, metricsRegistry);
    Option<HoodieGauge<Long>> gaugeOpt = metrics.registerGauge(action);
    gaugeOpt.ifPresent(gauge -> gauge.setValue(gauge.getValue() + value));
  }

  protected void setMetric(String action, long value) {
    metrics.registerGauge(action, value);
  }

  public MetricRegistry registry() {
    return metricsRegistry;
  }
}
