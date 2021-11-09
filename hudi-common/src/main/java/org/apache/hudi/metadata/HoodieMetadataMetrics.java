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

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieMetadataMetrics implements Serializable {

  // Metric names
  public static final String LOOKUP_PARTITIONS_STR = "lookup_partitions";
  public static final String LOOKUP_FILES_STR = "lookup_files";
  public static final String SCAN_STR = "scan";
  public static final String BASEFILE_READ_STR = "basefile_read";
  public static final String INITIALIZE_STR = "initialize";
  public static final String REBOOTSTRAP_STR = "rebootstrap";
  public static final String BOOTSTRAP_ERR_STR = "bootstrap_error";

  // Stats names
  public static final String STAT_TOTAL_BASE_FILE_SIZE = "totalBaseFileSizeInBytes";
  public static final String STAT_TOTAL_LOG_FILE_SIZE = "totalLogFileSizeInBytes";
  public static final String STAT_COUNT_BASE_FILES = "baseFileCount";
  public static final String STAT_COUNT_LOG_FILES = "logFileCount";
  public static final String STAT_COUNT_PARTITION = "partitionCount";
  public static final String STAT_LAST_COMPACTION_TIMESTAMP = "lastCompactionTimestamp";

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataMetrics.class);

  private final Registry metricsRegistry;

  public HoodieMetadataMetrics(Registry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
  }

  public Map<String, String> getStats(boolean detailed, HoodieTableMetaClient metaClient, HoodieTableMetadata metadata) {
    try {
      metaClient.reloadActiveTimeline();
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
      return getStats(fsView, detailed, metadata);
    } catch (IOException ioe) {
      throw new HoodieIOException("Unable to get metadata stats.", ioe);
    }
  }

  private Map<String, String> getStats(HoodieTableFileSystemView fsView, boolean detailed, HoodieTableMetadata tableMetadata) throws IOException {
    Map<String, String> stats = new HashMap<>();

    // Total size of the metadata and count of base/log files
    for (String metadataPartition : MetadataPartitionType.all()) {
      List<FileSlice> latestSlices = fsView.getLatestFileSlices(metadataPartition).collect(Collectors.toList());

      // Total size of the metadata and count of base/log files
      long totalBaseFileSizeInBytes = 0;
      long totalLogFileSizeInBytes = 0;
      int baseFileCount = 0;
      int logFileCount = 0;

      for (FileSlice slice : latestSlices) {
        if (slice.getBaseFile().isPresent()) {
          totalBaseFileSizeInBytes += slice.getBaseFile().get().getFileStatus().getLen();
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

  protected void updateMetrics(String action, long durationInMs) {
    if (metricsRegistry == null) {
      return;
    }

    // Update sum of duration and total for count
    String countKey = action + ".count";
    String durationKey = action + ".totalDuration";
    incrementMetric(countKey, 1);
    incrementMetric(durationKey, durationInMs);
  }

  public void updateSizeMetrics(HoodieTableMetaClient metaClient, HoodieBackedTableMetadata metadata) {
    Map<String, String> stats = getStats(false, metaClient, metadata);
    for (Map.Entry<String, String> e : stats.entrySet()) {
      incrementMetric(e.getKey(), Long.parseLong(e.getValue()));
    }
  }

  protected void incrementMetric(String action, long value) {
    LOG.info(String.format("Updating metadata metrics (%s=%d) in %s", action, value, metricsRegistry));
    metricsRegistry.add(action, value);
  }

  public Registry registry() {
    return metricsRegistry;
  }
}
