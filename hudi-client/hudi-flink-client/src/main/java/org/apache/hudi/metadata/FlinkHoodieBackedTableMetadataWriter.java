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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlinkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(FlinkHoodieBackedTableMetadataWriter.class);

  public static HoodieTableMetadataWriter create(Configuration conf, HoodieWriteConfig writeConfig, HoodieEngineContext context) {
    return new FlinkHoodieBackedTableMetadataWriter(conf, writeConfig, context);
  }

  FlinkHoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    super(hadoopConf, writeConfig, engineContext);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      // should support executor metrics
      Registry registry = Registry.getRegistry("HoodieMetadata");
      this.metrics = Option.of(new HoodieMetadataMetrics(registry));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void initialize(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient) {
    try {
      if (enabled) {
        bootstrapIfNeeded(engineContext, datasetMetaClient);
      }
    } catch (IOException e) {
      LOG.error("Failed to initialize metadata table. Disabling the writer.", e);
      enabled = false;
    }
  }

  @Override
  protected void commit(List<HoodieRecord> records, String partitionName, String instantTime) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");
    List<HoodieRecord> recordRDD = prepRecords(records, partitionName);

    try (HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(engineContext, metadataWriteConfig, true)) {
      writeClient.startCommitWithTime(instantTime);
      this.metaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);

      List<WriteStatus> statuses = writeClient.upsertPreppedRecords(recordRDD, instantTime);
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });
      writeClient.commit(instantTime, statuses, Option.empty(), HoodieActiveTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap());
      // trigger cleaning, compaction, with suffixes based on the same instant time. This ensures that any future
      // delta commits synced over will not have an instant time lesser than the last completed instant on the
      // metadata table.
      if (writeClient.scheduleCompactionAtInstant(instantTime + "001", Option.empty())) {
        writeClient.compact(instantTime + "001");
      }
      writeClient.clean(instantTime + "002");
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> {
      try {
        Map<String, String> stats = m.getStats(false, metaClient, metadata);
        m.updateMetrics(Long.parseLong(stats.get(HoodieMetadataMetrics.STAT_TOTAL_BASE_FILE_SIZE)),
            Long.parseLong(stats.get(HoodieMetadataMetrics.STAT_TOTAL_LOG_FILE_SIZE)),
            Integer.parseInt(stats.get(HoodieMetadataMetrics.STAT_COUNT_BASE_FILES)),
            Integer.parseInt(stats.get(HoodieMetadataMetrics.STAT_COUNT_LOG_FILES)));
      } catch (HoodieIOException e) {
        LOG.error("Could not publish metadata size metrics", e);
      }
    });
  }

  /**
   * Tag each record with the location.
   * <p>
   * Since we only read the latest base file in a partition, we tag the records with the instant time of the latest
   * base file.
   */
  private List<HoodieRecord> prepRecords(List<HoodieRecord> records, String partitionName) {
    HoodieTable table = HoodieFlinkTable.create(metadataWriteConfig, (HoodieFlinkEngineContext) engineContext);
    TableFileSystemView.SliceView fsView = table.getSliceView();
    List<HoodieBaseFile> baseFiles = fsView.getLatestFileSlices(partitionName)
        .map(FileSlice::getBaseFile)
        .filter(Option::isPresent)
        .map(Option::get)
        .collect(Collectors.toList());

    // All the metadata fits within a single base file
    if (partitionName.equals(MetadataPartitionType.FILES.partitionPath())) {
      if (baseFiles.size() > 1) {
        throw new HoodieMetadataException("Multiple base files found in metadata partition");
      }
    }

    String fileId;
    String instantTime;
    if (!baseFiles.isEmpty()) {
      fileId = baseFiles.get(0).getFileId();
      instantTime = "U";
    } else {
      // If there is a log file then we can assume that it has the data
      List<HoodieLogFile> logFiles = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath())
          .map(FileSlice::getLatestLogFile)
          .filter(Option::isPresent)
          .map(Option::get)
          .collect(Collectors.toList());
      if (logFiles.isEmpty()) {
        // No base and log files. All are new inserts
        fileId = FSUtils.createNewFileIdPfx();
        instantTime = "I";
      } else {
        fileId = logFiles.get(0).getFileId();
        instantTime = "U";
      }
    }

    return records.stream().map(r -> r.setCurrentLocation(new HoodieRecordLocation(instantTime, fileId))).collect(Collectors.toList());
  }
}
