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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBackedTableMetadataWriter.class);

  public static HoodieTableMetadataWriter create(Configuration conf, HoodieWriteConfig writeConfig, HoodieEngineContext context) {
    return new SparkHoodieBackedTableMetadataWriter(conf, writeConfig, context);
  }

  SparkHoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    super(hadoopConf, writeConfig, engineContext);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      Registry registry;
      if (metadataWriteConfig.isExecutorMetricsEnabled()) {
        registry = Registry.getRegistry("HoodieMetadata", DistributedRegistry.class.getName());
      } else {
        registry = Registry.getRegistry("HoodieMetadata");
      }
      this.metrics = Option.of(new HoodieMetadataMetrics(registry));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void initialize(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient) {
    try {
      metrics.map(HoodieMetadataMetrics::registry).ifPresent(registry -> {
        if (registry instanceof DistributedRegistry) {
          HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
          ((DistributedRegistry) registry).register(sparkEngineContext.getJavaSparkContext());
        }
      });

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
    JavaRDD<HoodieRecord> recordRDD = prepFileListingRecords(records);
    commit(recordRDD, instantTime);
  }

  @Override
  protected void commit(List<HoodieRecord> fileListRecords, List<HoodieRecord> rangeIndexRecords, String instantTime) {
    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    JavaRDD<HoodieRecord> fileRecordsRDD = prepFileListingRecords(fileListRecords);
    JavaRDD<HoodieRecord> rangeRecordsRDD = prepRangeIndexRecords(rangeIndexRecords, instantTime, "column");

    commit(jsc.union(fileRecordsRDD, rangeRecordsRDD), instantTime);
  }
  
  private void commit(JavaRDD<HoodieRecord> recordRDD, String instantTime) {
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, metadataWriteConfig, true)) {
      writeClient.startCommitWithTime(instantTime);
      List<WriteStatus> statuses = writeClient.upsertPreppedRecords(recordRDD, instantTime).collect();
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });
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
   * Return the timestamp of the latest instant synced.
   *
   * To sync a instant on dataset, we create a corresponding delta-commit on the metadata table. So return the latest
   * delta-commit.
   */
  @Override
  public Option<String> getLatestSyncedInstantTime() {
    if (!enabled) {
      return Option.empty();
    }

    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    return timeline.getDeltaCommitTimeline().filterCompletedInstants()
        .lastInstant().map(HoodieInstant::getTimestamp);
  }

  /**
   * Tag each record with the location.
   *
   * Since we only read the latest base file in a partition, we tag the records with the instant time of the latest
   * base file.
   */
  private JavaRDD<HoodieRecord> prepFileListingRecords(List<HoodieRecord> records) {
    HoodieTable table = HoodieSparkTable.create(metadataWriteConfig, engineContext);
    TableFileSystemView.SliceView fsView = table.getSliceView();
    List<HoodieBaseFile> baseFiles = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath())
        .map(FileSlice::getBaseFile)
        .filter(Option::isPresent)
        .map(Option::get)
        .collect(Collectors.toList());

    // All the metadata fits within a single base file
    if (baseFiles.size() > 1) {
      throw new HoodieMetadataException("Multiple base files found in metadata partition");
    }

    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    String fileId;
    String instantTime;
    if (!baseFiles.isEmpty()) {
      fileId = baseFiles.get(0).getFileId();
      instantTime = baseFiles.get(0).getCommitTime();
    } else {
      // If there is a log file then we can assume that it has the data
      List<HoodieLogFile> logFiles = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath())
          .map(FileSlice::getLatestLogFile)
          .filter(Option::isPresent)
          .map(Option::get)
          .collect(Collectors.toList());
      if (logFiles.isEmpty()) {
        // No base and log files. All are new inserts
        return jsc.parallelize(records, 1);
      }

      fileId = logFiles.get(0).getFileId();
      instantTime = logFiles.get(0).getBaseCommitTime();
    }

    return jsc.parallelize(records, 1).map(r -> r.setCurrentLocation(new HoodieRecordLocation(instantTime, fileId)));
  }

  /**
   * Tag each record with the location.
   * we try to keep all relevant records in a single file in metadata table. The 'relevance' can be defined by one of the columns in 'HoodieKey'.
   * 
   * This is basically poor man's version of 'bucketing'. After we have bucketing in hudi, we can use that feature and replace this with bucketing.
   * 
   * For example, lets say main table schema has 10 columns. We want to store all meatadata of column1 in same file  f1 on metadata table.
   * Similarly, metadata for column2 across all partitions in main table in f2 on metadata table and so on.
   * 
   * In that scenario, we can use 'column' as keyAttribute.
   * 
   * If we know a query is interested only in column1, column2, then on metadata table, we only need to read files f1 and f2. 
   */
  private JavaRDD<HoodieRecord> prepRangeIndexRecords(List<HoodieRecord> records, String instantTime, String keyAttribute) {
    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    return jsc.parallelize(records).map(r -> {
      // we want to create deterministic fileId for given key. So generate fileId based on key (key is typically either column_name or partitionPath of main dataset)
      String fileIdPfx =  FSUtils.createFileIdPfxFromKey(HoodieMetadataPayload.getAttributeFromRecordKey(r.getRecordKey(), keyAttribute));
      r.setCurrentLocation(new HoodieRecordLocation(instantTime, fileIdPfx + "-" + 0));
      return r;
    });
  }
}
