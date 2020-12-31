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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 *
 * If the metadata table does not exist, RPC calls are used to retrieve file listings from the file system.
 * No updates are applied to the table and it is not synced.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadata.class);

  private final String metadataBasePath;
  private HoodieTableMetaClient metaClient;

  // Readers for the base and log file which store the metadata
  private transient HoodieFileReader<GenericRecord> baseFileReader;
  private transient HoodieMetadataMergedLogRecordScanner logRecordScanner;

  public HoodieBackedTableMetadata(Configuration conf, String datasetBasePath, String spillableMapDirectory,
                                   boolean enabled, boolean validateLookups, boolean assumeDatePartitioning) {
    this(conf, datasetBasePath, spillableMapDirectory, enabled, validateLookups, false, assumeDatePartitioning);
  }

  public HoodieBackedTableMetadata(Configuration conf, String datasetBasePath, String spillableMapDirectory,
                                   boolean enabled, boolean validateLookups, boolean enableMetrics,
                                   boolean assumeDatePartitioning) {
    super(conf, datasetBasePath, spillableMapDirectory, enabled, validateLookups, enableMetrics, assumeDatePartitioning);
    this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(datasetBasePath);
    if (enabled) {
      try {
        this.metaClient = new HoodieTableMetaClient(hadoopConf.get(), metadataBasePath);
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path " + metadataBasePath);
        this.enabled = false;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.enabled = false;
      }
    } else {
      LOG.info("Metadata table is disabled.");
    }
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key) throws IOException {
    openBaseAndLogFiles();

    // Retrieve record from base file
    HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
    if (baseFileReader != null) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      Option<GenericRecord> baseRecord = baseFileReader.getRecordByKey(key);
      if (baseRecord.isPresent()) {
        hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
            metaClient.getTableConfig().getPayloadClass());
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, timer.endTimer()));
      }
    }

    // Retrieve record from log file
    Option<HoodieRecord<HoodieMetadataPayload>> logHoodieRecord = logRecordScanner.getRecordByKey(key);
    if (logHoodieRecord.isPresent()) {
      if (hoodieRecord != null) {
        // Merge the payloads
        HoodieRecordPayload mergedPayload = logHoodieRecord.get().getData().preCombine(hoodieRecord.getData());
        hoodieRecord = new HoodieRecord(hoodieRecord.getKey(), mergedPayload);
      } else {
        hoodieRecord = logHoodieRecord.get();
      }
    }

    return Option.ofNullable(hoodieRecord);
  }

  /**
   * Open readers to the base and log files.
   */
  protected synchronized void openBaseAndLogFiles() throws IOException {
    if (logRecordScanner != null) {
      // Already opened
      return;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();

    // Metadata is in sync till the latest completed instant on the dataset
    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
    String latestInstantTime = datasetMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
        .map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    // Find the latest file slice
    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    List<FileSlice> latestSlices = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath()).collect(Collectors.toList());
    ValidationUtils.checkArgument(latestSlices.size() == 1);

    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = latestSlices.get(0).getBaseFile();
    if (basefile.isPresent()) {
      String basefilePath = basefile.get().getPath();
      baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(basefilePath));
      LOG.info("Opened metadata base file from " + basefilePath + " at instant " + basefile.get().getCommitTime());
    }

    // Open the log record scanner using the log files from the latest file slice
    List<String> logFilePaths = latestSlices.get(0).getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
        .map(o -> o.getPath().toString())
        .collect(Collectors.toList());

    Option<HoodieInstant> lastInstant = timeline.filterCompletedInstants().lastInstant();
    String latestMetaInstantTimestamp = lastInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());

    logRecordScanner =
        new HoodieMetadataMergedLogRecordScanner(metaClient.getFs(), metadataBasePath,
            logFilePaths, schema, latestMetaInstantTimestamp, MAX_MEMORY_SIZE_IN_BYTES, BUFFER_SIZE,
            spillableMapDirectory, null);

    LOG.info("Opened metadata log files from " + logFilePaths + " at instant " + latestInstantTime
        + "(dataset instant=" + latestInstantTime + ", metadata instant=" + latestMetaInstantTimestamp + ")");

    metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR, timer.endTimer()));
  }

  public void closeReaders() {
    if (baseFileReader != null) {
      baseFileReader.close();
      baseFileReader = null;
    }
    logRecordScanner = null;
  }

  /**
   * Return an ordered list of instants which have not been synced to the Metadata Table.
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  protected List<HoodieInstant> findInstantsToSync(HoodieTableMetaClient datasetMetaClient) {
    HoodieActiveTimeline metaTimeline = metaClient.reloadActiveTimeline();

    // All instants on the data timeline, which are greater than the last instant on metadata timeline
    // are candidates for sync.
    Option<HoodieInstant> latestMetadataInstant = metaTimeline.filterCompletedInstants().lastInstant();
    ValidationUtils.checkArgument(latestMetadataInstant.isPresent(),
        "At least one completed instant should exist on the metadata table, before syncing.");
    String latestMetadataInstantTime = latestMetadataInstant.get().getTimestamp();
    HoodieDefaultTimeline candidateTimeline = datasetMetaClient.getActiveTimeline().findInstantsAfter(latestMetadataInstantTime, Integer.MAX_VALUE);
    Option<HoodieInstant> earliestIncompleteInstant = candidateTimeline.filterInflightsAndRequested().firstInstant();

    if (earliestIncompleteInstant.isPresent()) {
      return candidateTimeline.filterCompletedInstants()
          .findInstantsBefore(earliestIncompleteInstant.get().getTimestamp())
          .getInstants().collect(Collectors.toList());
    } else {
      return candidateTimeline.filterCompletedInstants()
          .getInstants().collect(Collectors.toList());
    }
  }

  /**
   * Return the timestamp of the latest compaction instant.
   */
  @Override
  public Option<String> getSyncedInstantTime() {
    if (!enabled) {
      return Option.empty();
    }

    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    return timeline.getDeltaCommitTimeline().filterCompletedInstants()
        .lastInstant().map(HoodieInstant::getTimestamp);
  }

  public boolean enabled() {
    return enabled;
  }

  public SerializableConfiguration getHadoopConf() {
    return hadoopConf;
  }

  public String getDatasetBasePath() {
    return datasetBasePath;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Map<String, String> stats() {
    return metrics.map(m -> m.getStats(true, metaClient, this)).orElse(new HashMap<>());
  }
}
