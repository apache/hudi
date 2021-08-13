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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBackedTableMetadataWriter.class);
  private List<JavaRDD<HoodieRecord>> recordsQueuedForCommit;

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
  protected void initialize(HoodieEngineContext engineContext) {
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
  protected void commit(String instantTime, boolean isInsert) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");

    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    JavaRDD<HoodieRecord> recordRDD = recordsQueuedForCommit.isEmpty() ? jsc.emptyRDD() : recordsQueuedForCommit.get(0);
    for (int index = 1; index < recordsQueuedForCommit.size(); ++index) {
      recordRDD = recordRDD.union(recordsQueuedForCommit.get(index));
    }

    // Ensure each record is tagged
    recordRDD.foreach(r -> {
      if (r.getCurrentLocation() == null) {
        throw new HoodieMetadataException("Record is not tagged with a location");
      }
    });
    recordsQueuedForCommit.clear();

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, metadataWriteConfig, true)) {
      writeClient.startCommitWithTime(instantTime);
      List<WriteStatus> statuses;
      if (isInsert) {
        statuses = writeClient.insertPreppedRecords(recordRDD, instantTime).collect();
      } else {
        statuses = writeClient.upsertPreppedRecords(recordRDD, instantTime).collect();
      }
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });

      // reload timeline
      metaClient.reloadActiveTimeline();

      compactIfNecessary(writeClient, instantTime);
      cleanIfNecessary(writeClient, instantTime);
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metaClient, metadata));
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
   *  Perform a compaction on the Metadata Table.
   *
   * Cases to be handled:
   *   1. We cannot perform compaction if there are previous inflight operations on the dataset. This is because
   *      a compacted metadata base file at time Tx should represent all the actions on the dataset till time Tx.
   *
   *   2. In multi-writer scenario, a parallel operation with a greater instantTime may have completed creating a
   *      deltacommit.
   */
  private void compactIfNecessary(SparkRDDWriteClient writeClient, String instantTime) {
    String latestDeltacommitTime = metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant()
        .get().getTimestamp();
    List<HoodieInstant> pendingInstants = datasetMetaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .findInstantsBefore(latestDeltacommitTime).getInstants().collect(Collectors.toList());

    if (!pendingInstants.isEmpty()) {
      LOG.info(String.format("Cannot compact metadata table as there are %d inflight instants before latest deltacommit %s: %s",
          pendingInstants.size(), latestDeltacommitTime, Arrays.toString(pendingInstants.toArray())));
      return;
    }

    // Trigger compaction with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    final String compactionInstantTime = latestDeltacommitTime + "001";
    if (writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty())) {
      writeClient.compact(compactionInstantTime);
    }
  }

  private void cleanIfNecessary(SparkRDDWriteClient writeClient, String instantTime) {
    // Trigger cleaning with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    writeClient.clean(instantTime + "002");
  }

  @Override
  protected TableFileSystemView.BaseFileOnlyView getTableFileSystemView() {
    HoodieTable table = HoodieSparkTable.create(datasetWriteConfig, engineContext);
    return (TableFileSystemView.BaseFileOnlyView)table.getFileSystemView();
  }

  @Override
  protected Pair<JavaRDD<HoodieRecord>, Long> readRecordKeysFromBaseFiles(HoodieEngineContext engineContext,
      List<Pair<String, String>> partitionBaseFilePairs) {
    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(partitionBaseFilePairs, partitionBaseFilePairs.size()).flatMap(p -> {
      final String partition = p.getKey();
      final String filename = p.getValue();
      Path dataFilePath = new Path(datasetWriteConfig.getBasePath(), partition + Path.SEPARATOR + filename);

      final String fileId = FSUtils.getFileId(filename);
      final String instantTime = FSUtils.getCommitTime(filename);
      HoodieFileReader reader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), dataFilePath);
      Iterator<String> recordKeyIterator = reader.getRecordKeyIterator();

      return new Iterator<HoodieRecord>() {
        @Override
        public boolean hasNext() {
          return recordKeyIterator.hasNext();
        }

        @Override
        public HoodieRecord next() {
          return HoodieMetadataPayload.createRecordLevelIndexRecord(recordKeyIterator.next(), partition, fileId, instantTime);
        }
      };
    });

    if (recordRDD.getStorageLevel() == StorageLevel.NONE()) {
      recordRDD.persist(StorageLevel.MEMORY_AND_DISK());
    }

    return Pair.of(recordRDD, recordRDD.count());
  }

  @Override
  protected void queueForUpdate(JavaRDD<HoodieRecord> records, MetadataPartitionType partitionType, String instantTime) {
    List<FileSlice> shards = HoodieTableMetadataUtil.loadPartitionShards(metaClient, partitionType.partitionPath());

    JavaRDD<HoodieRecord> taggedRecordRDD = records.map(r -> {
      int shardIndex = Math.abs(HoodieTableMetadataUtil.keyToShard(r.getRecordKey(), shards.size()));
      HoodieRecordLocation loc = new HoodieRecordLocation(shards.get(shardIndex).getBaseInstantTime(), shards.get(shardIndex).getFileId());
      HoodieRecord taggedRecord = HoodieIndexUtils.getTaggedRecord(r, Option.of(loc));
      if (taggedRecord.getCurrentLocation() == null) {
        throw new HoodieMetadataException("Tagged record does not have a location set");
      }

      return taggedRecord;
    });

    if (recordsQueuedForCommit == null) {
      recordsQueuedForCommit = new ArrayList<>();
    }

    recordsQueuedForCommit.add(taggedRecordRDD);
  }

  @Override
  protected void queueForUpdate(List<HoodieRecord> records, MetadataPartitionType partitionType, String instantTime) {
    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    queueForUpdate(jsc.parallelize(records, 1), partitionType, instantTime);
  }
}
