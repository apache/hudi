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
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metrics.DistributedRegistry;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
  protected void commit(List<HoodieRecord> records, String partitionName, String instantTime) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");
    JavaRDD<HoodieRecord> recordRDD = prepRecords(records, partitionName, 1);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, metadataWriteConfig, true)) {
      writeClient.startCommitWithTime(instantTime);
      List<WriteStatus> statuses = writeClient.upsertPreppedRecords(recordRDD, instantTime).collect();
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });

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
   * We cannot perform compaction if there are previous inflight operations on the dataset. This is because
   * a compacted metadata base file at time Tx should represent all the actions on the dataset till time Tx.
   */
  private void compactIfNecessary(SparkRDDWriteClient writeClient, String instantTime) {
    List<HoodieInstant> pendingInstants = datasetMetaClient.reloadActiveTimeline().filterInflightsAndRequested().findInstantsBefore(instantTime)
        .getInstants().collect(Collectors.toList());
    if (!pendingInstants.isEmpty()) {
      LOG.info(String.format("Cannot compact metadata table as there are %d inflight instants before latest deltacommit %s: %s",
          pendingInstants.size(), instantTime, Arrays.toString(pendingInstants.toArray())));
      return;
    }

    // Trigger compaction with suffixes based on the same instant time. This ensures that any future
    // delta commits synced over will not have an instant time lesser than the last completed instant on the
    // metadata table.
    final String compactionInstantTime = instantTime + "001";
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

  /**
   * Tag each record with the location in the given partition.
   *
   * The record is sharded to a file based on its record key.
   */
  private JavaRDD<HoodieRecord> prepRecords(List<HoodieRecord> records, String partitionName, int shardCount) {
    List<FileSlice> shards = HoodieTableMetadataUtil.loadPartitionShards(metaClient, partitionName);
    ValidationUtils.checkArgument(shards.size() == shardCount, String.format("Invalid number of shards: found=%d, required=%d", shards.size(), shardCount));

    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    return jsc.parallelize(records, 1).map(r -> {
      FileSlice slice = shards.get(HoodieTableMetadataUtil.keyToShard(r.getRecordKey(), shardCount));
      r.setCurrentLocation(new HoodieRecordLocation(slice.getBaseInstantTime(), slice.getFileId()));
      return r;
    });
  }
}
