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

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
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

import java.io.IOException;
import java.util.List;

public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBackedTableMetadataWriter.class);

  /**
   * Return a Spark based implementation of {@code HoodieTableMetadataWriter} which can be used to
   * write to the metadata table.
   *
   * If the metadata table does not exist, an attempt is made to bootstrap it but there is no guarantted that
   * table will end up bootstrapping at this time.
   *
   * @param conf
   * @param writeConfig
   * @param context
   * @param actionMetadata
   * @param inflightInstantTimestamp Timestamp of an instant which is in-progress. This instant is ignored while
   *                                 attempting to bootstrap the table.
   * @return An instance of the {@code HoodieTableMetadataWriter}
   */
  public static <T extends SpecificRecordBase> HoodieTableMetadataWriter create(Configuration conf,
                                                                                HoodieWriteConfig writeConfig,
                                                                                HoodieEngineContext context,
                                                                                Option<T> actionMetadata,
                                                                                Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(conf, writeConfig, context, actionMetadata,
                                                    inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return create(conf, writeConfig, context, Option.empty(), Option.empty());
  }

  <T extends SpecificRecordBase> SparkHoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                                                      HoodieWriteConfig writeConfig,
                                                                      HoodieEngineContext engineContext,
                                                                      Option<T> actionMetadata,
                                                                      Option<String> inflightInstantTimestamp) {
    super(hadoopConf, writeConfig, engineContext, actionMetadata, inflightInstantTimestamp);
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
  protected <T extends SpecificRecordBase> void initialize(HoodieEngineContext engineContext,
                                                           Option<T> actionMetadata,
                                                           Option<String> inflightInstantTimestamp) {
    try {
      metrics.map(HoodieMetadataMetrics::registry).ifPresent(registry -> {
        if (registry instanceof DistributedRegistry) {
          HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
          ((DistributedRegistry) registry).register(sparkEngineContext.getJavaSparkContext());
        }
      });

      if (enabled) {
        bootstrapIfNeeded(engineContext, dataMetaClient, actionMetadata, inflightInstantTimestamp);
      }
    } catch (IOException e) {
      LOG.error("Failed to initialize metadata table. Disabling the writer.", e);
      enabled = false;
    }
  }

  protected void commit(HoodieData<HoodieRecord> hoodieDataRecords, String partitionName, String instantTime, boolean canTriggerTableService) {
    ValidationUtils.checkState(metadataMetaClient != null, "Metadata table is not fully initialized yet.");
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");
    JavaRDD<HoodieRecord> records = (JavaRDD<HoodieRecord>) hoodieDataRecords.get();
    JavaRDD<HoodieRecord> recordRDD = prepRecords(records, partitionName, 1);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, metadataWriteConfig, true)) {
      if (!metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(instantTime)) {
        // if this is a new commit being applied to metadata for the first time
        writeClient.startCommitWithTime(instantTime);
      } else {
        // this code path refers to a re-attempted commit that got committed to metadata table, but failed in datatable.
        // for eg, lets say compaction c1 on 1st attempt succeeded in metadata table and failed before committing to datatable.
        // when retried again, data table will first rollback pending compaction. these will be applied to metadata table, but all changes
        // are upserts to metadata table and so only a new delta commit will be created.
        // once rollback is complete, compaction will be retried again, which will eventually hit this code block where the respective commit is
        // already part of completed commit. So, we have to manually remove the completed instant and proceed.
        // and it is for the same reason we enabled withAllowMultiWriteOnSameInstant for metadata table.
        HoodieInstant alreadyCompletedInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.getTimestamp().equals(instantTime)).lastInstant().get();
        HoodieActiveTimeline.deleteInstantFile(metadataMetaClient.getFs(), metadataMetaClient.getMetaPath(), alreadyCompletedInstant);
        metadataMetaClient.reloadActiveTimeline();
      }
      List<WriteStatus> statuses = writeClient.upsertPreppedRecords(recordRDD, instantTime).collect();
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });

      // reload timeline
      metadataMetaClient.reloadActiveTimeline();
      if (canTriggerTableService) {
        compactIfNecessary(writeClient, instantTime);
        doClean(writeClient, instantTime);
        writeClient.archive();
      }
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metadataMetaClient, metadata));
  }

  /**
   * Tag each record with the location in the given partition.
   *
   * The record is tagged with respective file slice's location based on its record key.
   */
  private JavaRDD<HoodieRecord> prepRecords(JavaRDD<HoodieRecord> recordsRDD, String partitionName, int numFileGroups) {
    List<FileSlice> fileSlices = HoodieTableMetadataUtil.loadPartitionFileGroupsWithLatestFileSlices(metadataMetaClient, partitionName, false);
    ValidationUtils.checkArgument(fileSlices.size() == numFileGroups, String.format("Invalid number of file groups: found=%d, required=%d", fileSlices.size(), numFileGroups));

    return recordsRDD.map(r -> {
      FileSlice slice = fileSlices.get(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(), numFileGroups));
      r.setCurrentLocation(new HoodieRecordLocation(slice.getBaseInstantTime(), slice.getFileId()));
      return r;
    });
  }
}
