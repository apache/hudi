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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBackedTableMetadataWriter.class);

  public static HoodieTableMetadataWriter create(Configuration conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return create(conf, writeConfig, context, Option.empty());
  }

  public static <T extends SpecificRecordBase> HoodieTableMetadataWriter create(Configuration conf,
                                                                                HoodieWriteConfig writeConfig,
                                                                                HoodieEngineContext context,
                                                                                Option<T> actionMetadata) {
    return new SparkHoodieBackedTableMetadataWriter(conf, writeConfig, context, actionMetadata);
  }

  <T extends SpecificRecordBase> SparkHoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                                                      HoodieWriteConfig writeConfig,
                                                                      HoodieEngineContext engineContext,
                                                                      Option<T> actionMetadata) {
    super(hadoopConf, writeConfig, engineContext, actionMetadata);
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
                                                           Option<T> actionMetadata) {
    try {
      metrics.map(HoodieMetadataMetrics::registry).ifPresent(registry -> {
        if (registry instanceof DistributedRegistry) {
          HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
          ((DistributedRegistry) registry).register(sparkEngineContext.getJavaSparkContext());
        }
      });

      if (enabled) {
        bootstrapIfNeeded(engineContext, dataMetaClient, actionMetadata);
      }
    } catch (IOException e) {
      LOG.error("Failed to initialize metadata table. Disabling the writer.", e);
      enabled = false;
    }
  }

  @Override
  protected void commit(List<HoodieRecord> records, String partitionName, String instantTime) {
    commit(instantTime, new HashMap<String, List<HoodieRecord>>() {
      {
        put(partitionName, records);
      }
    });
  }

  @Override
  protected void commit(String instantTime, Map<String, List<HoodieRecord>> partitionRecordsMap) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");
    JavaRDD<HoodieRecord> recordRDD = prepRecords(partitionRecordsMap);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, metadataWriteConfig, true)) {
      if (!metadataMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(instantTime)) {
        // if this is a new commit being applied to metadata for the first time
        writeClient.startCommitWithTime(instantTime);
      } else {
        // this code path refers to a re-attempted commit that got committed to metadata table, but failed in datatable.
        // for eg, lets say compaction c1 on 1st attempt succeeded in metadata table and failed before committing to
        // datatable.
        // when retried again, data table will first rollback pending compaction. these will be applied to metadata
        // table, but all changes
        // are upserts to metadata table and so only a new delta commit will be created.
        // once rollback is complete, compaction will be retried again, which will eventually hit this code block
        // where the respective commit is
        // already part of completed commit. So, we have to manually remove the completed instant and proceed.
        // and it is for the same reason we enabled withAllowMultiWriteOnSameInstant for metadata table.
        HoodieInstant alreadyCompletedInstant =
            metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.getTimestamp().equals(instantTime)).lastInstant().get();
        HoodieActiveTimeline.deleteInstantFile(metadataMetaClient.getFs(), metadataMetaClient.getMetaPath(),
            alreadyCompletedInstant);
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
      compactIfNecessary(writeClient, instantTime);
      doClean(writeClient, instantTime);
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metadataMetaClient, metadata));
  }

  /**
   * Tag each record with the location in the given partition.
   * <p>
   * The record is tagged with respective file slice's location based on its record key.
   */
  private JavaRDD<HoodieRecord> prepRecords(Map<String, List<HoodieRecord>> partitionRecordsMap) {

    // TODO: generalize this
    final int numFileGroups = 1;
    JavaRDD<HoodieRecord> rddAllPartitionRecords = null;

    for (Map.Entry<String, List<HoodieRecord>> entry : partitionRecordsMap.entrySet()) {
      final String partitionName = entry.getKey();
      final List<HoodieRecord> records = entry.getValue();

      List<FileSlice> fileSlices =
          HoodieTableMetadataUtil.loadPartitionFileGroupsWithLatestFileSlices(metadataMetaClient, partitionName);
      ValidationUtils.checkArgument(fileSlices.size() == numFileGroups,
          String.format("Invalid number of file groups: found=%d, required=%d", fileSlices.size(), numFileGroups));

      JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
      JavaRDD<HoodieRecord> rddSinglePartitionRecords = jsc.parallelize(records, 1).map(r -> {
        FileSlice slice = fileSlices.get(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(),
            numFileGroups));
        r.setCurrentLocation(new HoodieRecordLocation(slice.getBaseInstantTime(), slice.getFileId()));
        return r;
      });

      if (rddAllPartitionRecords == null) {
        rddAllPartitionRecords = rddSinglePartitionRecords;
      } else {
        rddAllPartitionRecords.union(rddSinglePartitionRecords);
      }
    }

    return rddAllPartitionRecords;
  }
}
