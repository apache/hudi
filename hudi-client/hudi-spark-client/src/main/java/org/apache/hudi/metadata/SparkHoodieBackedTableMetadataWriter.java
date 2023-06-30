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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;

public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieBackedTableMetadataWriter.class);
  private transient BaseHoodieWriteClient writeClient;

  /**
   * Return a Spark based implementation of {@code HoodieTableMetadataWriter} which can be used to
   * write to the metadata table.
   * <p>
   * If the metadata table does not exist, an attempt is made to bootstrap it but there is no guaranteed that
   * table will end up bootstrapping at this time.
   *
   * @param conf
   * @param writeConfig
   * @param context
   * @param inflightInstantTimestamp Timestamp of an instant which is in-progress. This instant is ignored while
   *                                 attempting to bootstrap the table.
   * @return An instance of the {@code HoodieTableMetadataWriter}
   */
  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return create(conf, writeConfig, context, Option.empty());
  }

  SparkHoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      Registry registry;
      if (metadataWriteConfig.isExecutorMetricsEnabled()) {
        registry = Registry.getRegistry("HoodieMetadata", DistributedRegistry.class.getName());
        HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
        ((DistributedRegistry) registry).register(sparkEngineContext.getJavaSparkContext());
      } else {
        registry = Registry.getRegistry("HoodieMetadata");
      }
      this.metrics = Option.of(new HoodieMetadataMetrics(registry));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap) {
    commitInternal(instantTime, partitionRecordsMap, Option.empty());
  }

  protected void bulkCommit(
      String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records,
      int fileGroupCount) {
    SparkHoodieMetadataBulkInsertPartitioner partitioner = new SparkHoodieMetadataBulkInsertPartitioner(fileGroupCount);
    commitInternal(instantTime, Collections.singletonMap(partitionType, records), Option.of(partitioner));
  }

  private void commitInternal(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap,
                              Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    ValidationUtils.checkState(metadataMetaClient != null, "Metadata table is not fully initialized yet.");
    HoodieData<HoodieRecord> preppedRecords = prepRecords(partitionRecordsMap);
    JavaRDD<HoodieRecord> preppedRecordRDD = HoodieJavaRDD.getJavaRDD(preppedRecords);

    try (SparkRDDWriteClient writeClient = (SparkRDDWriteClient) getWriteClient()) {
      // rollback partially failed writes if any.
      if (dataWriteConfig.getFailedWritesCleanPolicy().isEager()
          && writeClient.rollbackFailedWrites()) {
        metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient);
      }

      if (!metadataMetaClient.getActiveTimeline().getCommitsTimeline().containsInstant(instantTime)) {
        // if this is a new commit being applied to metadata for the first time
        LOG.info("New commit at " + instantTime + " being applied to MDT.");
      } else {
        // this code path refers to a re-attempted commit that:
        //   1. got committed to metadata table, but failed in datatable.
        //   2. failed while committing to metadata table
        // for e.g., let's say compaction c1 on 1st attempt succeeded in metadata table and failed before committing to datatable.
        // when retried again, data table will first rollback pending compaction. these will be applied to metadata table, but all changes
        // are upserts to metadata table and so only a new delta commit will be created.
        // once rollback is complete in datatable, compaction will be retried again, which will eventually hit this code block where the respective commit is
        // already part of completed commit. So, we have to manually rollback the completed instant and proceed.
        Option<HoodieInstant> alreadyCompletedInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.getTimestamp().equals(instantTime))
            .lastInstant();
        LOG.info(String.format("%s completed commit at %s being applied to MDT.",
            alreadyCompletedInstant.isPresent() ? "Already" : "Partially", instantTime));

        // Rollback the previous commit
        if (!writeClient.rollback(instantTime)) {
          throw new HoodieMetadataException("Failed to rollback deltacommit at " + instantTime + " from MDT");
        }
        metadataMetaClient.reloadActiveTimeline();
      }

      writeClient.startCommitWithTime(instantTime);
      if (bulkInsertPartitioner.isPresent()) {
        engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Bulk inserting at %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
        writeClient.bulkInsertPreppedRecords(preppedRecordRDD, instantTime, bulkInsertPartitioner).collect();
      } else {
        engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Upserting at %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
        writeClient.upsertPreppedRecords(preppedRecordRDD, instantTime).collect();
      }

      // reload timeline
      metadataMetaClient.reloadActiveTimeline();
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metadataMetaClient, metadata, dataMetaClient.getTableConfig().getMetadataPartitions()));
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    List<String> partitionsToDrop = partitions.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toList());
    LOG.info("Deleting Metadata Table partitions: " + partitionsToDrop);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, metadataWriteConfig, true)) {
      String actionType = CommitUtils.getCommitActionType(WriteOperationType.DELETE_PARTITION, HoodieTableType.MERGE_ON_READ);
      writeClient.startCommitWithTime(instantTime, actionType);
      writeClient.deletePartitions(partitionsToDrop, instantTime);
    }
    closeInternal();
  }

  @Override
  public BaseHoodieWriteClient getWriteClient() {
    if (writeClient == null) {
      writeClient = new SparkRDDWriteClient(engineContext, metadataWriteConfig, true);
    }
    return writeClient;
  }
}
