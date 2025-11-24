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

package org.apache.hudi.util;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;
import org.apache.hudi.table.HoodieFlinkTable;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Utilities for flink hudi compaction.
 */
public class CompactionUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionUtil.class);

  /**
   * Schedules a new compaction instant.
   *
   * @param writeClient         The write client
   * @param deltaTimeCompaction Whether the compaction is trigger by elapsed delta time
   * @param committed           Whether the last instant was committed successfully
   */
  public static void scheduleCompaction(
      HoodieFlinkWriteClient<?> writeClient,
      boolean deltaTimeCompaction,
      boolean committed) {
    if (committed) {
      writeClient.scheduleCompaction(Option.empty());
    } else if (deltaTimeCompaction) {
      // if there are no new commits and the compaction trigger strategy is based on elapsed delta time,
      // schedules the compaction anyway.
      writeClient.scheduleCompaction(Option.empty());
    }
  }

  /**
   * Sets up the avro schema string into the give configuration {@code conf}
   * through reading from the hoodie table metadata.
   *
   * @param conf The configuration
   */
  public static void setAvroSchema(Configuration conf, HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    HoodieSchema tableAvroSchema = HoodieSchema.fromAvroSchema(tableSchemaResolver.getTableAvroSchema(false));
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, tableAvroSchema.toString());
  }

  /**
   * Sets up the avro schema string into the HoodieWriteConfig {@code HoodieWriteConfig}
   * through reading from the hoodie table metadata.
   *
   * @param writeConfig The HoodieWriteConfig
   */
  public static void setAvroSchema(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    HoodieSchema tableAvroSchema = HoodieSchema.fromAvroSchema(tableSchemaResolver.getTableAvroSchema(false));
    writeConfig.setSchema(tableAvroSchema.toString());
  }

  /**
   * Sets up the preCombine field into the given configuration {@code conf}
   * through reading from the hoodie table metadata.
   * <p>
   * This value is non-null as compaction can only be performed on MOR tables.
   * Of which, MOR tables will have non-null precombine fields.
   *
   * @param conf The configuration
   */
  public static void setOrderingFields(Configuration conf, HoodieTableMetaClient metaClient) {
    String orderingFields = metaClient.getTableConfig().getOrderingFieldsStr().orElse(null);
    if (orderingFields != null) {
      conf.set(FlinkOptions.ORDERING_FIELDS, orderingFields);
    }
  }

  /**
   * Sets up the partition field into the given configuration {@code conf}
   * through reading from the hoodie table metadata.
   *
   * @param conf The configuration
   * @param metaClient The meta client
   */
  public static void setPartitionField(Configuration conf, HoodieTableMetaClient metaClient) {
    Option<String[]> partitionKeys = metaClient.getTableConfig().getPartitionFields();
    if (partitionKeys.isPresent()) {
      conf.set(FlinkOptions.PARTITION_PATH_FIELD, String.join(",", partitionKeys.get()));
    }
  }

  /**
   * Infers the changelog mode based on the data file schema(including metadata fields).
   *
   * <p>We can improve the code if the changelog mode is set up as table config.
   *
   * @param conf The configuration
   * @param metaClient The meta client
   */
  public static void inferChangelogMode(Configuration conf, HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    HoodieSchema tableAvroSchema = HoodieSchema.fromAvroSchema(tableSchemaResolver.getTableAvroSchemaFromDataFile());
    if (tableAvroSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD).isPresent()) {
      conf.set(FlinkOptions.CHANGELOG_ENABLED, true);
    }
  }

  /**
   * Infers the metadata config based on the existence of metadata folder.
   *
   * <p>We can improve the code if the metadata config is set up as table config.
   *
   * @param conf The configuration
   * @param metaClient The meta client
   */
  public static void inferMetadataConf(Configuration conf, HoodieTableMetaClient metaClient) {
    String path = HoodieTableMetadata.getMetadataTableBasePath(conf.get(FlinkOptions.PATH));
    if (!StreamerUtil.tableExists(path, (org.apache.hadoop.conf.Configuration) metaClient.getStorageConf().unwrap())) {
      conf.set(FlinkOptions.METADATA_ENABLED, false);
    }
  }

  public static void rollbackCompaction(HoodieFlinkTable<?> table, String instantTime, TransactionManager transactionManager) {
    HoodieInstant inflightInstant = table.getInstantGenerator().getCompactionInflightInstant(instantTime);
    if (table.getMetaClient().reloadActiveTimeline().filterPendingCompactionTimeline().containsInstant(inflightInstant)) {
      LOG.warn("Failed to rollback compaction instant: [{}]", instantTime);
      table.rollbackInflightCompaction(inflightInstant, transactionManager);
    }
  }

  /**
   * Force rolls back all the inflight compaction instants, especially for job failover restart.
   *
   * @param table The hoodie table
   */
  public static void rollbackCompaction(HoodieFlinkTable<?> table, HoodieFlinkWriteClient writeClient) {
    HoodieTimeline inflightCompactionTimeline = table.getActiveTimeline()
        .filterPendingCompactionTimeline()
        .filter(instant ->
            instant.getState() == HoodieInstant.State.INFLIGHT);
    inflightCompactionTimeline.getInstants().forEach(inflightInstant -> {
      LOG.info("Rollback the inflight compaction instant: " + inflightInstant + " for failover");
      table.rollbackInflightCompaction(inflightInstant, commitToRollback -> writeClient.getTableServiceClient().getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false),
          writeClient.getTransactionManager());
      table.getMetaClient().reloadActiveTimeline();
    });
  }

  /**
   * Rolls back the earliest compaction if there exists.
   *
   * <p>Makes the strategy not that radical: firstly check whether there exists inflight compaction instants,
   * rolls back the first inflight instant only if it has timed out. That means, if there are
   * multiple timed out instants on the timeline, we only roll back the first one at a time.
   */
  public static void rollbackEarliestCompaction(HoodieFlinkTable<?> table, Configuration conf) {
    Option<HoodieInstant> earliestInflight = table.getActiveTimeline()
        .filterPendingCompactionTimeline()
        .filter(instant ->
            instant.getState() == HoodieInstant.State.INFLIGHT).firstInstant();
    if (earliestInflight.isPresent()) {
      HoodieInstant instant = earliestInflight.get();
      String currentTime = HoodieInstantTimeGenerator.getCurrentInstantTimeStr();
      int timeout = conf.get(FlinkOptions.COMPACTION_TIMEOUT_SECONDS);
      if (StreamerUtil.instantTimeDiffSeconds(currentTime, instant.requestedTime()) >= timeout) {
        LOG.info("Rollback the inflight compaction instant: " + instant + " for timeout(" + timeout + "s)");
        try (TransactionManager transactionManager = new TransactionManager(table.getConfig(), table.getStorage())) {
          table.rollbackInflightCompaction(instant, transactionManager);
        }
        table.getMetaClient().reloadActiveTimeline();
      }
    }
  }

  /**
   * Returns whether the execution sequence is LIFO.
   */
  public static boolean isLIFO(String seq) {
    return seq.toUpperCase(Locale.ROOT).equals(FlinkCompactionConfig.SEQ_LIFO);
  }
}
