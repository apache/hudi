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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;
import org.apache.hudi.table.HoodieFlinkTable;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

/**
 * Utilities for flink hudi compaction.
 */
public class CompactionUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionUtil.class);

  /**
   * Schedules a new compaction instant.
   *
   * @param metaClient          The metadata client
   * @param writeClient         The write client
   * @param deltaTimeCompaction Whether the compaction is trigger by elapsed delta time
   * @param committed           Whether the last instant was committed successfully
   */
  public static void scheduleCompaction(
      HoodieTableMetaClient metaClient,
      HoodieFlinkWriteClient<?> writeClient,
      boolean deltaTimeCompaction,
      boolean committed) {
    if (committed) {
      writeClient.scheduleCompaction(Option.empty());
    } else if (deltaTimeCompaction) {
      // if there are no new commits and the compaction trigger strategy is based on elapsed delta time,
      // schedules the compaction anyway.
      metaClient.reloadActiveTimeline();
      Option<String> compactionInstantTime = CompactionUtil.getCompactionInstantTime(metaClient);
      if (compactionInstantTime.isPresent()) {
        writeClient.scheduleCompactionAtInstant(compactionInstantTime.get(), Option.empty());
      }
    }
  }

  /**
   * Gets compaction Instant time.
   */
  public static Option<String> getCompactionInstantTime(HoodieTableMetaClient metaClient) {
    Option<HoodieInstant> firstPendingInstant = metaClient.getCommitsTimeline()
        .filterPendingExcludingCompaction().firstInstant();
    Option<HoodieInstant> lastCompleteInstant = metaClient.getActiveTimeline().getWriteTimeline()
        .filterCompletedAndCompactionInstants().lastInstant();
    if (firstPendingInstant.isPresent() && lastCompleteInstant.isPresent()) {
      String firstPendingTimestamp = firstPendingInstant.get().getTimestamp();
      String lastCompleteTimestamp = lastCompleteInstant.get().getTimestamp();
      // Committed and pending compaction instants should have strictly lower timestamps
      return StreamerUtil.medianInstantTime(firstPendingTimestamp, lastCompleteTimestamp);
    } else if (!lastCompleteInstant.isPresent()) {
      LOG.info("No instants to schedule the compaction plan");
      return Option.empty();
    } else {
      return Option.of(HoodieActiveTimeline.createNewInstantTime());
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
    Schema tableAvroSchema = tableSchemaResolver.getTableAvroSchema(false);
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, tableAvroSchema.toString());
  }

  /**
   * Sets up the avro schema string into the HoodieWriteConfig {@code HoodieWriteConfig}
   * through reading from the hoodie table metadata.
   *
   * @param writeConfig The HoodieWriteConfig
   */
  public static void setAvroSchema(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    Schema tableAvroSchema = tableSchemaResolver.getTableAvroSchema(false);
    writeConfig.setSchema(tableAvroSchema.toString());
  }

  /**
   * Sets up the preCombine field into the given configuration {@code conf}
   * through reading from the hoodie table metadata.
   *
   * This value is non-null as compaction can only be performed on MOR tables.
   * Of which, MOR tables will have non-null precombine fields.
   *
   * @param conf The configuration
   */
  public static void setPreCombineField(Configuration conf, HoodieTableMetaClient metaClient) {
    String preCombineField = metaClient.getTableConfig().getPreCombineField();
    conf.setString(FlinkOptions.PRECOMBINE_FIELD, preCombineField);
  }

  /**
   * Infers the changelog mode based on the data file schema(including metadata fields).
   *
   * <p>We can improve the code if the changelog mode is set up as table config.
   *
   * @param conf The configuration
   */
  public static void inferChangelogMode(Configuration conf, HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    Schema tableAvroSchema = tableSchemaResolver.getTableAvroSchemaFromDataFile();
    if (tableAvroSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD) != null) {
      conf.setBoolean(FlinkOptions.CHANGELOG_ENABLED, true);
    }
  }

  /**
   * Cleans the metadata file for given instant {@code instant}.
   */
  public static void cleanInstant(HoodieTableMetaClient metaClient, HoodieInstant instant) {
    Path commitFilePath = new Path(metaClient.getMetaAuxiliaryPath(), instant.getFileName());
    try {
      if (metaClient.getFs().exists(commitFilePath)) {
        boolean deleted = metaClient.getFs().delete(commitFilePath, false);
        if (deleted) {
          LOG.info("Removed instant " + instant);
        } else {
          throw new HoodieIOException("Could not delete instant " + instant);
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not remove requested commit " + commitFilePath, e);
    }
  }

  public static void rollbackCompaction(HoodieFlinkTable<?> table, String instantTime) {
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(instantTime);
    if (table.getMetaClient().reloadActiveTimeline().filterPendingCompactionTimeline().containsInstant(inflightInstant)) {
      LOG.warn("Rollback failed compaction instant: [" + instantTime + "]");
      table.rollbackInflightCompaction(inflightInstant);
    }
  }

  /**
   * Force rolls back all the inflight compaction instants, especially for job failover restart.
   *
   * @param table The hoodie table
   */
  public static void rollbackCompaction(HoodieFlinkTable<?> table) {
    HoodieTimeline inflightCompactionTimeline = table.getActiveTimeline()
        .filterPendingCompactionTimeline()
        .filter(instant ->
            instant.getState() == HoodieInstant.State.INFLIGHT);
    inflightCompactionTimeline.getInstants().forEach(inflightInstant -> {
      LOG.info("Rollback the inflight compaction instant: " + inflightInstant + " for failover");
      table.rollbackInflightCompaction(inflightInstant);
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
      String currentTime = HoodieActiveTimeline.createNewInstantTime();
      int timeout = conf.getInteger(FlinkOptions.COMPACTION_TIMEOUT_SECONDS);
      if (StreamerUtil.instantTimeDiffSeconds(currentTime, instant.getTimestamp()) >= timeout) {
        LOG.info("Rollback the inflight compaction instant: " + instant + " for timeout(" + timeout + "s)");
        table.rollbackInflightCompaction(instant);
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
