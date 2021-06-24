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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.table.HoodieFlinkTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utilities for flink hudi compaction.
 */
public class CompactionUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionUtil.class);

  /**
   * Creates the metaClient.
   */
  public static HoodieTableMetaClient createMetaClient(Configuration conf) {
    return HoodieTableMetaClient.builder().setBasePath(conf.getString(FlinkOptions.PATH)).setConf(FlinkClientUtil.getHadoopConf()).build();
  }

  /**
   * Gets compaction Instant time.
   */
  public static String getCompactionInstantTime(HoodieTableMetaClient metaClient) {
    Option<HoodieInstant> firstPendingInstant = metaClient.getCommitsTimeline()
        .filterPendingExcludingCompaction().firstInstant();
    Option<HoodieInstant> lastCompleteInstant = metaClient.getActiveTimeline().getWriteTimeline()
        .filterCompletedAndCompactionInstants().lastInstant();
    if (firstPendingInstant.isPresent() && lastCompleteInstant.isPresent()) {
      String firstPendingTimestamp = firstPendingInstant.get().getTimestamp();
      String lastCompleteTimestamp = lastCompleteInstant.get().getTimestamp();
      // Committed and pending compaction instants should have strictly lower timestamps
      return StreamerUtil.medianInstantTime(firstPendingTimestamp, lastCompleteTimestamp);
    } else {
      return HoodieActiveTimeline.createNewInstantTime();
    }
  }

  /**
   * Sets up the avro schema string into the give configuration {@code conf}
   * through reading from the hoodie table metadata.
   *
   * @param conf    The configuration
   */
  public static void setAvroSchema(Configuration conf, HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    Schema tableAvroSchema = tableSchemaResolver.getTableAvroSchema(false);
    conf.setString(FlinkOptions.READ_AVRO_SCHEMA, tableAvroSchema.toString());
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

  public static void rollbackCompaction(HoodieFlinkTable<?> table, Configuration conf) {
    String curInstantTime = HoodieActiveTimeline.createNewInstantTime();
    int deltaSeconds = conf.getInteger(FlinkOptions.COMPACTION_DELTA_SECONDS);
    HoodieTimeline inflightCompactionTimeline = table.getActiveTimeline()
        .filterPendingCompactionTimeline()
        .filter(instant ->
            instant.getState() == HoodieInstant.State.INFLIGHT
                && StreamerUtil.instantTimeDiff(curInstantTime, instant.getTimestamp()) >= deltaSeconds);
    inflightCompactionTimeline.getInstants().forEach(inflightInstant -> {
      LOG.info("Rollback the pending compaction instant: " + inflightInstant);
      table.rollback(table.getContext(), HoodieActiveTimeline.createNewInstantTime(), inflightInstant, true);
      table.getMetaClient().reloadActiveTimeline();
    });
  }
}
