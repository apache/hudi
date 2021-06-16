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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for flink hudi compaction.
 */
public class CompactionUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionUtil.class);

  /**
   * Creates the metaClient.
   * */
  public static HoodieTableMetaClient createMetaClient(Configuration conf) {
    return HoodieTableMetaClient.builder().setBasePath(conf.getString(FlinkOptions.PATH)).setConf(FlinkClientUtil.getHadoopConf()).build();
  }

  /**
   * Gets compaction Instant time.
   * */
  public static String getCompactionInstantTime(HoodieTableMetaClient metaClient) {
    Option<HoodieInstant> hoodieInstantOption = metaClient.getCommitsTimeline().filterPendingExcludingCompaction().firstInstant();
    if (hoodieInstantOption.isPresent()) {
      HoodieInstant firstInstant = hoodieInstantOption.get();
      String newCommitTime = StreamerUtil.instantTimeSubtract(firstInstant.getTimestamp(), 1);
      // Committed and pending compaction instants should have strictly lower timestamps
      List<HoodieInstant> conflictingInstants = metaClient.getActiveTimeline()
              .getWriteTimeline().filterCompletedAndCompactionInstants().getInstants()
              .filter(instant -> HoodieTimeline.compareTimestamps(
                      instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS, newCommitTime))
              .collect(Collectors.toList());
      ValidationUtils.checkArgument(conflictingInstants.isEmpty(),
              "Following instants have timestamps >= compactionInstant (" + newCommitTime + ") Instants :"
                      + conflictingInstants);
      return newCommitTime;
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
}
