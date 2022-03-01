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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

import java.util.Objects;

public class IncrSourceHelper {

  private static final String DEFAULT_BEGIN_TIMESTAMP = "000";
  /**
   * Kafka reset offset strategies.
   */
  public enum MissingCheckpointStrategy {
    // read from latest commit in hoodie source table
    READ_LATEST,
    // read everything upto latest commit
    READ_UPTO_LATEST_COMMIT
  }

  /**
   * Get a timestamp which is the next value in a descending sequence.
   *
   * @param timestamp Timestamp
   */
  private static String getStrictlyLowerTimestamp(String timestamp) {
    long ts = Long.parseLong(timestamp);
    ValidationUtils.checkArgument(ts > 0, "Timestamp must be positive");
    long lower = ts - 1;
    return "" + lower;
  }

  /**
   * Find begin and end instants to be set for the next fetch.
   *
   * @param jssc                            Java Spark Context
   * @param srcBasePath                     Base path of Hudi source table
   * @param numInstantsPerFetch             Max Instants per fetch
   * @param beginInstant                    Last Checkpoint String
   * @param missingCheckpointStrategy when begin instant is missing, allow reading based on missing checkpoint strategy
   * @return begin and end instants along with query type.
   */
  public static Pair<String, Pair<String, String>> calculateBeginAndEndInstants(JavaSparkContext jssc, String srcBasePath,
                                                                                 int numInstantsPerFetch, Option<String> beginInstant, MissingCheckpointStrategy missingCheckpointStrategy) {
    ValidationUtils.checkArgument(numInstantsPerFetch > 0,
        "Make sure the config hoodie.deltastreamer.source.hoodieincr.num_instants is set to a positive value");
    HoodieTableMetaClient srcMetaClient = HoodieTableMetaClient.builder().setConf(jssc.hadoopConfiguration()).setBasePath(srcBasePath).setLoadActiveTimelineOnLoad(true).build();

    final HoodieTimeline activeCommitTimeline =
        srcMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

    String beginInstantTime = beginInstant.orElseGet(() -> {
      if (missingCheckpointStrategy != null) {
        if (missingCheckpointStrategy == MissingCheckpointStrategy.READ_LATEST) {
          Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
          return lastInstant.map(hoodieInstant -> getStrictlyLowerTimestamp(hoodieInstant.getTimestamp())).orElse(DEFAULT_BEGIN_TIMESTAMP);
        } else {
          return DEFAULT_BEGIN_TIMESTAMP;
        }
      } else {
        throw new IllegalArgumentException("Missing begin instant for incremental pull. For reading from latest "
            + "committed instant set hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy to a valid value");
      }
    });

    if (missingCheckpointStrategy == MissingCheckpointStrategy.READ_LATEST || !activeCommitTimeline.isBeforeTimelineStarts(beginInstantTime)) {
      Option<HoodieInstant> nthInstant = Option.fromJavaOptional(activeCommitTimeline
          .findInstantsAfter(beginInstantTime, numInstantsPerFetch).getInstants().reduce((x, y) -> y));
      return Pair.of(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL(), Pair.of(beginInstantTime, nthInstant.map(HoodieInstant::getTimestamp).orElse(beginInstantTime)));
    } else {
      // when MissingCheckpointStrategy is set to read everything until latest, trigger snapshot query.
      Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
      return Pair.of(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL(), Pair.of(beginInstantTime, lastInstant.get().getTimestamp()));
    }
  }

  /**
   * Validate instant time seen in the incoming row.
   *
   * @param row          Input Row
   * @param instantTime  Hoodie Instant time of the row
   * @param sinceInstant begin instant of the batch
   * @param endInstant   end instant of the batch
   */
  public static void validateInstantTime(Row row, String instantTime, String sinceInstant, String endInstant) {
    Objects.requireNonNull(instantTime);
    ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.GREATER_THAN, sinceInstant),
        "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime + "but expected to be between "
            + sinceInstant + "(excl) - " + endInstant + "(incl)");
    ValidationUtils.checkArgument(
        HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant),
        "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime + "but expected to be between "
            + sinceInstant + "(excl) - " + endInstant + "(incl)");
  }
}
