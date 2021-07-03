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
   * @param jssc Java Spark Context
   * @param srcBasePath Base path of Hudi source table
   * @param numInstantsPerFetch Max Instants per fetch
   * @param beginInstant Last Checkpoint String
   * @param readLatestOnMissingBeginInstant when begin instant is missing, allow reading from latest committed instant
   * @return begin and end instants
   */
  public static Pair<String, String> calculateBeginAndEndInstants(JavaSparkContext jssc, String srcBasePath,
      int numInstantsPerFetch, Option<String> beginInstant, boolean readLatestOnMissingBeginInstant) {
    ValidationUtils.checkArgument(numInstantsPerFetch > 0,
        "Make sure the config hoodie.deltastreamer.source.hoodieincr.num_instants is set to a positive value");
    HoodieTableMetaClient srcMetaClient = HoodieTableMetaClient.builder().setConf(jssc.hadoopConfiguration()).setBasePath(srcBasePath).setLoadActiveTimelineOnLoad(true).build();

    final HoodieTimeline activeCommitTimeline =
        srcMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

    String beginInstantTime = beginInstant.orElseGet(() -> {
      if (readLatestOnMissingBeginInstant) {
        Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
        return lastInstant.map(hoodieInstant -> getStrictlyLowerTimestamp(hoodieInstant.getTimestamp())).orElse("000");
      } else {
        throw new IllegalArgumentException("Missing begin instant for incremental pull. For reading from latest "
            + "committed instant set hoodie.deltastreamer.source.hoodieincr.read_latest_on_missing_ckpt to true");
      }
    });

    Option<HoodieInstant> nthInstant = Option.fromJavaOptional(activeCommitTimeline
        .findInstantsAfter(beginInstantTime, numInstantsPerFetch).getInstants().reduce((x, y) -> y));
    return Pair.of(beginInstantTime, nthInstant.map(HoodieInstant::getTimestamp).orElse(beginInstantTime));
  }

  /**
   * Validate instant time seen in the incoming row.
   *
   * @param row Input Row
   * @param instantTime Hoodie Instant time of the row
   * @param sinceInstant begin instant of the batch
   * @param endInstant end instant of the batch
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
