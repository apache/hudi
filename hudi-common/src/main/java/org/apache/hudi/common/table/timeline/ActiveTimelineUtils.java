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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LOG_COMPACTION_ACTION;

public class ActiveTimelineUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveTimelineUtils.class);

  public static final Set<String> NOT_PARSABLE_TIMESTAMPS = new HashSet<String>(3) {
    {
      add(HoodieTimeline.INIT_INSTANT_TS);
      add(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS);
      add(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS);
    }
  };

  /**
   * Parse the timestamp of an Instant and return a {@code Date}.
   * Throw ParseException if timestamp is not valid format as
   *  {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator#SECS_INSTANT_TIMESTAMP_FORMAT}.
   *
   * @param timestamp a timestamp String which follow pattern as
   *  {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator#SECS_INSTANT_TIMESTAMP_FORMAT}.
   * @return Date of instant timestamp
   */
  public static Date parseDateFromInstantTime(String timestamp) throws ParseException {
    return HoodieInstantTimeGenerator.parseDateFromInstantTime(timestamp);
  }

  /**
   * The same parsing method as above, but this method will mute ParseException.
   * If the given timestamp is invalid, returns {@code Option.empty}.
   * Or a corresponding Date value if these timestamp strings are provided
   *  {@link org.apache.hudi.common.table.timeline.HoodieTimeline#INIT_INSTANT_TS},
   *  {@link org.apache.hudi.common.table.timeline.HoodieTimeline#METADATA_BOOTSTRAP_INSTANT_TS},
   *  {@link org.apache.hudi.common.table.timeline.HoodieTimeline#FULL_BOOTSTRAP_INSTANT_TS}.
   * This method is useful when parsing timestamp for metrics
   *
   * @param timestamp a timestamp String which follow pattern as
   *  {@link org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator#SECS_INSTANT_TIMESTAMP_FORMAT}.
   * @return {@code Option<Date>} of instant timestamp, {@code Option.empty} if invalid timestamp
   */
  public static Option<Date> parseDateFromInstantTimeSafely(String timestamp) {
    Option<Date> parsedDate;
    try {
      parsedDate = Option.of(HoodieInstantTimeGenerator.parseDateFromInstantTime(timestamp));
    } catch (ParseException e) {
      if (NOT_PARSABLE_TIMESTAMPS.contains(timestamp)) {
        parsedDate = Option.of(new Date(Integer.parseInt(timestamp)));
      } else {
        LOG.warn("Failed to parse timestamp {}: {}", timestamp, e.getMessage());
        parsedDate = Option.empty();
      }
    }
    return parsedDate;
  }

  /**
   * Format the Date to a String representing the timestamp of a Hoodie Instant.
   */
  public static String formatDate(Date timestamp) {
    return HoodieInstantTimeGenerator.formatDate(timestamp);
  }

  /**
   * Returns next instant time in the correct format.
   * Ensures each instant time is at least 1 millisecond apart since we create instant times at millisecond granularity.
   *
   * @param shouldLock whether the lock should be enabled to get the instant time.
   * @param timeGenerator TimeGenerator used to generate the instant time.
   */
  public static String createNewInstantTime(boolean shouldLock, TimeGenerator timeGenerator) {
    return createNewInstantTime(shouldLock, timeGenerator, 0L);
  }

  /**
   * Returns next instant time in the correct format.
   * Ensures each instant time is at least 1 millisecond apart since we create instant times at millisecond granularity.
   *
   * @param shouldLock whether the lock should be enabled to get the instant time.
   * @param timeGenerator TimeGenerator used to generate the instant time.
   * @param milliseconds Milliseconds to add to current time while generating the new instant time
   */
  public static String createNewInstantTime(boolean shouldLock, TimeGenerator timeGenerator, long milliseconds) {
    return HoodieInstantTimeGenerator.createNewInstantTime(shouldLock, timeGenerator, milliseconds);
  }

  /**
   * Delete Instant file from storage
   * @param storage Hoodie Storage.
   * @param metaPath Path.
   * @param instant instant to delete.
   * @param factory Factory to generate file name.
   */
  public static void deleteInstantFile(HoodieStorage storage, StoragePath metaPath, HoodieInstant instant, InstantFileNameFactory factory) {
    String filePath = factory.getFileName(instant);
    try {
      storage.deleteFile(new StoragePath(metaPath, filePath));
    } catch (IOException e) {
      throw new HoodieIOException("Could not delete instant file" + filePath, e);
    }
  }

  /**
   * Returns the inflight instant corresponding to the instant being passed. Takes care of changes in action names
   * between inflight and completed instants (compaction <=> commit) and (logcompaction <==> deltacommit).
   * @param instant Hoodie Instant
   * @param metaClient Hoodie metaClient to fetch tableType and fileSystem.
   * @return Inflight Hoodie Instant
   */
  public static HoodieInstant getInflightInstant(final HoodieInstant instant, final HoodieTableMetaClient metaClient) {
    InstantFactory factory = metaClient.getTimelineLayout().getInstantFactory();
    if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
      if (instant.getAction().equals(COMMIT_ACTION)) {
        return factory.createNewInstant(HoodieInstant.State.INFLIGHT, COMPACTION_ACTION, instant.getRequestTime());
      } else if (instant.getAction().equals(DELTA_COMMIT_ACTION)) {
        // Deltacommit is used by both ingestion and logcompaction.
        // So, distinguish both of them check for the inflight file being present.
        HoodieActiveTimeline rawActiveTimeline = metaClient.getTimelineLayout().getTimelineFactory().createActiveTimeline(metaClient, false);
        Option<org.apache.hudi.common.table.timeline.HoodieInstant> logCompactionInstant = Option.fromJavaOptional(rawActiveTimeline.getInstantsAsStream()
            .filter(hoodieInstant -> hoodieInstant.getRequestTime().equals(instant.getRequestTime())
                && LOG_COMPACTION_ACTION.equals(hoodieInstant.getAction())).findFirst());
        if (logCompactionInstant.isPresent()) {
          return factory.createNewInstant(HoodieInstant.State.INFLIGHT, LOG_COMPACTION_ACTION, instant.getRequestTime());
        }
      }
    }
    return factory.createNewInstant(HoodieInstant.State.INFLIGHT, instant.getAction(), instant.getRequestTime());
  }
}
