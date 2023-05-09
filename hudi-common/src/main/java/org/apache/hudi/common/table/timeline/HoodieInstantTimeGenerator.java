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

import org.apache.hudi.common.model.HoodieTimelineTimeZone;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class to generate and parse timestamps used in Instants.
 */
public class HoodieInstantTimeGenerator {
  // Format of the timestamp used for an Instant
  public static final String SECS_INSTANT_TIMESTAMP_FORMAT = "yyyyMMddHHmmss";
  public static final int SECS_INSTANT_ID_LENGTH = SECS_INSTANT_TIMESTAMP_FORMAT.length();
  public static final String MILLIS_INSTANT_TIMESTAMP_FORMAT = "yyyyMMddHHmmssSSS";
  public static final int MILLIS_INSTANT_ID_LENGTH = MILLIS_INSTANT_TIMESTAMP_FORMAT.length();
  public static final int MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH = MILLIS_INSTANT_TIMESTAMP_FORMAT.length();
  // Formatter to generate Instant timestamps
  // Unfortunately millisecond format is not parsable as is https://bugs.openjdk.java.net/browse/JDK-8031085. hence have to do appendValue()
  private static DateTimeFormatter MILLIS_INSTANT_TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern(SECS_INSTANT_TIMESTAMP_FORMAT)
      .appendValue(ChronoField.MILLI_OF_SECOND, 3).toFormatter();
  private static final String MILLIS_GRANULARITY_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private static DateTimeFormatter MILLIS_GRANULARITY_DATE_FORMATTER = DateTimeFormatter.ofPattern(MILLIS_GRANULARITY_DATE_FORMAT);

  // The last Instant timestamp generated
  private static AtomicReference<String> lastInstantTime = new AtomicReference<>(String.valueOf(Integer.MIN_VALUE));

  // The default number of milliseconds that we add if they are not present
  // We prefer the max timestamp as it mimics the current behavior with second granularity
  // when performing comparisons such as LESS_THAN_OR_EQUAL_TO
  private static final String DEFAULT_MILLIS_EXT = "999";

  private static HoodieTimelineTimeZone commitTimeZone = HoodieTimelineTimeZone.LOCAL;

  /**
   * Returns next instant time that adds N milliseconds to the current time.
   * Ensures each instant time is atleast 1 second apart since we create instant times at second granularity
   *
   * @param milliseconds Milliseconds to add to current time while generating the new instant time
   */
  public static String createNewInstantTime(long milliseconds) {
    return lastInstantTime.updateAndGet((oldVal) -> {
      String newCommitTime;
      do {
        if (commitTimeZone.equals(HoodieTimelineTimeZone.UTC)) {
          LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
          newCommitTime = now.format(MILLIS_INSTANT_TIME_FORMATTER);
        } else {
          Date d = new Date(System.currentTimeMillis() + milliseconds);
          newCommitTime = MILLIS_INSTANT_TIME_FORMATTER.format(convertDateToTemporalAccessor(d));
        }
      } while (HoodieTimeline.compareTimestamps(newCommitTime, HoodieActiveTimeline.LESSER_THAN_OR_EQUALS, oldVal));
      return newCommitTime;
    });
  }

  public static Date parseDateFromInstantTime(String timestamp) throws ParseException {
    try {
      // Enables backwards compatibility with non-millisecond granularity instants
      String timestampInMillis = timestamp;
      if (isSecondGranularity(timestamp)) {
        // Add milliseconds to the instant in order to parse successfully
        timestampInMillis = timestamp + DEFAULT_MILLIS_EXT;
      } else if (timestamp.length() > MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH) {
        // compaction and cleaning in metadata has special format. handling it by trimming extra chars and treating it with ms granularity
        timestampInMillis = timestamp.substring(0, MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH);
      }

      LocalDateTime dt = LocalDateTime.parse(timestampInMillis, MILLIS_INSTANT_TIME_FORMATTER);
      return Date.from(dt.atZone(ZoneId.systemDefault()).toInstant());
    } catch (DateTimeParseException e) {
      throw new ParseException(e.getMessage(), e.getErrorIndex());
    }
  }

  private static boolean isSecondGranularity(String instant) {
    return instant.length() == SECS_INSTANT_ID_LENGTH;
  }

  public static String formatDate(Date timestamp) {
    return getInstantFromTemporalAccessor(convertDateToTemporalAccessor(timestamp));
  }

  public static String getInstantFromTemporalAccessor(TemporalAccessor temporalAccessor) {
    return MILLIS_INSTANT_TIME_FORMATTER.format(temporalAccessor);
  }

  /**
   * Creates an instant string given a valid date-time string.
   * @param dateString A date-time string in the format yyyy-MM-dd HH:mm:ss[:SSS]
   * @return A timeline instant
   * @throws ParseException If we cannot parse the date string
   */
  public static String getInstantForDateString(String dateString) {
    try {
      return getInstantFromTemporalAccessor(LocalDateTime.parse(dateString, MILLIS_GRANULARITY_DATE_FORMATTER));
    } catch (Exception e) {
      // Attempt to add the milliseconds in order to complete parsing
      return getInstantFromTemporalAccessor(LocalDateTime.parse(
          String.format("%s:%s", dateString, DEFAULT_MILLIS_EXT), MILLIS_GRANULARITY_DATE_FORMATTER));
    }
  }

  private static TemporalAccessor convertDateToTemporalAccessor(Date d) {
    return d.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
  }

  public static void setCommitTimeZone(HoodieTimelineTimeZone commitTimeZone) {
    HoodieInstantTimeGenerator.commitTimeZone = commitTimeZone;
  }

  public static boolean isValidInstantTime(String instantTime) {
    try {
      Long.parseLong(instantTime);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
