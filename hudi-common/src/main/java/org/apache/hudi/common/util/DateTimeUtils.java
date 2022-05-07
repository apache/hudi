/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DateTimeUtils {
  private static final Map<String, ChronoUnit> LABEL_TO_UNIT_MAP =
      Collections.unmodifiableMap(initMap());

  /**
   * Converts provided microseconds (from epoch) to {@link Instant}
   */
  public static Instant microsToInstant(long microsFromEpoch) {
    long epochSeconds = microsFromEpoch / (1_000_000L);
    long nanoAdjustment = (microsFromEpoch % (1_000_000L)) * 1_000L;

    return Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
  }

  /**
   * Converts provided {@link Instant} to microseconds (from epoch)
   */
  public static long instantToMicros(Instant instant) {
    long seconds = instant.getEpochSecond();
    int nanos = instant.getNano();

    if (seconds < 0 && nanos > 0) {
      long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
      long adjustment = (nanos / 1_000L) - 1_000_000;

      return Math.addExact(micros, adjustment);
    } else {
      long micros = Math.multiplyExact(seconds, 1_000_000L);

      return Math.addExact(micros, nanos / 1_000L);
    }
  }

  /**
   * Parse input String to a {@link java.time.Instant}.
   *
   * @param s Input String should be Epoch time in millisecond or ISO-8601 format.
   */
  public static Instant parseDateTime(String s) throws DateTimeParseException {
    ValidationUtils.checkArgument(Objects.nonNull(s), "Input String cannot be null.");
    try {
      return Instant.ofEpochMilli(Long.parseLong(s));
    } catch (NumberFormatException e) {
      return Instant.parse(s);
    }
  }

  /**
   * Parse the given string to a java {@link Duration}. The string is in format "{length
   * value}{time unit label}", e.g. "123ms", "321 s". If no time unit label is specified, it will
   * be considered as milliseconds.
   *
   * <p>Supported time unit labels are:
   *
   * <ul>
   *   <li>DAYS： "d", "day"
   *   <li>HOURS： "h", "hour"
   *   <li>MINUTES： "min", "minute"
   *   <li>SECONDS： "s", "sec", "second"
   *   <li>MILLISECONDS： "ms", "milli", "millisecond"
   *   <li>MICROSECONDS： "µs", "micro", "microsecond"
   *   <li>NANOSECONDS： "ns", "nano", "nanosecond"
   * </ul>
   *
   * @param text string to parse.
   */
  public static Duration parseDuration(String text) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(text));

    final String trimmed = text.trim();
    ValidationUtils.checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

    final int len = trimmed.length();
    int pos = 0;

    char current;
    while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
      pos++;
    }

    final String number = trimmed.substring(0, pos);
    final String unitLabel = trimmed.substring(pos).trim().toLowerCase(Locale.US);

    if (number.isEmpty()) {
      throw new NumberFormatException("text does not start with a number");
    }

    final long value;
    try {
      value = Long.parseLong(number); // this throws a NumberFormatException on overflow
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "The value '"
              + number
              + "' cannot be re represented as 64bit number (numeric overflow).");
    }

    if (unitLabel.isEmpty()) {
      return Duration.of(value, ChronoUnit.MILLIS);
    }

    ChronoUnit unit = LABEL_TO_UNIT_MAP.get(unitLabel);
    if (unit != null) {
      return Duration.of(value, unit);
    } else {
      throw new IllegalArgumentException(
          "Time interval unit label '"
              + unitLabel
              + "' does not match any of the recognized units: "
              + TimeUnit.getAllUnits());
    }
  }

  private static Map<String, ChronoUnit> initMap() {
    Map<String, ChronoUnit> labelToUnit = new HashMap<>();
    for (TimeUnit timeUnit : TimeUnit.values()) {
      for (String label : timeUnit.getLabels()) {
        labelToUnit.put(label, timeUnit.getUnit());
      }
    }
    return labelToUnit;
  }

  /**
   * Convert UNIX_TIMESTAMP to string in given format.
   *
   * @param unixTimestamp UNIX_TIMESTAMP
   * @param timeFormat string time format
   */
  public static String formatUnixTimestamp(long unixTimestamp, String timeFormat) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(timeFormat));
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(timeFormat);
    return LocalDateTime
        .ofInstant(Instant.ofEpochSecond(unixTimestamp), ZoneId.systemDefault())
        .format(dtf);
  }

  /**
   * Enum which defines time unit, mostly used to parse value from configuration file.
   */
  private enum TimeUnit {
    DAYS(ChronoUnit.DAYS, singular("d"), plural("day")),
    HOURS(ChronoUnit.HOURS, singular("h"), plural("hour")),
    MINUTES(ChronoUnit.MINUTES, singular("min"), plural("minute")),
    SECONDS(ChronoUnit.SECONDS, singular("s"), plural("sec"), plural("second")),
    MILLISECONDS(ChronoUnit.MILLIS, singular("ms"), plural("milli"), plural("millisecond")),
    MICROSECONDS(ChronoUnit.MICROS, singular("µs"), plural("micro"), plural("microsecond")),
    NANOSECONDS(ChronoUnit.NANOS, singular("ns"), plural("nano"), plural("nanosecond"));

    private static final String PLURAL_SUFFIX = "s";

    private final List<String> labels;

    private final ChronoUnit unit;

    TimeUnit(ChronoUnit unit, String[]... labels) {
      this.unit = unit;
      this.labels =
          Arrays.stream(labels)
              .flatMap(Arrays::stream)
              .collect(Collectors.toList());
    }

    /**
     * @param label the original label
     * @return the singular format of the original label
     */
    private static String[] singular(String label) {
      return new String[] {label};
    }

    /**
     * @param label the original label
     * @return both the singular format and plural format of the original label
     */
    private static String[] plural(String label) {
      return new String[] {label, label + PLURAL_SUFFIX};
    }

    public List<String> getLabels() {
      return labels;
    }

    public ChronoUnit getUnit() {
      return unit;
    }

    public static String getAllUnits() {
      return Arrays.stream(TimeUnit.values())
          .map(TimeUnit::createTimeUnitString)
          .collect(Collectors.joining(", "));
    }

    private static String createTimeUnitString(TimeUnit timeUnit) {
      return timeUnit.name() + ": (" + String.join(" | ", timeUnit.getLabels()) + ")";
    }
  }
}
