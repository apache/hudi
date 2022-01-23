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

package org.apache.hudi.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.exceptions.HBaseException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class PrettyPrinter {

  private static final Logger LOG = LoggerFactory.getLogger(PrettyPrinter.class);

  private static final String INTERVAL_REGEX = "((\\d+)\\s*SECONDS?\\s*\\()?\\s*" +
      "((\\d+)\\s*DAYS?)?\\s*((\\d+)\\s*HOURS?)?\\s*" +
      "((\\d+)\\s*MINUTES?)?\\s*((\\d+)\\s*SECONDS?)?\\s*\\)?";
  private static final Pattern INTERVAL_PATTERN = Pattern.compile(INTERVAL_REGEX,
      Pattern.CASE_INSENSITIVE);

  public enum Unit {
    TIME_INTERVAL,
    LONG,
    BOOLEAN,
    NONE
  }

  public static String format(final String value, final Unit unit) {
    StringBuilder human = new StringBuilder();
    switch (unit) {
      case TIME_INTERVAL:
        human.append(humanReadableTTL(Long.parseLong(value)));
        break;
      case LONG:
        byte[] longBytes = Bytes.toBytesBinary(value);
        human.append(String.valueOf(Bytes.toLong(longBytes)));
        break;
      case BOOLEAN:
        byte[] booleanBytes = Bytes.toBytesBinary(value);
        human.append(String.valueOf(Bytes.toBoolean(booleanBytes)));
        break;
      default:
        human.append(value);
    }
    return human.toString();
  }

  /**
   * Convert a human readable string to its value.
   * @see org.apache.hadoop.hbase.util.PrettyPrinter#format(String, Unit)
   * @param pretty
   * @param unit
   * @return the value corresponding to the human readable string
   */
  public static String valueOf(final String pretty, final Unit unit) throws HBaseException {
    StringBuilder value = new StringBuilder();
    switch (unit) {
      case TIME_INTERVAL:
        value.append(humanReadableIntervalToSec(pretty));
        break;
      default:
        value.append(pretty);
    }
    return value.toString();
  }

  private static String humanReadableTTL(final long interval){
    StringBuilder sb = new StringBuilder();
    int days, hours, minutes, seconds;

    // edge cases first
    if (interval == Integer.MAX_VALUE) {
      sb.append("FOREVER");
      return sb.toString();
    }
    if (interval < HConstants.MINUTE_IN_SECONDS) {
      sb.append(interval);
      sb.append(" SECOND").append(interval == 1 ? "" : "S");
      return sb.toString();
    }

    days  =   (int) (interval / HConstants.DAY_IN_SECONDS);
    hours =   (int) (interval - HConstants.DAY_IN_SECONDS * days) / HConstants.HOUR_IN_SECONDS;
    minutes = (int) (interval - HConstants.DAY_IN_SECONDS * days
        - HConstants.HOUR_IN_SECONDS * hours) / HConstants.MINUTE_IN_SECONDS;
    seconds = (int) (interval - HConstants.DAY_IN_SECONDS * days
        - HConstants.HOUR_IN_SECONDS * hours - HConstants.MINUTE_IN_SECONDS * minutes);

    sb.append(interval);
    sb.append(" SECONDS (");

    if (days > 0) {
      sb.append(days);
      sb.append(" DAY").append(days == 1 ? "" : "S");
    }

    if (hours > 0) {
      sb.append(days > 0 ? " " : "");
      sb.append(hours);
      sb.append(" HOUR").append(hours == 1 ? "" : "S");
    }

    if (minutes > 0) {
      sb.append(days + hours > 0 ? " " : "");
      sb.append(minutes);
      sb.append(" MINUTE").append(minutes == 1 ? "" : "S");
    }

    if (seconds > 0) {
      sb.append(days + hours + minutes > 0 ? " " : "");
      sb.append(seconds);
      sb.append(" SECOND").append(minutes == 1 ? "" : "S");
    }

    sb.append(")");

    return sb.toString();
  }

  /**
   * Convert a human readable time interval to seconds. Examples of the human readable
   * time intervals are: 50 DAYS 1 HOUR 30 MINUTES , 25000 SECONDS etc.
   * The units of time specified can be in uppercase as well as lowercase. Also, if a
   * single number is specified without any time unit, it is assumed to be in seconds.
   * @param humanReadableInterval
   * @return value in seconds
   */
  private static long humanReadableIntervalToSec(final String humanReadableInterval)
      throws HBaseException {
    if (humanReadableInterval == null || humanReadableInterval.equalsIgnoreCase("FOREVER")) {
      return HConstants.FOREVER;
    }

    try {
      return Long.parseLong(humanReadableInterval);
    } catch(NumberFormatException ex) {
      LOG.debug("Given interval value is not a number, parsing for human readable format");
    }

    String days = null;
    String hours = null;
    String minutes = null;
    String seconds = null;
    String expectedTtl = null;
    long ttl;

    Matcher matcher = PrettyPrinter.INTERVAL_PATTERN.matcher(humanReadableInterval);
    if (matcher.matches()) {
      expectedTtl = matcher.group(2);
      days = matcher.group(4);
      hours = matcher.group(6);
      minutes = matcher.group(8);
      seconds = matcher.group(10);
    }
    ttl = 0;
    ttl += days != null ? Long.parseLong(days)*HConstants.DAY_IN_SECONDS:0;
    ttl += hours != null ? Long.parseLong(hours)*HConstants.HOUR_IN_SECONDS:0;
    ttl += minutes != null ? Long.parseLong(minutes)*HConstants.MINUTE_IN_SECONDS:0;
    ttl += seconds != null ? Long.parseLong(seconds):0;

    if (expectedTtl != null && Long.parseLong(expectedTtl) != ttl) {
      throw new HBaseException("Malformed TTL string: TTL values in seconds and human readable" +
          "format do not match");
    }
    return ttl;
  }

  /**
   * Pretty prints a collection of any type to a string. Relies on toString() implementation of the
   * object type.
   * @param collection collection to pretty print.
   * @return Pretty printed string for the collection.
   */
  public static String toString(Collection<?> collection) {
    List<String> stringList = new ArrayList<>();
    for (Object o: collection) {
      stringList.add(Objects.toString(o));
    }
    return "[" + String.join(",", stringList) + "]";
  }

}
