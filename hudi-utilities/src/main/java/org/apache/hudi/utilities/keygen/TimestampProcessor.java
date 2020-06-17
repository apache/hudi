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

package org.apache.hudi.utilities.keygen;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;
import org.apache.hudi.utilities.keygen.common.KeyGeneratorTimestampConfig;
import org.apache.hudi.utilities.keygen.common.TimestampTypeEnum;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Timestamp processor helps to build timestamp-based KeyGenerator and deal with some timestamp format converting.
 */
public class TimestampProcessor {

  private final TimeUnit timeUnit;

  private SimpleDateFormat sdf;

  private String inputDateFormat;

  private final String outputDateFormat;

  /**
   * TimeZone detailed settings reference.
   * https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html
   */
  private final TimeZone timeZone;

  public TimestampProcessor(TypedProperties config) {
    DataSourceUtils.checkRequiredProperties(config,
        Arrays.asList(KeyGeneratorTimestampConfig.TIMESTAMP_TYPE_FIELD_PROP, KeyGeneratorTimestampConfig.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP));
    this.outputDateFormat = config.getString(KeyGeneratorTimestampConfig.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);
    this.timeZone = TimeZone.getTimeZone(config.getString(KeyGeneratorTimestampConfig.TIMESTAMP_TIMEZONE_FORMAT_PROP, "GMT"));

    TimestampTypeEnum timestampType = TimestampTypeEnum.valueOf(config.getString(KeyGeneratorTimestampConfig.TIMESTAMP_TYPE_FIELD_PROP));
    if (timestampType == TimestampTypeEnum.DATE_STRING || timestampType == TimestampTypeEnum.MIXED) {
      DataSourceUtils.checkRequiredProperties(config,
          Collections.singletonList(KeyGeneratorTimestampConfig.TIMESTAMP_INPUT_DATE_FORMAT_PROP));
      this.inputDateFormat = config.getString(KeyGeneratorTimestampConfig.TIMESTAMP_INPUT_DATE_FORMAT_PROP);
    }

    switch (timestampType) {
      case EPOCHMILLISECONDS:
        timeUnit = MILLISECONDS;
        break;
      case UNIX_TIMESTAMP:
        timeUnit = SECONDS;
        break;
      case SCALAR:
        String timeUnitStr = config.getString(KeyGeneratorTimestampConfig.INPUT_TIME_UNIT, TimeUnit.SECONDS.toString());
        timeUnit = TimeUnit.valueOf(timeUnitStr.toUpperCase());
        break;
      default:
        timeUnit = null;
    }
  }

  /**
   * Convert partitionVal to long based on its type.
   *
   * @param partitionVal Value of partitionPath
   * @return PartitionVal of long type
   */
  public long convertPartitionValToLong(Object partitionVal) {
    long timeMs;
    try {
      if (partitionVal instanceof Double) {
        timeMs = convertLongTimeToMillis(((Double) partitionVal).longValue());
      } else if (partitionVal instanceof Float) {
        timeMs = convertLongTimeToMillis(((Float) partitionVal).longValue());
      } else if (partitionVal instanceof Long) {
        timeMs = convertLongTimeToMillis((Long) partitionVal);
      } else if (partitionVal instanceof CharSequence) {
        timeMs = convertTimeStringToMillis(partitionVal.toString(), inputDateFormat);
      } else {
        throw new HoodieNotSupportedException(
            "Unexpected type for partition field: " + partitionVal.getClass().getName());
      }
    } catch (ParseException pe) {
      throw new HoodieDeltaStreamerException("Unable to parse input partition field :" + partitionVal, pe);
    }
    return timeMs;
  }

  /**
   * Convert string type time in {@paramref pattern} pattern to long type.
   *
   * @param timeString datetime in string format
   * @param pattern    time pattern
   */
  private long convertTimeStringToMillis(String timeString, String pattern) throws ParseException {
    sdf = new SimpleDateFormat(pattern);
    sdf.setTimeZone(timeZone);
    return sdf.parse(timeString).getTime();
  }

  /**
   * Convert time to millis.
   *
   * @param partitionVal partitionPath represented by timestamp
   */
  private long convertLongTimeToMillis(Long partitionVal) {
    if (timeUnit == null) {
      // should not be possible
      throw new RuntimeException(KeyGeneratorTimestampConfig.INPUT_TIME_UNIT + " is not specified but scalar it supplied as time value");
    }
    return MILLISECONDS.convert(partitionVal, timeUnit);
  }

  /**
   * Convert partitionPath of timestamp(long) type to string.
   *
   * @param timestamp partitionPath represented by timestamp
   */
  public String formatPartitionPath(long timestamp) {
    sdf = new SimpleDateFormat(outputDateFormat);
    sdf.setTimeZone(timeZone);
    return sdf.format(new Date(timestamp));
  }

}
