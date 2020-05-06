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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

/**
 * Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class MultiFormatTimestampBasedKeyGenerator extends SimpleKeyGenerator {

  enum TimestampType implements Serializable {
    UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS
  }

  private final TimestampType timestampType;
  private final String outputDateFormat;
  private final String configInputDateFormatList;
  private final String configInputDateFormatDelimiter;


  // TimeZone detailed settings reference
  // https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html
  private final DateTimeZone inputDateTimeZone;
  private final DateTimeZone outputDateTimeZone;

  /**
   * Supported configs.
   */
  static class Config {
    // One value from TimestampType above
    private static final String TIMESTAMP_TYPE_FIELD_PROP                             = "hoodie.deltastreamer.keygen.timebased.timestamp.type";

    private static final String TIMESTAMP_INPUT_DATE_FORMAT_LIST_PROP                 = "hoodie.deltastreamer.keygen.timebased.input.dateformatlist";
    private static final String TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMETER_REGEX_PROP = "hoodie.deltastreamer.keygen.timebased.input.dateformatlistdelimiterregex";
    private static final String TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP                  = "hoodie.deltastreamer.keygen.timebased.input.timezone";

    private static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP                     = "hoodie.deltastreamer.keygen.timebased.output.dateformat";
    private static final String TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP                 = "hoodie.deltastreamer.keygen.timebased.output.timezone";
  }

  public MultiFormatTimestampBasedKeyGenerator(TypedProperties config) {
    super(config);
    DataSourceUtils.checkRequiredProperties(config,
        Arrays.asList(Config.TIMESTAMP_TYPE_FIELD_PROP, Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP));

    String inputTimeZone = config.getString(Config.TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP, "");
    String outputTimeZone = config.getString(Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, "");

    this.timestampType = TimestampType.valueOf(config.getString(Config.TIMESTAMP_TYPE_FIELD_PROP));
    this.outputDateFormat = config.getString(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);
    this.configInputDateFormatList = config.getString(Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_PROP, "");

    String inputDateFormatDelimiter = this.config.getString(Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMETER_REGEX_PROP, ",").trim();
    inputDateFormatDelimiter = inputDateFormatDelimiter.isEmpty() ? "," : inputDateFormatDelimiter;
    this.configInputDateFormatDelimiter = inputDateFormatDelimiter;

    if (inputTimeZone != null && !inputTimeZone.trim().isEmpty()) {
      this.inputDateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(config.getString(Config.TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP, "")));
    } else {
      this.inputDateTimeZone = null;
    }
    if (outputTimeZone != null && !outputTimeZone.trim().isEmpty()) {
      this.outputDateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(config.getString(Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, "")));
    } else {
      this.outputDateTimeZone = null;
    }

    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      DataSourceUtils.checkRequiredProperties(config,
          Collections.singletonList(Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_PROP));
    }
  }

  private DateTimeFormatter getInputDateFormatter() {
    if (this.configInputDateFormatList.isEmpty()) {
      throw new IllegalArgumentException(Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_PROP + " configuration is required");
    }

    DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .append(
                    null,
                    Arrays.asList(
                            this.configInputDateFormatList.split(this.configInputDateFormatDelimiter))
                            .stream()
                            .map(String::trim)
                            .map(DateTimeFormat::forPattern)
                            .map(DateTimeFormatter::getParser)
                            .toArray(DateTimeParser[]::new))
            .toFormatter();
    if (this.inputDateTimeZone != null) {
      formatter = formatter.withZone(this.inputDateTimeZone);
    } else {
      formatter = formatter.withOffsetParsed();
    }

    return formatter;
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    Object partitionVal = DataSourceUtils.getNestedFieldVal(record, partitionPathField, true);
    if (partitionVal == null) {
      partitionVal = 1L;
    }

    DateTimeFormatter inputFormatter = this.getInputDateFormatter();
    DateTimeFormatter partitionFormatter = DateTimeFormat.forPattern(outputDateFormat);
    if (this.outputDateTimeZone != null) {
      partitionFormatter = partitionFormatter.withZone(outputDateTimeZone);
    }

    try {
      long unixTime;
      if (partitionVal instanceof Double) {
        unixTime = ((Double) partitionVal).longValue();
      } else if (partitionVal instanceof Float) {
        unixTime = ((Float) partitionVal).longValue();
      } else if (partitionVal instanceof Long) {
        unixTime = (Long) partitionVal;
      } else if (partitionVal instanceof CharSequence) {
        DateTime parsedDateTime = inputFormatter.parseDateTime(partitionVal.toString());

        if (this.outputDateTimeZone == null) {
          // Use the timezone that came off the date that was passed in, if it had one
          partitionFormatter = partitionFormatter.withZone(parsedDateTime.getZone());
        }

        unixTime = inputFormatter.parseDateTime(partitionVal.toString()).getMillis() / 1000;
      } else {
        throw new HoodieNotSupportedException(
          "Unexpected type for partition field: " + partitionVal.getClass().getName());
      }
      DateTime timestamp = this.timestampType == TimestampType.EPOCHMILLISECONDS ? new DateTime(unixTime, outputDateTimeZone) : new DateTime(unixTime * 1000, outputDateTimeZone);

      String recordKey = DataSourceUtils.getNestedFieldValAsString(record, recordKeyField, true);
      if (recordKey == null || recordKey.isEmpty()) {
        throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
      }

      String partitionPath = hiveStylePartitioning ? partitionPathField + "=" + timestamp.toString(partitionFormatter)
              : timestamp.toString(partitionFormatter);
      return new HoodieKey(recordKey, partitionPath);
    } catch (Exception e) {
      throw new HoodieDeltaStreamerException("Unable to parse MultiFormatTimestampBasedKeyGenerator input partition field :" + partitionVal, e);
    }
  }
}
