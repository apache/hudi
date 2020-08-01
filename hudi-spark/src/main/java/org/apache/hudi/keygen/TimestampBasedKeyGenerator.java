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

package org.apache.hudi.keygen;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieDeltaStreamerException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.keygen.parser.HoodieDateTimeParser;
import org.apache.hudi.keygen.parser.HoodieDateTimeParserImpl;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class TimestampBasedKeyGenerator extends SimpleKeyGenerator {

  public enum TimestampType implements Serializable {
    UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS, SCALAR
  }

  private final TimeUnit timeUnit;
  private final TimestampType timestampType;
  private final String outputDateFormat;
  private DateTimeFormatter inputFormatter;
  private final HoodieDateTimeParser parser;

  // TimeZone detailed settings reference
  // https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html
  private final DateTimeZone outputDateTimeZone;

  /**
   * Supported configs.
   */
  public static class Config {

    // One value from TimestampType above
    public static final String TIMESTAMP_TYPE_FIELD_PROP = "hoodie.deltastreamer.keygen.timebased.timestamp.type";
    public static final String INPUT_TIME_UNIT =
        "hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit";
    //This prop can now accept list of input date formats.
    public static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.input.dateformat";
    public static final String TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP = "hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex";
    public static final String TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.input.timezone";
    public static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.output.dateformat";
    //still keeping this prop for backward compatibility so that functionality for existing users does not break.
    public static final String TIMESTAMP_TIMEZONE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.timezone";
    public static final String TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.output.timezone";
    static final String DATE_TIME_PARSER_PROP = "hoodie.deltastreamer.keygen.datetime.parser.class";
  }

  public TimestampBasedKeyGenerator(TypedProperties config) throws IOException {
    super(config);
    String dateTimeParserClass = config.getString(Config.DATE_TIME_PARSER_PROP, HoodieDateTimeParserImpl.class.getName());
    this.parser = DataSourceUtils.createDateTimeParser(config, dateTimeParserClass);
    this.outputDateTimeZone = parser.getOutputDateTimeZone();
    this.outputDateFormat = parser.getOutputDateFormat();
    this.inputFormatter = parser.getInputFormatter();
    this.timestampType = TimestampType.valueOf(config.getString(Config.TIMESTAMP_TYPE_FIELD_PROP));

    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      this.inputFormatter = parser.getInputFormatter();
    }

    switch (this.timestampType) {
      case EPOCHMILLISECONDS:
        timeUnit = MILLISECONDS;
        break;
      case UNIX_TIMESTAMP:
        timeUnit = SECONDS;
        break;
      case SCALAR:
        String timeUnitStr = config.getString(Config.INPUT_TIME_UNIT, TimeUnit.SECONDS.toString());
        timeUnit = TimeUnit.valueOf(timeUnitStr.toUpperCase());
        break;
      default:
        timeUnit = null;
    }
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    String recordKey = getRecordKey(record);
    String partitionPath = getPartitionPath(record, partitionPathField);
    return new HoodieKey(recordKey, partitionPath);
  }

  String getPartitionPath(GenericRecord record, String partitionPathField) {
    Object partitionVal = DataSourceUtils.getNestedFieldVal(record, partitionPathField, true);
    if (partitionVal == null) {
      partitionVal = 1L;
    }

    DateTimeFormatter partitionFormatter = DateTimeFormat.forPattern(outputDateFormat);
    if (this.outputDateTimeZone != null) {
      partitionFormatter = partitionFormatter.withZone(outputDateTimeZone);
    }

    try {
      long timeMs;
      if (partitionVal instanceof Double) {
        timeMs = convertLongTimeToMillis(((Double) partitionVal).longValue());
      } else if (partitionVal instanceof Float) {
        timeMs = convertLongTimeToMillis(((Float) partitionVal).longValue());
      } else if (partitionVal instanceof Long) {
        timeMs = convertLongTimeToMillis((Long) partitionVal);
      } else if (partitionVal instanceof CharSequence) {
        DateTime parsedDateTime = inputFormatter.parseDateTime(partitionVal.toString());
        if (this.outputDateTimeZone == null) {
          // Use the timezone that came off the date that was passed in, if it had one
          partitionFormatter = partitionFormatter.withZone(parsedDateTime.getZone());
        }

        timeMs = inputFormatter.parseDateTime(partitionVal.toString()).getMillis();
      } else {
        throw new HoodieNotSupportedException(
          "Unexpected type for partition field: " + partitionVal.getClass().getName());
      }
      DateTime timestamp = new DateTime(timeMs, outputDateTimeZone);
      return hiveStylePartitioning ? partitionPathField + "=" + timestamp.toString(partitionFormatter)
        : timestamp.toString(partitionFormatter);
    } catch (Exception e) {
      throw new HoodieDeltaStreamerException("Unable to parse input partition field :" + partitionVal, e);
    }
  }

  private long convertLongTimeToMillis(Long partitionVal) {
    if (timeUnit == null) {
      // should not be possible
      throw new RuntimeException(Config.INPUT_TIME_UNIT + " is not specified but scalar it supplied as time value");
    }
    return MILLISECONDS.convert(partitionVal, timeUnit);
  }
}
