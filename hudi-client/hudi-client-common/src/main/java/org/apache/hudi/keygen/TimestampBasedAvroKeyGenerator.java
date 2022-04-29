/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.parser.BaseHoodieDateTimeParser;
import org.apache.hudi.keygen.parser.HoodieDateTimeParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Avro Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class TimestampBasedAvroKeyGenerator extends SimpleAvroKeyGenerator {
  public enum TimestampType implements Serializable {
    UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS, SCALAR
  }

  private final TimeUnit timeUnit;
  private final TimestampType timestampType;
  private final String outputDateFormat;
  private transient Option<DateTimeFormatter> inputFormatter;
  private transient DateTimeFormatter partitionFormatter;
  private final BaseHoodieDateTimeParser parser;

  // TimeZone detailed settings reference
  // https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html
  private final DateTimeZone inputDateTimeZone;
  private final DateTimeZone outputDateTimeZone;

  protected final boolean encodePartitionPath;

  public TimestampBasedAvroKeyGenerator(TypedProperties config) throws IOException {
    this(config, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()),
        config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
  }

  TimestampBasedAvroKeyGenerator(TypedProperties config, String partitionPathField) throws IOException {
    this(config, null, partitionPathField);
  }

  TimestampBasedAvroKeyGenerator(TypedProperties config, String recordKeyField, String partitionPathField) throws IOException {
    super(config, recordKeyField, partitionPathField);
    String dateTimeParserClass = config.getString(KeyGeneratorOptions.Config.DATE_TIME_PARSER_PROP, HoodieDateTimeParser.class.getName());
    this.parser = KeyGenUtils.createDateTimeParser(config, dateTimeParserClass);
    this.inputDateTimeZone = parser.getInputDateTimeZone();
    this.outputDateTimeZone = parser.getOutputDateTimeZone();
    this.outputDateFormat = parser.getOutputDateFormat();
    this.timestampType = TimestampType.valueOf(config.getString(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP));

    switch (this.timestampType) {
      case EPOCHMILLISECONDS:
        timeUnit = MILLISECONDS;
        break;
      case UNIX_TIMESTAMP:
        timeUnit = SECONDS;
        break;
      case SCALAR:
        String timeUnitStr = config.getString(KeyGeneratorOptions.Config.INPUT_TIME_UNIT, TimeUnit.SECONDS.toString());
        timeUnit = TimeUnit.valueOf(timeUnitStr.toUpperCase());
        break;
      default:
        timeUnit = null;
    }
    this.encodePartitionPath = config.getBoolean(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key(),
        Boolean.parseBoolean(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.defaultValue()));
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    Object partitionVal = HoodieAvroUtils.getNestedFieldVal(record, getPartitionPathFields().get(0), true, isConsistentLogicalTimestampEnabled());
    if (partitionVal == null) {
      partitionVal = getDefaultPartitionVal();
    }
    try {
      return getPartitionPath(partitionVal);
    } catch (Exception e) {
      throw new HoodieKeyGeneratorException("Unable to parse input partition field :" + partitionVal, e);
    }
  }

  /**
   * Set default value to partitionVal if the input value of partitionPathField is null.
   */
  public Object getDefaultPartitionVal() {
    Object result = 1L;
    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      // since partitionVal is null, we can set a default value of any format as TIMESTAMP_INPUT_DATE_FORMAT_PROP
      // configured, here we take the first.
      // {Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP} won't be null, it has been checked in the initialization process of
      // inputFormatter
      String delimiter = parser.getConfigInputDateFormatDelimiter();
      String format = config.getString(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, "").split(delimiter)[0];

      // if both input and output timeZone are not configured, use GMT.
      if (null != inputDateTimeZone) {
        return new DateTime(result, inputDateTimeZone).toString(format);
      } else if (null != outputDateTimeZone) {
        return new DateTime(result, outputDateTimeZone).toString(format);
      } else {
        return new DateTime(result, DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT"))).toString(format);
      }
    }
    return result;
  }

  /**
   * The function takes care of lazily initialising dateTimeFormatter variables only once.
   */
  private void initIfNeeded() {
    if (this.inputFormatter == null) {
      this.inputFormatter = parser.getInputFormatter();
    }
    if (this.partitionFormatter == null) {
      this.partitionFormatter = DateTimeFormat.forPattern(outputDateFormat);
      if (this.outputDateTimeZone != null) {
        partitionFormatter = partitionFormatter.withZone(outputDateTimeZone);
      }
    }
  }

  /**
   * Parse and fetch partition path based on data type.
   *
   * @param partitionVal partition path object value fetched from record/row
   * @return the parsed partition path based on data type
   */
  public String getPartitionPath(Object partitionVal) {
    initIfNeeded();
    long timeMs;
    if (partitionVal instanceof Double) {
      timeMs = convertLongTimeToMillis(((Double) partitionVal).longValue());
    } else if (partitionVal instanceof Float) {
      timeMs = convertLongTimeToMillis(((Float) partitionVal).longValue());
    } else if (partitionVal instanceof Long) {
      timeMs = convertLongTimeToMillis((Long) partitionVal);
    } else if (partitionVal instanceof Timestamp && isConsistentLogicalTimestampEnabled()) {
      timeMs = ((Timestamp) partitionVal).getTime();
    } else if (partitionVal instanceof Integer) {
      timeMs = convertLongTimeToMillis(((Integer) partitionVal).longValue());
    } else if (partitionVal instanceof BigDecimal) {
      timeMs = convertLongTimeToMillis(((BigDecimal) partitionVal).longValue());
    } else if (partitionVal instanceof LocalDate) {
      // Avro uses LocalDate to represent the Date value internal.
      timeMs = convertLongTimeToMillis(((LocalDate) partitionVal).toEpochDay());
    } else if (partitionVal instanceof CharSequence) {
      if (!inputFormatter.isPresent()) {
        throw new HoodieException("Missing inputformatter. Ensure " + KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP + " config is set when timestampType is DATE_STRING or MIXED!");
      }
      DateTime parsedDateTime = inputFormatter.get().parseDateTime(partitionVal.toString());
      if (this.outputDateTimeZone == null) {
        // Use the timezone that came off the date that was passed in, if it had one
        partitionFormatter = partitionFormatter.withZone(parsedDateTime.getZone());
      }

      timeMs = inputFormatter.get().parseDateTime(partitionVal.toString()).getMillis();
    } else {
      throw new HoodieNotSupportedException(
          "Unexpected type for partition field: " + partitionVal.getClass().getName());
    }
    DateTime timestamp = new DateTime(timeMs, outputDateTimeZone);
    String partitionPath = timestamp.toString(partitionFormatter);
    if (encodePartitionPath) {
      partitionPath = PartitionPathEncodeUtils.escapePathName(partitionPath);
    }
    return hiveStylePartitioning ? getPartitionPathFields().get(0) + "=" + partitionPath : partitionPath;
  }

  private long convertLongTimeToMillis(Long partitionVal) {
    if (timeUnit == null) {
      // should not be possible
      throw new RuntimeException(KeyGeneratorOptions.Config.INPUT_TIME_UNIT + " is not specified but scalar it supplied as time value");
    }
    return MILLISECONDS.convert(partitionVal, timeUnit);
  }
}
