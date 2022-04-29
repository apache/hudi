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

package org.apache.hudi.keygen.parser;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.TimestampType;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;

public class HoodieDateTimeParser extends BaseHoodieDateTimeParser {

  private String configInputDateFormatList;

  // TimeZone detailed settings reference
  // https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html
  private final DateTimeZone inputDateTimeZone;

  public HoodieDateTimeParser(TypedProperties config) {
    super(config);
    KeyGenUtils.checkRequiredProperties(config, Arrays.asList(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP, KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP));
    this.inputDateTimeZone = getInputDateTimeZone();
  }

  private DateTimeFormatter getInputDateFormatter() {
    if (this.configInputDateFormatList.isEmpty()) {
      throw new IllegalArgumentException(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP + " configuration is required");
    }

    DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        .append(
        null,
        Arrays.stream(
          this.configInputDateFormatList.split(super.configInputDateFormatDelimiter))
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
  public String getOutputDateFormat() {
    return config.getString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);
  }

  @Override
  public Option<DateTimeFormatter> getInputFormatter() {
    TimestampType timestampType = TimestampType.valueOf(config.getString(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP));
    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      KeyGenUtils.checkRequiredProperties(config,
          Collections.singletonList(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP));
      this.configInputDateFormatList = config.getString(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, "");
      return Option.of(getInputDateFormatter());
    }

    return Option.empty();
  }

  @Override
  public DateTimeZone getInputDateTimeZone() {
    String inputTimeZone;
    if (config.containsKey(KeyGeneratorOptions.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP)) {
      inputTimeZone = config.getString(KeyGeneratorOptions.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, "GMT");
    } else {
      inputTimeZone = config.getString(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP, "");
    }
    return !inputTimeZone.trim().isEmpty() ? DateTimeZone.forTimeZone(TimeZone.getTimeZone(inputTimeZone)) : null;
  }

  @Override
  public DateTimeZone getOutputDateTimeZone() {
    String outputTimeZone;
    if (config.containsKey(KeyGeneratorOptions.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP)) {
      outputTimeZone = config.getString(KeyGeneratorOptions.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, "GMT");
    } else {
      outputTimeZone = config.getString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, "");
    }
    return !outputTimeZone.trim().isEmpty() ? DateTimeZone.forTimeZone(TimeZone.getTimeZone(outputTimeZone)) : null;
  }

}
