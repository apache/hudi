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

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;

import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD;
import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

public class HoodieDateTimeParser extends BaseHoodieDateTimeParser {

  private String configInputDateFormatList;

  // TimeZone detailed settings reference
  // https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html
  private final DateTimeZone inputDateTimeZone;

  public HoodieDateTimeParser(TypedProperties config) {
    super(config);
    checkRequiredConfigProperties(
        config, Arrays.asList(TIMESTAMP_TYPE_FIELD, TIMESTAMP_OUTPUT_DATE_FORMAT));
    this.inputDateTimeZone = getInputDateTimeZone();
  }

  private DateTimeFormatter getInputDateFormatter() {
    if (this.configInputDateFormatList.isEmpty()) {
      throw new IllegalArgumentException(TIMESTAMP_INPUT_DATE_FORMAT.key() + " configuration is required");
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
  public Option<DateTimeFormatter> getInputFormatter() {
    TimestampType timestampType = TimestampType.valueOf(
        getStringWithAltKeys(config, TIMESTAMP_TYPE_FIELD));
    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      checkRequiredConfigProperties(config,
          Collections.singletonList(TIMESTAMP_INPUT_DATE_FORMAT));
      this.configInputDateFormatList = getStringWithAltKeys(config, TIMESTAMP_INPUT_DATE_FORMAT, true);
      return Option.of(getInputDateFormatter());
    }

    return Option.empty();
  }

  @Override
  public DateTimeZone getInputDateTimeZone() {
    String inputTimeZone;
    if (containsConfigProperty(config, TIMESTAMP_TIMEZONE_FORMAT)) {
      inputTimeZone = getStringWithAltKeys(config, TIMESTAMP_TIMEZONE_FORMAT, "GMT");
    } else {
      inputTimeZone = getStringWithAltKeys(config, TIMESTAMP_INPUT_TIMEZONE_FORMAT, "");
    }
    return !inputTimeZone.trim().isEmpty() ? DateTimeZone.forTimeZone(TimeZone.getTimeZone(inputTimeZone)) : null;
  }

  @Override
  public DateTimeZone getOutputDateTimeZone() {
    String outputTimeZone;
    if (containsConfigProperty(config, TIMESTAMP_TIMEZONE_FORMAT)) {
      outputTimeZone = getStringWithAltKeys(config, TIMESTAMP_TIMEZONE_FORMAT, "GMT");
    } else {
      outputTimeZone = getStringWithAltKeys(config, TIMESTAMP_OUTPUT_TIMEZONE_FORMAT, "");
    }
    return !outputTimeZone.trim().isEmpty() ? DateTimeZone.forTimeZone(TimeZone.getTimeZone(outputTimeZone)) : null;
  }

}
