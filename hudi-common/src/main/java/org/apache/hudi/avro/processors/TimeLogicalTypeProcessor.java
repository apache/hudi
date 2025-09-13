/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro.processors;

import org.apache.hudi.avro.AvroLogicalTypeEnum;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * Base Class for converting object to avro logical type TimeMilli/TimeMicro.
 */
public abstract class TimeLogicalTypeProcessor extends JsonFieldProcessor {

  // Logical type the processor is handling.
  private final AvroLogicalTypeEnum logicalTypeEnum;

  public TimeLogicalTypeProcessor(AvroLogicalTypeEnum logicalTypeEnum) {
    this.logicalTypeEnum = logicalTypeEnum;
  }

  /**
   * Main function that convert input to Object with java data type specified by schema
   */
  public Pair<Boolean, Object> convertCommon(Parser parser, Object value, Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType == null) {
      return Pair.of(false, null);
    }
    logicalType.validate(schema);
    if (value instanceof Number) {
      return parser.handleNumberValue((Number) value);
    }
    if (value instanceof String) {
      String valStr = (String) value;
      if (ALL_DIGITS_WITH_OPTIONAL_SIGN.matcher(valStr).matches()) {
        return parser.handleStringNumber(valStr);
      } else {
        return parser.handleStringValue(valStr);
      }
    }
    return Pair.of(false, null);
  }

  protected DateTimeFormatter getDateTimeFormatter() {
    DateTimeParseContext ctx = DATE_TIME_PARSE_CONTEXT_MAP.get(logicalTypeEnum);
    return ctx == null ? null : ctx.dateTimeFormatter;
  }

  protected Pattern getDateTimePattern() {
    DateTimeParseContext ctx = DATE_TIME_PARSE_CONTEXT_MAP.get(logicalTypeEnum);
    return ctx == null ? null : ctx.dateTimePattern;
  }

  // Depending on the logical type the processor handles, they use different parsing context
  // when they need to parse a timestamp string in handleStringValue.
  private static class DateTimeParseContext {
    public DateTimeParseContext(DateTimeFormatter dateTimeFormatter, Pattern dateTimePattern) {
      this.dateTimeFormatter = dateTimeFormatter;
      this.dateTimePattern = dateTimePattern;
    }

    public final Pattern dateTimePattern;

    public final DateTimeFormatter dateTimeFormatter;
  }

  private static final Map<AvroLogicalTypeEnum, DateTimeParseContext> DATE_TIME_PARSE_CONTEXT_MAP = getParseContext();

  private static Map<AvroLogicalTypeEnum, DateTimeParseContext> getParseContext() {
    // The pattern is derived from ISO_LOCAL_DATE_TIME definition with the relaxation on the separator.
    DateTimeFormatter localDateTimeFormatter = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral('T')
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()
        .append(ISO_LOCAL_TIME)
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT)
        .withChronology(IsoChronology.INSTANCE);
    // Formatter for parsing timestamp.
    // The pattern is derived from ISO_OFFSET_DATE_TIME definition with the relaxation on the separator.
    // Pattern asserts the string is
    // <optional sign><Year>-<Month>-<Day><separator><Hour>:<Minute> + optional <second> + optional <fractional second> + optional <zone offset>
    // <separator> is 'T' or ' '
    // For example, "2024-07-13T11:36:01.951Z", "2024-07-13T11:36:01.951+01:00",
    // "2024-07-13T11:36:01Z", "2024-07-13T11:36:01+01:00",
    // "2024-07-13 11:36:01.951Z", "2024-07-13 11:36:01.951+01:00".
    // See TestMercifulJsonConverter#timestampLogicalTypeGoodCaseTest
    // and #timestampLogicalTypeBadTest for supported and unsupported cases.
    DateTimeParseContext dateTimestampParseContext = new DateTimeParseContext(
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(localDateTimeFormatter)
            .optionalStart()
            .appendOffsetId()
            .optionalEnd()
            .parseDefaulting(ChronoField.OFFSET_SECONDS, 0L)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
            .withChronology(IsoChronology.INSTANCE),
        null /* match everything*/);
    // Formatter for parsing time of day. The pattern is derived from ISO_LOCAL_TIME definition.
    // Pattern asserts the string is
    // <optional sign><Hour>:<Minute> + optional <second> + optional <fractional second>
    // For example, "11:36:01.951".
    // See TestMercifulJsonConverter#timeLogicalTypeTest
    // and #timeLogicalTypeBadCaseTest for supported and unsupported cases.
    DateTimeParseContext dateTimeParseContext = new DateTimeParseContext(
        ISO_LOCAL_TIME,
        Pattern.compile("^[+-]?\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?"));
    // Formatter for parsing local timestamp.
    // The pattern is derived from ISO_LOCAL_DATE_TIME definition with the relaxation on the separator.
    // Pattern asserts the string is
    // <optional sign><Year>-<Month>-<Day><separator><Hour>:<Minute> + optional <second> + optional <fractional second>
    // <separator> is 'T' or ' '
    // For example, "2024-07-13T11:36:01.951", "2024-07-13 11:36:01.951".
    // See TestMercifulJsonConverter#localTimestampLogicalTypeGoodCaseTest
    // and #localTimestampLogicalTypeBadTest for supported and unsupported cases.
    DateTimeParseContext localTimestampParseContext = new DateTimeParseContext(
        localDateTimeFormatter,
        Pattern.compile("^[+-]?\\d{4,10}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?")
    );
    // Formatter for parsing local date. The pattern is derived from ISO_LOCAL_DATE definition.
    // Pattern asserts the string is
    // <optional sign><Year>-<Month>-<Day>
    // For example, "2024-07-13".
    // See TestMercifulJsonConverter#dateLogicalTypeTest for supported and unsupported cases.
    DateTimeParseContext localDateParseContext = new DateTimeParseContext(
        ISO_LOCAL_DATE,
        Pattern.compile("^[+-]?\\d{4,10}-\\d{2}-\\d{2}?")
    );

    EnumMap<AvroLogicalTypeEnum, DateTimeParseContext> ctx = new EnumMap<>(AvroLogicalTypeEnum.class);
    ctx.put(AvroLogicalTypeEnum.TIME_MICROS, dateTimeParseContext);
    ctx.put(AvroLogicalTypeEnum.TIME_MILLIS, dateTimeParseContext);
    ctx.put(AvroLogicalTypeEnum.DATE, localDateParseContext);
    ctx.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MICROS, localTimestampParseContext);
    ctx.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS, localTimestampParseContext);
    ctx.put(AvroLogicalTypeEnum.TIMESTAMP_MICROS, dateTimestampParseContext);
    ctx.put(AvroLogicalTypeEnum.TIMESTAMP_MILLIS, dateTimestampParseContext);
    return Collections.unmodifiableMap(ctx);
  }

  // Pattern validating if it is an number in string form.
  // Only check at most 19 digits as this is the max num of digits for LONG.MAX_VALUE to contain the cost of regex matching.
  protected static final Pattern ALL_DIGITS_WITH_OPTIONAL_SIGN = Pattern.compile("^[-+]?\\d{1,19}$");

  /**
   * Check if the given string is a well-formed date time string.
   * If no pattern is defined, it will always return true.
   */
  protected boolean isWellFormedDateTime(String value) {
    Pattern pattern = getDateTimePattern();
    return pattern == null || pattern.matcher(value).matches();
  }

  protected Pair<Boolean, Instant> convertToInstantTime(String input) {
    // Parse the input timestamp
    // The input string is assumed in the format:
    // <optional sign><Year>-<Month>-<Day><separator><Hour>:<Minute> + optional <second> + optional <fractional second> + optional <zone offset>
    // <separator> is 'T' or ' '
    Instant time = null;
    try {
      ZonedDateTime dateTime = ZonedDateTime.parse(input, getDateTimeFormatter());
      time = dateTime.toInstant();
    } catch (DateTimeParseException ignore) {
      /* ignore */
    }
    return Pair.of(time != null, time);
  }

  protected Pair<Boolean, LocalTime> convertToLocalTime(String input) {
    // Parse the input timestamp, DateTimeFormatter.ISO_LOCAL_TIME is implied here
    LocalTime time = null;
    try {
      // Try parsing as an ISO date
      time = LocalTime.parse(input);
    } catch (DateTimeParseException ignore) {
      /* ignore */
    }
    return Pair.of(time != null, time);
  }

  protected Pair<Boolean, LocalDateTime> convertToLocalDateTime(String input) {
    // Parse the input timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME is implied here
    LocalDateTime time = null;
    try {
      // Try parsing as an ISO date
      time = LocalDateTime.parse(input, getDateTimeFormatter());
    } catch (DateTimeParseException ignore) {
      /* ignore */
    }
    return Pair.of(time != null, time);
  }

  protected Pair<Boolean, Object> convertDateTime(
      String value,
      Function<LocalTime, Object> localTimeFunction,
      Function<Instant, Object> instantTimeFunction) {

    if (!isWellFormedDateTime(value)) {
      return Pair.of(false, null);
    }

    if (localTimeFunction != null) {
      Pair<Boolean, LocalTime> result = convertToLocalTime(value);
      if (!result.getLeft()) {
        return Pair.of(false, null);
      }
      return Pair.of(true, localTimeFunction.apply(result.getRight()));
    }

    if (instantTimeFunction != null) {
      Pair<Boolean, Instant> result = convertToInstantTime(value);
      if (!result.getLeft()) {
        return Pair.of(false, null);
      }
      return Pair.of(true, instantTimeFunction.apply(result.getRight()));
    }

    return Pair.of(false, null);  // Fallback in case of error
  }
}
