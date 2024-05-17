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

package org.apache.hudi.avro;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Converts Json record to Avro Generic Record.
 */
public class MercifulJsonConverter {

  // For each schema (keyed by full name), stores a mapping of schema field name to json field name to account for sanitization of fields
  private static final Map<String, Map<String, String>> SANITIZED_FIELD_MAPPINGS = new ConcurrentHashMap<>();

  private final ObjectMapper mapper;

  private final String invalidCharMask;
  private final boolean shouldSanitize;

  /**
   * Uses a default objectMapper to deserialize a json string.
   */
  public MercifulJsonConverter() {
    this(false, "__");
  }


  /**
   * Allows enabling sanitization and allows choice of invalidCharMask for sanitization
   */
  public MercifulJsonConverter(boolean shouldSanitize, String invalidCharMask) {
    this(new ObjectMapper(), shouldSanitize, invalidCharMask);
  }

  /**
   * Allows a configured ObjectMapper to be passed for converting json records to avro record.
   */
  public MercifulJsonConverter(ObjectMapper mapper, boolean shouldSanitize, String invalidCharMask) {
    this.mapper = mapper;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
  }

  /**
   * Converts json to Avro generic record.
   * NOTE: if sanitization is needed for avro conversion, the schema input to this method is already sanitized.
   *       During the conversion here, we sanitize the fields in the data
   *
   * @param json Json record
   * @param schema Schema
   */
  public GenericRecord convert(String json, Schema schema) {
    try {
      Map<String, Object> jsonObjectMap = mapper.readValue(json, Map.class);
      return convertJsonToAvro(jsonObjectMap, schema, shouldSanitize, invalidCharMask);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  /**
   * Clear between fetches. If the schema changes or if two tables have the same schemaFullName then
   * can be issues
   */
  public static void clearCache(String schemaFullName) {
    SANITIZED_FIELD_MAPPINGS.remove(schemaFullName);
  }

  private static GenericRecord convertJsonToAvro(Map<String, Object> inputJson, Schema schema, boolean shouldSanitize, String invalidCharMask) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      Object val = shouldSanitize ? getFieldFromJson(f, inputJson, schema.getFullName(), invalidCharMask) : inputJson.get(f.name());
      if (val != null) {
        avroRecord.put(f.pos(), convertJsonToAvroField(val, f.name(), f.schema(), shouldSanitize, invalidCharMask));
      }
    }
    return avroRecord;
  }

  private static Object getFieldFromJson(final Schema.Field fieldSchema, final Map<String, Object> inputJson, final String schemaFullName, final String invalidCharMask) {
    Map<String, String> schemaToJsonFieldNames = SANITIZED_FIELD_MAPPINGS.computeIfAbsent(schemaFullName, unused -> new ConcurrentHashMap<>());
    if (!schemaToJsonFieldNames.containsKey(fieldSchema.name())) {
      // if we don't have field mapping, proactively populate as many as possible based on input json
      for (String inputFieldName : inputJson.keySet()) {
        // we expect many fields won't need sanitization so check if un-sanitized field name is already present
        if (!schemaToJsonFieldNames.containsKey(inputFieldName)) {
          String sanitizedJsonFieldName = HoodieAvroUtils.sanitizeName(inputFieldName, invalidCharMask);
          schemaToJsonFieldNames.putIfAbsent(sanitizedJsonFieldName, inputFieldName);
        }
      }
    }
    Object match = inputJson.get(schemaToJsonFieldNames.getOrDefault(fieldSchema.name(), fieldSchema.name()));
    if (match != null) {
      return match;
    }
    // Check if there is an alias match
    for (String alias : fieldSchema.aliases()) {
      if (inputJson.containsKey(alias)) {
        return inputJson.get(alias);
      }
    }
    return null;
  }

  private static Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    Schema.Type firstType = types.get(0).getType();
    return firstType.equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

  private static boolean isOptional(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION) && schema.getTypes().size() == 2
        && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
        || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }

  private static Object convertJsonToAvroField(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {

    if (isOptional(schema)) {
      if (value == null) {
        return null;
      } else {
        schema = getNonNull(schema);
      }
    } else if (value == null) {
      // Always fail on null for non-nullable schemas
      throw new HoodieJsonToAvroConversionException(null, name, schema, shouldSanitize, invalidCharMask);
    }

    return JsonToAvroFieldProcessorUtil.convertToAvro(value, name, schema, shouldSanitize, invalidCharMask);
  }

  private static class JsonToAvroFieldProcessorUtil {
    /**
     * Base Class for converting json to avro fields.
     */
    private abstract static class JsonToAvroFieldProcessor implements Serializable {

      public Object convertToAvro(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        Pair<Boolean, Object> res = convert(value, name, schema, shouldSanitize, invalidCharMask);
        if (!res.getLeft()) {
          throw new HoodieJsonToAvroConversionException(value, name, schema, shouldSanitize, invalidCharMask);
        }
        return res.getRight();
      }

      protected abstract Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask);
    }

    public static Object convertToAvro(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
      JsonToAvroFieldProcessor processor = getProcessorForSchema(schema);
      return processor.convertToAvro(value, name, schema, shouldSanitize, invalidCharMask);
    }

    private static JsonToAvroFieldProcessor getProcessorForSchema(Schema schema) {
      JsonToAvroFieldProcessor processor = null;

      // 3 cases to consider: customized logicalType, logicalType, and type.
      String customizedLogicalType = schema.getProp("logicalType");
      LogicalType logicalType = schema.getLogicalType();
      Type type = schema.getType();
      if (customizedLogicalType != null && !customizedLogicalType.isEmpty()) {
        processor = AVRO_LOGICAL_TYPE_FIELD_PROCESSORS.get(customizedLogicalType);
      } else if (logicalType != null) {
        processor = AVRO_LOGICAL_TYPE_FIELD_PROCESSORS.get(logicalType.getName());
      } else {
        processor = AVRO_TYPE_FIELD_TYPE_PROCESSORS.get(type);
      }

      if (processor == null) {
        throw new IllegalArgumentException(String.format("JsonConverter cannot handle type: %s", type));
      }
      return processor;
    }

    // Avro primitive and complex type processors.
    private static final Map<Schema.Type, JsonToAvroFieldProcessor> AVRO_TYPE_FIELD_TYPE_PROCESSORS = getFieldTypeProcessors();
    // Avro logical type processors.
    private static final Map<String, JsonToAvroFieldProcessor> AVRO_LOGICAL_TYPE_FIELD_PROCESSORS = getLogicalFieldTypeProcessors();

    /**
     * Build type processor map for each avro type.
     */
    private static Map<Schema.Type, JsonToAvroFieldProcessor> getFieldTypeProcessors() {
      Map<Schema.Type, JsonToAvroFieldProcessor> fieldTypeProcessors = new EnumMap<>(Schema.Type.class);
      fieldTypeProcessors.put(Type.STRING, generateStringTypeHandler());
      fieldTypeProcessors.put(Type.BOOLEAN, generateBooleanTypeHandler());
      fieldTypeProcessors.put(Type.DOUBLE, generateDoubleTypeHandler());
      fieldTypeProcessors.put(Type.FLOAT, generateFloatTypeHandler());
      fieldTypeProcessors.put(Type.INT, generateIntTypeHandler());
      fieldTypeProcessors.put(Type.LONG, generateLongTypeHandler());
      fieldTypeProcessors.put(Type.ARRAY, generateArrayTypeHandler());
      fieldTypeProcessors.put(Type.RECORD, generateRecordTypeHandler());
      fieldTypeProcessors.put(Type.ENUM, generateEnumTypeHandler());
      fieldTypeProcessors.put(Type.MAP, generateMapTypeHandler());
      fieldTypeProcessors.put(Type.BYTES, generateBytesTypeHandler());
      fieldTypeProcessors.put(Type.FIXED, generateFixedTypeHandler());
      return Collections.unmodifiableMap(fieldTypeProcessors);
    }

    private static Map<String, JsonToAvroFieldProcessor> getLogicalFieldTypeProcessors() {
      Map<String, JsonToAvroFieldProcessor> logicalFieldTypeProcessors = new HashMap<>();
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.DECIMAL.getValue(), new DecimalLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIME_MICROS.getValue(), new TimeMicroLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIME_MILLIS.getValue(), new TimeMilliLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.DATE.getValue(), new DateLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MICROS.getValue(), new LocalTimestampMicroLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS.getValue(), new LocalTimestampMilliLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIMESTAMP_MICROS.getValue(), new TimestampMicroLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIMESTAMP_MILLIS.getValue(), new TimestampMilliLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.DURATION.getValue(), new DurationLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.UUID.getValue(), generateStringTypeHandler());
      return Collections.unmodifiableMap(logicalFieldTypeProcessors);
    }

    private static class DecimalLogicalTypeProcessor extends JsonToAvroFieldProcessor {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {

        if (!isValidDecimalTypeConfig(schema)) {
          return Pair.of(false, null);
        }

        // Case 1: Input is a list. It is expected to be raw Fixed byte array input, and we only support
        // parsing it to Fixed avro type.
        if (value instanceof List<?> && schema.getType() == Type.FIXED) {
          JsonToAvroFieldProcessor processor = generateFixedTypeHandler();
          return processor.convert(value, name, schema, shouldSanitize, invalidCharMask);
        }

        // Case 2: Input is a number or String number.
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        Pair<Boolean, BigDecimal> parseResult = parseObjectToBigDecimal(value);
        if (Boolean.FALSE.equals(parseResult.getLeft())) {
          return Pair.of(false, null);
        }
        BigDecimal bigDecimal = parseResult.getRight();

        // As we don't do rounding, the validation will enforce the scale part and the integer part are all within the
        // limit. As a result, if scale is 2 precision is 5, we only allow 3 digits for the integer.
        // Allowed: 123.45, 123, 0.12
        // Disallowed: 1234 (4 digit integer while the scale has already reserved 2 digit out of the 5 digit precision)
        //             123456, 0.12345
        if (bigDecimal.scale() > decimalType.getScale()
            || (bigDecimal.precision() - bigDecimal.scale()) > (decimalType.getPrecision() - decimalType.getScale())) {
          // Correspond to case
          // org.apache.avro.AvroTypeException: Cannot encode decimal with scale 5 as scale 2 without rounding.
          // org.apache.avro.AvroTypeException: Cannot encode decimal with scale 3 as scale 2 without rounding
          return Pair.of(false, null);
        }

        switch (schema.getType()) {
          case BYTES:
            // Convert to primitive Arvo type that logical type Decimal uses.
            ByteBuffer byteBuffer = new Conversions.DecimalConversion().toBytes(bigDecimal, schema, decimalType);
            return Pair.of(true, byteBuffer);
          case FIXED:
            GenericFixed fixedValue = new Conversions.DecimalConversion().toFixed(bigDecimal, schema, decimalType);
            return Pair.of(true, fixedValue);
          default: {
            return Pair.of(false, null);
          }
        }
      }

      /**
       * Check if the given schema is a valid decimal type configuration.
       */
      private static boolean isValidDecimalTypeConfig(Schema schema) {
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        // At the time when the schema is found not valid when it is parsed, the Avro Schema.parse will just silently
        // set the schema to be null instead of throwing exceptions. Correspondingly, we just check if it is null here.
        if (decimalType == null) {
          return false;
        }
        // Even though schema is validated at schema parsing phase, still validate here to be defensive.
        decimalType.validate(schema);
        return true;
      }

      /**
       * Parse the object to BigDecimal.
       *
       * @param obj Object to be parsed
       * @return Pair object, with left as boolean indicating if the parsing was successful and right as the
       * BigDecimal value.
       */
      private static Pair<Boolean, BigDecimal> parseObjectToBigDecimal(Object obj) {
        // Case 1: Object is a number.
        if (obj instanceof Number) {
          return Pair.of(true, BigDecimal.valueOf(((Number) obj).doubleValue()));
        }

        // Case 2: Object is a number in String format.
        if (obj instanceof String) {
          BigDecimal bigDecimal = null;
          try {
            bigDecimal = new BigDecimal(((String) obj));
          } catch (java.lang.NumberFormatException ignored) {
            /* ignore */
          }
          return Pair.of(bigDecimal != null, bigDecimal);
        }
        return Pair.of(false, null);
      }
    }

    private static class DurationLogicalTypeProcessor extends JsonToAvroFieldProcessor {
      private static final int NUM_ELEMENTS_FOR_DURATION_TYPE = 3;

      /**
       * We expect the input to be a list of 3 integers representing months, days and milliseconds.
       */
      private boolean isValidDurationInput(Object value) {
        if (!(value instanceof List<?>)) {
          return false;
        }
        List<?> list = (List<?>) value;
        if (list.size() != NUM_ELEMENTS_FOR_DURATION_TYPE) {
          return false;
        }
        for (Object element : list) {
          if (!(element instanceof Integer)) {
            return false;
          }
        }
        return true;
      }

      /**
       * Convert the given object to Avro object with schema whose logical type is duration.
       */
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {

        if (!isValidDurationTypeConfig(schema)) {
          return Pair.of(false, null);
        }
        if (!isValidDurationInput(value)) {
          return Pair.of(false, null);
        }
        // After the validation the input can be safely cast to List<Integer> with 3 elements.
        List<?> list = (List<?>) value;
        List<Integer> converval = list.stream()
            .filter(Integer.class::isInstance)
            .map(Integer.class::cast)
            .collect(Collectors.toList());

        ByteBuffer buffer = ByteBuffer.allocate(schema.getFixedSize()).order(ByteOrder.LITTLE_ENDIAN);
        for (Integer element : converval) {
          buffer.putInt(element);  // months
        }
        return Pair.of(true, new GenericData.Fixed(schema, buffer.array()));
      }

      /**
       * Check if the given schema is a valid decimal type configuration.
       */
      private static boolean isValidDurationTypeConfig(Schema schema) {
        String durationTypeName = AvroLogicalTypeEnum.DURATION.getValue();
        LogicalType durationType = schema.getLogicalType();
        String durationTypeProp = schema.getProp("logicalType");
        // 1. The Avro type should be "Fixed".
        // 2. Fixed size must be of 12 bytes as it hold 3 integers.
        // 3. Logical type name should be "duration". The name might be stored in different places based on Avro version
        //    being used here.
        return schema.getType().equals(Type.FIXED)
            && schema.getFixedSize() == Integer.BYTES * NUM_ELEMENTS_FOR_DURATION_TYPE
            && (durationType != null && durationType.getName().equals(durationTypeName)
            || durationTypeProp != null && durationTypeProp.equals(durationTypeName));
      }
    }

    /**
     * Processor utility handling Number inputs. Consumed by TimeLogicalTypeProcessor. 
     */
    private interface NumericParser {
      // Convert the input number to Avro data type according to the class
      // implementing this interface.
      Pair<Boolean, Object> handleNumberValue(Number value);

      // Convert the input number to Avro data type according to the class
      // implementing this interface.
      // @param value the input number in string format.
      Pair<Boolean, Object> handleStringNumber(String value);

      interface IntParser extends NumericParser {
        @Override
        default Pair<Boolean, Object> handleNumberValue(Number value) {
          return Pair.of(true, value.intValue());
        }

        @Override
        default Pair<Boolean, Object> handleStringNumber(String value) {
          return Pair.of(true, Integer.parseInt(value));
        }
      }

      interface LongParser extends NumericParser {
        @Override
        default Pair<Boolean, Object> handleNumberValue(Number value) {
          return Pair.of(true, value.longValue());
        }

        @Override
        default Pair<Boolean, Object> handleStringNumber(String value) {
          return Pair.of(true, Long.parseLong(value));
        }
      }
    }

    /**
     * Base Class for converting object to avro logical type TimeMilli/TimeMicro.
     */
    private abstract static class TimeLogicalTypeProcessor extends JsonToAvroFieldProcessor implements NumericParser {

      protected static final LocalDateTime LOCAL_UNIX_EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0);

      // Logical type the processor is handling.
      private final AvroLogicalTypeEnum logicalTypeEnum;

      public TimeLogicalTypeProcessor(AvroLogicalTypeEnum logicalTypeEnum) {
        this.logicalTypeEnum = logicalTypeEnum;
      }

      /**
       * Main function that convert input to Object with java data type specified by schema
       */
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType == null) {
          return Pair.of(false, null);
        }
        logicalType.validate(schema);
        if (value instanceof Number) {
          return handleNumberValue((Number) value);
        }
        if (value instanceof String) {
          String valStr = (String) value;
          if (ALL_DIGITS_WITH_OPTIONAL_SIGN.matcher(valStr).matches()) {
            return handleStringNumber(valStr);
          } else if (isWellFormedDateTime(valStr)) {
            return handleStringValue(valStr);
          }
        }
        return Pair.of(false, null);
      }

      // Handle the case when the input is a string that may be parsed as a time.
      protected abstract Pair<Boolean, Object> handleStringValue(String value);

      protected DateTimeFormatter getDateTimeFormatter() {
        DateTimeParseContext ctx = DATE_TIME_PARSE_CONTEXT_MAP.get(logicalTypeEnum);
        assert ctx != null : String.format("%s should have configured date time context.", logicalTypeEnum.getValue());
        return ctx.dateTimeFormatter;
      }

      protected Pattern getDateTimePattern() {
        DateTimeParseContext ctx = DATE_TIME_PARSE_CONTEXT_MAP.get(logicalTypeEnum);
        assert ctx != null : String.format("%s should have configured date time context.", logicalTypeEnum.getValue());
        return ctx.dateTimePattern;
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
        // Formatter for parsing local timestamp. It ignores the time zone info of the timestamp.
        // No pattern is defined as ISO_INSTANT format internal is not clear.
        DateTimeParseContext dateTimestampParseContext = new DateTimeParseContext(
            DateTimeFormatter.ISO_INSTANT,
            null /* match everything*/);
        // Formatter for parsing timestamp with time zone. The pattern is derived from ISO_LOCAL_TIME definition.
        // Pattern asserts the string is
        // <optional sign><Hour>:<Minute> + optional <second> + optional <fractional second>
        DateTimeParseContext dateTimeParseContext = new DateTimeParseContext(
            DateTimeFormatter.ISO_LOCAL_TIME,
            Pattern.compile("^[+-]?\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?"));
        // Formatter for parsing local time. The pattern is derived from ISO_LOCAL_TIME definition.
        // Pattern asserts the string is
        // <optional sign><Year>-<Month>-<Day>T<Hour>:<Minute> + optional <second> + optional <fractional second>
        DateTimeParseContext localTimestampParseContext = new DateTimeParseContext(
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            Pattern.compile("^[+-]?\\d{4,10}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?")
        );
        // Formatter for parsing local date. The pattern is derived from ISO_LOCAL_DATE definition.
        // Pattern asserts the string is
        // <optional sign><Year>-<Month>-<Day>
        DateTimeParseContext localDateParseContext = new DateTimeParseContext(
            DateTimeFormatter.ISO_LOCAL_DATE,
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
      private boolean isWellFormedDateTime(String value) {
        Pattern pattern = getDateTimePattern();
        return pattern == null || pattern.matcher(value).matches();
      }

      protected Pair<Boolean, Instant> convertToInstantTime(String input) {
        // Parse the input timestamp, DateTimeFormatter.ISO_INSTANT is implied here
        Instant time = null;
        try {
          time = Instant.parse(input);
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
    }

    private static class DateLogicalTypeProcessor extends TimeLogicalTypeProcessor
        implements NumericParser.IntParser {
      public DateLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.DATE);
      }

      @Override
      public Pair<Boolean, Object> handleStringValue(String value) {
        Pair<Boolean, LocalDate> result = convertToLocalDate(value);
        if (!result.getLeft()) {
          return Pair.of(false, null);
        }
        LocalDate date = result.getRight();
        int daysSinceEpoch = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
        return Pair.of(true, daysSinceEpoch);
      }

      private Pair<Boolean, LocalDate> convertToLocalDate(String input) {
        // Parse the input timestamp, DateTimeFormatter.ISO_LOCAL_TIME is implied here
        LocalDate date = null;
        try {
          // Try parsing as an ISO date
          date = LocalDate.parse(input);
        } catch (DateTimeParseException ignore) {
          /* ignore */
        }
        return Pair.of(date != null, date);
      }
    }

    /**
     * Processor for TimeMilli logical type.
     */
    private static class TimeMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor
        implements NumericParser.IntParser {
      public TimeMilliLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIME_MILLIS);
      }

      @Override
      public Pair<Boolean, Object> handleStringValue(String value) {
        Pair<Boolean, LocalTime> result = convertToLocalTime(value);
        if (!result.getLeft()) {
          return Pair.of(false, null);
        }
        LocalTime time = result.getRight();
        Integer millisOfDay = time.toSecondOfDay() * 1000 + time.getNano() / 1000000;
        return Pair.of(true, millisOfDay);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class TimeMicroLogicalTypeProcessor extends TimeLogicalTypeProcessor
        implements NumericParser.LongParser {
      public TimeMicroLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIME_MICROS);
      }

      @Override
      public Pair<Boolean, Object> handleStringValue(String value) {
        Pair<Boolean, LocalTime> result = convertToLocalTime(value);
        if (!result.getLeft()) {
          return Pair.of(false, null);
        }
        LocalTime time = result.getRight();
        Long microsOfDay = (long) time.toSecondOfDay() * 1000000 + time.getNano() / 1000;
        return Pair.of(true, microsOfDay);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class LocalTimestampMicroLogicalTypeProcessor extends TimeLogicalTypeProcessor
        implements NumericParser.LongParser {
      public LocalTimestampMicroLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MICROS);
      }

      @Override
      public Pair<Boolean, Object> handleStringValue(String value) {
        Pair<Boolean, LocalDateTime> result = convertToLocalDateTime(value);
        if (!result.getLeft()) {
          return Pair.of(false, null);
        }
        LocalDateTime time = result.getRight();

        // Calculate the difference in milliseconds
        long diffInMicros = LOCAL_UNIX_EPOCH.until(time, ChronoField.MICRO_OF_SECOND.getBaseUnit());
        return Pair.of(true, diffInMicros);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class TimestampMicroLogicalTypeProcessor extends TimeLogicalTypeProcessor
        implements NumericParser.LongParser {
      public TimestampMicroLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIMESTAMP_MICROS);
      }

      @Override
      public Pair<Boolean, Object> handleStringValue(String value) {
        Pair<Boolean, Instant> result = convertToInstantTime(value);
        if (!result.getLeft()) {
          return Pair.of(false, null);
        }
        Instant time = result.getRight();

        // Calculate the difference in milliseconds
        long diffInMicro = Instant.EPOCH.until(time, ChronoField.MICRO_OF_SECOND.getBaseUnit());
        return Pair.of(true, diffInMicro);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class LocalTimestampMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor
        implements NumericParser.LongParser {
      public LocalTimestampMilliLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS);
      }

      @Override
      public Pair<Boolean, Object> handleStringValue(String value) {
        Pair<Boolean, LocalDateTime> result = convertToLocalDateTime(value);
        if (!result.getLeft()) {
          return Pair.of(false, null);
        }
        LocalDateTime time = result.getRight();

        // Calculate the difference in milliseconds
        long diffInMillis = LOCAL_UNIX_EPOCH.until(time, ChronoField.MILLI_OF_SECOND.getBaseUnit());
        return Pair.of(true, diffInMillis);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class TimestampMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor
        implements NumericParser.LongParser {
      public TimestampMilliLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIMESTAMP_MILLIS);
      }

      @Override
      public Pair<Boolean, Object> handleStringValue(String value) {
        Pair<Boolean, Instant> result = convertToInstantTime(value);
        if (!result.getLeft()) {
          return Pair.of(false, null);
        }
        Instant time = result.getRight();

        // Calculate the difference in milliseconds
        long diffInMillis = Instant.EPOCH.until(time, ChronoField.MILLI_OF_SECOND.getBaseUnit());
        return Pair.of(true, diffInMillis);
      }
    }

    private static JsonToAvroFieldProcessor generateBooleanTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Boolean) {
            return Pair.of(true, value);
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateIntTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).intValue());
          } else if (value instanceof String) {
            return Pair.of(true, Integer.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateDoubleTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).doubleValue());
          } else if (value instanceof String) {
            return Pair.of(true, Double.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateFloatTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).floatValue());
          } else if (value instanceof String) {
            return Pair.of(true, Float.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateLongTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).longValue());
          } else if (value instanceof String) {
            return Pair.of(true, Long.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateStringTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, value.toString());
        }
      };
    }

    private static JsonToAvroFieldProcessor generateBytesTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          // Should return ByteBuffer (see GenericData.isBytes())
          return Pair.of(true, ByteBuffer.wrap(value.toString().getBytes()));
        }
      };
    }

    private static JsonToAvroFieldProcessor generateFixedTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          // The ObjectMapper use List to represent FixedType
          // eg: "decimal_val": [0, 0, 14, -63, -52] will convert to ArrayList<Integer>
          List<Integer> converval = (List<Integer>) value;
          byte[] src = new byte[converval.size()];
          for (int i = 0; i < converval.size(); i++) {
            src[i] = converval.get(i).byteValue();
          }
          byte[] dst = new byte[schema.getFixedSize()];
          System.arraycopy(src, 0, dst, 0, Math.min(schema.getFixedSize(), src.length));
          return Pair.of(true, new GenericData.Fixed(schema, dst));
        }
      };
    }

    private static JsonToAvroFieldProcessor generateEnumTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (schema.getEnumSymbols().contains(value.toString())) {
            return Pair.of(true, new GenericData.EnumSymbol(schema, value.toString()));
          }
          throw new HoodieJsonToAvroConversionException(String.format("Symbol %s not in enum", value.toString()),
              schema.getFullName(), schema, shouldSanitize, invalidCharMask);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateRecordTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, convertJsonToAvro((Map<String, Object>) value, schema, shouldSanitize, invalidCharMask));
        }
      };
    }

    private static JsonToAvroFieldProcessor generateArrayTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          Schema elementSchema = schema.getElementType();
          List<Object> listRes = new ArrayList<>();
          for (Object v : (List) value) {
            listRes.add(convertJsonToAvroField(v, name, elementSchema, shouldSanitize, invalidCharMask));
          }
          return Pair.of(true, new GenericData.Array<>(schema, listRes));
        }
      };
    }

    private static JsonToAvroFieldProcessor generateMapTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          Schema valueSchema = schema.getValueType();
          Map<String, Object> mapRes = new HashMap<>();
          for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
            mapRes.put(v.getKey(), convertJsonToAvroField(v.getValue(), name, valueSchema, shouldSanitize, invalidCharMask));
          }
          return Pair.of(true, mapRes);
        }
      };
    }
  }

  /**
   * Exception Class for any schema conversion issue.
   */
  public static class HoodieJsonToAvroConversionException extends HoodieException {

    private final Object value;

    private final String fieldName;
    private final Schema schema;
    private final boolean shouldSanitize;
    private final String invalidCharMask;

    public HoodieJsonToAvroConversionException(Object value, String fieldName, Schema schema, boolean shouldSanitize, String invalidCharMask) {
      this.value = value;
      this.fieldName = fieldName;
      this.schema = schema;
      this.shouldSanitize = shouldSanitize;
      this.invalidCharMask = invalidCharMask;
    }

    @Override
    public String toString() {
      if (shouldSanitize) {
        return String.format("Json to Avro Type conversion error for field %s, %s for %s. Field sanitization is enabled with a mask of %s.", fieldName, value, schema, invalidCharMask);
      }
      return String.format("Json to Avro Type conversion error for field %s, %s for %s", fieldName, value, schema);
    }
  }
}
