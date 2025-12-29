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

import org.apache.hudi.avro.processors.DateLogicalTypeProcessor;
import org.apache.hudi.avro.processors.DecimalLogicalTypeProcessor;
import org.apache.hudi.avro.processors.DurationLogicalTypeProcessor;
import org.apache.hudi.avro.processors.EnumTypeProcessor;
import org.apache.hudi.avro.processors.FixedTypeProcessor;
import org.apache.hudi.avro.processors.JsonFieldProcessor;
import org.apache.hudi.avro.processors.LocalTimestampMicroLogicalTypeProcessor;
import org.apache.hudi.avro.processors.LocalTimestampMilliLogicalTypeProcessor;
import org.apache.hudi.avro.processors.Parser;
import org.apache.hudi.avro.processors.TimeMicroLogicalTypeProcessor;
import org.apache.hudi.avro.processors.TimeMilliLogicalTypeProcessor;
import org.apache.hudi.avro.processors.TimestampMicroLogicalTypeProcessor;
import org.apache.hudi.avro.processors.TimestampMilliLogicalTypeProcessor;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieJsonToAvroConversionException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Converts Json record to Avro Generic Record.
 */
public class MercifulJsonConverter {

  // For each schema (keyed by full name), stores a mapping of schema field name to json field name to account for sanitization of fields
  private static final Map<String, Map<String, String>> SANITIZED_FIELD_MAPPINGS = new ConcurrentHashMap<>();
  private final Map<HoodieSchemaType, JsonFieldProcessor> fieldTypeProcessorMap;
  private final Map<String, JsonFieldProcessor> fieldLogicalTypeProcessorMap;

  protected final ObjectMapper mapper;

  protected final String invalidCharMask;
  protected final boolean shouldSanitize;

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
    this(new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS), shouldSanitize, invalidCharMask);
  }

  /**
   * Allows a configured ObjectMapper to be passed for converting json records to avro record.
   */
  public MercifulJsonConverter(ObjectMapper mapper, boolean shouldSanitize, String invalidCharMask) {
    this.mapper = mapper;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
    this.fieldTypeProcessorMap = getFieldTypeProcessors();
    this.fieldLogicalTypeProcessorMap = getLogicalFieldTypeProcessors();
  }

  /**
   * Converts json to Avro generic record.
   * NOTE: if sanitization is needed for avro conversion, the schema input to this method is already sanitized.
   *       During the conversion here, we sanitize the fields in the data
   *
   * @param json Json record
   * @param schema Schema
   */
  public GenericRecord convert(String json, HoodieSchema schema) {
    try {
      Map<String, Object> jsonObjectMap = mapper.readValue(json, Map.class);
      return convertJsonToAvro(jsonObjectMap, schema);
    } catch (HoodieException | IOException e) {
      throw new HoodieJsonToAvroConversionException("failed to convert json to avro", e);
    }
  }

  /**
   * Clear between fetches. If the schema changes or if two tables have the same schemaFullName then
   * can be issues
   */
  public static void clearCache(String schemaFullName) {
    SANITIZED_FIELD_MAPPINGS.remove(schemaFullName);
  }

  private GenericRecord convertJsonToAvro(Map<String, Object> inputJson, HoodieSchema schema) {
    GenericRecord avroRecord = new GenericData.Record(schema.toAvroSchema());
    for (HoodieSchemaField f : schema.getFields()) {
      Object val = shouldSanitize ? getFieldFromJson(f, inputJson, schema.getFullName(), invalidCharMask) : inputJson.get(f.name());
      if (val != null) {
        avroRecord.put(f.pos(), convertJsonField(val, f.name(), f.schema()));
      }
    }
    return avroRecord;
  }

  protected static Object getFieldFromJson(final HoodieSchemaField fieldSchema, final Map<String, Object> inputJson, final String schemaFullName, final String invalidCharMask) {
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

  protected Object convertJsonField(Object value, String name, HoodieSchema schema) {

    if (schema.isNullable()) {
      if (value == null) {
        return null;
      } else {
        schema = schema.getNonNullType();
      }
    } else if (value == null) {
      // Always fail on null for non-nullable schemas
      throw buildConversionException(String.format("Symbol %s not in enum", value.toString()),
          schema.getFullName(), schema, shouldSanitize, invalidCharMask);
    }

    return convertField(value, name, schema);
  }

  private Object convertField(Object value, String name, HoodieSchema schema) {
    JsonFieldProcessor processor = getProcessorForSchema(schema);
    return processor.convertField(value, name, schema);
  }

  protected JsonFieldProcessor getProcessorForSchema(HoodieSchema schema) {
    JsonFieldProcessor processor = null;

    // 3 cases to consider: customized logicalType, logicalType, and type.
    String customizedLogicalType = (String) schema.getProp("logicalType");
    LogicalType logicalType = schema.toAvroSchema().getLogicalType();
    HoodieSchemaType type = schema.getType();
    if (customizedLogicalType != null && !customizedLogicalType.isEmpty()) {
      processor = fieldLogicalTypeProcessorMap.get(customizedLogicalType);
    } else if (logicalType != null) {
      processor = fieldLogicalTypeProcessorMap.get(logicalType.getName());
    } else {
      processor = fieldTypeProcessorMap.get(type);
    }

    ValidationUtils.checkArgument(
        processor != null, String.format("JsonConverter cannot handle type: %s", type));
    return processor;
  }

  /**
   * Build type processor map for each avro type.
   */
  private Map<HoodieSchemaType, JsonFieldProcessor> getFieldTypeProcessors() {
    Map<HoodieSchemaType, JsonFieldProcessor> fieldTypeProcessors = new EnumMap<>(HoodieSchemaType.class);
    fieldTypeProcessors.put(HoodieSchemaType.STRING, generateStringTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.BOOLEAN, generateBooleanTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.DOUBLE, generateDoubleTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.FLOAT, generateFloatTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.INT, generateIntTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.LONG, generateLongTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.ARRAY, generateArrayTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.RECORD, generateRecordTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.ENUM, generateEnumTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.MAP, generateMapTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.BYTES, generateBytesTypeHandler());
    fieldTypeProcessors.put(HoodieSchemaType.FIXED, generateFixedTypeHandler());
    return Collections.unmodifiableMap(fieldTypeProcessors);
  }

  private Map<String, JsonFieldProcessor> getLogicalFieldTypeProcessors() {
    return CollectionUtils.createImmutableMap(
      Pair.of(AvroLogicalTypeEnum.DECIMAL.getValue(), generateDecimalLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.TIME_MICROS.getValue(), generateTimeMicroLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.TIME_MILLIS.getValue(), generateTimeMilliLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.DATE.getValue(), generateDateLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MICROS.getValue(), generateLocalTimeStampMicroLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS.getValue(), generateLocalTimeStampMilliLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.TIMESTAMP_MICROS.getValue(), generateTimestampMicroLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.TIMESTAMP_MILLIS.getValue(), generateTimestampMilliLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.DURATION.getValue(), generateDurationLogicalTypeHandler()),
      Pair.of(AvroLogicalTypeEnum.UUID.getValue(), generateStringTypeHandler()));
  }

  protected JsonFieldProcessor generateDecimalLogicalTypeHandler() {
    return new DecimalToAvroLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateTimeMicroLogicalTypeHandler() {
    return new TimeMicroLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateTimeMilliLogicalTypeHandler() {
    return new TimeMilliLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateDateLogicalTypeHandler() {
    return new DateToAvroLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateLocalTimeStampMicroLogicalTypeHandler() {
    return new LocalTimestampMicroLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateLocalTimeStampMilliLogicalTypeHandler() {
    return new LocalTimestampMilliLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateTimestampMicroLogicalTypeHandler() {
    return new TimestampMicroLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateTimestampMilliLogicalTypeHandler() {
    return new TimestampMilliLogicalTypeProcessor();
  }

  protected JsonFieldProcessor generateDurationLogicalTypeHandler() {
    return new DurationToAvroLogicalTypeProcessor();
  }

  private class DecimalToAvroLogicalTypeProcessor extends DecimalLogicalTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      if (!isValidDecimalTypeConfig(schema)) {
        return Pair.of(false, null);
      }

      // Case 1: Input is a list. It is expected to be raw Fixed byte array input, and we only support
      // parsing it to Fixed avro type.
      if (value instanceof List<?> && schema.getType() == HoodieSchemaType.FIXED) {
        JsonFieldProcessor processor = generateFixedTypeHandler();
        return processor.convert(value, name, schema);
      }

      // Case 2: Input is a number or String number.
      LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.toAvroSchema().getLogicalType();
      Pair<Boolean, BigDecimal> parseResult = parseObjectToBigDecimal(value, schema);
      if (Boolean.FALSE.equals(parseResult.getLeft())) {
        return Pair.of(false, null);
      }
      BigDecimal bigDecimal = parseResult.getRight();

      switch (schema.getType()) {
        case BYTES:
          // Convert to primitive Arvo type that logical type Decimal uses.
          ByteBuffer byteBuffer = new Conversions.DecimalConversion().toBytes(bigDecimal, schema.toAvroSchema(), decimalType);
          return Pair.of(true, byteBuffer);
        case FIXED:
          GenericFixed fixedValue = new Conversions.DecimalConversion().toFixed(bigDecimal, schema.toAvroSchema(), decimalType);
          return Pair.of(true, fixedValue);
        default: {
          return Pair.of(false, null);
        }
      }
    }
  }

  private static class DurationToAvroLogicalTypeProcessor extends DurationLogicalTypeProcessor {

    /**
     * Convert the given object to Avro object with schema whose logical type is duration.
     */
    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {

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
      return Pair.of(true, new GenericData.Fixed(schema.toAvroSchema(), buffer.array()));
    }
  }

  private static class DateToAvroLogicalTypeProcessor extends DateLogicalTypeProcessor {

    @Override
    public Pair<Boolean, Object> convert(
        Object value, String name, HoodieSchema schema) {
      return convertCommon(
          new Parser.IntParser() {
            @Override
            public Pair<Boolean, Object> handleStringValue(String value) {
              if (!isWellFormedDateTime(value)) {
                return Pair.of(false, null);
              }
              Pair<Boolean, LocalDate> result = convertToLocalDate(value);
              if (!result.getLeft()) {
                return Pair.of(false, null);
              }
              LocalDate date = result.getRight();
              int daysSinceEpoch = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
              return Pair.of(true, daysSinceEpoch);
            }
          },
          value, schema);
    }
  }

  protected JsonFieldProcessor generateBooleanTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        if (value instanceof Boolean) {
          return Pair.of(true, value);
        }
        return Pair.of(false, null);
      }
    };
  }

  protected JsonFieldProcessor generateIntTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        if (value instanceof Number) {
          return Pair.of(true, ((Number) value).intValue());
        } else if (value instanceof String) {
          return Pair.of(true, Integer.valueOf((String) value));
        }
        return Pair.of(false, null);
      }
    };
  }

  protected JsonFieldProcessor generateDoubleTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        if (value instanceof Number) {
          return Pair.of(true, ((Number) value).doubleValue());
        } else if (value instanceof String) {
          return Pair.of(true, Double.valueOf((String) value));
        }
        return Pair.of(false, null);
      }
    };
  }

  protected JsonFieldProcessor generateFloatTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        if (value instanceof Number) {
          return Pair.of(true, ((Number) value).floatValue());
        } else if (value instanceof String) {
          return Pair.of(true, Float.valueOf((String) value));
        }
        return Pair.of(false, null);
      }
    };
  }

  protected JsonFieldProcessor generateLongTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        if (value instanceof Number) {
          return Pair.of(true, ((Number) value).longValue());
        } else if (value instanceof String) {
          return Pair.of(true, Long.valueOf((String) value));
        }
        return Pair.of(false, null);
      }
    };
  }

  private static JsonFieldProcessor generateStringTypeHandler() {
    return new StringProcessor();
  }

  private static class StringProcessor extends JsonFieldProcessor {
    private static final ObjectMapper STRING_MAPPER = new ObjectMapper();

    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      if (value instanceof String) {
        return Pair.of(true, value);
      } else {
        try {
          return Pair.of(true, STRING_MAPPER.writeValueAsString(value));
        } catch (IOException ex) {
          return Pair.of(false, null);
        }
      }
    }
  }

  protected JsonFieldProcessor generateBytesTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        // Should return ByteBuffer (see GenericData.isBytes())
        return Pair.of(true, ByteBuffer.wrap(value.toString().getBytes()));
      }
    };
  }

  protected JsonFieldProcessor generateFixedTypeHandler() {
    return new AvroFixedTypeProcessor();
  }

  private static class AvroFixedTypeProcessor extends FixedTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      return Pair.of(true, new GenericData.Fixed(
          schema.toAvroSchema(), convertToJavaObject(value, name, schema)));
    }
  }

  private static class AvroEnumTypeProcessor extends EnumTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      return Pair.of(true, new GenericData.EnumSymbol(schema.toAvroSchema(), convertToJavaObject(value, name, schema)));
    }
  }

  protected JsonFieldProcessor generateEnumTypeHandler() {
    return new AvroEnumTypeProcessor();
  }

  protected JsonFieldProcessor generateRecordTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        return Pair.of(true, convertJsonToAvro((Map<String, Object>) value, schema));
      }
    };
  }

  protected JsonFieldProcessor generateArrayTypeHandler() {
    return new JsonFieldProcessor() {
      private List<Object> convertToJavaObject(Object value, String name, HoodieSchema schema) {
        HoodieSchema elementSchema = schema.getElementType();
        List<Object> listRes = new ArrayList<>();
        for (Object v : (List) value) {
          listRes.add(convertJsonField(v, name, elementSchema));
        }
        return listRes;
      }

      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        return Pair.of(true, new GenericData.Array<>(
            schema.toAvroSchema(),
            convertToJavaObject(
                value,
                name,
                schema)));
      }
    };
  }

  protected JsonFieldProcessor generateMapTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        HoodieSchema valueSchema = schema.getValueType();
        Map<String, Object> mapRes = new HashMap<>();
        for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
          mapRes.put(v.getKey(), convertJsonField(v.getValue(), name, valueSchema));
        }
        return Pair.of(true, mapRes);
      }
    };
  }

  protected HoodieJsonToAvroConversionException buildConversionException(Object value, String fieldName, HoodieSchema schema, boolean shouldSanitize, String invalidCharMask) {
    String errorMsg;
    if (shouldSanitize) {
      errorMsg = String.format("Json to Avro Type conversion error for field %s, %s for %s. Field sanitization is enabled with a mask of %s.", fieldName, value, schema, invalidCharMask);
    } else {
      errorMsg = String.format("Json to Avro Type conversion error for field %s, %s for %s", fieldName, value, schema);
    }
    return new HoodieJsonToAvroConversionException(errorMsg);
  }

}