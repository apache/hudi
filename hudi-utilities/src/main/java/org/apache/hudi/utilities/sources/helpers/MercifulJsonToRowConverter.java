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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.avro.processors.DateLogicalTypeProcessor;
import org.apache.hudi.avro.processors.DecimalLogicalTypeProcessor;
import org.apache.hudi.avro.processors.DurationLogicalTypeProcessor;
import org.apache.hudi.avro.processors.EnumTypeProcessor;
import org.apache.hudi.avro.processors.FixedTypeProcessor;
import org.apache.hudi.avro.processors.JsonFieldProcessor;
import org.apache.hudi.avro.processors.LocalTimestampMicroLogicalTypeProcessor;
import org.apache.hudi.avro.processors.LocalTimestampMilliLogicalTypeProcessor;
import org.apache.hudi.avro.processors.Parser;
import org.apache.hudi.avro.processors.TimestampMicroLogicalTypeProcessor;
import org.apache.hudi.avro.processors.TimestampMilliLogicalTypeProcessor;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.stats.SparkValueMetadataUtils;
import org.apache.hudi.stats.ValueType;
import org.apache.hudi.utilities.exception.HoodieJsonToRowConversionException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

/**
 * Converts Json record to Row Record.
 */
public class MercifulJsonToRowConverter extends MercifulJsonConverter {
  private final boolean useJava8api;

  /**
   * Allows enabling sanitization and allows choice of invalidCharMask for sanitization
   */
  public MercifulJsonToRowConverter(boolean shouldSanitize, String invalidCharMask, boolean useJava8api) {
    this(new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS), shouldSanitize, invalidCharMask, useJava8api);
  }

  /**
   * Allows a configured ObjectMapper to be passed for converting json records to row.
   */
  public MercifulJsonToRowConverter(ObjectMapper mapper, boolean shouldSanitize, String invalidCharMask, boolean useJava8api) {
    super(mapper, shouldSanitize, invalidCharMask);
    this.useJava8api = useJava8api;
  }

  /**
   * Converts json to row.
   * NOTE: if sanitization is needed for row conversion, the schema input to this method is already sanitized.
   * During the conversion here, we sanitize the fields in the data
   *
   * @param json   Json record
   * @param schema HoodieSchema
   */
  public Row convertToRow(String json, HoodieSchema schema) {
    try {
      Map<String, Object> jsonObjectMap = mapper.readValue(json, Map.class);
      return convertJsonToRow(jsonObjectMap, schema);
    } catch (HoodieException | IOException e) {
      throw new HoodieJsonToRowConversionException("Failed to convert json to row", e);
    }
  }

  private Row convertJsonToRow(Map<String, Object> inputJson, HoodieSchema schema) {
    List<HoodieSchemaField> fields = schema.getFields();
    List<Object> values = new ArrayList<>(Collections.nCopies(fields.size(), null));

    for (HoodieSchemaField f : fields) {
      Object val = shouldSanitize ? getFieldFromJson(f, inputJson, schema.getFullName(), invalidCharMask) : inputJson.get(f.name());
      if (val != null) {
        values.set(f.pos(), SparkValueMetadataUtils.convertJavaTypeToSparkType(convertJsonField(val, f.name(), f.schema()), useJava8api));
      }
    }
    return RowFactory.create(values.toArray());
  }

  private class DecimalToRowLogicalTypeProcessor extends DecimalLogicalTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      if (!isValidDecimalTypeConfig(schema)) {
        return Pair.of(false, null);
      }

      HoodieSchema.Decimal decimalSchema = (HoodieSchema.Decimal) schema;
      if (decimalSchema.isFixed() && value instanceof List<?>) {
        // Case 1: Input is a list. It is expected to be raw Fixed byte array input, and we only support
        // parsing it to Fixed type.
        JsonFieldProcessor processor = generateFixedTypeHandler();
        Pair<Boolean, Object> fixedTypeResult = processor.convert(value, name, schema);
        if (fixedTypeResult.getLeft()) {
          byte[] byteArray = (byte[]) fixedTypeResult.getRight();
          Schema avroSchema = schema.toAvroSchema();
          GenericFixed fixedValue = new GenericData.Fixed(avroSchema, byteArray);
          // Convert the GenericFixed to BigDecimal
          return Pair.of(true, new Conversions
              .DecimalConversion()
              .fromFixed(
                  fixedValue,
                  avroSchema,
                  avroSchema.getLogicalType()
              )
          );
        }
      }

      // Case 2: Input is a number or String number or base64 encoded string number
      Pair<Boolean, BigDecimal> parseResult = parseObjectToBigDecimal(value, decimalSchema);
      return Pair.of(parseResult.getLeft(), parseResult.getRight());
    }
  }

  @Override
  protected JsonFieldProcessor generateDecimalLogicalTypeHandler() {
    return new DecimalToRowLogicalTypeProcessor();
  }

  @Override
  protected JsonFieldProcessor generateDateLogicalTypeHandler() {
    return new DateToRowLogicalTypeProcessor();
  }

  @Override
  protected JsonFieldProcessor generateDurationLogicalTypeHandler() {
    return new DurationToRowLogicalTypeProcessor();
  }

  private static class DurationToRowLogicalTypeProcessor extends DurationLogicalTypeProcessor {

    @Override
    public Pair<Boolean, Object> convert(
        Object value, String name, HoodieSchema schema) {
      throw new HoodieJsonToRowConversionException("Duration type is not supported in Row object");
    }
  }

  private static class DateToRowLogicalTypeProcessor extends DateLogicalTypeProcessor {

    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      return convertCommon(new Parser.DateParser(), value, schema);
    }
  }

  @Override
  protected JsonFieldProcessor generateBytesTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        return Pair.of(true, value.toString().getBytes());
      }
    };
  }

  @Override
  protected JsonFieldProcessor generateFixedTypeHandler() {
    return new FixedToRowTypeProcessor();
  }

  private static class FixedToRowTypeProcessor extends FixedTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      return Pair.of(true, convertToJavaObject(value, name, schema));
    }
  }

  @Override
  protected JsonFieldProcessor generateEnumTypeHandler() {
    return new EnumToRowTypeProcessor();
  }

  private static class EnumToRowTypeProcessor extends EnumTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
      return Pair.of(true, convertToJavaObject(value, name, schema));
    }
  }

  @Override
  protected JsonFieldProcessor generateRecordTypeHandler() {
    return new JsonFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        return Pair.of(true, convertJsonToRow((Map<String, Object>) value, schema));
      }
    };
  }

  @Override
  protected JsonFieldProcessor generateTimestampMilliLogicalTypeHandler() {
    return new TimestampMilliToRowLogicalTypeProcessor();
  }

  private static class TimestampMilliToRowLogicalTypeProcessor extends TimestampMilliLogicalTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(
        Object value, String name, HoodieSchema schema) {
      Pair<Boolean, Object> result = convertCommon(
          new Parser.LongParser() {
            @Override
            public Pair<Boolean, Object> handleStringValue(String value) {
              return convertDateTime(
                  value,
                  null,
                  time -> Instant.EPOCH.until(time, ChronoField.MILLI_OF_SECOND.getBaseUnit()));  // Diff in millis
            }
          },
          value, schema);
      if (result.getLeft()) {
        return Pair.of(true, Instant.ofEpochMilli((Long) result.getRight()));
      }
      return Pair.of(false, null);
    }
  }

  @Override
  protected JsonFieldProcessor generateTimestampMicroLogicalTypeHandler() {
    return new TimestampMicroToRowLogicalTypeProcessor();
  }

  @Override
  protected JsonFieldProcessor generateLocalTimeStampMicroLogicalTypeHandler() {
    return new LocalTimestampMicroToRowLogicalTypeProcessor();
  }

  @Override
  protected JsonFieldProcessor generateLocalTimeStampMilliLogicalTypeHandler() {
    return new LocalTimestampMilliToRowLogicalTypeProcessor();
  }

  private static class TimestampMicroToRowLogicalTypeProcessor extends TimestampMicroLogicalTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(
        Object value, String name, HoodieSchema schema) {
      Pair<Boolean, Object> result = convertCommon(
          new Parser.LongParser() {
            @Override
            public Pair<Boolean, Object> handleStringValue(String value) {
              return convertDateTime(
                  value,
                  null,
                  time -> Instant.EPOCH.until(time, ChronoField.MICRO_OF_SECOND.getBaseUnit()));  // Diff in micro
            }
          },
          value, schema);
      if (result.getLeft()) {
        return Pair.of(true, DateTimeUtils.microsToInstant((Long) result.getRight()));
      }
      return Pair.of(false, null);
    }
  }

  private static class LocalTimestampMicroToRowLogicalTypeProcessor extends LocalTimestampMicroLogicalTypeProcessor {

    @Override
    public Pair<Boolean, Object> convert(
        Object value, String name, HoodieSchema schema) {
      return convertCommon(
          new Parser.LongParser() {
            @Override
            public Pair<Boolean, Object> handleNumberValue(Number value) {
              Pair<Boolean, Object> result = super.handleNumberValue(value);
              if (result.getLeft()) {
                return Pair.of(true, ValueType.castToLocalTimestampMicros(result.getRight(), null));
              }
              return result;
            }

            @Override
            public Pair<Boolean, Object> handleStringNumber(String value) {
              Pair<Boolean, Object> result = super.handleStringNumber(value);
              if (result.getLeft()) {
                return Pair.of(true, ValueType.castToLocalTimestampMicros(result.getRight(), null));
              }
              return result;
            }

            @Override
            public Pair<Boolean, Object> handleStringValue(String value) {
              if (!isWellFormedDateTime(value)) {
                return Pair.of(false, null);
              }
              Pair<Boolean, LocalDateTime> result = convertToLocalDateTime(value);
              if (!result.getLeft()) {
                return Pair.of(false, null);
              }
              return Pair.of(true, result.getRight());
            }
          },
          value, schema);
    }
  }

  private static class LocalTimestampMilliToRowLogicalTypeProcessor extends LocalTimestampMilliLogicalTypeProcessor {
    @Override
    public Pair<Boolean, Object> convert(
        Object value, String name, HoodieSchema schema) {
      return convertCommon(
          new Parser.LongParser() {
            @Override
            public Pair<Boolean, Object> handleNumberValue(Number value) {
              Pair<Boolean, Object> result = super.handleNumberValue(value);
              if (result.getLeft()) {
                return Pair.of(true, ValueType.castToLocalTimestampMillis(result.getRight(), null));
              }
              return result;
            }

            @Override
            public Pair<Boolean, Object> handleStringNumber(String value) {
              Pair<Boolean, Object> result = super.handleStringNumber(value);
              if (result.getLeft()) {
                return Pair.of(true, ValueType.castToLocalTimestampMillis(result.getRight(), null));
              }
              return result;
            }
            
            @Override
            public Pair<Boolean, Object> handleStringValue(String value) {
              if (!isWellFormedDateTime(value)) {
                return Pair.of(false, null);
              }
              Pair<Boolean, LocalDateTime> result = convertToLocalDateTime(value);
              if (!result.getLeft()) {
                return Pair.of(false, null);
              }
              return Pair.of(true, result.getRight());
            }
          },
          value, schema);
    }
  }

  @Override
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
        return Pair.of(true,
            convertToJavaObject(
                value,
                name,
                schema).toArray());
      }
    };
  }

  @Override
  protected JsonFieldProcessor generateMapTypeHandler() {
    return new JsonFieldProcessor() {
      public Map<String, Object> convertToJavaObject(
          Object value,
          String name,
          HoodieSchema schema) {
        HoodieSchema valueSchema = schema.getValueType();
        Map<String, Object> mapRes = new HashMap<>();
        for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
          mapRes.put(v.getKey(), convertJsonField(v.getValue(), name, valueSchema));
        }
        return mapRes;
      }

      @Override
      public Pair<Boolean, Object> convert(Object value, String name, HoodieSchema schema) {
        return Pair.of(true, JavaConverters
            .mapAsScalaMapConverter(
                convertToJavaObject(
                    value,
                    name,
                    schema)).asScala());
      }
    };
  }
}