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
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Converts Json record to Avro Generic Record.
 */
public class MercifulJsonConverter {

  private static final Map<Schema.Type, JsonToAvroFieldProcessor> FIELD_TYPE_PROCESSORS = getFieldTypeProcessors();

  // For each schema (keyed by full name), stores a mapping of schema field name to json field name to account for sanitization of fields
  private static final Map<String, Map<String, String>> SANITIZED_FIELD_MAPPINGS = new ConcurrentHashMap<>();

  private final ObjectMapper mapper;

  private final String invalidCharMask;
  private final boolean shouldSanitize;
  
  /**
   * Build type processor map for each avro type.
   */
  private static Map<Schema.Type, JsonToAvroFieldProcessor> getFieldTypeProcessors() {
    return Collections.unmodifiableMap(new HashMap<Schema.Type, JsonToAvroFieldProcessor>() {
      {
        put(Type.STRING, generateStringTypeHandler());
        put(Type.BOOLEAN, generateBooleanTypeHandler());
        put(Type.DOUBLE, generateDoubleTypeHandler());
        put(Type.FLOAT, generateFloatTypeHandler());
        put(Type.INT, generateIntTypeHandler());
        put(Type.LONG, generateLongTypeHandler());
        put(Type.ARRAY, generateArrayTypeHandler());
        put(Type.RECORD, generateRecordTypeHandler());
        put(Type.ENUM, generateEnumTypeHandler());
        put(Type.MAP, generateMapTypeHandler());
        put(Type.BYTES, generateBytesTypeHandler());
        put(Type.FIXED, generateFixedTypeHandler());
      }
    });
  }

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

    JsonToAvroFieldProcessor processor = FIELD_TYPE_PROCESSORS.get(schema.getType());
    if (null != processor) {
      return processor.convertToAvro(value, name, schema, shouldSanitize, invalidCharMask);
    }
    throw new IllegalArgumentException("JsonConverter cannot handle type: " + schema.getType());
  }

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
        return Pair.of(true, ByteBuffer.wrap(getUTF8Bytes(value.toString())));
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
        GenericRecord result = new GenericData.Record(schema);
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

  /**
   * Exception Class for any schema conversion issue.
   */
  public static class HoodieJsonToAvroConversionException extends HoodieException {

    private Object value;
    private String fieldName;
    private Schema schema;

    private boolean shouldSanitize;
    private String invalidCharMask;

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
