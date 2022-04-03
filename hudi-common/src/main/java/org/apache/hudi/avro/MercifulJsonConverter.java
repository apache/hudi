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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Json record to Avro Generic Record.
 */
public class MercifulJsonConverter {

  private static final Map<Schema.Type, JsonToAvroFieldProcessor> FIELD_TYPE_PROCESSORS = getFieldTypeProcessors();

  private final ObjectMapper mapper;

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
    this(new ObjectMapper());
  }

  /**
   * Allows a configured ObjectMapper to be passed for converting json records to avro record.
   */
  public MercifulJsonConverter(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /**
   * Converts json to Avro generic record.
   *
   * @param json Json record
   * @param schema Schema
   */
  public GenericRecord convert(String json, Schema schema) {
    try {
      Map<String, Object> jsonObjectMap = mapper.readValue(json, Map.class);
      return convertJsonToAvro(jsonObjectMap, schema);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private static GenericRecord convertJsonToAvro(Map<String, Object> inputJson, Schema schema) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      Object val = inputJson.get(f.name());
      if (val != null) {
        avroRecord.put(f.pos(), convertJsonToAvroField(val, f.name(), f.schema()));
      }
    }
    return avroRecord;
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

  private static Object convertJsonToAvroField(Object value, String name, Schema schema) {

    if (isOptional(schema)) {
      if (value == null) {
        return null;
      } else {
        schema = getNonNull(schema);
      }
    } else if (value == null) {
      // Always fail on null for non-nullable schemas
      throw new HoodieJsonToAvroConversionException(null, name, schema);
    }

    JsonToAvroFieldProcessor processor = FIELD_TYPE_PROCESSORS.get(schema.getType());
    if (null != processor) {
      return processor.convertToAvro(value, name, schema);
    }
    throw new IllegalArgumentException("JsonConverter cannot handle type: " + schema.getType());
  }

  /**
   * Base Class for converting json to avro fields.
   */
  private abstract static class JsonToAvroFieldProcessor implements Serializable {

    public Object convertToAvro(Object value, String name, Schema schema) {
      Pair<Boolean, Object> res = convert(value, name, schema);
      if (!res.getLeft()) {
        throw new HoodieJsonToAvroConversionException(value, name, schema);
      }
      return res.getRight();
    }

    protected abstract Pair<Boolean, Object> convert(Object value, String name, Schema schema);
  }

  private static JsonToAvroFieldProcessor generateBooleanTypeHandler() {
    return new JsonToAvroFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
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
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
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
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
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
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
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
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
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
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
        return Pair.of(true, value.toString());
      }
    };
  }

  private static JsonToAvroFieldProcessor generateBytesTypeHandler() {
    return new JsonToAvroFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
        // Should return ByteBuffer (see GenericData.isBytes())
        return Pair.of(true, ByteBuffer.wrap(value.toString().getBytes()));
      }
    };
  }

  private static JsonToAvroFieldProcessor generateFixedTypeHandler() {
    return new JsonToAvroFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
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
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
        if (schema.getEnumSymbols().contains(value.toString())) {
          return Pair.of(true, new GenericData.EnumSymbol(schema, value.toString()));
        }
        throw new HoodieJsonToAvroConversionException(String.format("Symbol %s not in enum", value.toString()),
            schema.getFullName(), schema);
      }
    };
  }

  private static JsonToAvroFieldProcessor generateRecordTypeHandler() {
    return new JsonToAvroFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
        GenericRecord result = new GenericData.Record(schema);
        return Pair.of(true, convertJsonToAvro((Map<String, Object>) value, schema));
      }
    };
  }

  private static JsonToAvroFieldProcessor generateArrayTypeHandler() {
    return new JsonToAvroFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
        Schema elementSchema = schema.getElementType();
        List<Object> listRes = new ArrayList<>();
        for (Object v : (List) value) {
          listRes.add(convertJsonToAvroField(v, name, elementSchema));
        }
        return Pair.of(true, new GenericData.Array<>(schema, listRes));
      }
    };
  }

  private static JsonToAvroFieldProcessor generateMapTypeHandler() {
    return new JsonToAvroFieldProcessor() {
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema) {
        Schema valueSchema = schema.getValueType();
        Map<String, Object> mapRes = new HashMap<>();
        for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
          mapRes.put(v.getKey(), convertJsonToAvroField(v.getValue(), name, valueSchema));
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

    public HoodieJsonToAvroConversionException(Object value, String fieldName, Schema schema) {
      this.value = value;
      this.fieldName = fieldName;
      this.schema = schema;
    }

    @Override
    public String toString() {
      return String.format("Json to Avro Type conversion error for field %s, %s for %s", fieldName, value, schema);
    }
  }
}
