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

package com.uber.hoodie.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Marjority of this is copied from
 * https://github.com/jwills/avro-json/blob/master/src/main/java/com/cloudera/science/avro/
 * common/JsonConverter.java Adjusted for expected behavior of our use cases
 */
public class MercifulJsonConverter {

  private final ObjectMapper mapper = new ObjectMapper();
  private final Schema baseSchema;

  public MercifulJsonConverter(Schema schema) {
    this.baseSchema = schema;
  }


  public GenericRecord convert(String json) throws IOException {
    try {
      return convert(mapper.readValue(json, Map.class), baseSchema);
    } catch (IOException e) {
      throw new IOException("Failed to parse as Json: " + json + "\n\n" + e.getMessage());
    }
  }

  private GenericRecord convert(Map<String, Object> raw, Schema schema)
      throws IOException {
    GenericRecord result = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      String name = f.name();
      Object rawValue = raw.get(name);
      if (rawValue != null) {
        result.put(f.pos(), typeConvert(rawValue, name, f.schema()));
      }
    }

    return result;
  }

  private Object typeConvert(Object value, String name, Schema schema) throws IOException {
    if (isOptional(schema)) {
      if (value == null) {
        return null;
      } else {
        schema = getNonNull(schema);
      }
    } else if (value == null) {
      // Always fail on null for non-nullable schemas
      throw new JsonConversionException(null, name, schema);
    }

    switch (schema.getType()) {
      case BOOLEAN:
        if (value instanceof Boolean) {
          return value;
        }
        break;
      case DOUBLE:
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        break;
      case FLOAT:
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        }
        break;
      case INT:
        if (value instanceof Number) {
          return ((Number) value).intValue();
        }
        break;
      case LONG:
        if (value instanceof Number) {
          return ((Number) value).longValue();
        }
        break;
      case STRING:
        return value.toString();
      case ENUM:
        if (schema.getEnumSymbols().contains(value.toString())) {
          return new GenericData.EnumSymbol(schema, value.toString());
        }
        throw new JsonConversionException(String.format("Symbol %s not in enum", value.toString()),
            schema.getFullName(), schema);
      case RECORD:
        return convert((Map<String, Object>) value, schema);
      case ARRAY:
        Schema elementSchema = schema.getElementType();
        List listRes = new ArrayList();
        for (Object v : (List) value) {
          listRes.add(typeConvert(v, name, elementSchema));
        }
        return listRes;
      case MAP:
        Schema valueSchema = schema.getValueType();
        Map<String, Object> mapRes = new HashMap<String, Object>();
        for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
          mapRes.put(v.getKey(), typeConvert(v.getValue(), name, valueSchema));
        }
        return mapRes;
      default:
        throw new IllegalArgumentException(
            "JsonConverter cannot handle type: " + schema.getType());
    }
    throw new JsonConversionException(value, name, schema);
  }

  private boolean isOptional(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION)
        && schema.getTypes().size() == 2
        && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
        || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }

  private Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

  public static class JsonConversionException extends RuntimeException {

    private Object value;
    private String fieldName;
    private Schema schema;

    public JsonConversionException(Object value, String fieldName, Schema schema) {
      this.value = value;
      this.fieldName = fieldName;
      this.schema = schema;
    }

    @Override
    public String toString() {
      return String.format("Type conversion error for field %s, %s for %s",
          fieldName, value, schema);
    }
  }
}
