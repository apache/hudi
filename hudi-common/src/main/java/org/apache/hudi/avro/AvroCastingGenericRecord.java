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

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;


/**
 * Implementation of avro generic record that uses casting for implicit schema evolution
 */
public class AvroCastingGenericRecord implements GenericRecord {
  private final IndexedRecord record;
  private final Schema readerSchema;
  private final Map<Integer, UnaryOperator<Object>> fieldConverters;

  private AvroCastingGenericRecord(IndexedRecord record, Schema readerSchema, Map<Integer, UnaryOperator<Object>> fieldConverters) {
    this.record = record;
    this.readerSchema = readerSchema;
    this.fieldConverters = fieldConverters;
  }

  @Override
  public void put(int i, Object v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int i) {
    if (fieldConverters.containsKey(i)) {
      return fieldConverters.get(i).apply(record.get(i));
    }
    return record.get(i);
  }

  @Override
  public Schema getSchema() {
    return readerSchema;
  }

  @Override
  public void put(String key, Object v) {
    Schema.Field field = getSchema().getField(key);
    if (field == null) {
      throw new AvroRuntimeException("Not a valid schema field: " + key);
    }
    put(field.pos(), v);
  }

  @Override
  public Object get(String key) {
    Schema.Field field = getSchema().getField(key);
    if (field == null) {
      throw new AvroRuntimeException("Not a valid schema field: " + key);
    }
    return get(field.pos());
  }


  /**
   * Avro schema evolution does not support promotion from numbers to string. This function returns true if
   * it will be necessary to rewrite the record to support the evolution.
   * NOTE: this does not determine whether the schema evolution is valid. It is just trying to find if the schema evolves from
   * a number to string, as quick as possible.
   */
  public static Boolean recordNeedsRewriteForExtendedAvroSchemaEvolution(Schema writerSchema, Schema readerSchema) {
    if (writerSchema.equals(readerSchema)) {
      return false;
    }
    switch (readerSchema.getType()) {
      case RECORD:
        Map<String, Schema.Field> writerFields = new HashMap<>();
        for (Schema.Field field : writerSchema.getFields()) {
          writerFields.put(field.name(), field);
        }
        for (Schema.Field field : readerSchema.getFields()) {
          if (writerFields.containsKey(field.name())) {
            if (recordNeedsRewriteForExtendedAvroSchemaEvolution(writerFields.get(field.name()).schema(), field.schema())) {
              return true;
            }
          }
        }
        return false;
      case ARRAY:
        if (writerSchema.getType().equals(ARRAY)) {
          return recordNeedsRewriteForExtendedAvroSchemaEvolution(writerSchema.getElementType(), readerSchema.getElementType());
        }
        return false;
      case MAP:
        if (writerSchema.getType().equals(MAP)) {
          return recordNeedsRewriteForExtendedAvroSchemaEvolution(writerSchema.getValueType(), readerSchema.getValueType());
        }
        return false;
      case UNION:
        return recordNeedsRewriteForExtendedAvroSchemaEvolution(getActualSchemaFromUnion(writerSchema), getActualSchemaFromUnion(readerSchema));
      case ENUM:
      case STRING:
      case BYTES:
        return needsRewriteToString(writerSchema);
      default:
        return false;

    }
  }

  private static Boolean needsRewriteToString(Schema schema) {
    switch (schema.getType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
        return true;
      default:
        return false;
    }
  }

  public static UnaryOperator<Object> getConverter(Schema oldSchema, Schema newSchema) {
    switch (newSchema.getType()) {
      case RECORD: {
        Map<Integer, UnaryOperator<Object>> fieldsMap = new HashMap<>();
        for (Schema.Field field : newSchema.getFields()) {
          if (oldSchema.getField(field.name()) != null) {
            Schema.Field oldField = oldSchema.getField(field.name());
            UnaryOperator<Object> op = getConverter(oldField.schema(), field.schema());
            if (op != null) {
              fieldsMap.put(oldField.pos(), op);
            }
          }
        }
        if (fieldsMap.isEmpty()) {
          return null;
        }
        return rec -> {
          ValidationUtils.checkArgument(rec instanceof IndexedRecord, "cannot rewrite record with different type");
          return new AvroCastingGenericRecord((IndexedRecord) rec, newSchema, fieldsMap);
        };
      }
      case ENUM: {
        ValidationUtils.checkArgument(
            oldSchema.getType() == STRING || oldSchema.getType() == Schema.Type.ENUM,
            "Only ENUM or STRING type can be converted ENUM type");
        if (oldSchema.getType() == STRING) {
          return s -> new GenericData.EnumSymbol(newSchema, s);
        }
        return null;
      }
      case ARRAY: {
        UnaryOperator<Object> op = getConverter(oldSchema.getElementType(), newSchema.getElementType());
        if (op == null) {
          return null;
        }
        return a -> {
          ValidationUtils.checkArgument(a instanceof Collection, "cannot rewrite record with different type");
          Collection array = (Collection) a;
          List<Object> newArray = new ArrayList(array.size());
          for (Object element : array) {
            newArray.add(op.apply(element));
          }
          return newArray;
        };
      }

      case MAP: {
        UnaryOperator<Object> op = getConverter(oldSchema.getValueType(), newSchema.getValueType());
        if (op == null) {
          return null;
        }
        return m -> {
          ValidationUtils.checkArgument(m instanceof Map, "cannot rewrite record with different type");
          Map<Object, Object> map = (Map<Object, Object>) m;
          Map<Object, Object> newMap = new HashMap<>(map.size());
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            newMap.put(entry.getKey(), op.apply(entry.getValue()));
          }
          return newMap;
        };
      }
      case UNION:
        return getConverter(getActualSchemaFromUnion(oldSchema), getActualSchemaFromUnion(newSchema));
      case STRING:
        switch (oldSchema.getType()) {
          case ENUM:
            return String::valueOf;
          case BYTES:
            return b -> String.valueOf(((ByteBuffer) b));
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
            return Object::toString;
          default:
            return null;
        }
      case BYTES:
        if (oldSchema.getType() == STRING) {
          return s -> ByteBuffer.wrap((s.toString()).getBytes(StandardCharsets.UTF_8));
        }
        return null;
      default:
        return null;
    }
  }

  private static Schema getActualSchemaFromUnion(Schema schema) {
    Schema actualSchema;
    if (!schema.getType().equals(UNION)) {
      return schema;
    }
    if (schema.getTypes().size() == 2
        && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
      actualSchema = schema.getTypes().get(1);
    } else if (schema.getTypes().size() == 2
        && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
      actualSchema = schema.getTypes().get(0);
    } else if (schema.getTypes().size() == 1) {
      actualSchema = schema.getTypes().get(0);
    } else {
      return schema;
    }
    return actualSchema;
  }
}
