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

package org.apache.hudi.utilities.mongo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

public class SchemaUtils {

  // OpLog avro schema field names
  public static final String ID_FIELD = "_id";
  public static final String OP_FIELD = "_op";
  public static final String TS_MS_FIELD = "_ts_ms";
  public static final String PATCH_FIELD = "_patch";
  private static final Logger LOG = LogManager.getLogger(SchemaUtils.class);

  @SuppressWarnings("deprecation") // Backward compatible with Mongo libraries
  public static final JsonWriterSettings STRICT_JSON = JsonWriterSettings.builder()
      .outputMode(JsonMode.STRICT).build();

  public static Schema buildOplogBaseSchema() {
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
        .record("MongoOplog").namespace("com.wish.log").fields();
    return fieldAssembler.optionalString(ID_FIELD).optionalString(OP_FIELD)
        .optionalLong(TS_MS_FIELD).optionalString(PATCH_FIELD).endRecord();
  }

  /**
   * Extract fields from json value to generic record.
   *
   * @param document Bson document
   * @param record   Generic record partially built
   * @param schema   Current Avro schema
   */
  public static void extractAvroValues(BsonDocument document, GenericRecord record, Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      if (field.name().equals(SchemaUtils.ID_FIELD)) {
        continue;
      }

      BsonValue fieldValue = getFieldValue(field, document);
      if (!fieldValue.isNull()) {
        try {
          Object targetValue = typeTransform(field.name(), fieldValue, field.schema());
          record.put(field.name(), targetValue);
        } catch (Exception e) {
          logFieldTypeError(field.name(), field.schema(), fieldValue);
        }
      }
    }
  }

  /**
   * Check if field schema is a Union with NULL as type.
   *
   * @param schema Field schema
   * @return True if it is a union with NULL type
   */
  public static boolean isOptional(Schema schema) {
    if (schema.getType().equals(Type.UNION)) {
      for (Schema element : schema.getTypes()) {
        if (element.getType().equals(Type.NULL)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get non-null element if Union schema, or the schema itself.
   *
   * @param schema Input Avro schema
   * @return Result avro schema
   */
  public static Schema getNonNull(Schema schema) {
    if (schema.getType().equals(Type.UNION)) {
      for (Schema element : schema.getTypes()) {
        if (!element.getType().equals(Type.NULL)) {
          return element;
        }
      }
    }
    return schema;
  }

  private static BsonValue getFieldValue(Schema.Field field, BsonDocument document) {
    for (String aliasName : field.aliases()) {
      BsonValue value = document.get(aliasName, BsonNull.VALUE);
      if (!value.isNull()) {
        return value;
      }
    }
    return document.get(field.name(), BsonNull.VALUE);
  }

  /**
   * Get field by name from Avro record schema.
   *
   * @param fieldName Schema field name or alias
   * @param schema    Record schema
   * @return Schema field
   */
  public static Schema.Field getFieldByName(String fieldName, Schema schema) {
    Schema.Field field = schema.getField(fieldName);
    if (field == null) {
      for (Schema.Field item : schema.getFields()) {
        for (String aliasName : item.aliases()) {
          if (aliasName.equals(fieldName)) {
            field = item;
            break;
          }
        }
      }
    }
    return field;
  }

  /**
   * Convert Bson array to  array list.
   */
  private static ArrayList<Object> toArrayList(String fieldName, BsonValue value, Schema schema) {
    BsonArray array = value.asArray();
    ArrayList<Object> list = new ArrayList<>(array.size());
    for (BsonValue element : array.getValues()) {
      list.add(typeTransform(fieldName, element, schema.getElementType()));
    }
    return list;
  }

  /**
   * Transform Bson value to Generic record field value.
   *
   * @param fieldName Field name
   * @param valueBson Bson value
   * @param schema    Field schema
   * @return Genetic record field value
   */
  public static Object typeTransform(String fieldName, BsonValue valueBson, Schema schema) {
    if (valueBson.isNull()) {
      if (!SchemaUtils.isOptional(schema)) {
        throw new HoodieIOException("Null value is not allowed by the schema: " + fieldName);
      }
      return null;
    }

    Schema targetSchema = SchemaUtils.getNonNull(schema);
    Type targetType = targetSchema.getType();
    Object value = null;
    if (targetType.equals(Type.LONG)) {
      if (valueBson.isNumber()) {
        value = Long.valueOf(valueBson.asNumber().longValue());
      } else if (valueBson.isDateTime()) {
        value = Long.valueOf(valueBson.asDateTime().getValue());
      } else if (valueBson.isTimestamp()) {
        value = Long.valueOf(1000L * valueBson.asTimestamp().getTime());
      } else if (valueBson.isString()) {
        value = Long.parseLong(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.INT)) {
      if (valueBson.isNumber()) {
        value = Integer.valueOf(valueBson.asNumber().intValue());
      } else if (valueBson.isString()) {
        value = Integer.parseInt(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.FLOAT)) {
      if (valueBson.isNumber()) {
        value = Float.valueOf((float) valueBson.asNumber().doubleValue());
      } else if (valueBson.isString()) {
        value = Float.parseFloat(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.DOUBLE)) {
      if (valueBson.isNumber()) {
        value = Double.valueOf(valueBson.asNumber().doubleValue());
      } else if (valueBson.isString()) {
        value = Double.parseDouble(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.BOOLEAN)) {
      if (valueBson.isBoolean()) {
        value = Boolean.valueOf(valueBson.asBoolean().getValue());
      } else if (valueBson.isString()) {
        value = Boolean.parseBoolean(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.STRING)) {
      if (valueBson.isString()) {
        value = valueBson.asString().getValue();
      } else if (valueBson.isObjectId()) {
        value = valueBson.asObjectId().getValue().toString();
      } else if (valueBson.isDocument()) {
        value = valueBson.asDocument().toJson(STRICT_JSON);
      }
    } else if (targetType.equals(Type.ARRAY) && valueBson.isArray()) {
      value = toArrayList(fieldName, valueBson, targetSchema);
    } else if (targetType.equals(Type.BYTES) && valueBson.isBinary()) {
      value = ByteBuffer.wrap(valueBson.asBinary().getData());
    }
    if (value == null) {
      logFieldTypeError(fieldName, targetSchema, valueBson);
    }
    return value;
  }

  private static void logFieldTypeError(String fieldName, Schema schema, BsonValue fieldValue) {
    LOG.error(String.format("Field (%s) value cannot be cast to %s: %s", fieldName,
        SchemaUtils.getNonNull(schema).getType().getName(), fieldValue.toString()));
  }
}
