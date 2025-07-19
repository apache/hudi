/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.apache.hudi.avro.AvroSchemaUtils.isNullable;
import static org.apache.hudi.avro.HoodieAvroUtils.toJavaDate;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

public class HoodieArrayWritableAvroUtils {

  public static ArrayWritable rewriteRecordWithNewSchema(ArrayWritable writable, Schema oldSchema, Schema newSchema, Map<String, String> renameCols) {
    return (ArrayWritable) rewriteRecordWithNewSchema(writable, oldSchema, newSchema, renameCols, new LinkedList<>());
  }

  private static Writable rewriteRecordWithNewSchema(Writable writable, Schema oldAvroSchema, Schema newAvroSchema, Map<String, String> renameCols, Deque<String> fieldNames) {
    if (writable == null) {
      return null;
    }
    Schema oldSchema = AvroSchemaUtils.resolveNullableSchema(oldAvroSchema);
    Schema newSchema = AvroSchemaUtils.resolveNullableSchema(newAvroSchema);
    if (oldSchema.equals(newSchema)) {
      return writable;
    }
    return rewriteRecordWithNewSchemaInternal(writable, oldSchema, newSchema, renameCols, fieldNames);
  }

  private static Writable rewriteRecordWithNewSchemaInternal(Writable writable, Schema oldSchema, Schema newSchema, Map<String, String> renameCols, Deque<String> fieldNames) {
    switch (newSchema.getType()) {
      case RECORD:
        if (!(writable instanceof ArrayWritable)) {
          throw new SchemaCompatibilityException("cannot rewrite record with different type");
        }

        ArrayWritable arrayWritable = (ArrayWritable) writable;
        List<Schema.Field> fields = newSchema.getFields();
        // projection will keep the size from the "from" schema because it gets recycled
        // and if the size changes the reader will fail
        Writable[] values = new Writable[Math.max(fields.size(), arrayWritable.get().length)];
        for (int i = 0; i < fields.size(); i++) {
          Schema.Field field = fields.get(i);
          String fieldName = field.name();
          fieldNames.push(fieldName);
          Schema.Field oldField = oldSchema.getField(field.name());
          if (oldField != null && !renameCols.containsKey(field.name())) {
            values[i] = rewriteRecordWithNewSchema(arrayWritable.get()[oldField.pos()], oldField.schema(), field.schema(), renameCols, fieldNames);
          } else {
            String fieldFullName = HoodieAvroUtils.createFullName(fieldNames);
            String fieldNameFromOldSchema = renameCols.get(fieldFullName);
            // deal with rename
            Schema.Field oldFieldRenamed = fieldNameFromOldSchema == null ? null : oldSchema.getField(fieldNameFromOldSchema);
            if (oldFieldRenamed != null) {
              // find rename
              values[i] = rewriteRecordWithNewSchema(arrayWritable.get()[oldFieldRenamed.pos()], oldFieldRenamed.schema(), field.schema(), renameCols, fieldNames);
            } else {
              // deal with default value
              if (field.defaultVal() instanceof JsonProperties.Null) {
                values[i] = NullWritable.get();
              } else {
                if (!isNullable(field.schema()) && field.defaultVal() == null) {
                  throw new SchemaCompatibilityException("Field " + fieldFullName + " has no default value and is non-nullable");
                }
                if (field.defaultVal() != null) {
                  switch (AvroSchemaUtils.resolveNullableSchema(field.schema()).getType()) {
                    case BOOLEAN:
                      values[i] = new BooleanWritable((Boolean) field.defaultVal());
                      break;
                    case INT:
                      values[i] = new IntWritable((Integer) field.defaultVal());
                      break;
                    case LONG:
                      values[i] = new LongWritable((Long) field.defaultVal());
                      break;
                    case FLOAT:
                      values[i] = new FloatWritable((Float) field.defaultVal());
                      break;
                    case DOUBLE:
                      values[i] = new DoubleWritable((Double) field.defaultVal());
                      break;
                    case STRING:
                      values[i] = new Text(field.defaultVal().toString());
                      break;
                    default:
                      throw new SchemaCompatibilityException("Field " + fieldFullName + " has no default value");
                  }
                }
              }
            }
          }
          fieldNames.pop();
        }
        return new ArrayWritable(Writable.class, values);
        //arrayWritable.set(values);
        //return arrayWritable;

      case ENUM:
        if ((writable instanceof BytesWritable)) {
          return writable;
        }
        if (oldSchema.getType() != Schema.Type.STRING && oldSchema.getType() != Schema.Type.ENUM) {
          throw new SchemaCompatibilityException(String.format("Only ENUM or STRING type can be converted ENUM type. Schema type was %s", oldSchema.getType().getName()));
        }
        if (oldSchema.getType() == Schema.Type.STRING) {
          return new BytesWritable(((Text) writable).copyBytes());
        }
        return writable;
      case ARRAY:
        if (!(writable instanceof ArrayWritable)) {
          throw new SchemaCompatibilityException(String.format("Cannot rewrite %s as an array", writable.getClass().getName()));
        }
        ArrayWritable array = (ArrayWritable) writable;
        fieldNames.push("element");
        for (int i = 0; i < array.get().length; i++) {
          array.get()[i] = rewriteRecordWithNewSchema(array.get()[i], oldSchema.getElementType(), newSchema.getElementType(), renameCols, fieldNames);
        }
        fieldNames.pop();
        return array;
      case MAP:
        if (!(writable instanceof ArrayWritable)) {
          throw new SchemaCompatibilityException(String.format("Cannot rewrite %s as a map", writable.getClass().getName()));
        }
        ArrayWritable map = (ArrayWritable) writable;
        fieldNames.push("value");
        for (int i = 0; i < map.get().length; i++) {
          Writable mapEntry = map.get()[i];
          ((ArrayWritable) mapEntry).get()[1] = rewriteRecordWithNewSchema(((ArrayWritable) mapEntry).get()[1], oldSchema.getValueType(), newSchema.getValueType(), renameCols, fieldNames);
        }
        return map;

      case UNION:
        throw new IllegalArgumentException("should not be here?");

      default:
        return rewritePrimaryType(writable, oldSchema, newSchema);
    }
  }

  public static Writable rewritePrimaryType(Writable writable, Schema oldSchema, Schema newSchema) {
    if (oldSchema.getType() == newSchema.getType()) {
      switch (oldSchema.getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING:
          return writable;
        case FIXED:
          if (oldSchema.getFixedSize() != newSchema.getFixedSize()) {
            // Check whether this is a [[Decimal]]'s precision change
            if (oldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
              LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) oldSchema.getLogicalType();
              return HiveDecimalUtils.enforcePrecisionScale((HiveDecimalWritable) writable, new DecimalTypeInfo(decimal.getPrecision(), decimal.getScale()));
            } else {
              throw new HoodieAvroSchemaException("Fixed type size change is not currently supported");
            }
          }

          // For [[Fixed]] data type both size and name have to match
          //
          // NOTE: That for values wrapped into [[Union]], to make sure that reverse lookup (by
          //       full-name) is working we have to make sure that both schema's name and namespace
          //       do match
          if (Objects.equals(oldSchema.getFullName(), newSchema.getFullName())) {
            return writable;
          } else {
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) oldSchema.getLogicalType();
            return HiveDecimalUtils.enforcePrecisionScale((HiveDecimalWritable) writable, new DecimalTypeInfo(decimal.getPrecision(), decimal.getScale()));
          }

        default:
          throw new HoodieAvroSchemaException("Unknown schema type: " + newSchema.getType());
      }
    } else {
      return rewritePrimaryTypeWithDiffSchemaType(writable, oldSchema, newSchema);
    }
  }

  private static Writable rewritePrimaryTypeWithDiffSchemaType(Writable writable, Schema oldSchema, Schema newSchema) {
    switch (newSchema.getType()) {
      case NULL:
      case BOOLEAN:
        break;
      case INT:
        if (newSchema.getLogicalType() == LogicalTypes.date() && oldSchema.getType() == Schema.Type.STRING) {
          return new IntWritable(HoodieAvroUtils.fromJavaDate(java.sql.Date.valueOf(writable.toString())));
        }
        break;
      case LONG:
        if (oldSchema.getType() == Schema.Type.INT) {
          return new LongWritable(((IntWritable) writable).get());
        }
        break;
      case FLOAT:
        if ((oldSchema.getType() == Schema.Type.INT)
            || (oldSchema.getType() == Schema.Type.LONG)) {
          return oldSchema.getType() == Schema.Type.INT ? new FloatWritable(((IntWritable) writable).get()) : new FloatWritable(((LongWritable) writable).get());
        }
        break;
      case DOUBLE:
        if (oldSchema.getType() == Schema.Type.FLOAT) {
          // java float cannot convert to double directly, deal with float precision change
          return new DoubleWritable(Double.parseDouble(((FloatWritable) writable).get() + ""));
        } else if (oldSchema.getType() == Schema.Type.INT) {
          return new DoubleWritable(((IntWritable) writable).get());
        } else if (oldSchema.getType() == Schema.Type.LONG) {
          return new DoubleWritable(((LongWritable) writable).get());
        }
        break;
      case BYTES:
        if (oldSchema.getType() == Schema.Type.STRING) {
          return new BytesWritable(getUTF8Bytes(writable.toString()));
        }
        break;
      case STRING:
        if (oldSchema.getType() == Schema.Type.ENUM) {
          return writable;
        }
        if (oldSchema.getType() == Schema.Type.BYTES) {
          return new Text(StringUtils.fromUTF8Bytes(((BytesWritable) writable).getBytes()));
        }
        if (oldSchema.getLogicalType() == LogicalTypes.date()) {
          return new Text(toJavaDate(((IntWritable) writable).get()).toString());
        }
        if (oldSchema.getType() == Schema.Type.INT
            || oldSchema.getType() == Schema.Type.LONG
            || oldSchema.getType() == Schema.Type.FLOAT
            || oldSchema.getType() == Schema.Type.DOUBLE) {
          return new Text(writable.toString());
        }
        throw new IllegalStateException("need to do this");
      case FIXED:
        throw new UnsupportedOperationException("need to do this");
      default:
    }
    throw new HoodieAvroSchemaException(String.format("cannot support rewrite value for schema type: %s since the old schema type is: %s", newSchema, oldSchema));
  }

  private static final Cache<Pair<Schema, Schema>, int[]>
      PROJECTION_CACHE = Caffeine.newBuilder().maximumSize(1000).build();

  public static int[] getProjection(Schema from, Schema to) {
    return PROJECTION_CACHE.get(Pair.of(from, to), schemas -> {
      List<Schema.Field> toFields = to.getFields();
      int[] newProjection = new int[toFields.size()];
      for (int i = 0; i < newProjection.length; i++) {
        newProjection[i] = from.getField(toFields.get(i).name()).pos();
      }
      return newProjection;
    });
  }

  /**
   * Projection will keep the size from the "from" schema because it gets recycled
   * and if the size changes the reader will fail
   */
  public static UnaryOperator<ArrayWritable> projectRecord(Schema from, Schema to) {
    //TODO: [HUDI-8261] add casting to the projection
    int[] projection = getProjection(from, to);
    return arrayWritable -> {
      Writable[] values = new Writable[arrayWritable.get().length];
      for (int i = 0; i < projection.length; i++) {
        values[i] = arrayWritable.get()[projection[i]];
      }
      arrayWritable.set(values);
      return arrayWritable;
    };
  }

  public static int[] getReverseProjection(Schema from, Schema to) {
    return PROJECTION_CACHE.get(Pair.of(from, to), schemas -> {
      List<Schema.Field> fromFields = from.getFields();
      int[] newProjection = new int[fromFields.size()];
      for (int i = 0; i < newProjection.length; i++) {
        newProjection[i] = to.getField(fromFields.get(i).name()).pos();
      }
      return newProjection;
    });
  }

  /**
   * After the reading and merging etc is done, we need to put the records
   * into the positions of the original schema
   */
  public static UnaryOperator<ArrayWritable> reverseProject(Schema from, Schema to) {
    int[] projection = getReverseProjection(from, to);
    return arrayWritable -> {
      Writable[] values = new Writable[to.getFields().size()];
      for (int i = 0; i < projection.length; i++) {
        values[projection[i]] = arrayWritable.get()[i];
      }
      arrayWritable.set(values);
      return arrayWritable;
    };
  }

  public static Object getWritableValue(ArrayWritable arrayWritable, ArrayWritableObjectInspector objectInspector, String name) {
    return objectInspector.getStructFieldData(arrayWritable, objectInspector.getStructFieldRef(name));
  }
}


