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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCompatibility;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import org.apache.hadoop.hive.common.type.HiveDecimal;
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.apache.hudi.avro.HoodieAvroUtils.createFullName;
import static org.apache.hudi.avro.HoodieAvroUtils.createNamePrefix;
import static org.apache.hudi.avro.HoodieAvroUtils.getOldFieldNameWithRenaming;
import static org.apache.hudi.avro.HoodieAvroUtils.toJavaDate;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

public class HoodieArrayWritableSchemaUtils {

  public static ArrayWritable rewriteRecordWithNewSchema(ArrayWritable writable, HoodieSchema oldSchema, HoodieSchema newSchema, Map<String, String> renameCols) {
    return (ArrayWritable) rewriteRecordWithNewSchema(writable, oldSchema, newSchema, renameCols, new LinkedList<>());
  }

  private static Writable rewriteRecordWithNewSchema(Writable writable, HoodieSchema oldSchema, HoodieSchema newSchema, Map<String, String> renameCols, Deque<String> fieldNames) {
    if (writable == null) {
      return null;
    }
    HoodieSchema oldSchemaNonNull = HoodieSchemaUtils.getNonNullTypeFromUnion(oldSchema);
    HoodieSchema newSchemaNonNull = HoodieSchemaUtils.getNonNullTypeFromUnion(newSchema);
    if (HoodieSchemaCompatibility.areSchemasProjectionEquivalent(oldSchemaNonNull, newSchemaNonNull)) {
      return writable;
    }
    return rewriteRecordWithNewSchemaInternal(writable, oldSchemaNonNull, newSchemaNonNull, renameCols, fieldNames);
  }

  private static Writable rewriteRecordWithNewSchemaInternal(Writable writable, HoodieSchema oldSchema, HoodieSchema newSchema, Map<String, String> renameCols, Deque<String> fieldNames) {
    switch (newSchema.getType()) {
      case RECORD:
        if (!(writable instanceof ArrayWritable)) {
          throw new SchemaCompatibilityException(String.format("Cannot rewrite %s as a record", writable.getClass().getName()));
        }

        ArrayWritable arrayWritable = (ArrayWritable) writable;
        List<HoodieSchemaField> fields = newSchema.getFields();
        // projection will keep the size from the "from" schema because it gets recycled
        // and if the size changes the reader will fail
        boolean noFieldsRenaming = renameCols.isEmpty();
        String namePrefix = createNamePrefix(noFieldsRenaming, fieldNames);
        Writable[] values = new Writable[Math.max(fields.size(), arrayWritable.get().length)];
        for (int i = 0; i < fields.size(); i++) {
          HoodieSchemaField newField = newSchema.getFields().get(i);
          String newFieldName = newField.name();
          fieldNames.push(newFieldName);
          Option<HoodieSchemaField> oldFieldOpt = noFieldsRenaming
              ? oldSchema.getField(newFieldName)
              : oldSchema.getField(getOldFieldNameWithRenaming(namePrefix, newFieldName, renameCols));
          if (oldFieldOpt.isPresent()) {
            HoodieSchemaField oldField = oldFieldOpt.get();
            values[i] = rewriteRecordWithNewSchema(arrayWritable.get()[oldField.pos()], oldField.schema(), newField.schema(), renameCols, fieldNames);
          } else if (newField.defaultVal().isPresent() && newField.defaultVal().get().equals(HoodieSchema.NULL_VALUE)) {
            values[i] = NullWritable.get();
          } else if (!newField.schema().isNullable() && newField.defaultVal().isEmpty()) {
            throw new SchemaCompatibilityException("Field " + createFullName(fieldNames) + " has no default value and is non-nullable");
          } else if (newField.defaultVal().isPresent()) {
            switch (HoodieSchemaUtils.getNonNullTypeFromUnion(newField.schema()).getType()) {
              case BOOLEAN:
                values[i] = new BooleanWritable((Boolean) newField.defaultVal().get());
                break;
              case INT:
                values[i] = new IntWritable((Integer) newField.defaultVal().get());
                break;
              case LONG:
                values[i] = new LongWritable((Long) newField.defaultVal().get());
                break;
              case FLOAT:
                values[i] = new FloatWritable((Float) newField.defaultVal().get());
                break;
              case DOUBLE:
                values[i] = new DoubleWritable((Double) newField.defaultVal().get());
                break;
              case STRING:
                values[i] = new Text(newField.defaultVal().toString());
                break;
              default:
                throw new SchemaCompatibilityException("Field " + createFullName(fieldNames) + " has no default value");
            }
          }
          fieldNames.pop();
        }
        return new ArrayWritable(Writable.class, values);

      case ENUM:
        if ((writable instanceof BytesWritable)) {
          return writable;
        }
        if (oldSchema.getType() != HoodieSchemaType.STRING && oldSchema.getType() != HoodieSchemaType.ENUM) {
          throw new SchemaCompatibilityException(String.format("Only ENUM or STRING type can be converted ENUM type. Schema type was %s", oldSchema.getType()));
        }
        if (oldSchema.getType() == HoodieSchemaType.STRING) {
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

  public static Writable rewritePrimaryType(Writable writable, HoodieSchema oldSchema, HoodieSchema newSchema) {
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
        case DECIMAL:
          if (oldSchema.getFixedSize() != newSchema.getFixedSize()) {
            // Check whether this is a [[Decimal]]'s precision change
            if (oldSchema.getType() == HoodieSchemaType.DECIMAL) {
              HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) oldSchema;
              return HiveDecimalUtils.enforcePrecisionScale((HiveDecimalWritable) writable, new DecimalTypeInfo(decimal.getPrecision(), decimal.getScale()));
            } else {
              throw new HoodieSchemaException("Fixed type size change is not currently supported");
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
            HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) oldSchema;
            return HiveDecimalUtils.enforcePrecisionScale((HiveDecimalWritable) writable, new DecimalTypeInfo(decimal.getPrecision(), decimal.getScale()));
          }

        default:
          throw new HoodieSchemaException("Unknown schema type: " + newSchema.getType());
      }
    } else {
      return rewritePrimaryTypeWithDiffSchemaType(writable, oldSchema, newSchema);
    }
  }

  private static Writable rewritePrimaryTypeWithDiffSchemaType(Writable writable, HoodieSchema oldSchema, HoodieSchema newSchema) {
    switch (newSchema.getType()) {
      case NULL:
      case BOOLEAN:
      case INT:
        break;
      case DATE:
        if (oldSchema.getType() == HoodieSchemaType.STRING) {
          return HoodieHiveUtils.getDateWriteable((HoodieAvroUtils.fromJavaDate(java.sql.Date.valueOf(writable.toString()))));
        }
        break;
      case LONG:
        if (oldSchema.getType() == HoodieSchemaType.INT) {
          return new LongWritable(((IntWritable) writable).get());
        }
        break;
      case FLOAT:
        if ((oldSchema.getType() == HoodieSchemaType.INT)
            || (oldSchema.getType() == HoodieSchemaType.LONG)) {
          return oldSchema.getType() == HoodieSchemaType.INT
              ? new FloatWritable(((IntWritable) writable).get())
              : new FloatWritable(((LongWritable) writable).get());
        }
        break;
      case DOUBLE:
        if (oldSchema.getType() == HoodieSchemaType.FLOAT) {
          // java float cannot convert to double directly, deal with float precision change
          return new DoubleWritable(Double.parseDouble(((FloatWritable) writable).get() + ""));
        } else if (oldSchema.getType() == HoodieSchemaType.INT) {
          return new DoubleWritable(((IntWritable) writable).get());
        } else if (oldSchema.getType() == HoodieSchemaType.LONG) {
          return new DoubleWritable(((LongWritable) writable).get());
        }
        break;
      case BYTES:
        if (oldSchema.getType() == HoodieSchemaType.STRING) {
          return new BytesWritable(getUTF8Bytes(writable.toString()));
        }
        break;
      case STRING:
        if (oldSchema.getType() == HoodieSchemaType.ENUM) {
          return writable;
        }
        if (oldSchema.getType() == HoodieSchemaType.BYTES) {
          return new Text(StringUtils.fromUTF8Bytes(((BytesWritable) writable).getBytes()));
        }
        if (oldSchema.getType() == HoodieSchemaType.DATE) {
          return new Text(toJavaDate(((IntWritable) writable).get()).toString());
        }
        if (oldSchema.getType() == HoodieSchemaType.INT
            || oldSchema.getType() == HoodieSchemaType.LONG
            || oldSchema.getType() == HoodieSchemaType.FLOAT
            || oldSchema.getType() == HoodieSchemaType.DOUBLE) {
          return new Text(writable.toString());
        }
        if (oldSchema instanceof HoodieSchema.Decimal) {
          HiveDecimalWritable hdw = (HiveDecimalWritable) writable;
          return new Text(hdw.getHiveDecimal().bigDecimalValue().toPlainString());
        }
        break;
      case FIXED:
      case DECIMAL:
        if (newSchema instanceof HoodieSchema.Decimal) {
          HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) newSchema;
          DecimalTypeInfo decimalTypeInfo = new DecimalTypeInfo(decimal.getPrecision(), decimal.getScale());

          if (oldSchema.getType() == HoodieSchemaType.STRING
              || oldSchema.getType() == HoodieSchemaType.INT
              || oldSchema.getType() == HoodieSchemaType.LONG
              || oldSchema.getType() == HoodieSchemaType.FLOAT
              || oldSchema.getType() == HoodieSchemaType.DOUBLE) {
            // loses trailing zeros due to hive issue. Since we only read with hive, I think this is fine
            HiveDecimalWritable converted = new HiveDecimalWritable(HiveDecimal.create(new BigDecimal(writable.toString())));
            return HiveDecimalUtils.enforcePrecisionScale(converted, decimalTypeInfo);
          }

          if (oldSchema.getType() == HoodieSchemaType.BYTES) {
            ByteBuffer buffer = ByteBuffer.wrap(((BytesWritable) writable).getBytes());
            BigDecimal bd = new BigDecimal(new BigInteger(buffer.array()), decimal.getScale());
            HiveDecimalWritable converted = new HiveDecimalWritable(HiveDecimal.create(bd));
            return HiveDecimalUtils.enforcePrecisionScale(converted, decimalTypeInfo);
          }
        }
        break;
      default:
    }
    throw new HoodieSchemaException(String.format("cannot support rewrite value for schema type: %s since the old schema type is: %s", newSchema, oldSchema));
  }

  private static int[] getReverseProjectionMapping(HoodieSchema from, HoodieSchema to) {
    List<HoodieSchemaField> fromFields = from.getFields();
    int[] newProjection = new int[fromFields.size()];
    for (int i = 0; i < newProjection.length; i++) {
      String fieldName = fromFields.get(i).name();
      newProjection[i] = to.getField(fieldName).orElseThrow(() -> new IllegalArgumentException("Schema missing field with name: " + fieldName)).pos();
    }
    return newProjection;
  }

  /**
   * After the reading and merging etc is done, we need to put the records
   * into the positions of the original schema
   */
  public static UnaryOperator<ArrayWritable> getReverseProjection(HoodieSchema from, HoodieSchema to) {
    int[] projection = getReverseProjectionMapping(from, to);
    return arrayWritable -> {
      Writable[] values = new Writable[to.getFields().size()];
      for (int i = 0; i < projection.length; i++) {
        values[projection[i]] = arrayWritable.get()[i];
      }
      arrayWritable.set(values);
      return arrayWritable;
    };
  }
}
