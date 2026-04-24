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
import org.apache.hudi.common.schema.HoodieProjectionMask;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCompatibility;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
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
    return rewriteRecordWithNewSchema(writable, oldSchema, newSchema, renameCols, HoodieProjectionMask.all());
  }

  /**
   * Rewrite variant aware of an upstream reader having compacted nested struct projection.
   *
   * <p>{@code physicalMask} describes the actual ArrayWritable layout per record level:
   * which sub-fields are present and at what physical index. Pass {@link HoodieProjectionMask#all()}
   * when the input is canonical-shaped — the rewrite then reduces to position-by-canonical-pos
   * access exactly like the legacy 4-arg overload.
   */
  public static ArrayWritable rewriteRecordWithNewSchema(ArrayWritable writable, HoodieSchema oldSchema, HoodieSchema newSchema,
                                                          Map<String, String> renameCols, HoodieProjectionMask physicalMask) {
    return (ArrayWritable) rewriteRecordWithNewSchema(writable, oldSchema, newSchema, renameCols, new LinkedList<>(), physicalMask);
  }

  private static Writable rewriteRecordWithNewSchema(Writable writable, HoodieSchema oldSchema, HoodieSchema newSchema,
                                                     Map<String, String> renameCols, Deque<String> fieldNames, HoodieProjectionMask physicalMask) {
    if (writable == null) {
      return null;
    }
    HoodieSchema oldSchemaNonNull = oldSchema.getNonNullType();
    HoodieSchema newSchemaNonNull = newSchema.getNonNullType();
    // Only short-circuit when the input is in canonical shape; an upstream-projected
    // ArrayWritable needs the recursive rewrite to remap slots, even when the schema
    // pair would otherwise be equivalent.
    if (physicalMask.isAll() && HoodieSchemaCompatibility.areSchemasProjectionEquivalent(oldSchemaNonNull, newSchemaNonNull)) {
      return writable;
    }
    return rewriteRecordWithNewSchemaInternal(writable, oldSchemaNonNull, newSchemaNonNull, renameCols, fieldNames, physicalMask);
  }

  private static Writable rewriteRecordWithNewSchemaInternal(Writable writable, HoodieSchema oldSchema, HoodieSchema newSchema,
                                                             Map<String, String> renameCols, Deque<String> fieldNames, HoodieProjectionMask physicalMask) {
    switch (newSchema.getType()) {
      // BLOB/VARIANT are physically records; share the RECORD field-by-name rewrite.
      case BLOB:
      case VARIANT:
      case RECORD:
        if (!(writable instanceof ArrayWritable)) {
          throw new SchemaCompatibilityException(String.format("Cannot rewrite %s as a record", writable.getClass().getName()));
        }
        if (physicalMask.isCanonicalAtThisLevel()) {
          return rewriteCanonicalRecord((ArrayWritable) writable, oldSchema, newSchema, renameCols, fieldNames, physicalMask);
        }
        return rewriteCompactedRecord((ArrayWritable) writable, oldSchema, newSchema, renameCols, fieldNames, physicalMask);

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
          array.get()[i] = rewriteRecordWithNewSchema(array.get()[i], oldSchema.getElementType(), newSchema.getElementType(), renameCols, fieldNames, HoodieProjectionMask.all());
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
          ((ArrayWritable) mapEntry).get()[1] =
              rewriteRecordWithNewSchema(((ArrayWritable) mapEntry).get()[1], oldSchema.getValueType(), newSchema.getValueType(), renameCols, fieldNames, HoodieProjectionMask.all());
        }
        return map;

      case UNION:
        throw new IllegalArgumentException("should not be here?");

      default:
        return rewritePrimaryType(writable, oldSchema, newSchema);
    }
  }

  /**
   * Canonical record rewrite: input ArrayWritable matches {@code oldSchema}'s field
   * positions (with possibly trailing nulls); output is sized to
   * {@code max(newSchema field count, input array length)} so any trailing slots Hive
   * left in a recycled row buffer are preserved, while schema-evolution semantics still
   * apply to the leading {@code newSchema}-sized region (default values, null padding,
   * non-nullable-without-default throws).
   */
  private static Writable rewriteCanonicalRecord(ArrayWritable arrayWritable, HoodieSchema oldSchema, HoodieSchema newSchema,
                                                 Map<String, String> renameCols, Deque<String> fieldNames, HoodieProjectionMask physicalMask) {
    List<HoodieSchemaField> fields = newSchema.getFields();
    boolean noFieldsRenaming = renameCols.isEmpty();
    String namePrefix = createNamePrefix(noFieldsRenaming, fieldNames);
    Writable[] values = new Writable[Math.max(fields.size(), arrayWritable.get().length)];
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchemaField newField = fields.get(i);
      String newFieldName = newField.name();
      fieldNames.push(newFieldName);
      Option<HoodieSchemaField> oldFieldOpt = noFieldsRenaming
          ? oldSchema.getField(newFieldName)
          : oldSchema.getField(getOldFieldNameWithRenaming(namePrefix, newFieldName, renameCols));

      // Bounds-check because some upstream readers may still hand back arrays shorter
      // than the schema declares (no projection mask supplied).
      if (oldFieldOpt.isPresent() && oldFieldOpt.get().pos() < arrayWritable.get().length) {
        HoodieSchemaField oldField = oldFieldOpt.get();
        HoodieProjectionMask childMask = physicalMask.childOrAll(oldField.name());
        values[i] = rewriteRecordWithNewSchema(arrayWritable.get()[oldField.pos()], oldField.schema(), newField.schema(), renameCols, fieldNames, childMask);
      } else if (newField.defaultVal().isPresent() && newField.defaultVal().get().equals(HoodieSchema.NULL_VALUE)) {
        values[i] = NullWritable.get();
      } else if (!newField.schema().isNullable() && newField.defaultVal().isEmpty()) {
        throw new SchemaCompatibilityException("Field " + createFullName(fieldNames) + " has no default value and is non-nullable");
      } else if (newField.defaultVal().isPresent()) {
        switch (newField.getNonNullSchema().getType()) {
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
  }

  /**
   * Compacted record rewrite: input ArrayWritable matches the upstream reader's
   * projected schema (only the projected sub-fields, in declared order). The output
   * preserves the same physical layout — Hive's downstream {@code ArrayWritableObjectInspector}
   * was constructed from the projected schema and expects compacted positions.
   * Per-element schema conversion still applies (e.g. plain STRING → canonical ENUM).
   */
  private static Writable rewriteCompactedRecord(ArrayWritable arrayWritable, HoodieSchema oldSchema, HoodieSchema newSchema,
                                                 Map<String, String> renameCols, Deque<String> fieldNames, HoodieProjectionMask physicalMask) {
    Writable[] inputs = arrayWritable.get();
    Writable[] values = new Writable[inputs.length];
    List<String> order = physicalMask.physicalOrder();
    for (int physIdx = 0; physIdx < order.size(); physIdx++) {
      if (physIdx >= inputs.length) {
        // Mask claims a field at a position the reader didn't fill; defensive — leave null.
        continue;
      }
      String physicalFieldName = order.get(physIdx);
      Option<HoodieSchemaField> oldFieldOpt = oldSchema.getField(physicalFieldName);
      Option<HoodieSchemaField> newFieldOpt = newSchema.getField(physicalFieldName);
      if (oldFieldOpt.isEmpty() || newFieldOpt.isEmpty()) {
        // The reader returned a sub-field neither side knows about — pass through.
        values[physIdx] = inputs[physIdx];
        continue;
      }
      fieldNames.push(physicalFieldName);
      HoodieProjectionMask childMask = physicalMask.childOrAll(physicalFieldName);
      values[physIdx] = rewriteRecordWithNewSchema(inputs[physIdx], oldFieldOpt.get().schema(), newFieldOpt.get().schema(), renameCols, fieldNames, childMask);
      fieldNames.pop();
    }
    return new ArrayWritable(Writable.class, values);
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
      case VECTOR:
        // Parquet stores VECTOR as a bare FIXED_LEN_BYTE_ARRAY without a logical-type
        // annotation (see AvroSchemaConverterWithTimestampNTZ#convertField VECTOR branch),
        // so Hive's Parquet reader reconstructs the Avro schema as plain FIXED named after
        // the column. When Hudi then projects that record to the canonical VECTOR schema
        // (fixed named vector_<elem>_<dim> with logicalType=vector), oldSchema.getType() is
        // FIXED while newSchema.getType() is VECTOR. The byte layout is identical for
        // StorageBacking.FIXED_BYTES as long as sizes match, so the rewrite is a pass-through.
        if (oldSchema.getType() == HoodieSchemaType.FIXED
            && newSchema instanceof HoodieSchema.Vector) {
          HoodieSchema.Vector vector = (HoodieSchema.Vector) newSchema;
          if (vector.getStorageBacking() == HoodieSchema.Vector.StorageBacking.FIXED_BYTES
              && oldSchema.getFixedSize() == vector.getFixedSize()) {
            return writable;
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
