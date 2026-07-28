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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.action.TableChangesHelper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Helper methods for schema Change.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SchemaChangeUtils {

  /**
   * Parses the {@code hoodie.write.timestamp.logical.type.overrides} value into a per-field map of
   * the target timestamp {@link Type}. The value is a comma-separated list of {@code field:type}
   * pairs, where type is one of timestamp-micros, timestamp-millis, local-timestamp-micros,
   * local-timestamp-millis (case-insensitive). The tokens are a Hudi-owned vocabulary decoupled
   * from any serialization format.
   *
   * <p>Splits on the last {@code ':'} so dotted nested field names ({@code parent.child}) work
   * unchanged. Field names containing a literal {@code ':'} are not supported.
   *
   * @param value the raw config value (may be null or empty)
   * @return an unmodifiable map from field name to the pinned timestamp type; empty if unset
   */
  public static Map<String, Type> parseTimestampLogicalTypeOverrides(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Type> result = new LinkedHashMap<>();
    for (String pair : value.split(",")) {
      String trimmed = pair.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int sep = trimmed.lastIndexOf(':');
      if (sep <= 0 || sep == trimmed.length() - 1) {
        throw new IllegalArgumentException("Invalid timestamp logical type override entry '" + trimmed
            + "'. Expected 'field:type' where type is one of timestamp-micros, timestamp-millis, "
            + "local-timestamp-micros, local-timestamp-millis.");
      }
      String field = trimmed.substring(0, sep).trim();
      Type type = timestampTypeFromToken(trimmed.substring(sep + 1).trim());
      result.put(field, type);
    }
    return Collections.unmodifiableMap(result);
  }

  private static Type timestampTypeFromToken(String token) {
    switch (token.toLowerCase(Locale.ROOT)) {
      case "timestamp-micros":
        return Types.TimestampType.get();
      case "timestamp-millis":
        return Types.TimestampMillisType.get();
      case "local-timestamp-micros":
        return Types.LocalTimestampMicrosType.get();
      case "local-timestamp-millis":
        return Types.LocalTimestampMillisType.get();
      default:
        throw new IllegalArgumentException("Unknown timestamp logical type token '" + token
            + "'. Expected one of timestamp-micros, timestamp-millis, local-timestamp-micros, "
            + "local-timestamp-millis.");
    }
  }

  /**
   * Whether a column type change is a timestamp precision change that must be authorized by an
   * explicit per-field override (see {@code hoodie.write.timestamp.logical.type.overrides}). This
   * covers timestamp-micros/millis flips, local-timestamp-micros/millis flips, and the forward-fix
   * from a bare {@code long} to a local-timestamp logical type that 0.x dropped.
   */
  public static boolean isGatedTimestampChange(Type src, Type dst) {
    if (src.equals(dst)) {
      return false;
    }
    if (isUtcTimestamp(src) && isUtcTimestamp(dst)) {
      return true;
    }
    if (isLocalTimestamp(src) && isLocalTimestamp(dst)) {
      return true;
    }
    return src.typeId() == Type.TypeID.LONG && isLocalTimestamp(dst);
  }

  /**
   * Whether a column type change crosses the UTC/local timestamp boundary. Unlike a precision
   * change this has no value-level repair: the same long denotes a different instant under each
   * interpretation, so rescaling cannot express the conversion. A zone change is therefore always
   * rejected and no per-field override authorizes it.
   */
  public static boolean isCrossZoneTimestampChange(Type src, Type dst) {
    return (isUtcTimestamp(src) && isLocalTimestamp(dst)) || (isLocalTimestamp(src) && isUtcTimestamp(dst));
  }

  private static boolean isUtcTimestamp(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP || type.typeId() == Type.TypeID.TIMESTAMP_MILLIS;
  }

  private static boolean isLocalTimestamp(Type type) {
    return type.typeId() == Type.TypeID.LOCAL_TIMESTAMP_MILLIS || type.typeId() == Type.TypeID.LOCAL_TIMESTAMP_MICROS;
  }

  /**
   * Whether to allow the column type to be updated.
   * now only support:
   * int => long/float/double/String/Decimal
   * long => float/double/String/Decimal
   * float => double/String/Decimal
   * double => String/Decimal
   * Decimal => Decimal/String
   * String => date/decimal
   * date => String
   * TODO: support more type update.
   *
   * @param src origin column type.
   * @param dst new column type.
   * @return whether to allow the column type to be updated.
   */
  public static boolean isTypeUpdateAllow(Type src, Type dst, boolean allowTimestampPrecisionEvolution) {
    if (src.isNestedType() || dst.isNestedType()) {
      throw new IllegalArgumentException("only support update primitive type");
    }
    if (src.equals(dst)) {
      return true;
    }
    return isTypeUpdateAllowInternal(src, dst, allowTimestampPrecisionEvolution);
  }

  public static boolean shouldPromoteType(Type src, Type dst) {
    if (src.equals(dst) || src.isNestedType() || dst.isNestedType()) {
      return false;
    }
    return isTypeUpdateAllowInternal(src, dst, false);
  }

  private static boolean isTypeUpdateAllowInternal(Type src, Type dst, boolean allowTimestampPrecisionEvolution) {
    switch (src.typeId()) {
      case INT:
        return dst == Types.LongType.get() || dst == Types.FloatType.get()
            || dst == Types.DoubleType.get() || dst == Types.StringType.get() || dst.typeId() == Type.TypeID.DECIMAL || dst.typeId() == Type.TypeID.DECIMAL_FIXED;
      case LONG:
        if (allowTimestampPrecisionEvolution
            && (dst.typeId() == Type.TypeID.LOCAL_TIMESTAMP_MILLIS || dst.typeId() == Type.TypeID.LOCAL_TIMESTAMP_MICROS)) {
          // Forward-fix path: 0.x stored local-timestamp columns as bare long because its converter
          // did not recognize the logical type. Allow attaching the logical type when the gate is open.
          return true;
        }
        return dst == Types.FloatType.get() || dst == Types.DoubleType.get() || dst == Types.StringType.get() || dst.typeId() == Type.TypeID.DECIMAL || dst.typeId() == Type.TypeID.DECIMAL_FIXED;
      case FLOAT:
        return dst == Types.DoubleType.get() || dst == Types.StringType.get() || dst.typeId() == Type.TypeID.DECIMAL || dst.typeId() == Type.TypeID.DECIMAL_FIXED;
      case DOUBLE:
        return dst == Types.StringType.get() || dst.typeId() == Type.TypeID.DECIMAL || dst.typeId() == Type.TypeID.DECIMAL_FIXED;
      case DATE:
      case BINARY:
        return dst == Types.StringType.get();
      case DECIMAL_BYTES:
        return isDecimalBytesUpdateAllowInternal(src, dst);
      case DECIMAL:
      case DECIMAL_FIXED:
        return isDecimalFixedUpdateAllowInternal(src, dst);
      case STRING:
        return dst == Types.DateType.get() || dst.typeId() == Type.TypeID.DECIMAL || dst.typeId() == Type.TypeID.DECIMAL_FIXED || dst == Types.BinaryType.get();
      case TIMESTAMP:
      case TIMESTAMP_MILLIS:
        if (!allowTimestampPrecisionEvolution) {
          return false;
        }
        return dst.typeId() == Type.TypeID.TIMESTAMP || dst.typeId() == Type.TypeID.TIMESTAMP_MILLIS;
      case LOCAL_TIMESTAMP_MILLIS:
      case LOCAL_TIMESTAMP_MICROS:
        if (!allowTimestampPrecisionEvolution) {
          return false;
        }
        return dst.typeId() == Type.TypeID.LOCAL_TIMESTAMP_MILLIS || dst.typeId() == Type.TypeID.LOCAL_TIMESTAMP_MICROS;
      default:
        return false;
    }
  }

  private static boolean isDecimalBytesUpdateAllowInternal(Type src, Type dst) {
    if (dst.typeId() == Type.TypeID.DECIMAL_BYTES || dst.typeId() == Type.TypeID.DECIMAL_FIXED || dst.typeId() == Type.TypeID.DECIMAL) {
      return isDecimalUpdateAllowInternalBase((Types.DecimalBase)src, (Types.DecimalBase)dst);
    }
    return dst.typeId() == Type.TypeID.STRING;
  }

  private static boolean isDecimalUpdateAllowInternalBase(Types.DecimalBase  src, Types.DecimalBase  dst) {
    if (dst.isWiderThan(src)) {
      return true;
    }
    if (dst.precision() >= src.precision() && dst.scale() == src.scale()) {
      return true;
    }
    return false;
  }

  private static boolean isDecimalFixedUpdateAllowInternal(Type src, Type dst) {
    if (dst instanceof Types.DecimalBase) {
      if (dst.typeId() == Type.TypeID.DECIMAL_FIXED || dst.typeId() == Type.TypeID.DECIMAL) {
        Types.DecimalTypeFixed decimalSrc = (Types.DecimalTypeFixed)src;
        Types.DecimalTypeFixed decimaldst = (Types.DecimalTypeFixed)dst;
        if (decimalSrc.getFixedSize() > decimaldst.getFixedSize()) {
          return false;
        }
      }
      return isDecimalUpdateAllowInternalBase((Types.DecimalBase)src, (Types.DecimalBase)dst);
    }
    return dst.typeId() == Type.TypeID.STRING;
  }

  /**
   * Apply all the DDL add operations to internalSchema to produce a new internalSchema.
   *
   * @param internalSchema origin internalSchema.
   * @param adds a wrapper class for all the DDL add operations.
   * @return a new internalSchema.
   */
  public static InternalSchema applyTableChanges2Schema(InternalSchema internalSchema, TableChanges.ColumnAddChange adds) {
    Types.RecordType newType = (Types.RecordType)applyTableChange2Type(internalSchema.getRecord(), adds);
    // deal with root level changes
    List<Types.Field> newFields = TableChangesHelper.applyAddChange2Fields(newType.fields(),
        adds.getParentId2AddCols().get(-1), adds.getPositionChangeMap().get(-1));
    return new InternalSchema(Types.RecordType.get(newFields, newType.name()));
  }

  /**
   * Apply all the DDL add operations to Type to produce a new internalSchema.
   * do not call this method directly. expose this method only for UT.
   *
   * @param type origin hudi Type.
   * @param adds a wrapper class for all the DDL add operations.
   * @return a new internalSchema.
   */
  public static Type applyTableChange2Type(Type type, TableChanges.ColumnAddChange adds) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Type> newTypes = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          Type newType = applyTableChange2Type(f.type(), adds);
          // try to apply add
          newTypes.add(newType.isNestedType() ? adds.applyAdd(f, newType) : newType);
        }
        List<Types.Field> newFields = new ArrayList<>();
        boolean hasChanged = false;
        for (int i = 0; i < newTypes.size(); i++) {
          Type newType = newTypes.get(i);
          Types.Field oldfield = record.fields().get(i);
          if (oldfield.type() == newType) {
            newFields.add(oldfield);
          } else {
            hasChanged = true;
            newFields.add(Types.Field.get(oldfield.fieldId(), oldfield.isOptional(), oldfield.name(), newType, oldfield.doc()));
          }
        }
        return hasChanged ? Types.RecordType.get(newFields, record.name()) : record;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Type newElementType;
        Types.Field elementField = array.field(array.elementId());
        newElementType = applyTableChange2Type(array.elementType(), adds);
        // try to apply add
        newElementType = adds.applyAdd(elementField, newElementType);
        if (newElementType == array.elementType()) {
          return array;
        }
        return Types.ArrayType.get(array.elementId(), array.isElementOptional(), newElementType);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Type newValueType;
        Types.Field valueField = map.field(map.valueId());
        if (adds.getParentId2AddCols().containsKey(map.keyId())) {
          throw new IllegalArgumentException("Cannot add fields to map keys: " + map);
        }
        newValueType = applyTableChange2Type(map.valueType(), adds);
        // try to apply add
        newValueType = adds.applyAdd(valueField, newValueType);
        if (newValueType == map.valueType()) {
          return map;
        }
        return Types.MapType.get(map.keyId(), map.valueId(), map.keyType(), newValueType, map.isValueOptional());
      default:
        return type;
    }
  }

  /**
   * Apply all the DDL delete operations to internalSchema to produce a new internalSchema.
   *
   * @param internalSchema origin internalSchema.
   * @param deletes a wrapper class for all the DDL delete operations.
   * @return a new internalSchema.
   */
  public static InternalSchema applyTableChanges2Schema(InternalSchema internalSchema, TableChanges.ColumnDeleteChange deletes) {
    return new InternalSchema((Types.RecordType)applyTableChange2Type(internalSchema.getRecord(), deletes));
  }

  /**
   * Apply all the DDL delete operations to Type to produce a new internalSchema.
   * do not call this method directly. expose this method only for UT.
   *
   * @param type origin type.
   * @param deletes a wrapper class for all the DDL delete operations.
   * @return a new internalSchema.
   */
  private static Type applyTableChange2Type(Type type, TableChanges.ColumnDeleteChange deletes) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Types.Field> fields = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          Type newType = applyTableChange2Type(f.type(), deletes);
          // apply delete
          newType = deletes.applyDelete(f.fieldId(), newType);
          if (newType != null) {
            fields.add(Types.Field.get(f.fieldId(), f.isOptional(), f.name(), newType, f.doc()));
          }
        }
        if (fields.isEmpty()) {
          throw new UnsupportedOperationException("cannot support delete all columns from Struct");
        }
        return Types.RecordType.get(fields, record.name());
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Type newElementType = applyTableChange2Type(array.elementType(), deletes);
        newElementType = deletes.applyDelete(array.elementId(), newElementType);
        if (newElementType == null) {
          throw new IllegalArgumentException(String.format("cannot delete element from arrayType: %s", array));
        }
        return Types.ArrayType.get(array.elementId(), array.isElementOptional(), newElementType);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        int keyId = map.fields().get(0).fieldId();
        if (deletes.getDeletes().contains(keyId)) {
          throw new IllegalArgumentException(String.format("cannot delete key from mapType: %s", map));
        }
        Type newValueType = applyTableChange2Type(map.valueType(), deletes);
        newValueType = deletes.applyDelete(map.valueId(), newValueType);
        if (newValueType == null) {
          throw new IllegalArgumentException(String.format("cannot delete value from mapType: %s", map));
        }
        return Types.MapType.get(map.keyId(), map.valueId(), map.keyType(), newValueType, map.isValueOptional());
      default:
        return type;
    }
  }

  /**
   * Apply all the DDL update operations to internalSchema to produce a new internalSchema.
   *
   * @param internalSchema origin internalSchema.
   * @param updates a wrapper class for all the DDL update operations.
   * @return a new internalSchema.
   */
  public static InternalSchema applyTableChanges2Schema(InternalSchema internalSchema, TableChanges.ColumnUpdateChange updates) {
    Types.RecordType newType = (Types.RecordType)applyTableChange2Type(internalSchema.getRecord(), updates);
    // deal with root level changes
    List<Types.Field> newFields = TableChangesHelper.applyAddChange2Fields(newType.fields(),
        new ArrayList<>(), updates.getPositionChangeMap().get(-1));
    return new InternalSchema(Types.RecordType.get(newFields, newType.name()));
  }

  /**
   * Apply all the DDL update operations to type to produce a new internalSchema.
   * do not call this method directly. expose this method only for UT.
   *
   * @param type origin internalSchema.
   * @param updates a wrapper class for all the DDL update operations.
   * @return a new internalSchema.
   */
  private static Type applyTableChange2Type(Type type, TableChanges.ColumnUpdateChange updates) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Type> newTypes = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          Type newType = applyTableChange2Type(f.type(), updates);
          newTypes.add(updates.applyUpdates(f, newType));
        }
        List<Types.Field> newFields = new ArrayList<>();
        for (int i = 0; i < newTypes.size(); i++) {
          Type newType = newTypes.get(i);
          Types.Field oldField = record.fields().get(i);
          Types.Field updateField = updates.getUpdates().get(oldField.fieldId());
          if (updateField != null) {
            newFields.add(Types.Field.get(oldField.fieldId(), updateField.isOptional(), updateField.name(), newType, updateField.doc()));
          } else if (!oldField.type().equals(newType)) {
            newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), newType, oldField.doc()));
          } else {
            newFields.add(oldField);
          }
        }
        return Types.RecordType.get(newFields, record.name());
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Type newElementType;
        Types.Field elementField = array.fields().get(0);
        newElementType = applyTableChange2Type(array.elementType(), updates);
        newElementType = updates.applyUpdates(elementField, newElementType);
        Types.Field elementUpdate = updates.getUpdates().get(elementField.fieldId());
        boolean optional = elementUpdate == null ? array.isElementOptional() : elementUpdate.isOptional();
        if (optional == elementField.isOptional() && array.elementType() == newElementType) {
          return array;
        }
        return Types.ArrayType.get(array.elementId(), optional, newElementType);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Types.Field valueFiled = map.fields().get(1);
        Type newValueType;
        newValueType = applyTableChange2Type(map.valueType(), updates);
        newValueType = updates.applyUpdates(valueFiled, newValueType);
        Types.Field valueUpdate = updates.getUpdates().get(valueFiled.fieldId());
        boolean valueOptional = valueUpdate == null ? map.isValueOptional() : valueUpdate.isOptional();
        if (valueOptional == map.isValueOptional() && map.valueType() == newValueType) {
          return map;
        }
        return Types.MapType.get(map.keyId(), map.valueId(), map.keyType(), newValueType, valueOptional);
      default:
        return type;
    }
  }
}

