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

import java.util.ArrayList;
import java.util.List;

/**
 * Helper methods for schema Change.
 */
public class SchemaChangeUtils {
  private SchemaChangeUtils() {

  }

  /**
   * Whether to allow the column type to be updated.
   * now only support:
   * int => long/float/double/string
   * long => float/double/string
   * float => double/String
   * double => String/Decimal
   * Decimal => Decimal/String
   * String => date/decimal
   * date => String
   * TODO: support more type update.
   *
   * @param src origin column type.
   * @param dsr new column type.
   * @return whether to allow the column type to be updated.
   */
  public static boolean isTypeUpdateAllow(Type src, Type dsr) {
    if (src.isNestedType() || dsr.isNestedType()) {
      throw new IllegalArgumentException("only support update primitive type");
    }
    if (src.equals(dsr)) {
      return true;
    }
    switch (src.typeId()) {
      case INT:
        return dsr == Types.LongType.get() || dsr == Types.FloatType.get()
            || dsr == Types.DoubleType.get() || dsr == Types.StringType.get() || dsr.typeId() == Type.TypeID.DECIMAL;
      case LONG:
        return dsr == Types.FloatType.get() || dsr == Types.DoubleType.get() || dsr == Types.StringType.get() || dsr.typeId() == Type.TypeID.DECIMAL;
      case FLOAT:
        return dsr == Types.DoubleType.get() || dsr == Types.StringType.get() || dsr.typeId() == Type.TypeID.DECIMAL;
      case DOUBLE:
        return dsr == Types.StringType.get() || dsr.typeId() == Type.TypeID.DECIMAL;
      case DATE:
        return dsr == Types.StringType.get();
      case DECIMAL:
        if (dsr.typeId() == Type.TypeID.DECIMAL) {
          Types.DecimalType decimalSrc = (Types.DecimalType)src;
          Types.DecimalType decimalDsr = (Types.DecimalType)dsr;
          if (decimalDsr.isWiderThan(decimalSrc)) {
            return true;
          }
        } else if (dsr.typeId() == Type.TypeID.STRING) {
          return true;
        }
        break;
      case STRING:
        return dsr == Types.DateType.get() || dsr.typeId() == Type.TypeID.DECIMAL;
      default:
        return false;
    }
    return false;
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
    return new InternalSchema(newFields);
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
        return hasChanged ? Types.RecordType.get(newFields) : record;
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
    Types.RecordType newType = (Types.RecordType)applyTableChange2Type(internalSchema.getRecord(), deletes);
    return new InternalSchema(newType.fields());
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
        return Types.RecordType.get(fields);
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
    return new InternalSchema(newFields);
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
        return Types.RecordType.get(newFields);
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

