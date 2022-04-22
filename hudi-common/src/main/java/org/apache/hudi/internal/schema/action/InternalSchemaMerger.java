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

package org.apache.hudi.internal.schema.action;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import java.util.ArrayList;
import java.util.List;

/**
 * Auxiliary class.
 * help to merge file schema and query schema to produce final read schema for avro/parquet file
 */
public class InternalSchemaMerger {
  private final InternalSchema fileSchema;
  private final InternalSchema querySchema;
  // now there exist some bugs when we use spark update/merge api,
  // those operation will change col nullability from optional to required which is wrong.
  // Before that bug is fixed, we need to do adapt.
  // if mergeRequiredFiledForce is true, we will ignore the col's required attribute.
  private final boolean ignoreRequiredAttribute;
  // Whether to use column Type from file schema to read files when we find some column type has changed.
  // spark parquetReader need the original column type to read data, otherwise the parquetReader will failed.
  // eg: current column type is StringType, now we changed it to decimalType,
  // we should not pass decimalType to parquetReader, we must pass StringType to it; when we read out the data, we convert data from String to Decimal, everything is ok.
  // for log reader
  // since our reWriteRecordWithNewSchema function support rewrite directly, so we no need this parameter
  // eg: current column type is StringType, now we changed it to decimalType,
  // we can pass decimalType to reWriteRecordWithNewSchema directly, everything is ok.
  private boolean useColumnTypeFromFileSchema = true;

  // deal with rename
  // Whether to use column name from file schema to read files when we find some column name has changed.
  // spark parquetReader need the original column name to read data, otherwise the parquetReader will read nothing.
  // eg: current column name is colOldName, now we rename it to colNewName,
  // we should not pass colNewName to parquetReader, we must pass colOldName to it; when we read out the data.
  // for log reader
  // since our reWriteRecordWithNewSchema function support rewrite directly, so we no need this parameter
  // eg: current column name is colOldName, now we rename it to colNewName,
  // we can pass colNewName to reWriteRecordWithNewSchema directly, everything is ok.
  private boolean useColNameFromFileSchema = true;

  public InternalSchemaMerger(InternalSchema fileSchema, InternalSchema querySchema, boolean ignoreRequiredAttribute, boolean useColumnTypeFromFileSchema, boolean useColNameFromFileSchema) {
    this.fileSchema = fileSchema;
    this.querySchema = querySchema;
    this.ignoreRequiredAttribute = ignoreRequiredAttribute;
    this.useColumnTypeFromFileSchema = useColumnTypeFromFileSchema;
    this.useColNameFromFileSchema = useColNameFromFileSchema;
  }

  public InternalSchemaMerger(InternalSchema fileSchema, InternalSchema querySchema, boolean ignoreRequiredAttribute, boolean useColumnTypeFromFileSchema) {
    this(fileSchema, querySchema, ignoreRequiredAttribute, useColumnTypeFromFileSchema, true);
  }

  /**
   * Create final read schema to read avro/parquet file.
   *
   * @return read schema to read avro/parquet file.
   */
  public InternalSchema mergeSchema() {
    Types.RecordType record = (Types.RecordType) mergeType(querySchema.getRecord(), 0);
    return new InternalSchema(record.fields());
  }

  /**
   * Create final read schema to read avro/parquet file.
   * this is auxiliary function used by mergeSchema.
   */
  private Type mergeType(Type type, int currentTypeId) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Type> newTypes = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          Type newType = mergeType(f.type(), f.fieldId());
          newTypes.add(newType);
        }
        return Types.RecordType.get(buildRecordType(record.fields(), newTypes));
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Type newElementType;
        Types.Field elementField = array.fields().get(0);
        newElementType = mergeType(elementField.type(), elementField.fieldId());
        return buildArrayType(array, newElementType);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Type newValueType = mergeType(map.valueType(), map.valueId());
        return buildMapType(map, newValueType);
      default:
        return buildPrimitiveType((Type.PrimitiveType) type, currentTypeId);
    }
  }

  private List<Types.Field> buildRecordType(List<Types.Field> oldFields, List<Type> newTypes) {
    List<Types.Field> newFields = new ArrayList<>();
    for (int i = 0; i < newTypes.size(); i++) {
      Type newType = newTypes.get(i);
      Types.Field oldField = oldFields.get(i);
      int fieldId = oldField.fieldId();
      String fullName = querySchema.findfullName(fieldId);
      if (fileSchema.findField(fieldId) != null) {
        if (fileSchema.findfullName(fieldId).equals(fullName)) {
          // maybe col type changed, deal with it.
          newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), newType, oldField.doc()));
        } else {
          // find rename, deal with it.
          newFields.add(dealWithRename(fieldId, newType, oldField));
        }
      } else {
        // buildFullName
        fullName = normalizeFullName(fullName);
        if (fileSchema.findField(fullName) != null) {
          newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name() + "suffix", oldField.type(), oldField.doc()));
        } else {
          // find add column
          // now there exist some bugs when we use spark update/merge api, those operation will change col optional to required.
          if (ignoreRequiredAttribute) {
            newFields.add(Types.Field.get(oldField.fieldId(), true, oldField.name(), newType, oldField.doc()));
          } else {
            newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), newType, oldField.doc()));
          }
        }
      }
    }
    return newFields;
  }

  private Types.Field dealWithRename(int fieldId, Type newType, Types.Field oldField) {
    Types.Field fieldFromFileSchema = fileSchema.findField(fieldId);
    String nameFromFileSchema = fieldFromFileSchema.name();
    String nameFromQuerySchema = querySchema.findField(fieldId).name();
    String finalFieldName = useColNameFromFileSchema ? nameFromFileSchema : nameFromQuerySchema;
    Type typeFromFileSchema = fieldFromFileSchema.type();
    // Current design mechanism guarantees nestedType change is not allowed, so no need to consider.
    if (newType.isNestedType()) {
      return Types.Field.get(oldField.fieldId(), oldField.isOptional(),
         finalFieldName, newType, oldField.doc());
    } else {
      return Types.Field.get(oldField.fieldId(), oldField.isOptional(),
          finalFieldName, useColumnTypeFromFileSchema ? typeFromFileSchema : newType, oldField.doc());
    }
  }

  private String normalizeFullName(String fullName) {
    // find parent rename, and normalize fullName
    // eg: we renamed a nest field struct(c, d) to aa, the we delete a.d and add it back later.
    String[] nameParts = fullName.split("\\.");
    String[] normalizedNameParts = new String[nameParts.length];
    System.arraycopy(nameParts, 0, normalizedNameParts, 0, nameParts.length);
    for (int j = 0; j < nameParts.length - 1; j++) {
      StringBuilder sb = new StringBuilder();
      for (int k = 0; k <= j; k++) {
        sb.append(nameParts[k]);
      }
      String parentName = sb.toString();
      int parentFieldIdFromQuerySchema = querySchema.findIdByName(parentName);
      String parentNameFromFileSchema = fileSchema.findfullName(parentFieldIdFromQuerySchema);
      if (parentNameFromFileSchema.isEmpty()) {
        break;
      }
      if (!parentNameFromFileSchema.equalsIgnoreCase(parentName)) {
        // find parent rename, update nameParts
        String[] parentNameParts = parentNameFromFileSchema.split("\\.");
        System.arraycopy(parentNameParts, 0, normalizedNameParts, 0, parentNameParts.length);
      }
    }
    return StringUtils.join(normalizedNameParts, ".");
  }

  private Type buildArrayType(Types.ArrayType array, Type newType) {
    Types.Field elementField = array.fields().get(0);
    int elementId = elementField.fieldId();
    if (elementField.type() == newType) {
      return array;
    } else {
      return Types.ArrayType.get(elementId, elementField.isOptional(), newType);
    }
  }

  private Type buildMapType(Types.MapType map, Type newValue) {
    Types.Field valueFiled = map.fields().get(1);
    if (valueFiled.type() == newValue) {
      return map;
    } else {
      return Types.MapType.get(map.keyId(), map.valueId(), map.keyType(), newValue, map.isValueOptional());
    }
  }

  private Type buildPrimitiveType(Type.PrimitiveType typeFromQuerySchema, int currentPrimitiveTypeId) {
    Type typeFromFileSchema = fileSchema.findType(currentPrimitiveTypeId);
    if (typeFromFileSchema == null) {
      return typeFromQuerySchema;
    } else {
      return useColumnTypeFromFileSchema ? typeFromFileSchema : typeFromQuerySchema;
    }
  }
}

