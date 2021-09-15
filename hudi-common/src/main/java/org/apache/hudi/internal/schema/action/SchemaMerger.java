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
 * auxiliary class.
 * help to merge file schema and query schema to produce final read schema for avro/parquet file
 */
public class SchemaMerger {
  private final InternalSchema fileSchema;
  private final InternalSchema querySchema;
  // now there exist some bugs when we use spark update/merge api,
  // those operation will change col nullability from optional to required which is wrong.
  // Before that bug is fixed, we need to do adapt.
  // if mergeRequiredFiledForce is true, we will ignore the col's required attribute.
  private final Boolean mergeRequiredFiledForce;
  public SchemaMerger(InternalSchema fileSchema, InternalSchema querySchema, Boolean mergeRequiredFiledForce) {
    this.fileSchema = fileSchema;
    this.querySchema = querySchema;
    this.mergeRequiredFiledForce = mergeRequiredFiledForce;
  }

  public List<Types.Field> buildRecordType(List<Types.Field> oldFields, List<Type> newTypes) {
    List<Types.Field> newFields = new ArrayList<>();
    for (int i = 0; i < newTypes.size(); i++) {
      Type newType = newTypes.get(i);
      Types.Field oldField = oldFields.get(i);
      int fieldId = oldField.fieldId();
      String fullName = querySchema.findfullName(fieldId);
      if (fileSchema.findField(fieldId) != null) {
        if (fileSchema.findfullName(fieldId).equals(fullName)) {
          // maybe col type changed, deal with it.
          dealWithColTypeChanged(fieldId, newType, oldField, newFields);
        } else {
          // find rename, deal with it.
          dealWithRename(fieldId, newType, oldField, newFields);
        }
      } else {
        // buildFullName
        fullName = normalizeFullName(fullName);
        if (fileSchema.findField(fullName) != null) {
          newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name() + "suffix", oldField.type(), oldField.doc()));
        } else {
          // find add column
          // now there exist some bugs when we use spark update/merge api, those operation will change col optional to required.
          if (mergeRequiredFiledForce) {
            newFields.add(Types.Field.get(oldField.fieldId(), true, oldField.name(), newType, oldField.doc()));
          } else {
            newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), newType, oldField.doc()));
          }
        }
      }
    }
    return newFields;
  }

  private void dealWithColTypeChanged(int fieldId, Type newType, Types.Field oldField, List<Types.Field> newFields) {
    Type typeFromFileSchema = fileSchema.findField(fieldId).type();
    // Current design mechanism guarantees nestedType change is not allowed, so no need to consider.
    if (newType.isNestedType()) {
      if (newType != oldField.type()) {
        newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), newType, oldField.doc()));
      } else {
        newFields.add(oldField);
      }
    } else {
      newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), typeFromFileSchema, oldField.doc()));
    }
  }

  private void dealWithRename(int fieldId, Type newType, Types.Field oldField, List<Types.Field> newFields) {
    Types.Field fieldFromFileSchema = fileSchema.findField(fieldId);
    String nameFromFileSchema = fieldFromFileSchema.name();
    Type typeFromFileSchema = fieldFromFileSchema.type();
    // Current design mechanism guarantees nestedType change is not allowed, so no need to consider.
    if (newType.isNestedType()) {
      if (newType != oldField.type()) {
        newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), nameFromFileSchema, newType, oldField.doc()));
      } else {
        newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), nameFromFileSchema, oldField.type(), oldField.doc()));
      }
    } else {
      newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), nameFromFileSchema, typeFromFileSchema, oldField.doc()));
    }
  }

  private String normalizeFullName(String fullName) {
    // find parent rename, and normalize fullName eg: we renamed a nest field struct(c, d) to aa, the we delete a.d and add it back later.
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

  public Type buildArrayType(Types.ArrayType array, Type newType) {
    Types.Field elementField = array.fields().get(0);
    int elementId = elementField.fieldId();
    if (elementField.type() == newType) {
      return array;
    } else {
      return Types.ArrayType.get(elementId, elementField.isOptional(), newType);
    }
  }

  public Type buildMapType(Types.MapType map, Type newValue) {
    Types.Field arrayFiled = map.fields().get(1);
    if (arrayFiled.type() == newValue) {
      return map;
    } else {
      return Types.MapType.get(map.keyId(), map.valueId(), map.keyType(), newValue, map.isValueOptional());
    }
  }
}

