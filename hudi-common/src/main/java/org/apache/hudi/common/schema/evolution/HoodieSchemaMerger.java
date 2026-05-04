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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.types.Type;
import org.apache.hudi.common.schema.types.Types;
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-path schema merger: combines a file's stored schema with a query schema
 * (and tracks renamed fields) to produce the schema actually used to decode rows
 * from a base file or log block.
 *
 * <p>This is the read-path linchpin: {@code HoodieFileGroupReader},
 * {@code AbstractHoodieLogRecordScanner}, and the parquet readers all converge
 * on this interface. The algorithm walks the query schema and, for each field id,
 * either keeps the field as-is, projects file-side names/types onto it (renames
 * and type changes), suffix-disambiguates a query-only field that collides with
 * a name still present on the file side, or marks it nullable as an
 * "added" column.
 *
 * <p>Implementation note: walks {@link Types.RecordType} / {@link Types.Field}
 * internally and converts at the {@link HoodieSchema} boundary via
 * {@link HoodieSchemaInternalSchemaBridge}. The {@link InternalSchema} wrapper
 * exists only for its id-aware lookup methods ({@code findField(id)},
 * {@code findFullName(id)}, {@code findIdByName(name)}); once those are
 * available natively on {@code HoodieSchema} the bridge step can drop entirely.
 */
public class HoodieSchemaMerger {

  private final HoodieSchema fileSchema;
  private final HoodieSchema querySchema;
  private final InternalSchema fileInternal;
  private final InternalSchema queryInternal;
  // When the Spark update/merge API flips a column's nullability from optional
  // to required, the merger should still treat it as optional. Disabled for
  // strictly-typed callers that want the original required-ness preserved.
  private final boolean ignoreRequiredAttribute;
  // For columns whose type changed, prefer the file's type during decode (so
  // the parquet/avro reader sees the on-disk type) and let downstream record
  // rewriters apply the promotion. Disabled for log readers that promote at
  // record-rewrite time directly via reWriteRecordWithNewSchema.
  private final boolean useColumnTypeFromFileSchema;
  // For renamed columns, prefer the file's name during decode so the parquet
  // reader can locate the column. Disabled for log readers (which can read by
  // the new name and then rewrite).
  private final boolean useColNameFromFileSchema;

  private final Map<String, String> renamedFields = new HashMap<>();

  public HoodieSchemaMerger(HoodieSchema fileSchema,
                            HoodieSchema querySchema,
                            boolean ignoreRequiredAttribute,
                            boolean useColumnTypeFromFileSchema,
                            boolean useColNameFromFileSchema) {
    this.fileSchema = fileSchema;
    this.querySchema = querySchema;
    this.fileInternal = HoodieSchemaInternalSchemaBridge.toInternalSchema(fileSchema);
    this.queryInternal = HoodieSchemaInternalSchemaBridge.toInternalSchema(querySchema);
    this.ignoreRequiredAttribute = ignoreRequiredAttribute;
    this.useColumnTypeFromFileSchema = useColumnTypeFromFileSchema;
    this.useColNameFromFileSchema = useColNameFromFileSchema;
  }

  public HoodieSchemaMerger(HoodieSchema fileSchema,
                            HoodieSchema querySchema,
                            boolean ignoreRequiredAttribute,
                            boolean useColumnTypeFromFileSchema) {
    this(fileSchema, querySchema, ignoreRequiredAttribute, useColumnTypeFromFileSchema, true);
  }

  /**
   * Produces the merged read schema. Field ids carry through from the query schema;
   * column names and types follow the {@code useCol*FromFileSchema} flags set at
   * construction time.
   */
  public HoodieSchema mergeSchema() {
    Types.RecordType merged = (Types.RecordType) mergeType(queryInternal.getRecord(), 0);
    InternalSchema mergedInternal = new InternalSchema(merged);
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(mergedInternal, querySchema.getFullName());
  }

  /**
   * Same as {@link #mergeSchema()} but additionally returns the rename map
   * (query-side full name → file-side leaf name) so downstream record rewriters
   * can project correctly across renames.
   */
  public Pair<HoodieSchema, Map<String, String>> mergeSchemaGetRenamed() {
    HoodieSchema merged = mergeSchema();
    return Pair.of(merged, renamedFields);
  }

  public HoodieSchema getFileSchema() {
    return fileSchema;
  }

  public HoodieSchema getQuerySchema() {
    return querySchema;
  }

  // -------------------------------------------------------------------------
  // Internal Types-tree walk. Algorithm preserved verbatim from the prior
  // legacy InternalSchemaMerger so the bytes / behavior are unchanged.
  // -------------------------------------------------------------------------

  private Type mergeType(Type type, int currentTypeId) {
    switch (type.typeId()) {
      case RECORD: {
        Types.RecordType record = (Types.RecordType) type;
        List<Type> newTypes = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          newTypes.add(mergeType(f.type(), f.fieldId()));
        }
        return Types.RecordType.get(buildRecordType(record.fields(), newTypes));
      }
      case ARRAY: {
        Types.ArrayType array = (Types.ArrayType) type;
        Types.Field elementField = array.fields().get(0);
        Type newElementType = mergeType(elementField.type(), elementField.fieldId());
        return buildArrayType(array, newElementType);
      }
      case MAP: {
        Types.MapType map = (Types.MapType) type;
        Type newValueType = mergeType(map.valueType(), map.valueId());
        return buildMapType(map, newValueType);
      }
      default:
        return buildPrimitiveType((Type.PrimitiveType) type, currentTypeId);
    }
  }

  private List<Types.Field> buildRecordType(List<Types.Field> oldFields, List<Type> newTypes) {
    List<Types.Field> newFields = new ArrayList<>(newTypes.size());
    for (int i = 0; i < newTypes.size(); i++) {
      Type newType = newTypes.get(i);
      Types.Field oldField = oldFields.get(i);
      int fieldId = oldField.fieldId();
      String fullName = queryInternal.findFullName(fieldId);
      if (fileInternal.findField(fieldId) != null) {
        if (fileInternal.findFullName(fieldId).equals(fullName)) {
          // Same name on both sides — only the type may have changed; keep id and name.
          newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), newType, oldField.doc()));
        } else {
          newFields.add(dealWithRename(fieldId, newType, oldField));
        }
      } else {
        // Field id not present in file. It's either truly new (added column) or
        // a name collision with an old, dropped-then-readded column whose name
        // already exists in the file with a different id. Disambiguate the
        // latter by appending "suffix" so the parquet reader doesn't pick up
        // the old column's bytes for the new column.
        fullName = normalizeFullName(fullName);
        if (fileInternal.findField(fullName) != null) {
          newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name() + "suffix", oldField.type(), oldField.doc()));
        } else {
          // New column. Honor the optional override that papers over the
          // Spark update/merge nullability flip when ignoreRequiredAttribute is set.
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
    Types.Field fieldFromFileSchema = fileInternal.findField(fieldId);
    String nameFromFileSchema = fieldFromFileSchema.name();
    String nameFromQuerySchema = queryInternal.findField(fieldId).name();
    String finalFieldName = useColNameFromFileSchema ? nameFromFileSchema : nameFromQuerySchema;
    Type typeFromFileSchema = fieldFromFileSchema.type();
    if (!useColNameFromFileSchema) {
      // Use the full name as the rename key to disambiguate composite-rename
      // scenarios. Example: original schema ROW<f_row: ROW<f1 STRING, f2 BIGINT>, f3 STRING>;
      // rename f_row.f1 -> f_row.f_str AND f3 -> f_str. Keying renamedFields by
      // leaf name alone would have the second rename overwrite the first.
      renamedFields.put(queryInternal.findFullName(fieldId), nameFromFileSchema);
    }
    // Nested-type changes aren't allowed in current schema-evolution rules, so
    // if the new type is nested we know it's structurally identical to the file.
    if (newType.isNestedType()) {
      return Types.Field.get(oldField.fieldId(), oldField.isOptional(),
          finalFieldName, newType, oldField.doc());
    }
    return Types.Field.get(oldField.fieldId(), oldField.isOptional(),
        finalFieldName, useColumnTypeFromFileSchema ? typeFromFileSchema : newType, oldField.doc());
  }

  /**
   * Walks each prefix of {@code fullName} and substitutes any parent name that
   * was renamed on the file side, so a query-side full name like
   * {@code aa.d} (where {@code aa} used to be {@code a}) resolves to
   * {@code a.d} for the file-side lookup. Without this, dropped-and-readded
   * leaves under a renamed parent wouldn't trigger the suffix disambiguator.
   */
  private String normalizeFullName(String fullName) {
    String[] nameParts = fullName.split("\\.");
    String[] normalizedNameParts = new String[nameParts.length];
    System.arraycopy(nameParts, 0, normalizedNameParts, 0, nameParts.length);
    for (int j = 0; j < nameParts.length - 1; j++) {
      StringBuilder sb = new StringBuilder();
      for (int k = 0; k <= j; k++) {
        sb.append(nameParts[k]);
      }
      String parentName = sb.toString();
      int parentFieldIdFromQuerySchema = queryInternal.findIdByName(parentName);
      String parentNameFromFileSchema = fileInternal.findFullName(parentFieldIdFromQuerySchema);
      if (parentNameFromFileSchema.isEmpty()) {
        break;
      }
      if (!parentNameFromFileSchema.equalsIgnoreCase(parentName)) {
        String[] parentNameParts = parentNameFromFileSchema.split("\\.");
        System.arraycopy(parentNameParts, 0, normalizedNameParts, 0, parentNameParts.length);
      }
    }
    return StringUtils.join(normalizedNameParts, ".");
  }

  private Type buildArrayType(Types.ArrayType array, Type newType) {
    Types.Field elementField = array.fields().get(0);
    if (elementField.type() == newType) {
      return array;
    }
    return Types.ArrayType.get(elementField.fieldId(), elementField.isOptional(), newType);
  }

  private Type buildMapType(Types.MapType map, Type newValue) {
    Types.Field valueField = map.fields().get(1);
    if (valueField.type() == newValue) {
      return map;
    }
    return Types.MapType.get(map.keyId(), map.valueId(), map.keyType(), newValue, map.isValueOptional());
  }

  private Type buildPrimitiveType(Type.PrimitiveType typeFromQuerySchema, int currentPrimitiveTypeId) {
    Type typeFromFileSchema = fileInternal.findType(currentPrimitiveTypeId);
    if (typeFromFileSchema == null) {
      return typeFromQuerySchema;
    }
    return useColumnTypeFromFileSchema ? typeFromFileSchema : typeFromQuerySchema;
  }
}
