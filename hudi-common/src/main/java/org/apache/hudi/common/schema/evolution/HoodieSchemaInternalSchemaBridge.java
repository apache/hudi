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
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.types.Type;
import org.apache.hudi.common.schema.types.Types;
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.schema.evolution.legacy.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.evolution.legacy.utils.InternalSchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * One-way bridge from {@link InternalSchema} to {@link HoodieSchema} that preserves
 * column ids by stamping them as Avro custom properties on the HoodieSchema's
 * underlying schema tree.
 *
 * <p>This exists during the InternalSchema → HoodieSchema migration. The existing
 * {@link InternalSchemaConverter#convert(InternalSchema, String)} produces a
 * structurally-correct HoodieSchema but discards field ids. Downstream code in
 * the new evolution layer relies on {@code field-id} / {@code element-id} /
 * {@code key-id} / {@code value-id} properties being present, so we walk the
 * InternalSchema and stamped HoodieSchema in lock-step and copy ids over.</p>
 *
 * <p>The walk order matches {@code InternalSchemaConverter.visitInternalSchemaToBuildHoodieSchema}
 * (record fields in declared order; array element after array; map key + value after map),
 * so positional pairing is exact.</p>
 *
 * <p>Public for the migration period only — Phase 4 callsite migrations across
 * different packages need access to the conversion. Once Phase 5 rewrites the
 * action algebra on pure HoodieSchema, this bridge and its dependency on
 * {@code InternalSchema} go away.</p>
 */
public final class HoodieSchemaInternalSchemaBridge {

  private HoodieSchemaInternalSchemaBridge() {
  }

  /**
   * Converts a {@link HoodieSchema} to an {@link InternalSchema}, preserving column
   * ids carried as {@code field-id} / {@code element-id} / {@code key-id} /
   * {@code value-id} Avro custom properties. This is the inverse of
   * {@link #toHoodieSchema(InternalSchema, String)} and exists so the façade can
   * round-trip a HoodieSchema through the legacy applier without renumbering ids on
   * every call.
   *
   * <p>For HoodieSchemas that have not yet had ids assigned (e.g. freshly parsed
   * input), this falls back to the existing
   * {@link InternalSchemaConverter#convert(HoodieSchema)} which mints fresh ids.</p>
   */
  public static InternalSchema toInternalSchema(HoodieSchema hoodieSchema) {
    // Short-circuit only on the genuine empty sentinel — a record with no fields.
    // {@link HoodieSchema#isEmptySchema()} is schemaId-based and would mis-classify
    // any freshly-built HoodieSchema (default schemaId=-1) as empty, even when it
    // carries real fields, causing the round-trip to silently lose them.
    if (hoodieSchema == null
        || hoodieSchema.getType() != HoodieSchemaType.RECORD
        || hoodieSchema.getFields().isEmpty()) {
      return InternalSchema.getEmptyInternalSchema();
    }
    // Take the structurally-correct InternalSchema produced by the existing converter,
    // then walk both schemas in parallel and overwrite the InternalSchema's freshly-minted
    // ids with the ids carried as Avro properties on the HoodieSchema (where present).
    InternalSchema fresh = InternalSchemaConverter.convert(hoodieSchema, hoodieSchema.getNameToPosition());
    Types.RecordType originalRecord = fresh.getRecord();
    Types.RecordType reidentified = (Types.RecordType) reidentify(hoodieSchema, originalRecord);
    InternalSchema result = (originalRecord == reidentified)
        ? fresh
        : new InternalSchema(reidentified);
    long schemaId = hoodieSchema.schemaId();
    if (schemaId >= 0) {
      result.setSchemaId(schemaId);
    }
    int maxColumnId = hoodieSchema.maxColumnId();
    if (maxColumnId >= 0) {
      result.setMaxColumnId(maxColumnId);
    }
    return result;
  }

  /**
   * Walks a HoodieSchema and the corresponding InternalSchema {@link Type} in parallel
   * and produces a {@link Type} where each addressable id matches the HoodieSchema's
   * Avro custom property (when present). Returns the original {@code internalType}
   * unchanged when no overrides apply, so callers can short-circuit.
   */
  private static Type reidentify(HoodieSchema hoodieSchema, Type internalType) {
    HoodieSchema effective = hoodieSchema.isNullable() ? hoodieSchema.getNonNullType() : hoodieSchema;
    switch (internalType.typeId()) {
      case RECORD: {
        Types.RecordType record = (Types.RecordType) internalType;
        if (effective.getType() != HoodieSchemaType.RECORD) {
          return internalType;
        }
        List<Types.Field> originalFields = record.fields();
        List<Types.Field> rebuilt = new ArrayList<>(originalFields.size());
        boolean anyChange = false;
        for (int i = 0; i < originalFields.size(); i++) {
          Types.Field original = originalFields.get(i);
          HoodieSchemaField hf = effective.getFields().get(i);
          int overrideId = hf.fieldId();
          Type childType = reidentify(hf.schema(), original.type());
          int finalId = overrideId >= 0 ? overrideId : original.fieldId();
          if (finalId == original.fieldId() && childType == original.type()) {
            rebuilt.add(original);
          } else {
            rebuilt.add(Types.Field.get(finalId, original.isOptional(), original.name(), childType, original.doc()));
            anyChange = true;
          }
        }
        return anyChange ? Types.RecordType.get(rebuilt, record.name()) : record;
      }
      case ARRAY: {
        Types.ArrayType array = (Types.ArrayType) internalType;
        if (effective.getType() != HoodieSchemaType.ARRAY) {
          return internalType;
        }
        int overrideElementId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP), -1);
        Type newElement = reidentify(effective.getElementType(), array.elementType());
        int finalElementId = overrideElementId >= 0 ? overrideElementId : array.elementId();
        if (finalElementId == array.elementId() && newElement == array.elementType()) {
          return array;
        }
        return Types.ArrayType.get(finalElementId, array.isElementOptional(), newElement);
      }
      case MAP: {
        Types.MapType map = (Types.MapType) internalType;
        if (effective.getType() != HoodieSchemaType.MAP) {
          return internalType;
        }
        int overrideKeyId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP), -1);
        int overrideValueId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP), -1);
        Type newValue = reidentify(effective.getValueType(), map.valueType());
        int finalKeyId = overrideKeyId >= 0 ? overrideKeyId : map.keyId();
        int finalValueId = overrideValueId >= 0 ? overrideValueId : map.valueId();
        if (finalKeyId == map.keyId() && finalValueId == map.valueId() && newValue == map.valueType()) {
          return map;
        }
        return Types.MapType.get(finalKeyId, finalValueId, map.keyType(), newValue, map.isValueOptional());
      }
      default:
        return internalType;
    }
  }

  private static int readIntProp(Object raw, int fallback) {
    return raw instanceof Number ? ((Number) raw).intValue() : fallback;
  }

  /**
   * Converts an {@link InternalSchema} to a {@link HoodieSchema} and stamps every
   * sub-schema with the corresponding field id from the source. The schema-level
   * version id and max column id are also propagated.
   */
  public static HoodieSchema toHoodieSchema(InternalSchema internalSchema, String recordName) {
    HoodieSchema hoodieSchema = InternalSchemaConverter.convert(internalSchema, recordName);
    stampIds(hoodieSchema, internalSchema.getRecord());
    hoodieSchema.setSchemaId(internalSchema.schemaId());
    hoodieSchema.setMaxColumnId(internalSchema.getMaxColumnId());
    hoodieSchema.invalidateIdIndex();
    return hoodieSchema;
  }

  /**
   * Prunes {@code source} to the supplied leaf-name list, preserving field ids.
   * The returned HoodieSchema's record name is taken from {@code source}.
   *
   * <p>Single entry point for the bridge round-trip pattern that several call
   * sites had open-coded: bridge to {@link InternalSchema}, prune via
   * {@link InternalSchemaUtils#pruneInternalSchema}, then bridge back.</p>
   */
  public static HoodieSchema pruneByLeafNames(HoodieSchema source, List<String> leafNames) {
    InternalSchema pruned = InternalSchemaUtils.pruneInternalSchema(toInternalSchema(source), leafNames);
    return toHoodieSchema(pruned, source.getFullName());
  }

  /**
   * Prunes {@code source} down to the leaves of {@code requiredSchema}, preserving
   * field ids. Returns a HoodieSchema named after {@code requiredSchema}.
   */
  public static HoodieSchema pruneByRequiredSchema(HoodieSchema source, HoodieSchema requiredSchema) {
    InternalSchema pruned = InternalSchemaUtils.pruneInternalSchema(
        toInternalSchema(source), HoodieSchemaUtils.collectLeafNames(requiredSchema));
    return toHoodieSchema(pruned, requiredSchema.getFullName());
  }

  /**
   * Returns a HoodieSchema with the same fields and ids as {@code source} but with
   * its record name set to {@code recordName}. Walks HoodieSchema directly: the
   * top-level record is rebuilt with a new {@link org.apache.avro.Schema} record
   * name, the field list is rebuilt via {@link HoodieSchemaUtils#createNewSchemaField}
   * (which preserves Avro custom props — including {@code field-id}), and inner
   * schemas pass through by reference so their ids and structure are preserved.
   * The record-name argument is parsed as the legacy {@code namespace.Name}
   * convention if it carries a dot, with the substring after the last dot taken
   * as the simple name.
   */
  public static HoodieSchema withRecordName(HoodieSchema source, String recordName) {
    if (source == null || source.getType() != HoodieSchemaType.RECORD) {
      return source;
    }
    String simpleName;
    String namespace;
    int lastDot = recordName == null ? -1 : recordName.lastIndexOf('.');
    if (lastDot < 0) {
      simpleName = recordName;
      namespace = source.getNamespace().orElse(null);
    } else {
      namespace = recordName.substring(0, lastDot);
      simpleName = recordName.substring(lastDot + 1);
    }
    List<HoodieSchemaField> fields = source.getFields().stream()
        .map(HoodieSchemaUtils::createNewSchemaField)
        .collect(Collectors.toList());
    HoodieSchema renamed = HoodieSchema.createRecord(simpleName, namespace, source.getDoc().orElse(null), fields);
    if (source.schemaId() >= 0) {
      renamed.setSchemaId(source.schemaId());
    }
    if (source.maxColumnId() >= 0) {
      renamed.setMaxColumnId(source.maxColumnId());
    }
    renamed.invalidateIdIndex();
    return renamed;
  }

  private static void stampIds(HoodieSchema hoodieSchema, Type type) {
    HoodieSchema effective = hoodieSchema.isNullable() ? hoodieSchema.getNonNullType() : hoodieSchema;
    switch (type.typeId()) {
      case RECORD: {
        Types.RecordType record = (Types.RecordType) type;
        // The HoodieSchema produced by InternalSchemaConverter preserves the declared
        // field order, so positional pairing with InternalSchema is exact.
        if (effective.getType() != HoodieSchemaType.RECORD) {
          return;
        }
        for (int i = 0; i < record.fields().size(); i++) {
          Types.Field internalField = record.fields().get(i);
          HoodieSchemaField hoodieField = effective.getFields().get(i);
          hoodieField.getAvroField().addProp(HoodieSchema.FIELD_ID_PROP, internalField.fieldId());
          stampIds(hoodieField.schema(), internalField.type());
        }
        return;
      }
      case ARRAY: {
        Types.ArrayType array = (Types.ArrayType) type;
        if (effective.getType() != HoodieSchemaType.ARRAY) {
          return;
        }
        effective.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, array.elementId());
        stampIds(effective.getElementType(), array.elementType());
        return;
      }
      case MAP: {
        Types.MapType map = (Types.MapType) type;
        if (effective.getType() != HoodieSchemaType.MAP) {
          return;
        }
        effective.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, map.keyId());
        effective.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, map.valueId());
        stampIds(effective.getValueType(), map.valueType());
        return;
      }
      default:
        // primitives have no addressable child id
    }
  }
}
