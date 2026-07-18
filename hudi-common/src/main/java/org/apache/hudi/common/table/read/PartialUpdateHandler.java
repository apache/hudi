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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.VisibleForTesting;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CHANGED_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_RETAIN_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.extractWithPrefix;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

/**
 * This class implements the detailed partial update logic for different partial update modes,
 * which is wrapped into partial update mergers
 * {@link BufferedRecordMergerFactory.CommitTimePartialRecordMerger} and
 * {@link BufferedRecordMergerFactory.EventTimePartialRecordMerger}.
 */
public class PartialUpdateHandler<T> implements Serializable {
  private final RecordContext<T> recordContext;
  private final PartialUpdateMode partialUpdateMode;
  private final Map<String, String> mergeProperties;

  public PartialUpdateHandler(RecordContext<T> recordContext,
                              PartialUpdateMode partialUpdateMode,
                              TypedProperties props) {
    this.recordContext = recordContext;
    this.partialUpdateMode = partialUpdateMode;
    this.mergeProperties = parseMergeProperties(props);
  }


  /**
   * Merge records based on partial update mode.
   * Note that if the merging happens, we should always construct merged record with the newer schema (incoming schema).
   * When the incoming schema does not contain metadata fields for COW merging cases, the metadata fields will be
   * supplemented later in the file writer.
   *
   * @param highOrderRecord record with higher commit time or higher ordering value
   * @param lowOrderRecord  record with lower commit time or lower ordering value
   * @param highOrderSchema The schema of highOrderRecord
   * @param lowOrderSchema  The schema of the older record
   * @param newSchema       The schema of the new incoming record
   * @return Partial merged record.
   */
  BufferedRecord<T> partialMerge(BufferedRecord<T> highOrderRecord,
                                 BufferedRecord<T> lowOrderRecord,
                                 HoodieSchema highOrderSchema,
                                 HoodieSchema lowOrderSchema,
                                 HoodieSchema newSchema) {
    // Note that: When either highOrderRecord or lowOrderRecord is a delete record,
    //            skip partial update since delete records do not have meaningful columns.
    if (null == lowOrderRecord
        || highOrderRecord.isDelete()
        || lowOrderRecord.isDelete()) {
      return highOrderRecord;
    }

    switch (partialUpdateMode) {
      case IGNORE_DEFAULTS:
        return reconcileDefaultValues(
            highOrderRecord, lowOrderRecord, highOrderSchema, lowOrderSchema, newSchema);
      case FILL_UNAVAILABLE:
        return reconcileMarkerValues(
            highOrderRecord, lowOrderRecord, highOrderSchema, lowOrderSchema, newSchema);
      case FILL_UNCHANGED:
        return reconcileChangedColumns(
            highOrderRecord, lowOrderRecord, highOrderSchema, lowOrderSchema, newSchema);
      default:
        return highOrderRecord;
    }
  }

  /**
   * Merge two records using an explicit changed-columns list emitted by the CDC source.
   *
   * <p>Unlike {@link #reconcileMarkerValues} (sentinel-driven, string/bytes columns only), this strategy consumes a
   * comma-separated changed-columns field (e.g. Oracle Debezium's {@code _changed_columns}) carried on the higher-order
   * record. Only columns listed there take the incoming value; every other data column preserves the previous version's
   * value, for any column type. This restores partial-update correctness for CDC sources under primary-key-only
   * supplemental logging, where unchanged columns arrive as null/zero placeholders rather than a marker value.
   *
   * <p>CDC metadata columns configured via {@code hoodie.write.partial.update.retain.fields} (ordering/SCN/op, etc.) are
   * always taken from the higher-order record. When the changed-columns field is null or empty (insert / snapshot / full
   * before-after image) the higher-order record is returned unchanged.
   *
   * @param highOrderRecord record with higher commit time or higher ordering value
   * @param lowOrderRecord  record with lower commit time or lower ordering value
   * @param highOrderSchema The schema of highOrderRecord
   * @param lowOrderSchema  The schema of the older record
   * @param newSchema       The schema of the new incoming record
   * @return merged result preserving previous values for columns absent from the changed-columns list.
   */
  @VisibleForTesting
  BufferedRecord<T> reconcileChangedColumns(BufferedRecord<T> highOrderRecord,
                                            BufferedRecord<T> lowOrderRecord,
                                            HoodieSchema highOrderSchema,
                                            HoodieSchema lowOrderSchema,
                                            HoodieSchema newSchema) {
    String changedFieldsCol = mergeProperties.get(PARTIAL_UPDATE_CHANGED_FIELDS);
    // Without a configured changed-columns field there is nothing to reconcile; keep the newer record.
    if (isNullOrEmpty(changedFieldsCol)) {
      return highOrderRecord;
    }
    Object changedRaw = recordContext.getValue(highOrderRecord.getRecord(), highOrderSchema, changedFieldsCol);
    String changedStr = changedRaw == null ? null : recordContext.getTypeConverter().castToString(changedRaw);
    // Null/empty changed-columns denotes an insert, snapshot or full before-after image: take the newer record as-is.
    if (isNullOrEmpty(changedStr)) {
      return highOrderRecord;
    }

    Set<String> changedColumns = splitToSet(changedStr);
    Set<String> retainFromNewer = splitToSet(mergeProperties.get(PARTIAL_UPDATE_RETAIN_FIELDS));
    String unavailableMarker = mergeProperties.get(PARTIAL_UPDATE_UNAVAILABLE_VALUE);

    // The merged record's changed-columns field is the UNION of both records' changed sets, not just the higher
    // record's. A log-vs-log deltaMerge produces an intermediate record later merged against the base, and that base
    // merge uses this field to decide which columns are authoritative. Keeping only the higher record's set would drop
    // a column changed solely by the lower record (disjoint updates), letting the base overwrite it with a placeholder.
    // Mirrors the payload's mergeChangedColumnsSets (which likewise does not trim).
    Object lowChangedRaw = recordContext.getValue(lowOrderRecord.getRecord(), lowOrderSchema, changedFieldsCol);
    Set<String> unionChanged = new HashSet<>(changedColumns);
    unionChanged.addAll(splitToSet(lowChangedRaw == null ? null : recordContext.getTypeConverter().castToString(lowChangedRaw)));
    Object mergedChangedValue = unionChanged.isEmpty()
        ? null : recordContext.convertValueToEngineType(String.join(",", unionChanged));

    List<HoodieSchemaField> fields = newSchema.getFields();
    Object[] fieldVals = new Object[fields.size()];
    int idx = 0;
    boolean updated = false;
    for (HoodieSchemaField field : fields) {
      String fieldName = field.name();
      if (fieldName.equals(changedFieldsCol)) {
        fieldVals[idx++] = mergedChangedValue;
        updated = true;
        continue;
      }
      boolean takeFromNewer = changedColumns.contains(fieldName) || retainFromNewer.contains(fieldName);
      if (takeFromNewer) {
        Object newValue = recordContext.getValue(highOrderRecord.getRecord(), highOrderSchema, fieldName);
        // Toasted defense: a changed string/bytes column whose incoming value is the unavailable sentinel still falls
        // back to the previous value (mirrors FILL_UNAVAILABLE).
        if ((isStringTyped(field) || isBytesTyped(field))
            && null != unavailableMarker
            && unavailableMarker.equals(recordContext.getTypeConverter().castToString(newValue))) {
          fieldVals[idx++] = recordContext.getValue(lowOrderRecord.getRecord(), lowOrderSchema, fieldName);
          updated = true;
        } else {
          fieldVals[idx++] = newValue;
        }
      } else {
        // An unchanged data column: preserve the previous version's value regardless of type.
        fieldVals[idx++] = recordContext.getValue(lowOrderRecord.getRecord(), lowOrderSchema, fieldName);
        updated = true;
      }
    }
    if (!updated) {
      return highOrderRecord;
    }
    T engineRecord = recordContext.constructEngineRecord(newSchema, fieldVals);
    return new BufferedRecord<>(
        highOrderRecord.getRecordKey(),
        highOrderRecord.getOrderingValue(),
        engineRecord,
        newSchema == highOrderSchema ? highOrderRecord.getSchemaId() : lowOrderRecord.getSchemaId(),
        highOrderRecord.getHoodieOperation());
  }

  private static Set<String> splitToSet(String csv) {
    if (isNullOrEmpty(csv)) {
      return Collections.emptySet();
    }
    return Arrays.stream(csv.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toCollection(HashSet::new));
  }

  /**
   * Merge two records with partial merge strategy ignoring default values.
   *
   * @param highOrderRecord record with higher commit time or higher ordering value
   * @param lowOrderRecord  record with lower commit time or lower ordering value
   * @param highOrderSchema The schema of highOrderRecord
   * @param lowOrderSchema  The schema of the older record
   * @param newSchema       The schema of the new incoming record
   * @return merged result with partial updating by ignoring default values.
   */
  BufferedRecord<T> reconcileDefaultValues(BufferedRecord<T> highOrderRecord,
                                           BufferedRecord<T> lowOrderRecord,
                                           HoodieSchema highOrderSchema,
                                           HoodieSchema lowOrderSchema,
                                           HoodieSchema newSchema) {
    List<HoodieSchemaField> fields = newSchema.getFields();
    Object[] fieldVals = new Object[fields.size()];
    int idx = 0;
    boolean updated = false;
    // decide value for each field with default value in new record ignored.
    for (HoodieSchemaField field: fields) {
      String fieldName = field.name();
      // The default value only from the top-level data type is validated. That means,
      // for nested columns, we do not check the leaf level data type defaults.
      Object defaultValue = HoodieSchemaUtils.toJavaDefaultValue(field);
      Object newValue = recordContext.getValue(highOrderRecord.getRecord(), highOrderSchema, fieldName);
      if (defaultValue == newValue) {
        fieldVals[idx++] = recordContext.getValue(lowOrderRecord.getRecord(), lowOrderSchema, fieldName);
        updated = true;
      } else {
        fieldVals[idx++] = newValue;
      }
    }
    if (!updated) {
      return highOrderRecord;
    }
    T engineRecord = recordContext.constructEngineRecord(newSchema, fieldVals);

    return new BufferedRecord<>(
        highOrderRecord.getRecordKey(),
        highOrderRecord.getOrderingValue(),
        engineRecord,
        newSchema == highOrderSchema ? highOrderRecord.getSchemaId() : lowOrderRecord.getSchemaId(),
        highOrderRecord.getHoodieOperation());
  }

  /**
   * Merge two records with partial merge strategy ignoring marker values.
   *
   * @param highOrderRecord record with higher commit time or higher ordering value
   * @param lowOrderRecord  record with lower commit time or lower ordering value
   * @param highOrderSchema The schema of highOrderRecord
   * @param lowOrderSchema  The schema of the older record
   * @param newSchema       The schema of the new incoming record
   * @return merged result with partial updating by ignoring default values.
   */
  BufferedRecord<T> reconcileMarkerValues(BufferedRecord<T> highOrderRecord,
                                          BufferedRecord<T> lowOrderRecord,
                                          HoodieSchema highOrderSchema,
                                          HoodieSchema lowOrderSchema,
                                          HoodieSchema newSchema) {
    List<HoodieSchemaField> fields = newSchema.getFields();
    Object[] fieldVals = new Object[fields.size()];
    String partialUpdateCustomMarker = mergeProperties.get(PARTIAL_UPDATE_UNAVAILABLE_VALUE);
    int idx = 0;
    boolean updated = false;
    // decide value for each field with customized mark value ignored.
    for (HoodieSchemaField field: fields) {
      String fieldName = field.name();
      Object newValue = recordContext.getValue(highOrderRecord.getRecord(), highOrderSchema, fieldName);
      if ((isStringTyped(field) || isBytesTyped(field))
          && null != partialUpdateCustomMarker
          && (partialUpdateCustomMarker.equals(recordContext.getTypeConverter().castToString(newValue)))) {
        fieldVals[idx++] = recordContext.getValue(lowOrderRecord.getRecord(), lowOrderSchema, fieldName);
        updated = true;
      } else {
        fieldVals[idx++] = newValue;
      }
    }
    if (!updated) {
      return highOrderRecord;
    }
    T engineRecord = recordContext.constructEngineRecord(newSchema, fieldVals);
    return new BufferedRecord<>(
        highOrderRecord.getRecordKey(),
        highOrderRecord.getOrderingValue(),
        engineRecord,
        newSchema == highOrderSchema ? highOrderRecord.getSchemaId() : lowOrderRecord.getSchemaId(),
        highOrderRecord.getHoodieOperation());
  }

  static boolean isStringTyped(HoodieSchemaField field) {
    return hasTargetType(field.schema(), HoodieSchemaType.STRING);
  }

  static boolean isBytesTyped(HoodieSchemaField field) {
    return hasTargetType(field.schema(), HoodieSchemaType.BYTES);
  }

  static boolean hasTargetType(HoodieSchema schema, HoodieSchemaType targetType) {
    if (schema.getType() == targetType) {
      return true;
    } else if (schema.getType() == HoodieSchemaType.UNION) {
      // Stream is lazy, so this is efficient even with multiple types
      return schema.getTypes().stream().anyMatch(s -> s.getType() == targetType);
    }
    return false;
  }

  static Map<String, String> parseMergeProperties(TypedProperties props) {
    return extractWithPrefix(props, RECORD_MERGE_PROPERTY_PREFIX);
  }
}
