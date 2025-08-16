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
import org.apache.hudi.common.table.PartialUpdateMode;

import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.avro.HoodieAvroUtils.toJavaDefaultValue;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.extractWithPrefix;

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
   * Note that {@param newRecord} refers to the record with higher commit time if COMMIT_TIME_ORDERING mode is used,
   * or higher event time if EVENT_TIME_ORDERING mode us used.
   */
  BufferedRecord<T> partialMerge(BufferedRecord<T> newRecord,
                                 BufferedRecord<T> oldRecord,
                                 Schema newSchema,
                                 Schema oldSchema,
                                 boolean keepOldMetadataColumns) {
    // Note that: When either newRecord or oldRecord is a delete record,
    //            skip partial update since delete records do not have meaningful columns.
    if (null == oldRecord
        || newRecord.isDelete()
        || oldRecord.isDelete()) {
      return newRecord;
    }

    switch (partialUpdateMode) {
      case IGNORE_DEFAULTS:
        return reconcileDefaultValues(
            newRecord, oldRecord, newSchema, oldSchema, keepOldMetadataColumns);
      case FILL_UNAVAILABLE:
        return reconcileMarkerValues(
            newRecord, oldRecord, newSchema, oldSchema);
      default:
        return newRecord;
    }
  }

  /**
   * @param newRecord              The newer record determined by the merge mode.
   * @param oldRecord              The older record determined by the merge mode.
   * @param newSchema              The schema of the newer record.
   * @param oldSchema              The schema of the older record.
   * @param keepOldMetadataColumns Keep the metadata columns from the older record.
   * @return
   */
  BufferedRecord<T> reconcileDefaultValues(BufferedRecord<T> newRecord,
                                           BufferedRecord<T> oldRecord,
                                           Schema newSchema,
                                           Schema oldSchema,
                                           boolean keepOldMetadataColumns) {
    List<Schema.Field> fields = newSchema.getFields();
    Map<Integer, Object> updateValues = new HashMap<>();
    T engineRecord;
    // The default value only from the top-level data type is validated. That means,
    // for nested columns, we do not check the leaf level data type defaults.
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object defaultValue = toJavaDefaultValue(field);
      Object newValue = recordContext.getValue(
          newRecord.getRecord(), newSchema, fieldName);
      if (defaultValue == newValue
          || (keepOldMetadataColumns && HOODIE_META_COLUMNS_NAME_TO_POS.containsKey(fieldName))) {
        updateValues.put(field.pos(), recordContext.getValue(oldRecord.getRecord(), oldSchema, fieldName));
      }
    }
    if (updateValues.isEmpty()) {
      return newRecord;
    }
    engineRecord = recordContext.mergeWithEngineRecord(newSchema, updateValues, newRecord);
    return new BufferedRecord<>(
        newRecord.getRecordKey(),
        newRecord.getOrderingValue(),
        engineRecord,
        newRecord.getSchemaId(),
        newRecord.getHoodieOperation());
  }

  BufferedRecord<T> reconcileMarkerValues(BufferedRecord<T> newRecord,
                                          BufferedRecord<T> oldRecord,
                                          Schema newSchema,
                                          Schema oldSchema) {
    List<Schema.Field> fields = newSchema.getFields();
    Map<Integer, Object> updateValues = new HashMap<>();
    T engineRecord;
    String partialUpdateCustomMarker = mergeProperties.get(PARTIAL_UPDATE_UNAVAILABLE_VALUE);
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object newValue = recordContext.getValue(newRecord.getRecord(), newSchema, fieldName);
      if ((isStringTyped(field) || isBytesTyped(field))
          && null != partialUpdateCustomMarker
          && (partialUpdateCustomMarker.equals(recordContext.getTypeConverter().castToString(newValue)))) {
        updateValues.put(
            field.pos(),
            recordContext.getValue(oldRecord.getRecord(), oldSchema, fieldName));
      }
    }
    if (updateValues.isEmpty()) {
      return newRecord;
    }
    engineRecord = recordContext.mergeWithEngineRecord(newSchema, updateValues, newRecord);
    return new BufferedRecord<>(
        newRecord.getRecordKey(),
        newRecord.getOrderingValue(),
        engineRecord,
        newRecord.getSchemaId(),
        newRecord.getHoodieOperation());
  }

  static boolean isStringTyped(Schema.Field field) {
    return hasTargetType(field.schema(), Schema.Type.STRING);
  }

  static boolean isBytesTyped(Schema.Field field) {
    return hasTargetType(field.schema(), Schema.Type.BYTES);
  }

  static boolean hasTargetType(Schema schema, Schema.Type targetType) {
    if (schema.getType() == targetType) {
      return true;
    } else if (schema.getType() == Schema.Type.UNION) {
      // Stream is lazy, so this is efficient even with multiple types
      return schema.getTypes().stream().anyMatch(s -> s.getType() == targetType);
    }
    return false;
  }

  static Map<String, String> parseMergeProperties(TypedProperties props) {
    return extractWithPrefix(props, RECORD_MERGE_PROPERTY_PREFIX);
  }
}
