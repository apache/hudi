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
import java.util.List;
import java.util.Map;

import static org.apache.hudi.avro.HoodieAvroUtils.toJavaDefaultValue;
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
                                 Schema highOrderSchema,
                                 Schema lowOrderSchema,
                                 Schema newSchema) {
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
      default:
        return highOrderRecord;
    }
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
                                           Schema highOrderSchema,
                                           Schema lowOrderSchema,
                                           Schema newSchema) {
    List<Schema.Field> fields = newSchema.getFields();
    Object[] fieldVals = new Object[fields.size()];
    int idx = 0;
    boolean updated = false;
    // decide value for each field with default value in new record ignored.
    for (Schema.Field field: fields) {
      String fieldName = field.name();
      // The default value only from the top-level data type is validated. That means,
      // for nested columns, we do not check the leaf level data type defaults.
      Object defaultValue = toJavaDefaultValue(field);
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
                                          Schema highOrderSchema,
                                          Schema lowOrderSchema,
                                          Schema newSchema) {
    List<Schema.Field> fields = newSchema.getFields();
    Object[] fieldVals = new Object[fields.size()];
    String partialUpdateCustomMarker = mergeProperties.get(PARTIAL_UPDATE_UNAVAILABLE_VALUE);
    int idx = 0;
    boolean updated = false;
    // decide value for each field with customized mark value ignored.
    for (Schema.Field field: fields) {
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
