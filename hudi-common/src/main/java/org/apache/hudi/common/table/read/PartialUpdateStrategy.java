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
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED;

public class PartialUpdateStrategy<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PartialUpdateStrategy.class);
  private final HoodieReaderContext<T> readerContext;
  private final PartialUpdateMode partialUpdateMode;
  protected final boolean shouldKeepEventTimeMetadata;
  protected final Option<String> eventTimeFieldOpt;
  protected final boolean shouldKeepConsistentLogicalTimestamp;
  private Map<String, String> partialUpdateProperties;

  public PartialUpdateStrategy(HoodieReaderContext<T> readerContext,
                               PartialUpdateMode partialUpdateMode,
                               TypedProperties props) {
    this.readerContext = readerContext;
    this.partialUpdateMode = partialUpdateMode;
    this.partialUpdateProperties = parsePartialUpdateProperties(props);
    this.shouldKeepEventTimeMetadata = shouldKeepEventTimeMetadata(props);
    this.shouldKeepConsistentLogicalTimestamp = shouldKeepEventTimeMetadata(props);
    if (shouldKeepEventTimeMetadata) {
      this.eventTimeFieldOpt = Option.ofNullable(
          props.getProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY));
    } else {
      this.eventTimeFieldOpt = Option.empty();
      LOG.warn("No event time field is found when event time watermarker is enabled");
    }
  }

  /**
   * Merge records based on partial update mode.
   * Note that {@param newRecord} refers to the record with higher commit time if COMMIT_TIME_ORDERING mode is used,
   * or higher event time if EVENT_TIME_ORDERING mode us used.
   */
  BufferedRecord<T> reconcileFieldsWithOldRecord(BufferedRecord<T> newRecord,
                                                 BufferedRecord<T> oldRecord,
                                                 Schema newSchema,
                                                 Schema oldSchema,
                                                 boolean keepOldMetadataColumns,
                                                 boolean keepEventTimeMetadata) {
    // Note that: When either newRecord or oldRecord is a delete record,
    //            skip partial update since delete records do not have meaningful columns.
    if (partialUpdateMode == PartialUpdateMode.NONE
        || null == oldRecord
        || newRecord.isDelete()
        || oldRecord.isDelete()) {
      return newRecord;
    }

    switch (partialUpdateMode) {
      case KEEP_VALUES:
      case FILL_DEFAULTS:
        return newRecord;
      case IGNORE_DEFAULTS:
        return reconcileDefaultValues(
            newRecord, oldRecord, newSchema, oldSchema, keepOldMetadataColumns, keepEventTimeMetadata);
      case IGNORE_MARKERS:
        return reconcileMarkerValues(
            newRecord, oldRecord, newSchema, oldSchema, keepEventTimeMetadata);
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
                                           boolean keepOldMetadataColumns,
                                           boolean keepEventTimeMetadata) {
    List<Schema.Field> fields = newSchema.getFields();
    Map<Integer, Object> updateValues = new HashMap<>();
    // The default value only from the top-level data type is validated. That means,
    // for nested columns, we do not check the leaf level data type defaults.
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object defaultValue = field.defaultVal();
      Object newValue = readerContext.getValue(
          newRecord.getRecord(), newSchema, fieldName);
      if (defaultValue == newValue
          || (keepOldMetadataColumns && HOODIE_META_COLUMNS_NAME_TO_POS.containsKey(fieldName))) {
        updateValues.put(field.pos(), readerContext.getValue(oldRecord.getRecord(), oldSchema, fieldName));
      }
    }

    T engineRecord;
    BufferedRecord<T> reconciledRecord;
    if (updateValues.isEmpty()) {
      engineRecord = newRecord.getRecord();
      reconciledRecord = newRecord;
    } else {
      engineRecord = readerContext.constructEngineRecord(newSchema, updateValues, newRecord);
      reconciledRecord = new BufferedRecord<>(
          newRecord.getRecordKey(),
          newRecord.getOrderingValue(),
          engineRecord,
          newRecord.getSchemaId(),
          newRecord.isDelete());
    }

    // Add event_time watermarker is specified.
    if (keepEventTimeMetadata && shouldKeepEventTimeMetadata && eventTimeFieldOpt.isPresent()) {
      Option<Object> eventTimeOpt = extractEventTime(engineRecord, newSchema);
      if (eventTimeOpt.isPresent()) {
        reconciledRecord.setEventTime(eventTimeOpt.get().toString());
      }
    }
    return reconciledRecord;
  }

  BufferedRecord<T> reconcileMarkerValues(BufferedRecord<T> newRecord,
                                          BufferedRecord<T> oldRecord,
                                          Schema newSchema,
                                          Schema oldSchema,
                                          boolean keepEventTimeMetadata) {
    List<Schema.Field> fields = newSchema.getFields();
    Map<Integer, Object> updateValues = new HashMap<>();
    String partialUpdateCustomMarker = partialUpdateProperties.get(PARTIAL_UPDATE_CUSTOM_MARKER);
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object newValue = readerContext.getValue(newRecord.getRecord(), newSchema, fieldName);
      if ((isStringTyped(field) || isBytesTyped(field))
          && (partialUpdateCustomMarker.equals(readerContext.getTypeHandler().castToString(newValue)))) {
        updateValues.put(field.pos(), readerContext.getValue(oldRecord.getRecord(), oldSchema, fieldName));
      }
    }

    T engineRecord;
    BufferedRecord<T> reconciledRecord;
    if (updateValues.isEmpty()) {
      engineRecord = newRecord.getRecord();
      reconciledRecord = newRecord;
    } else {
      engineRecord = readerContext.constructEngineRecord(newSchema, updateValues, newRecord);
      reconciledRecord = new BufferedRecord<>(
          newRecord.getRecordKey(),
          newRecord.getOrderingValue(),
          engineRecord,
          newRecord.getSchemaId(),
          newRecord.isDelete());
    }

    // Add event_time watermarker is specified.
    if (keepEventTimeMetadata && shouldKeepEventTimeMetadata && eventTimeFieldOpt.isPresent()) {
      Option<Object> eventTimeOpt = extractEventTime(engineRecord, newSchema);
      if (eventTimeOpt.isPresent()) {
        reconciledRecord.setEventTime(eventTimeOpt.get().toString());
      }
    }
    return reconciledRecord;
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

  static Map<String, String> parsePartialUpdateProperties(TypedProperties props) {
    Map<String, String> properties = new HashMap<>();
    String raw = props.getProperty(HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES.key());
    if (StringUtils.isNullOrEmpty(raw)) {
      return properties;
    }
    String[] entries = raw.split(",");
    for (String entry : entries) {
      String trimmed = entry.trim();
      if (!trimmed.isEmpty()) {
        String[] kv = trimmed.split("=", 2);
        if (kv.length == 2) {
          String key = kv[0].trim();
          String value = kv[1].trim();
          if (!key.isEmpty()) {
            if (StringUtils.isNullOrEmpty(value)) {
              throw new IllegalArgumentException(
                  String.format("Property '%s' is not properly set", key));
            }
            properties.put(key, value);
          }
        }
      }
    }
    return properties;
  }

  static boolean shouldKeepEventTimeMetadata(TypedProperties props) {
    return props.getBoolean("hoodie.write.event.time.watermark.metadata.enabled");
  }

  /**
   * Should keep logical timestamp consistent.
   */
  static boolean shouldKeepConsistentLogicalTimestamp(TypedProperties props) {
    return Boolean.parseBoolean(props.getProperty(
        KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
  }

  /**
   * Extract and store event_time value for the record; later this information will be
   * stored to WriteStats.
   * This function should be only called when merge base record and log record.
   */
  private Option<Object> extractEventTime(T engineRecord, Schema readerSchema) {
    if (shouldKeepEventTimeMetadata) {
      Option<Object> eventTimeOpt = readerContext.getEventTime(
          engineRecord, readerSchema, eventTimeFieldOpt);
      if (eventTimeOpt.isPresent()) {
        Schema.Field field = readerSchema.getField(eventTimeFieldOpt.get());
        Object eventTime = readerContext.getTypeHandler().convertValueForAvroLogicalTypes(
            field.schema(), eventTimeOpt.get(), shouldKeepConsistentLogicalTimestamp);
        return Option.of(eventTime);
      }
    }
    return Option.empty();
  }
}