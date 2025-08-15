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

package org.apache.hudi.common.model;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.DEFAULT_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;

/**
 * Every Hoodie table has an implementation of the <code>HoodieRecordPayload</code> This abstracts out callbacks which depend on record specific logic.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public interface HoodieRecordPayload<T extends HoodieRecordPayload> extends Serializable {

  /**
   * This method is deprecated. Please use this {@link #preCombine(HoodieRecordPayload, Properties)} method.
   */
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  T preCombine(T oldValue);

  /**
   * When more than one HoodieRecord have the same HoodieKey in the incoming batch, this function combines them before attempting to insert/upsert by taking in a property map.
   * Implementation can leverage the property to decide their business logic to do preCombine.
   *
   * @param oldValue   instance of the old {@link HoodieRecordPayload} to be combined with.
   * @param properties Payload related properties. For example pass the ordering field(s) name to extract from value in storage.
   *
   * @return the combined value
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  default T preCombine(T oldValue, Properties properties) {
    return preCombine(oldValue);
  }

  /**
   * When more than one HoodieRecord have the same HoodieKey in the incoming batch, this function combines them before attempting to insert/upsert by taking in a schema.
   * Implementation can leverage the schema to decide their business logic to do preCombine.
   *
   * @param oldValue   instance of the old {@link HoodieRecordPayload} to be combined with.
   * @param schema     Payload related schema. For example use schema to overwrite old instance for specified fields that doesn't equal to default value.
   * @param properties Payload related properties. For example pass the ordering field(s) name to extract from value in storage.
   * @return the combined value
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default T preCombine(T oldValue, Schema schema, Properties properties) {
    return preCombine(oldValue, properties);
  }

  /**
   * This methods is deprecated. Please refer to {@link #combineAndGetUpdateValue(IndexedRecord, Schema, Properties)} for java docs.
   */
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException;

  /**
   * This methods lets you write custom merging/combining logic to produce new values as a function of current value on storage and whats contained
   * in this object. Implementations can leverage properties if required.
   * <p>
   * eg:
   * 1) You are updating counters, you may want to add counts to currentValue and write back updated counts
   * 2) You may be reading DB redo logs, and merge them with current image for a database row on storage
   * </p>
   *
   * @param currentValue Current value in storage, to merge/combine this payload with
   * @param schema Schema used for record
   * @param properties Payload related properties. For example pass the ordering field(s) name to extract from value in storage.
   * @return new combined/merged value to be written back to storage. EMPTY to skip writing this record.
   */
  default Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    return combineAndGetUpdateValue(currentValue, schema);
  }

  /**
   * This method is deprecated. Refer to {@link #getInsertValue(Schema, Properties)} for java docs.
   * @param schema Schema used for record
   * @return the {@link IndexedRecord} to be inserted.
   */
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  Option<IndexedRecord> getInsertValue(Schema schema) throws IOException;

  /**
   * Deserializes the HoodieRecordPayload into an {@link IndexedRecord}.
   * Unlike {@link #getInsertValue(Schema, Properties)}, this method is meant to solely perform deserialization.
   *
   * @param schema     Schema to use for reading the record
   * @param properties Properties for the current context
   * @return the {@link IndexedRecord} if one is available, otherwise returns an empty Option.
   * @throws IOException thrown if there is an error during deserialization
   */
  default Option<IndexedRecord> getIndexedRecord(Schema schema, Properties properties) throws IOException {
    return getInsertValue(schema, properties);
  }

  /**
   * Generates an avro record out of the given HoodieRecordPayload, to be written out to storage. Called when writing a new value for the given
   * HoodieKey, wherein there is no existing record in storage to be combined against. (i.e insert) Return EMPTY to skip writing this record.
   * Implementations can leverage properties if required.
   * @param schema Schema used for record
   * @param properties Payload related properties. For example pass the ordering field(s) name to extract from value in storage.
   * @return the {@link IndexedRecord} to be inserted.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  default Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
    return getInsertValue(schema);
  }

  /**
   * This method can be used to extract some metadata from HoodieRecordPayload. The metadata is passed to {@code WriteStatus.markSuccess()} and
   * {@code WriteStatus.markFailure()} in order to compute some aggregate metrics using the metadata in the context of a write success or failure.
   * @return the metadata in the form of Map<String, String> if any.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  default Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  /**
   * This method can be used to extract the ordering value of the payload for combining/merging,
   * or 0 if no value is specified which means natural order(arrival time is used).
   *
   * @return the ordering value
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  default Comparable<?> getOrderingValue() {
    // default natural order
    return 0;
  }

  static String getAvroPayloadForMergeMode(RecordMergeMode mergeMode, String payloadClassName) {
    switch (mergeMode) {
      //TODO: After we have merge mode working for writing, we should have a dummy payload that will throw exception when used
      case EVENT_TIME_ORDERING:
        if (!StringUtils.isNullOrEmpty(payloadClassName)
            && payloadClassName.contains(EventTimeAvroPayload.class.getName())) {
          return EventTimeAvroPayload.class.getName();
        }
        return DefaultHoodieRecordPayload.class.getName();
      case COMMIT_TIME_ORDERING:
        return OverwriteWithLatestAvroPayload.class.getName();
      case CUSTOM:
      default:
        return payloadClassName;
    }
  }

  static String getPayloadClassName(HoodieConfig config) {
    return getPayloadClassName(config.getProps());
  }

  static String getPayloadClassName(Properties props) {
    Option<String> payloadOpt = getPayloadClassNameIfPresent(props);
    if (payloadOpt.isPresent()) {
      return payloadOpt.get();
    }
    // Note: starting from version 9, payload class is not necessary set, but
    //       merge mode must exist. Therefore, we use merge mode to infer
    //       the payload class for certain corner cases, like for MIT command.
    if (ConfigUtils.containsConfigProperty(props, RECORD_MERGE_MODE)
        && ConfigUtils.getStringWithAltKeys(props, RECORD_MERGE_MODE, StringUtils.EMPTY_STRING)
        .equals(RecordMergeMode.COMMIT_TIME_ORDERING.name())) {
      return OverwriteWithLatestAvroPayload.class.getName();
    }
    return DEFAULT_PAYLOAD_CLASS_NAME;
  }

  // NOTE: PAYLOAD_CLASS_NAME is before LEGACY_PAYLOAD_CLASS_NAME to make sure
  // some temporary payload class setting is respected.
  static Option<String> getPayloadClassNameIfPresent(Properties props) {
    String payloadClassName = null;
    if (ConfigUtils.containsConfigProperty(props, PAYLOAD_CLASS_NAME)) {
      payloadClassName = ConfigUtils.getStringWithAltKeys(props, PAYLOAD_CLASS_NAME);
    } else if (props.containsKey(LEGACY_PAYLOAD_CLASS_NAME.key())) {
      payloadClassName = ConfigUtils.getStringWithAltKeys(props, LEGACY_PAYLOAD_CLASS_NAME);
    } else if (props.containsKey("hoodie.datasource.write.payload.class")) {
      payloadClassName = props.getProperty("hoodie.datasource.write.payload.class");
    }

    // There could be tables written with payload class from com.uber.hoodie.
    // Need to transparently change to org.apache.hudi.
    return Option.ofNullable(payloadClassName).map(className -> className.replace("com.uber.hoodie", "org.apache.hudi"));
  }
}
