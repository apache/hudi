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

import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.FlatLists.ComparableList;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Single Record managed by Hoodie.
 */
public abstract class HoodieRecord<T> implements HoodieRecordCompatibilityInterface, Serializable {

  public static final String COMMIT_TIME_METADATA_FIELD = HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName();
  public static final String COMMIT_SEQNO_METADATA_FIELD = HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD.getFieldName();
  public static final String RECORD_KEY_METADATA_FIELD = HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName();
  public static final String PARTITION_PATH_METADATA_FIELD = HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.getFieldName();
  public static final String FILENAME_METADATA_FIELD = HoodieMetadataField.FILENAME_METADATA_FIELD.getFieldName();
  public static final String OPERATION_METADATA_FIELD = HoodieMetadataField.OPERATION_METADATA_FIELD.getFieldName();
  public static final String HOODIE_IS_DELETED_FIELD = "_hoodie_is_deleted";

  public enum HoodieMetadataField {
    COMMIT_TIME_METADATA_FIELD("_hoodie_commit_time"),
    COMMIT_SEQNO_METADATA_FIELD("_hoodie_commit_seqno"),
    RECORD_KEY_METADATA_FIELD("_hoodie_record_key"),
    PARTITION_PATH_METADATA_FIELD("_hoodie_partition_path"),
    FILENAME_METADATA_FIELD("_hoodie_file_name"),
    OPERATION_METADATA_FIELD("_hoodie_operation");

    private final String fieldName;

    HoodieMetadataField(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }
  }

  /**
   * A special record returned by {@link HoodieRecordPayload}, which means we should just skip this record.
   * This record is only used for {@link HoodieRecordPayload} currently, so it should not
   * shuffle though network, we can compare the record locally by the equal method.
   * The HoodieRecordPayload#combineAndGetUpdateValue and HoodieRecordPayload#getInsertValue
   * have 3 kind of return:
   * 1、Option.empty
   * This means we should delete this record.
   * 2、IGNORE_RECORD
   * This means we should not process this record,just skip.
   * 3、Other non-empty record
   * This means we should process this record.
   *
   * We can see the usage of IGNORE_RECORD in
   * org.apache.spark.sql.hudi.command.payload.ExpressionPayload
   */
  public static final EmptyRecord SENTINEL = new EmptyRecord();

  public static final List<String> HOODIE_META_COLUMNS =
      CollectionUtils.createImmutableList(COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD,
          RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD, FILENAME_METADATA_FIELD);

  // Temporary to support the '_hoodie_operation' field, once we solve
  // the compatibility problem, it can be removed.
  public static final Set<String> HOODIE_META_COLUMNS_WITH_OPERATION =
      CollectionUtils.createImmutableSet(COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD,
          RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD, FILENAME_METADATA_FIELD,
          OPERATION_METADATA_FIELD);

  public static final Map<String, Integer> HOODIE_META_COLUMNS_NAME_TO_POS =
      IntStream.range(0, HOODIE_META_COLUMNS.size()).mapToObj(idx -> Pair.of(HOODIE_META_COLUMNS.get(idx), idx))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

  public static int RECORD_KEY_META_FIELD_ORD = HOODIE_META_COLUMNS_NAME_TO_POS.get(RECORD_KEY_METADATA_FIELD);
  public static int PARTITION_PATH_META_FIELD_ORD = HOODIE_META_COLUMNS_NAME_TO_POS.get(PARTITION_PATH_METADATA_FIELD);
  public static int FILENAME_META_FIELD_ORD = HOODIE_META_COLUMNS_NAME_TO_POS.get(FILENAME_METADATA_FIELD);
  public static int COMMIT_TIME_METADATA_FIELD_ORD = HOODIE_META_COLUMNS_NAME_TO_POS.get(COMMIT_TIME_METADATA_FIELD);
  public static int COMMIT_SEQNO_METADATA_FIELD_ORD = HOODIE_META_COLUMNS_NAME_TO_POS.get(COMMIT_SEQNO_METADATA_FIELD);

  /**
   * Identifies the record across the table.
   */
  protected HoodieKey key;

  /**
   * Actual payload of the record.
   */
  protected T data;

  /**
   * Current location of record on storage. Filled in by looking up index
   */
  private HoodieRecordLocation currentLocation;

  /**
   * New location of record on storage, after written.
   */
  private HoodieRecordLocation newLocation;

  /**
   * Indicates whether the object is sealed.
   */
  private boolean sealed;

  /**
   * The cdc operation.
   */
  protected HoodieOperation operation;

  public HoodieRecord(HoodieKey key, T data) {
    this(key, data, null);
  }

  public HoodieRecord(HoodieKey key, T data, HoodieOperation operation) {
    this.key = key;
    this.data = data;
    this.currentLocation = null;
    this.newLocation = null;
    this.sealed = false;
    this.operation = operation;
  }

  public HoodieRecord(HoodieRecord<T> record) {
    this(record.key, record.data, record.operation);
    this.currentLocation = record.currentLocation;
    this.newLocation = record.newLocation;
    this.sealed = record.sealed;
  }

  public HoodieRecord() {
  }

  public abstract HoodieRecord<T> newInstance();

  public abstract HoodieRecord<T> newInstance(HoodieKey key, HoodieOperation op);

  public abstract HoodieRecord<T> newInstance(HoodieKey key);

  public HoodieKey getKey() {
    return key;
  }

  public HoodieOperation getOperation() {
    return operation;
  }

  public abstract Comparable<?> getOrderingValue(Schema recordSchema, Properties props);

  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  /**
   * Release the actual payload, to ease memory pressure. To be called after the record has been written to storage.
   * Once deflated, cannot be inflated.
   */
  public void deflate() {
    this.data = null;
  }

  /**
   * Sets the current currentLocation of the record. This should happen exactly-once
   */
  public HoodieRecord setCurrentLocation(HoodieRecordLocation location) {
    checkState();
    assert currentLocation == null;
    this.currentLocation = location;
    return this;
  }

  public HoodieRecordLocation getCurrentLocation() {
    return currentLocation;
  }

  /**
   * Sets the new currentLocation of the record, after being written. This again should happen exactly-once.
   */
  public HoodieRecord setNewLocation(HoodieRecordLocation location) {
    checkState();
    assert newLocation == null;
    this.newLocation = location;
    return this;
  }

  public Option<HoodieRecordLocation> getNewLocation() {
    return Option.ofNullable(this.newLocation);
  }

  public boolean isCurrentLocationKnown() {
    return this.currentLocation != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieRecord that = (HoodieRecord) o;
    return Objects.equals(key, that.key) && Objects.equals(data, that.data)
        && Objects.equals(currentLocation, that.currentLocation) && Objects.equals(newLocation, that.newLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, data, currentLocation, newLocation);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieRecord{");
    sb.append("key=").append(key);
    sb.append(", currentLocation='").append(currentLocation).append('\'');
    sb.append(", newLocation='").append(newLocation).append('\'');
    sb.append('}');
    return sb.toString();
  }

  public String getPartitionPath() {
    assert key != null;
    return key.getPartitionPath();
  }

  public String getRecordKey() {
    assert key != null;
    return key.getRecordKey();
  }

  public abstract HoodieRecordType getRecordType();

  public abstract String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt);

  public abstract String getRecordKey(Schema recordSchema, String keyFieldName);

  public void seal() {
    this.sealed = true;
  }

  public void unseal() {
    this.sealed = false;
  }

  public void checkState() {
    if (sealed) {
      throw new UnsupportedOperationException("Not allowed to modify after sealed");
    }
  }

  /**
   * Get column in record to support RDDCustomColumnsSortPartitioner
   * @return
   */
  public abstract ComparableList getComparableColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled);

  /**
   * Support bootstrap.
   */
  public abstract HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) throws IOException;

  /**
   * Rewrite record into new schema(add meta columns)
   */
  public abstract HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema) throws IOException;

  /**
   * Support schema evolution.
   */
  public abstract HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException;

  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema) throws IOException {
    return rewriteRecordWithNewSchema(recordSchema, props, newSchema, Collections.emptyMap());
  }

  /**
   * This method could change in the future.
   * @temporary
   */
  public abstract HoodieRecord updateMetadataValues(Schema recordSchema, Properties props, MetadataValues metadataValues) throws IOException;

  public abstract boolean isDelete(Schema recordSchema, Properties props) throws IOException;

  /**
   * Is EmptyRecord. Generated by ExpressionPayload.
   */
  public abstract boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException;

  public abstract Option<Map<String, String>> getMetadata();

  public static String generateSequenceId(String instantTime, int partitionId, long recordIndex) {
    return instantTime + "_" + partitionId + "_" + recordIndex;
  }

  /**
   * A special record returned by {@link HoodieRecordPayload}, which means we should just skip this record.
   * This record is only used for {@link HoodieRecordPayload} currently, so it should not
   * shuffle though network, we can compare the record locally by the equal method.
   * The HoodieRecordPayload#combineAndGetUpdateValue and HoodieRecordPayload#getInsertValue
   * have 3 kind of return:
   * 1、Option.empty
   * This means we should delete this record.
   * 2、IGNORE_RECORD
   * This means we should not process this record,just skip.
   * 3、Other non-empty record
   * This means we should process this record.
   *
   * We can see the usage of IGNORE_RECORD in
   * org.apache.spark.sql.hudi.command.payload.ExpressionPayload
   */
  private static class EmptyRecord implements GenericRecord {
    private EmptyRecord() {}

    @Override
    public void put(int i, Object v) {}

    @Override
    public Object get(int i) {
      return null;
    }

    @Override
    public Schema getSchema() {
      return null;
    }

    @Override
    public void put(String key, Object v) {}

    @Override
    public Object get(String key) {
      return null;
    }
  }

  public enum HoodieRecordType {
    AVRO, SPARK
  }
}
