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

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Single Record managed by Hoodie.
 */
public abstract class HoodieRecord<T> implements HoodieRecordCompatibilityInterface, KryoSerializable, Serializable {

  private static final long serialVersionUID = 3015229555587559252L;
  public static final String COMMIT_TIME_METADATA_FIELD = HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName();
  public static final String COMMIT_SEQNO_METADATA_FIELD = HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD.getFieldName();
  public static final String RECORD_KEY_METADATA_FIELD = HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName();
  public static final String PARTITION_PATH_METADATA_FIELD = HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.getFieldName();
  public static final String FILENAME_METADATA_FIELD = HoodieMetadataField.FILENAME_METADATA_FIELD.getFieldName();
  public static final String OPERATION_METADATA_FIELD = HoodieMetadataField.OPERATION_METADATA_FIELD.getFieldName();
  public static final String HOODIE_IS_DELETED_FIELD = "_hoodie_is_deleted";
  // If the ordering value is not set, this default order value is set and
  // always treated as the commit time ordering.
  public static final int DEFAULT_ORDERING_VALUE = 0;

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
  protected HoodieRecordLocation currentLocation;

  /**
   * New location of record on storage, after written.
   */
  protected HoodieRecordLocation newLocation;

  /**
   * If set, not update index after written.
   */
  protected boolean ignoreIndexUpdate;

  /**
   * Indicates whether the object is sealed.
   */
  private boolean sealed;

  /**
   * The cdc operation.
   */
  protected HoodieOperation operation;

  /**
   * The metaData of the record.
   */
  protected Option<Map<String, String>> metaData;

  protected transient Comparable<?> orderingValue;

  public HoodieRecord(HoodieKey key, T data) {
    this(key, data, null, Option.empty());
  }

  public HoodieRecord(HoodieKey key, T data, HoodieOperation operation, Option<Map<String, String>> metaData) {
    this.key = key;
    this.data = data;
    this.currentLocation = null;
    this.newLocation = null;
    this.sealed = false;
    this.ignoreIndexUpdate = false;
    this.operation = operation;
    this.metaData = metaData;
  }

  public HoodieRecord(
      HoodieKey key,
      T data,
      HoodieOperation operation,
      HoodieRecordLocation currentLocation,
      HoodieRecordLocation newLocation) {
    this.key = key;
    this.data = data;
    this.currentLocation = currentLocation;
    this.newLocation = newLocation;
    this.operation = operation;
  }

  public HoodieRecord(HoodieRecord<T> record) {
    this(record.key, record.data, record.operation, record.metaData);
    this.currentLocation = record.currentLocation;
    this.newLocation = record.newLocation;
    this.sealed = record.sealed;
    this.ignoreIndexUpdate = record.ignoreIndexUpdate;
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

  /**
   * Get ordering value for the record from the cached variable, or extracting from the record if not cached.
   *
   * @param recordSchema Avro schema for the record
   * @param props Properties containing the necessary configurations
   * @param orderingFields name of the ordering fields
   * @return The ordering value for the record
   */
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props, String[] orderingFields) {
    if (orderingValue == null) {
      orderingValue = doGetOrderingValue(recordSchema, props, orderingFields);
    }
    return orderingValue;
  }

  /**
   * Extracting the ordering value from the record.
   *
   * @param recordSchema Avro schema for the record
   * @param props Properties containing the necessary configurations
   * @param orderingFields name of the ordering fields
   * @return The ordering value for the record
   */
  protected abstract Comparable<?> doGetOrderingValue(Schema recordSchema, Properties props, String[] orderingFields);

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

  @Nullable
  public HoodieRecordLocation getCurrentLocation() {
    return currentLocation;
  }

  /**
   * Sets the new currentLocation of the record, after being written. This again should happen exactly-once.
   */
  public void setNewLocation(HoodieRecordLocation location) {
    checkState();
    assert newLocation == null;
    this.newLocation = location;
  }

  @Nullable
  public HoodieRecordLocation getNewLocation() {
    return this.newLocation;
  }

  public boolean isCurrentLocationKnown() {
    return this.currentLocation != null;
  }

  public long getCurrentPosition() {
    if (isCurrentLocationKnown()) {
      return this.currentLocation.getPosition();
    }
    return HoodieRecordLocation.INVALID_POSITION;
  }

  /**
   * Sets the ignore flag.
   */
  public void setIgnoreIndexUpdate(boolean ignoreFlag) {
    this.ignoreIndexUpdate = ignoreFlag;
  }

  public boolean getIgnoreIndexUpdate() {
    return this.ignoreIndexUpdate;
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
        && Objects.equals(currentLocation, that.currentLocation) && Objects.equals(newLocation, that.newLocation)
        && Objects.equals(ignoreIndexUpdate, that.ignoreIndexUpdate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, data, currentLocation, newLocation);
  }

  @Override
  public String toString() {
    return "HoodieRecord{" + "key=" + key
        + ", currentLocation='" + currentLocation + '\''
        + ", newLocation='" + newLocation + '\''
        + '}';
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

  protected abstract void writeRecordPayload(T payload, Kryo kryo, Output output);

  protected abstract T readRecordPayload(Kryo kryo, Input input);

  /**
   * Clears the new currentLocation of the record. 
   *
   * This is required in the delete path so that Index can track that this record was deleted.
   */
  public void clearNewLocation() {
    checkState();
    this.newLocation = null;
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @Override
  public final void write(Kryo kryo, Output output) {
    kryo.writeObjectOrNull(output, key, HoodieKey.class);
    kryo.writeObjectOrNull(output, operation, HoodieOperation.class);
    // NOTE: We have to write actual class along with the object here,
    //       since [[HoodieRecordLocation]] has inheritors
    kryo.writeClassAndObject(output, currentLocation);
    kryo.writeClassAndObject(output, newLocation);
    // NOTE: Writing out actual record payload is relegated to the actual
    //       implementation
    writeRecordPayload(data, kryo, output);
    kryo.writeObjectOrNull(output, ignoreIndexUpdate, Boolean.class);
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @Override
  public final void read(Kryo kryo, Input input) {
    this.key = kryo.readObjectOrNull(input, HoodieKey.class);
    this.operation = kryo.readObjectOrNull(input, HoodieOperation.class);
    this.currentLocation = (HoodieRecordLocation) kryo.readClassAndObject(input);
    this.newLocation = (HoodieRecordLocation) kryo.readClassAndObject(input);
    // NOTE: Reading out actual record payload is relegated to the actual
    //       implementation
    this.data = readRecordPayload(kryo, input);
    this.ignoreIndexUpdate = kryo.readObjectOrNull(input, Boolean.class);

    // NOTE: We're always seal object after deserialization
    this.sealed = true;
  }

  /**
   * This method converts a value for a column with certain Avro Logical data types that require special handling.
   * <p>
   * E.g., Logical Date Type is converted to actual Date value instead of Epoch Integer which is how it is
   * represented/stored in parquet.
   * <p>
   * E.g., Decimal Data Type is converted to actual decimal value instead of bytes/fixed which is how it is
   * represented/stored in parquet.
   */
  public abstract Object convertColumnValueForLogicalType(
      Schema fieldSchema, Object fieldValue, boolean keepConsistentLogicalTimestamp);

  /**
   * Get column in record to support RDDCustomColumnsSortPartitioner
   * @return
   */
  public abstract Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled);

  /**
   * Get column as Java type in record to collect index stats: col_stats, secondary index, etc.
   * The Java type is required to keep the value engine agnostic.
   *
   * @return the column value
   */
  public abstract Object getColumnValueAsJava(Schema recordSchema, String column, Properties props);

  /**
   * Support bootstrap.
   */
  public abstract HoodieRecord joinWith(HoodieRecord other, Schema targetSchema);

  /**
   * Rewrites record into new target schema containing Hudi-specific meta-fields
   *
   * NOTE: This operation is idempotent
   */
  public abstract HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props);

  /**
   * Update a specific metadata field with given value.
   *
   * @param recordSchema the schema for the record
   * @param ordinal the ordinal for the target medata field
   * @param value the new value for the target metadata field
   * @return the new HoodieRecord with updated metadata value
   */
  public HoodieRecord updateMetaField(Schema recordSchema, int ordinal, String value) {
    throw new UnsupportedOperationException("updateMetaField is not supported yet for: " + this.getClass().getSimpleName());
  }

  /**
   * Support schema evolution.
   */
  public abstract HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols);

  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema) {
    return rewriteRecordWithNewSchema(recordSchema, props, newSchema, Collections.emptyMap());
  }

  public abstract boolean isDelete(Schema recordSchema, Properties props) throws IOException;

  /**
   * Is EmptyRecord. Generated by ExpressionPayload.
   */
  public abstract boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException;

  /**
   * This is used to copy data.
   */
  public abstract HoodieRecord<T> copy();

  public abstract Option<Map<String, String>> getMetadata();

  public static String generateSequenceId(String instantTime, int partitionId, long recordIndex) {
    return instantTime + "_" + partitionId + "_" + recordIndex;
  }

  protected static boolean hasMetaFields(Schema schema) {
    return schema.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD) != null;
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
    private EmptyRecord() {
    }

    @Override
    public void put(int i, Object v) {
    }

    @Override
    public Object get(int i) {
      return null;
    }

    @Override
    public Schema getSchema() {
      return null;
    }

    @Override
    public void put(String key, Object v) {
    }

    @Override
    public Object get(String key) {
      return null;
    }
  }

  public enum HoodieRecordType {
    AVRO, SPARK, HIVE, FLINK
  }
}
