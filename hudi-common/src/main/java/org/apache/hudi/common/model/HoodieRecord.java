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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Single Record managed by Hoodie.
 */
public abstract class HoodieRecord<T> implements Serializable {

  public static final String COMMIT_TIME_METADATA_FIELD = "_hoodie_commit_time";
  public static final String COMMIT_SEQNO_METADATA_FIELD = "_hoodie_commit_seqno";
  public static final String RECORD_KEY_METADATA_FIELD = "_hoodie_record_key";
  public static final String PARTITION_PATH_METADATA_FIELD = "_hoodie_partition_path";
  public static final String FILENAME_METADATA_FIELD = "_hoodie_file_name";
  public static final String OPERATION_METADATA_FIELD = "_hoodie_operation";
  public static final String HOODIE_IS_DELETED = "_hoodie_is_deleted";

  public static final List<String> HOODIE_META_COLUMNS =
      CollectionUtils.createImmutableList(COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD,
          RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD, FILENAME_METADATA_FIELD);

  // Temporary to support the '_hoodie_operation' field, once we solve
  // the compatibility problem, it can be removed.
  public static final List<String> HOODIE_META_COLUMNS_WITH_OPERATION =
      CollectionUtils.createImmutableList(COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD,
          RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD, FILENAME_METADATA_FIELD,
          OPERATION_METADATA_FIELD);

  public static final Map<String, Integer> HOODIE_META_COLUMNS_NAME_TO_POS =
      IntStream.range(0, HOODIE_META_COLUMNS.size()).mapToObj(idx -> Pair.of(HOODIE_META_COLUMNS.get(idx), idx))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

  public static int RECORD_KEY_META_FIELD_POS = HOODIE_META_COLUMNS_NAME_TO_POS.get(RECORD_KEY_METADATA_FIELD);
  public static int PARTITION_PATH_META_FIELD_POS = HOODIE_META_COLUMNS_NAME_TO_POS.get(PARTITION_PATH_METADATA_FIELD);
  public static int FILENAME_META_FIELD_POS = HOODIE_META_COLUMNS_NAME_TO_POS.get(FILENAME_METADATA_FIELD);

  /**
   * Identifies the record across the table.
   */
  private HoodieKey key;

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
  private HoodieOperation operation;

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
    this(record.key, record.data);
    this.currentLocation = record.currentLocation;
    this.newLocation = record.newLocation;
    this.sealed = record.sealed;
    this.operation = record.operation;
  }

  public HoodieRecord() {
  }

  public abstract HoodieRecord<T> newInstance();

  public HoodieKey getKey() {
    return key;
  }

  public HoodieOperation getOperation() {
    return operation;
  }

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

  public static String generateSequenceId(String instantTime, int partitionId, long recordIndex) {
    return instantTime + "_" + partitionId + "_" + recordIndex;
  }

  public String getPartitionPath() {
    assert key != null;
    return key.getPartitionPath();
  }

  public String getRecordKey() {
    assert key != null;
    return key.getRecordKey();
  }

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
}
