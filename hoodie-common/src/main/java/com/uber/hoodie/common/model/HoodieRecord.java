/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.Optional;

/**
 * A Single Record managed by Hoodie TODO - Make this generic
 */
public class HoodieRecord<T extends HoodieRecordPayload> implements Serializable {

  public static String COMMIT_TIME_METADATA_FIELD = "_hoodie_commit_time";
  public static String COMMIT_SEQNO_METADATA_FIELD = "_hoodie_commit_seqno";
  public static String RECORD_KEY_METADATA_FIELD = "_hoodie_record_key";
  public static String PARTITION_PATH_METADATA_FIELD = "_hoodie_partition_path";
  public static String FILENAME_METADATA_FIELD = "_hoodie_file_name";

  /**
   * Identifies the record across the table
   */
  private HoodieKey key;

  /**
   * Actual payload of the record
   */
  private T data;

  /**
   * Current location of record on storage. Filled in by looking up index
   */
  private HoodieRecordLocation currentLocation;

  /**
   * New location of record on storage, after written
   */
  private HoodieRecordLocation newLocation;

  public HoodieRecord(HoodieKey key, T data) {
    this.key = key;
    this.data = data;
    this.currentLocation = null;
    this.newLocation = null;
  }

  public HoodieKey getKey() {
    return key;
  }

  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  /**
   * Release the actual payload, to ease memory pressure. To be called after the record has been
   * written to storage. Once deflated, cannot be inflated.
   */
  public void deflate() {
    this.data = null;
  }


  /**
   * Sets the current currentLocation of the record. This should happen exactly-once
   */
  public HoodieRecord setCurrentLocation(HoodieRecordLocation location) {
    assert currentLocation == null;
    this.currentLocation = location;
    return this;
  }

  public HoodieRecordLocation getCurrentLocation() {
    return currentLocation;
  }

  /**
   * Sets the new currentLocation of the record, after being written. This again should happen
   * exactly-once.
   */
  public HoodieRecord setNewLocation(HoodieRecordLocation location) {
    assert newLocation == null;
    this.newLocation = location;
    return this;
  }

  public Optional<HoodieRecordLocation> getNewLocation() {
    return Optional.ofNullable(this.newLocation);
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
    return Objects.equal(key, that.key)
        && Objects.equal(data, that.data)
        && Objects.equal(currentLocation, that.currentLocation)
        && Objects.equal(newLocation, that.newLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, data, currentLocation, newLocation);
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

  public static String generateSequenceId(String commitTime, int partitionId, long recordIndex) {
    return commitTime + "_" + partitionId + "_" + recordIndex;
  }

  public String getPartitionPath() {
    assert key != null;
    return key.getPartitionPath();
  }

  public String getRecordKey() {
    assert key != null;
    return key.getRecordKey();
  }
}
