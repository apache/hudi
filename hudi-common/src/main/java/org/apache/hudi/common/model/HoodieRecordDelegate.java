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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Delegate for {@link HoodieRecord}.
 * <p>
 * This is used when write handles report back write operation's info and stats,
 * instead of passing back the full {@link HoodieRecord}, this lean delegate
 * of it will be passed instead.
 */
public class HoodieRecordDelegate implements Serializable, KryoSerializable {

  private HoodieKey hoodieKey;

  /**
   * Current location of record on storage. Filled in by looking up index
   */
  private Option<HoodieRecordLocation> currentLocation;

  /**
   * New location of record on storage, after written.
   */
  private Option<HoodieRecordLocation> newLocation;

  /**
   * If set, not update index after written.
   */
  private boolean ignoreIndexUpdate;

  private HoodieRecordDelegate(HoodieKey hoodieKey,
                               @Nullable HoodieRecordLocation currentLocation,
                               @Nullable HoodieRecordLocation newLocation,
                               boolean ignoreIndexUpdate) {
    this.hoodieKey = hoodieKey;
    this.currentLocation = Option.ofNullable(currentLocation);
    this.newLocation = Option.ofNullable(newLocation);
    this.ignoreIndexUpdate = ignoreIndexUpdate;
  }

  public static HoodieRecordDelegate create(String recordKey, String partitionPath) {
    return new HoodieRecordDelegate(new HoodieKey(recordKey, partitionPath), null, null, false);
  }

  public static HoodieRecordDelegate create(String recordKey,
                                            String partitionPath,
                                            HoodieRecordLocation currentLocation) {
    return new HoodieRecordDelegate(new HoodieKey(recordKey, partitionPath), currentLocation, null, false);
  }

  public static HoodieRecordDelegate create(String recordKey,
                                            String partitionPath,
                                            HoodieRecordLocation currentLocation,
                                            HoodieRecordLocation newLocation) {
    return new HoodieRecordDelegate(new HoodieKey(recordKey, partitionPath), currentLocation, newLocation, false);
  }

  public static HoodieRecordDelegate create(String recordKey,
                                            String partitionPath,
                                            HoodieRecordLocation currentLocation,
                                            HoodieRecordLocation newLocation,
                                            boolean ignoreIndexUpdate) {
    return new HoodieRecordDelegate(new HoodieKey(recordKey, partitionPath), currentLocation, newLocation, ignoreIndexUpdate);
  }

  public static HoodieRecordDelegate create(HoodieKey key) {
    return new HoodieRecordDelegate(key, null, null, false);
  }

  public static HoodieRecordDelegate create(HoodieKey key, HoodieRecordLocation currentLocation) {
    return new HoodieRecordDelegate(key, currentLocation, null, false);
  }

  public static HoodieRecordDelegate create(HoodieKey key,
                                            HoodieRecordLocation currentLocation,
                                            HoodieRecordLocation newLocation) {
    return new HoodieRecordDelegate(key, currentLocation, newLocation, false);
  }

  public static HoodieRecordDelegate fromHoodieRecord(HoodieRecord record) {
    return new HoodieRecordDelegate(record.getKey(), record.getCurrentLocation(), record.getNewLocation(), record.getIgnoreIndexUpdate());
  }

  public static HoodieRecordDelegate fromHoodieRecord(HoodieRecord record,
                                                      @Nullable HoodieRecordLocation newLocationOverride) {
    return new HoodieRecordDelegate(record.getKey(), record.getCurrentLocation(), newLocationOverride, record.getIgnoreIndexUpdate());
  }

  public String getRecordKey() {
    return hoodieKey.getRecordKey();
  }

  public String getPartitionPath() {
    return hoodieKey.getPartitionPath();
  }

  public HoodieKey getHoodieKey() {
    return hoodieKey;
  }

  public Option<HoodieRecordLocation> getCurrentLocation() {
    return currentLocation;
  }

  public Option<HoodieRecordLocation> getNewLocation() {
    return newLocation;
  }

  public boolean getIgnoreIndexUpdate() {
    return ignoreIndexUpdate;
  }

  @Override
  public String toString() {
    return "HoodieRecordDelegate{"
        + "hoodieKey=" + hoodieKey
        + ", currentLocation=" + currentLocation
        + ", newLocation=" + newLocation
        + ", ignoreIndexUpdate=" + ignoreIndexUpdate
        + '}';
  }

  @VisibleForTesting
  @Override
  public final void write(Kryo kryo, Output output) {
    kryo.writeObjectOrNull(output, hoodieKey, HoodieKey.class);
    kryo.writeClassAndObject(output, currentLocation.isPresent() ? currentLocation.get() : null);
    kryo.writeClassAndObject(output, newLocation.isPresent() ? newLocation.get() : null);
    kryo.writeObjectOrNull(output, ignoreIndexUpdate, Boolean.class);
  }

  @VisibleForTesting
  @Override
  public final void read(Kryo kryo, Input input) {
    this.hoodieKey = kryo.readObjectOrNull(input, HoodieKey.class);
    this.currentLocation = Option.ofNullable((HoodieRecordLocation) kryo.readClassAndObject(input));
    this.newLocation = Option.ofNullable((HoodieRecordLocation) kryo.readClassAndObject(input));
    this.ignoreIndexUpdate = kryo.readObjectOrNull(input, Boolean.class);
  }
}
