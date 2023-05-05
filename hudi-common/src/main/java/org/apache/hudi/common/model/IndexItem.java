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

import org.apache.hudi.common.util.Option;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

/**
 * A Record Index managed by Hoodie.
 */
public class IndexItem implements Serializable, KryoSerializable {


  /**
   * Identifies the record across the table.
   */
  private HoodieKey key;

  /**
   * Current location of record on storage. Filled in by looking up index
   */
  private HoodieRecordLocation currentLocation;

  /**
   * New location of record on storage, after written.
   */
  private HoodieRecordLocation newLocation;

  public String getRecordKey() {
    assert key != null;
    return key.getRecordKey();
  }

  public HoodieKey getKey() {
    return key;
  }

  public String getPartitionPath() {
    assert key != null;
    return key.getPartitionPath();
  }

  public HoodieRecordLocation getCurrentLocation() {
    return currentLocation;
  }

  public Option<HoodieRecordLocation> getNewLocation() {
    return Option.ofNullable(this.newLocation);
  }

  public IndexItem(HoodieKey key,
                   HoodieRecordLocation currentLocation,
                   HoodieRecordLocation newLocation) {
    this.key = key;
    this.currentLocation = currentLocation;
    this.newLocation = newLocation;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    kryo.writeObjectOrNull(output, key, HoodieKey.class);
    kryo.writeClassAndObject(output, currentLocation);
    kryo.writeClassAndObject(output, newLocation);
  }

  @Override
  public final void read(Kryo kryo, Input input) {
    this.key = kryo.readObjectOrNull(input, HoodieKey.class);
    this.currentLocation = (HoodieRecordLocation) kryo.readClassAndObject(input);
    this.newLocation = (HoodieRecordLocation) kryo.readClassAndObject(input);
  }
}
