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

/**
 * Lean class to hold HoodieRecord information.
 * This holds the HoodieKey along with the location information.
 */
public class HoodieKeyWithLocation {

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
  private Option<HoodieRecordLocation> newLocation;

  public  HoodieKeyWithLocation(HoodieKey key, HoodieRecordLocation currentLocation, Option<HoodieRecordLocation> newLocation) {
    this.key = key;
    this.currentLocation = currentLocation;
    this.newLocation = newLocation;
  }

  public HoodieKey getKey() {
    return key;
  }

  public HoodieRecordLocation getCurrentLocation() {
    return currentLocation;
  }

  public Option<HoodieRecordLocation> getNewLocation() {
    return newLocation;
  }

  public static HoodieKeyWithLocation toHoodieKeyWithLocation(HoodieRecord record) {
    return new HoodieKeyWithLocation(record.getKey(), record.getCurrentLocation(), record.getNewLocation());
  }
}
