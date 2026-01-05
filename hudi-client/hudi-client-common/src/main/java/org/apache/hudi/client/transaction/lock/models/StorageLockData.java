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

package org.apache.hudi.client.transaction.lock.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

/**
 * Pojo for conditional writes-based lock provider.
 */
@Getter
public class StorageLockData {

  private final boolean expired;
  private final long validUntil;
  private final String owner;

  /**
   * Initializes an object describing a conditionally written lock.
   * @param expired Whether the lock is expired.
   * @param validUntil The epoch in ms when the lock is expired.
   * @param owner The uuid owner of the owner of this lock.
   */
  @JsonCreator
  public StorageLockData(
      @JsonProperty(value = "expired", required = true) boolean expired,
      @JsonProperty(value = "validUntil", required = true) long validUntil,
      @JsonProperty(value = "owner", required = true) String owner) {
    this.expired = expired;
    this.validUntil = validUntil;
    this.owner = owner;
  }
}
