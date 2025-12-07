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

package org.apache.hudi.utilities.streamer;

import lombok.Getter;

import java.util.Objects;

/**
 * Error event is an event triggered during write or processing failure of a record.
 */
@Getter
public class ErrorEvent<T> {

  private final ErrorReason reason;
  private final T payload;

  public ErrorEvent(T payload, ErrorReason reason) {
    this.payload = payload;
    this.reason = reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorEvent<?> that = (ErrorEvent<?>) o;
    return reason == that.reason && Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reason, payload);
  }

  /**
   * The reason behind write or processing failure of a record
   */
  public enum ErrorReason {
    // Failure during json to avro record conversion
    JSON_AVRO_DESERIALIZATION_FAILURE,
    // Failure during json to row conversion
    JSON_ROW_DESERIALIZATION_FAILURE,
    // Failure during row to avro record conversion
    AVRO_DESERIALIZATION_FAILURE,
    // Failure during hudi writes
    HUDI_WRITE_FAILURES,
    // Failure during transformation of source to target RDD
    CUSTOM_TRANSFORMER_FAILURE,
    // record schema is not valid for the table
    INVALID_RECORD_SCHEMA,
    // exception when attempting to create HoodieRecord
    RECORD_CREATION
  }
}
