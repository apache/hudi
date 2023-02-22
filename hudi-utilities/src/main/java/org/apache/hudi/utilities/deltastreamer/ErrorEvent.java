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

package org.apache.hudi.utilities.deltastreamer;

public class ErrorEvent<T> {

  ErrorReason reason;
  T payload;

  public ErrorEvent(T payload, ErrorReason reason) {
    this.payload = payload;
    this.reason = reason;
  }

  public T getPayload() {
    return payload;
  }

  public ErrorReason getReason() {
    return reason;
  }

  public enum ErrorReason {
    JSON_AVRO_DESERIALIZATION_FAILURE,
    JSON_ROW_DESERIALIZATION_FAILURE,
    AVRO_DESERIALIZATION_FAILURE,
    HUDI_WRITE_FAILURES,
    CUSTOM_TRANSFORMER_FAILURE
  }
}
