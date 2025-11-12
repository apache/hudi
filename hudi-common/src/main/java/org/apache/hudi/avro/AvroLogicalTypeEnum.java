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

package org.apache.hudi.avro;

/**
 * Enum of Avro logical types that merciful json convertor are aware of.
 * Currently, all logical types offered by Avro 1.10 is supported here.
 * Check https://avro.apache.org/docs/1.10.0/spec.html#Logical+Types for more details.
 */
public enum AvroLogicalTypeEnum {
  DECIMAL("decimal"),
  UUID("uuid"),
  DATE("date"),
  TIME_MILLIS("time-millis"),
  TIME_MICROS("time-micros"),
  TIMESTAMP_MILLIS("timestamp-millis"),
  TIMESTAMP_MICROS("timestamp-micros"),
  LOCAL_TIMESTAMP_MILLIS("local-timestamp-millis"),
  LOCAL_TIMESTAMP_MICROS("local-timestamp-micros"),
  DURATION("duration");

  private final String value;

  AvroLogicalTypeEnum(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
