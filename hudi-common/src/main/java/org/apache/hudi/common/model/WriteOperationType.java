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

import org.apache.hudi.exception.HoodieException;

import java.util.Locale;

/**
 * The supported write operation types, used by commitMetadata.
 */
public enum WriteOperationType {
  // directly insert
  INSERT("insert"),
  INSERT_PREPPED("insert_prepped"),
  // update and insert
  UPSERT("upsert"),
  UPSERT_PREPPED("upsert_prepped"),
  // bulk insert
  BULK_INSERT("bulk_insert"),
  BULK_INSERT_PREPPED("bulk_insert_prepped"),
  // delete
  DELETE("delete"),
  BOOTSTRAP("bootstrap"),
  // insert overwrite with static partitioning
  INSERT_OVERWRITE("insert_overwrite"),
  // cluster
  CLUSTER("cluster"),
  // insert overwrite with dynamic partitioning
  INSERT_OVERWRITE_TABLE("insert_overwrite_table"),
  // used for old version
  UNKNOWN("unknown");

  private final String value;

  WriteOperationType(String value) {
    this.value = value;
  }

  /**
   * Convert string value to WriteOperationType.
   */
  public static WriteOperationType fromValue(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "insert":
        return INSERT;
      case "insert_prepped":
        return INSERT_PREPPED;
      case "upsert":
        return UPSERT;
      case "upsert_prepped":
        return UPSERT_PREPPED;
      case "bulk_insert":
        return BULK_INSERT;
      case "bulk_insert_prepped":
        return BULK_INSERT_PREPPED;
      case "delete":
        return DELETE;
      case "insert_overwrite":
        return INSERT_OVERWRITE;
      case "insert_overwrite_table":
        return INSERT_OVERWRITE_TABLE;
      default:
        throw new HoodieException("Invalid value of Type.");
    }
  }

  /**
   * Getter for value.
   * @return string form of WriteOperationType
   */
  public String value() {
    return value;
  }

  public static boolean isChangingRecords(WriteOperationType operationType) {
    return operationType == UPSERT || operationType == UPSERT_PREPPED || operationType == DELETE;
  }
}
