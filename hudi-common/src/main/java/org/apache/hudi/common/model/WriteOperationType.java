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
  DELETE_PREPPED("delete_prepped"),
  BOOTSTRAP("bootstrap"),
  // insert overwrite with static partitioning
  INSERT_OVERWRITE("insert_overwrite"),
  BUCKET_RESCALE("bucket_rescale"),
  // cluster
  CLUSTER("cluster"),
  // delete partition
  DELETE_PARTITION("delete_partition"),
  // insert overwrite with dynamic partitioning
  INSERT_OVERWRITE_TABLE("insert_overwrite_table"),
  // compact
  COMPACT("compact"),

  INDEX("index"),

  // alter schema
  ALTER_SCHEMA("alter_schema"),
  // log compact
  LOG_COMPACT("log_compact"),
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
      case "delete_prepped":
        return DELETE_PREPPED;
      case "insert_overwrite":
        return INSERT_OVERWRITE;
      case "delete_partition":
        return DELETE_PARTITION;
      case "insert_overwrite_table":
        return INSERT_OVERWRITE_TABLE;
      case "cluster":
        return CLUSTER;
      case "compact":
        return COMPACT;
      case "index":
        return INDEX;
      case "alter_schema":
        return ALTER_SCHEMA;
      case "log_compact":
        return LOG_COMPACT;
      case "bucket_rescale":
        return BUCKET_RESCALE;
      case "unknown":
        return UNKNOWN;
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
    return operationType == UPSERT || operationType == UPSERT_PREPPED || operationType == DELETE || operationType == DELETE_PREPPED;
  }

  public static boolean isOverwrite(WriteOperationType operationType) {
    return operationType == INSERT_OVERWRITE || operationType == INSERT_OVERWRITE_TABLE;
  }

  public boolean isInsertOverwriteOrDeletePartition() {
    return this == INSERT_OVERWRITE || this == INSERT_OVERWRITE_TABLE || this == DELETE_PARTITION;
  }

  /**
   * Whether the operation changes the dataset.
   */
  public static boolean yieldChanges(WriteOperationType operation) {
    return operation == WriteOperationType.INSERT
        || operation == WriteOperationType.UPSERT
        || operation == WriteOperationType.UPSERT_PREPPED
        || operation == WriteOperationType.DELETE
        || operation == WriteOperationType.DELETE_PREPPED
        || operation == WriteOperationType.BULK_INSERT
        || operation == WriteOperationType.DELETE_PARTITION
        || operation == WriteOperationType.INSERT_OVERWRITE
        || operation == WriteOperationType.INSERT_OVERWRITE_TABLE
        || operation == WriteOperationType.BOOTSTRAP;
  }

  public static boolean canUpdateSchema(WriteOperationType operation) {
    return !(operation == WriteOperationType.CLUSTER
        || operation == WriteOperationType.COMPACT
        || operation == WriteOperationType.INDEX
        || operation == WriteOperationType.LOG_COMPACT);
  }

  public static boolean isInsert(WriteOperationType operation) {
    return operation == WriteOperationType.INSERT
        || operation == WriteOperationType.INSERT_PREPPED
        || operation == WriteOperationType.BULK_INSERT
        || operation == WriteOperationType.BULK_INSERT_PREPPED
        || operation == WriteOperationType.INSERT_OVERWRITE
        || operation == WriteOperationType.INSERT_OVERWRITE_TABLE;
  }

  public static boolean isInsertWithoutReplace(WriteOperationType operation) {
    return operation == WriteOperationType.INSERT
        || operation == WriteOperationType.INSERT_PREPPED
        || operation == WriteOperationType.BULK_INSERT
        || operation == WriteOperationType.BULK_INSERT_PREPPED;
  }

  public static boolean isUpsert(WriteOperationType operation) {
    return operation == WriteOperationType.UPSERT
        || operation == WriteOperationType.UPSERT_PREPPED;
  }

  public static boolean isDelete(WriteOperationType operation) {
    return operation == DELETE || operation == DELETE_PREPPED;
  }

  public static boolean isPreppedWriteOperation(WriteOperationType operationType) {
    return operationType == BULK_INSERT_PREPPED || operationType == INSERT_PREPPED | operationType == UPSERT_PREPPED || operationType == DELETE_PREPPED;
  }

  public static boolean isCompactionOrClustering(WriteOperationType operationType) {
    return operationType == COMPACT || operationType == CLUSTER;
  }

  /**
   * @return true if streaming writes to metadata table is supported for a given {@link WriteOperationType}. false otherwise.
   */
  public static boolean streamingWritesToMetadataSupported(WriteOperationType writeOperationType) {
    return (isInsertWithoutReplace(writeOperationType) || isChangingRecords(writeOperationType) || isCompactionOrClustering(writeOperationType));
  }
}
