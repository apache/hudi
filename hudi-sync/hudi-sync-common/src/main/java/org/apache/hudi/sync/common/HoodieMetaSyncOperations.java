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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import org.apache.parquet.schema.MessageType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface HoodieMetaSyncOperations {

  String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";

  /**
   * Create the table.
   *
   * @param tableName         The table name.
   * @param storageSchema     The table schema.
   * @param inputFormatClass  The input format class of this table.
   * @param outputFormatClass The output format class of this table.
   * @param serdeClass        The serde class of this table.
   * @param serdeProperties   The serde properties of this table.
   * @param tableProperties   The table properties for this table.
   */
  default void createTable(String tableName,
                           MessageType storageSchema,
                           String inputFormatClass,
                           String outputFormatClass,
                           String serdeClass,
                           Map<String, String> serdeProperties,
                           Map<String, String> tableProperties) {

  }

  default boolean tableExists(String tableName) {
    return false;
  }

  default void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {

  }

  default void updatePartitionsToTable(String tableName, List<String> changedPartitions) {

  }

  default void dropPartitions(String tableName, List<String> partitionsToDrop) {

  }

  /**
   * Get all partitions for the table in the metastore.
   */
  default List<Partition> getAllPartitions(String tableName) {
    return Collections.emptyList();
  }

  /**
   * Check if a database already exists in the metastore.
   */
  default boolean databaseExists(String databaseName) {
    return false;
  }

  /**
   * Create a database in the metastore.
   */
  default void createDatabase(String databaseName) {

  }

  default Map<String, String> getSchemaFromMetastore(String tableName) {
    return Collections.emptyMap();
  }

  default MessageType getSchemaFromStorage() {
    return null;
  }

  /**
   * Update schema for the table in the metastore.
   */
  default void updateSchemaFromMetastore(String tableName, MessageType newSchema) {

  }

  default List<FieldSchema> getFieldSchemasFromMetastore(String tableName) {
    return Collections.emptyList();
  }

  default List<FieldSchema> getFieldSchemasFromStorage() {
    return Collections.emptyList();
  }

  default void updateTableComments(String tableName, List<FieldSchema> fromMetastore, List<FieldSchema> fromStorage) {

  }

  default Option<String> getLastCommitTimeSynced(String tableName) {
    return Option.empty();
  }

  default void updateLastCommitTimeSynced(String tableName) {

  }

  default void updateTableProperties(String tableName, Map<String, String> tableProperties) {

  }

  default Option<String> getLastReplicatedTime(String tableName) {
    return Option.empty();
  }

  default void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {

  }

  default void deleteLastReplicatedTimeStamp(String tableName) {

  }
}
