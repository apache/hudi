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

package org.apache.hudi.hive.ddl;

import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;

/**
 * DDLExceutor is the interface which defines the ddl functions for Hive.
 * There are two main implementations one is QueryBased other is based on HiveMetaStore
 * QueryBasedDDLExecutor also has two impls namely HiveQL based and other JDBC based.
 */
public interface DDLExecutor {
  /**
   * @param databaseName name of database to be created.
   */
  public void createDatabase(String databaseName);

  /**
   * Creates a table with the following properties.
   *
   * @param tableName
   * @param storageSchema
   * @param inputFormatClass
   * @param outputFormatClass
   * @param serdeClass
   * @param serdeProperties
   * @param tableProperties
   */
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties);

  /**
   * Updates the table with the newSchema.
   *
   * @param tableName
   * @param newSchema
   */
  public void updateTableDefinition(String tableName, MessageType newSchema);

  /**
   * Fetches tableSchema for a table.
   *
   * @param tableName
   * @return
   */
  public Map<String, String> getTableSchema(String tableName);

  /**
   * Adds partition to table.
   *
   * @param tableName
   * @param partitionsToAdd
   */
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd);

  /**
   * Updates partitions for a given table.
   *
   * @param tableName
   * @param changedPartitions
   */
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions);

  public void close();
}
