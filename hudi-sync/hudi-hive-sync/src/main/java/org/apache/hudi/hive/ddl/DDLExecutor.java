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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * DDLExecutor is the interface which defines the ddl functions for Hive.
 * There are two main implementations one is QueryBased other is based on HiveMetaStore
 * QueryBasedDDLExecutor also has two implementations namely HiveQL based and other JDBC based.
 */
public interface DDLExecutor {
  /**
   * @param databaseName name of database to be created.
   */
  void createDatabase(String databaseName);

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
  void createTable(String tableName, MessageType storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties);

  /**
   * Updates the table with the newSchema.
   *
   * @param tableName
   * @param newSchema
   */
  void updateTableDefinition(String tableName, MessageType newSchema);

  /**
   * Fetches tableSchema for a table.
   *
   * @param tableName
   * @return
   */
  Map<String, String> getTableSchema(String tableName);

  /**
   * Adds partition to table.
   *
   * @param tableName
   * @param partitionsToAdd
   */
  void addPartitionsToTable(String tableName, List<String> partitionsToAdd);

  /**
   * Updates partitions for a given table.
   *
   * @param tableName
   * @param changedPartitions
   */
  void updatePartitionsToTable(String tableName, List<String> changedPartitions);

  /**
   * Drop partitions for a given table.
   *
   * @param tableName
   * @param partitionsToDrop
   */
  void dropPartitionsToTable(String tableName, List<String> partitionsToDrop);

  /**
   * update table comments.
   *
   * @param tableName
   * @param newSchema
   */
  void updateTableComments(String tableName, Map<String, ImmutablePair<String,String>> newSchema);

  /**
   * close ddl executor such as close connection.
   */
  void close();

  /**
   *  extract partition value from storage partition path.
   * @param storagePartition
   * @return
   */
  List<String> extractPartitionValuesInPath(String storagePartition);

  /**
   *  scan table partitions.
   * @param databaseName
   * @param tableName
   * @return
   * @throws TException
   */
  List<Partition> scanTablePartitions(String databaseName, String tableName) throws TException;

  /**
   *  check table exist under a specified database.
   * @param databaseName
   * @param tableName
   * @return
   */
  boolean doesTableExist(String databaseName, String tableName);

  /**
   *  get last sync committed time from table.
   * @param databaseName
   * @param tableName
   * @return
   */
  Option<String> getLastCommitTimeSynced(String databaseName, String tableName);

  /**
   *  get last replicated time from table.
   * @param databaseName
   * @param tableName
   * @return
   */
  Option<String> getLastReplicatedTime(String databaseName, String tableName);

  /**
   *  update last replicated time to table.
   * @param activeTimeline
   * @param databaseName
   * @param tableName
   * @param timeStamp
   */
  void updateLastReplicatedTimeStamp(HoodieTimeline activeTimeline, String databaseName, String tableName, String timeStamp);

  /**
   * delete last replicated time from table.
   * @param databaseName
   * @param tableName
   */
  void deleteLastReplicatedTimeStamp(String databaseName, String tableName);

  /**
   *  update last committed time to table.
   * @param activeTimeline
   * @param databaseName
   * @param tableName
   */
  void updateLastCommitTimeSynced(HoodieTimeline activeTimeline, String databaseName, String tableName);

  /**
   *  get avro schema.
   * @param metaClient
   * @return
   */
  Schema getAvroSchemaWithoutMetadataFields(HoodieTableMetaClient metaClient);

  /**
   * use table comment.
   * @param databaseName
   * @param tableName
   * @return
   */
  List<FieldSchema> getTableCommentUsingMetastoreClient(String databaseName, String tableName);

  /**
   *  check whether a database exist with specified name.
   * @param databaseName
   * @return
   */
  boolean doesDataBaseExist(String databaseName);

  /**
   * update table properties.
   * @param databaseName
   * @param tableName
   * @param tableProperties
   */
  void updateTableProperties(String databaseName, String tableName, Map<String, String> tableProperties);
}
