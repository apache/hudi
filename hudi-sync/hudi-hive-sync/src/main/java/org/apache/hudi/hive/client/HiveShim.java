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

package org.apache.hudi.hive.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.thrift.TException;

/**
 * A shim layer to support different versions of Hive.
 *
 * This code and it's concrete implement is mainly copy from Apache/Flink.
 */
public interface HiveShim {

  /**
   * Create a Hive Metastore client based on the given HiveConf object.
   *
   * @param hiveConf HiveConf instance
   * @return an IMetaStoreClient instance
   */
  IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf);

  /**
   * Alters a Hive table.
   *
   * @param client       the Hive metastore client
   * @param databaseName the name of the database to which the table belongs
   * @param tableName    the name of the table to be altered
   * @param table        the new Hive table
   */
  void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table)
          throws InvalidOperationException, MetaException, TException;
}
