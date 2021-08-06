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

package org.apache.hudi.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.hive.client.HiveShim;

import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Wrapper class for Hive Metastore Client, which embeds a HiveShim layer to handle different Hive versions.
 * Methods provided mostly conforms to IMetaStoreClient interfaces except those that require shims.
 *
 * This code is mainly copy from Apache/Flink.
 */
public class HiveMetastoreClientWrapper implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreClientWrapper.class);

  private final IMetaStoreClient client;

  private final HiveConf hiveConf;

  private final HiveShim hiveShim;

  public HiveMetastoreClientWrapper(HiveConf hiveConf, String hiveVersion) {
    checkArgument(hiveVersion != null, "hive version cannot be null");
    this.hiveConf = hiveConf;
    hiveShim = HiveShimLoader.loadHiveShim(StringUtils.isNullOrEmpty(hiveVersion) ? HiveShimLoader.getHiveVersion() : hiveVersion);
    client = createMetastoreClient();
  }

  @Override
  public void close() {
    client.close();
  }

  public List<String> getAllTables(String databaseName) throws TException {
    return client.getAllTables(databaseName);
  }

  public List<Partition> listPartitions(String dbName, String tblName, short max) throws TException {
    return client.listPartitions(dbName, tblName, max);
  }

  public boolean tableExists(String databaseName, String tableName) throws TException {
    return client.tableExists(databaseName, tableName);
  }

  public Database getDatabase(String name) throws TException {
    return client.getDatabase(name);
  }

  public void createDatabase(Database database) throws TException {
    client.createDatabase(database);
  }

  public Table getTable(String databaseName, String tableName) throws TException {
    return client.getTable(databaseName, tableName);
  }

  public void createTable(Table table) throws TException {
    client.createTable(table);
  }

  //-------- Start of shimmed methods ----------

  private IMetaStoreClient createMetastoreClient() {
    return hiveShim.getHiveMetastoreClient(hiveConf);
  }

  public void alter_table(String defaultDatabaseName, String tblName, Table table) throws TException {
    hiveShim.alterTable(client, defaultDatabaseName, tblName, table);
  }

}