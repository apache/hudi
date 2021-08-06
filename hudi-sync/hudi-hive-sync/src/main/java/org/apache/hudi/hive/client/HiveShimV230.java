/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.hudi.hive.HoodieHiveSyncException;

import org.apache.thrift.TException;

import java.lang.reflect.Method;

/**
 * Shim for Hive version 2.3.0.
 */
public class HiveShimV230 extends HiveShimV220 {

  @Override
  public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
    try {
      Method method = RetryingMetaStoreClient.class.getMethod("getProxy", HiveConf.class, Boolean.TYPE);
      // getProxy is a static method
      return (IMetaStoreClient) method.invoke(null, hiveConf, true);
    } catch (Exception ex) {
      throw new HoodieHiveSyncException("Failed to create Hive Metastore client", ex);
    }
  }

  @Override
  public void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table) throws InvalidOperationException, MetaException, TException {
    client.alter_table(databaseName, tableName, table);
  }
}
