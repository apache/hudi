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
 * Shim for Hive version 1.2.0.
 */
public class HiveShimV120 extends HiveShimV111 {

  @Override
  public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
    try {
      Method method = RetryingMetaStoreClient.class.getMethod("getProxy", HiveConf.class);
      // getProxy is a static method
      return (IMetaStoreClient) method.invoke(null, (hiveConf));
    } catch (Exception ex) {
      throw new HoodieHiveSyncException("Failed to create Hive Metastore client", ex);
    }
  }

  @Override
  public void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table) throws InvalidOperationException, MetaException, TException {
    // For Hive-1.2.x, we need to tell HMS not to update stats. Otherwise, the stats we put in the table
    // parameters can be overridden. The extra config we add here will be removed by HMS after it's used.
    // Don't use StatsSetupConst.DO_NOT_UPDATE_STATS because it wasn't defined in Hive 1.1.x.
    table.getParameters().put("DO_NOT_UPDATE_STATS", "true");
    client.alter_table(databaseName, tableName, table);
  }
}
