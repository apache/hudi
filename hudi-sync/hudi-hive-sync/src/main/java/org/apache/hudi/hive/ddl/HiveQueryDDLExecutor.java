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

import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This class offers DDL executor backed by the hive.ql Driver This class preserves the old useJDBC = false way of doing things.
 */
public class HiveQueryDDLExecutor extends QueryBasedDDLExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(HiveQueryDDLExecutor.class);

  private final IMetaStoreClient metaStoreClient;
  private SessionState sessionState;
  private Driver hiveDriver;
  private HMSDDLExecutor hmsddlExecutor;

  public HiveQueryDDLExecutor(HiveSyncConfig config, IMetaStoreClient metaStoreClient) {
    super(config);
    this.metaStoreClient = metaStoreClient;
    try {
      this.sessionState = new SessionState(config.getHiveConf(),
          UserGroupInformation.getCurrentUser().getShortUserName());
      SessionState.start(this.sessionState);
      this.sessionState.setCurrentDatabase(databaseName);
      this.hiveDriver = new Driver(config.getHiveConf());
      this.hmsddlExecutor = new HMSDDLExecutor(config, metaStoreClient);
    } catch (Exception e) {
      if (sessionState != null) {
        try {
          this.sessionState.close();
        } catch (IOException ioException) {
          LOG.error("Error while closing SessionState", ioException);
        }
      }
      if (this.hiveDriver != null) {
        this.hiveDriver.close();
      }
      throw new HoodieHiveSyncException("Failed to create HiveQueryDDL object", e);
    }
  }

  @Override
  public void runSQL(String sql) {
    try {
      if (hiveDriver != null) {
        HoodieTimer timer = HoodieTimer.start();
        hiveDriver.run(sql);
        LOG.info("Time taken to execute [{}]: {} ms.", sql, timer.endTimer());
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed in executing SQL.", e);
    }
  }

  @Override
  public Map<String, String> getTableSchema(String tableName) {
    return hmsddlExecutor.getTableSchema(tableName);
  }

  @Override
  public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
    hmsddlExecutor.dropPartitionsToTable(tableName, partitionsToDrop);
  }

  @Override
  public void close() {
    if (hmsddlExecutor != null) {
      hmsddlExecutor.close();
    }
    if (metaStoreClient != null) {
      Hive.closeCurrent();
    }
  }
}
