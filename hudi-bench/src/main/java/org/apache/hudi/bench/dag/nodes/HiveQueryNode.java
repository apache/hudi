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

package org.apache.hudi.bench.dag.nodes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.bench.configuration.DeltaConfig.Config;
import org.apache.hudi.bench.dag.ExecutionContext;
import org.apache.hudi.bench.helpers.HiveServiceProvider;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hive.HiveSyncConfig;

public class HiveQueryNode extends DagNode<Boolean> {

  private HiveServiceProvider hiveServiceProvider;

  public HiveQueryNode(Config config) {
    this.config = config;
    this.hiveServiceProvider = new HiveServiceProvider(config);
  }

  @Override
  public void execute(ExecutionContext executionContext) throws Exception {
    log.info("Executing hive query node..." + this.getName());
    this.hiveServiceProvider.startLocalHiveServiceIfNeeded(executionContext.getDeltaWriter().getConfiguration());
    // this.hiveServiceProvider.syncToLocalHiveIfNeeded(writer);
    HiveSyncConfig hiveSyncConfig = DataSourceUtils
        .buildHiveSyncConfig(executionContext.getDeltaWriter().getDeltaStreamerWrapper()
                .getDeltaSyncService().getDeltaSync().getProps(),
            executionContext.getDeltaWriter().getDeltaStreamerWrapper()
                .getDeltaSyncService().getDeltaSync().getCfg().targetBasePath);
    this.hiveServiceProvider.syncToLocalHiveIfNeeded(executionContext.getDeltaWriter());
    Connection con = DriverManager.getConnection(hiveSyncConfig.jdbcUrl, hiveSyncConfig.hiveUser,
        hiveSyncConfig.hivePass);
    Statement stmt = con.createStatement();
    for (String hiveProperty : this.config.getHiveProperties()) {
      executeStatement(hiveProperty, stmt);
    }
    for (Pair<String, Integer> queryAndResult : this.config.getHiveQueries()) {
      log.info("Running => " + queryAndResult.getLeft());
      ResultSet res = stmt.executeQuery(queryAndResult.getLeft());
      if (res.getRow() == 0) {
        assert 0 == queryAndResult.getRight();
      } else {
        assert res.getInt(0) == queryAndResult.getRight();
      }
      log.info("Successfully validated query!");
    }
    this.hiveServiceProvider.stopLocalHiveServiceIfNeeded();
  }

  private void executeStatement(String query, Statement stmt) throws SQLException {
    log.info("Executing statement " + stmt.toString());
    stmt.execute(query);
  }

}
