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

package org.apache.hudi.integ.testsuite.dag.nodes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.helpers.HiveServiceProvider;

public class HiveQueryNode extends DagNode<Boolean> {

  private HiveServiceProvider hiveServiceProvider;

  public HiveQueryNode(DeltaConfig.Config config) {
    this.config = config;
    this.hiveServiceProvider = new HiveServiceProvider(config);
  }

  @Override
  public void execute(ExecutionContext executionContext) throws Exception {
    log.info("Executing hive query node {}", this.getName());
    this.hiveServiceProvider.startLocalHiveServiceIfNeeded(executionContext.getHoodieTestSuiteWriter().getConfiguration());
    HiveSyncConfig hiveSyncConfig = DataSourceUtils
        .buildHiveSyncConfig(executionContext.getHoodieTestSuiteWriter().getDeltaStreamerWrapper()
                .getDeltaSyncService().getDeltaSync().getProps(),
            executionContext.getHoodieTestSuiteWriter().getDeltaStreamerWrapper()
                .getDeltaSyncService().getDeltaSync().getCfg().targetBasePath,
            executionContext.getHoodieTestSuiteWriter().getDeltaStreamerWrapper()
                .getDeltaSyncService().getDeltaSync().getCfg().baseFileFormat);
    this.hiveServiceProvider.syncToLocalHiveIfNeeded(executionContext.getHoodieTestSuiteWriter());
    Connection con = DriverManager.getConnection(hiveSyncConfig.jdbcUrl, hiveSyncConfig.hiveUser,
        hiveSyncConfig.hivePass);
    Statement stmt = con.createStatement();
    stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
    for (String hiveProperty : this.config.getHiveProperties()) {
      executeStatement(hiveProperty, stmt);
    }
    for (Pair<String, Integer> queryAndResult : this.config.getHiveQueries()) {
      log.info("Running {}", queryAndResult.getLeft());
      ResultSet res = stmt.executeQuery(queryAndResult.getLeft());
      if (!res.next()) {
        log.info("res.next() was False - typically this means the query returned no rows.");
        assert 0 == queryAndResult.getRight();
      } else {
        Integer result = res.getInt(1);
        if (!queryAndResult.getRight().equals(result)) {
          throw new AssertionError(
              "QUERY: " + queryAndResult.getLeft()
                  + " | EXPECTED RESULT = " + queryAndResult.getRight()
                  + " | ACTUAL RESULT = " + result
          );
        }
      }
      log.info("Successfully validated query!");
    }
    this.hiveServiceProvider.stopLocalHiveServiceIfNeeded();
  }

  private void executeStatement(String query, Statement stmt) throws SQLException {
    log.info("Executing statement {}", stmt.toString());
    stmt.execute(query);
  }

}
