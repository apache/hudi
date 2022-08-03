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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.helpers.HiveServiceProvider;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;

/**
 * A hive query node in the DAG of operations for a workflow. used to perform a hive query with given config.
 */
public class HiveQueryNode extends BaseQueryNode {

  private HiveServiceProvider hiveServiceProvider;

  public HiveQueryNode(DeltaConfig.Config config) {
    this.config = config;
    this.hiveServiceProvider = new HiveServiceProvider(config);
  }

  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    log.info("Executing hive query node {}", this.getName());
    this.hiveServiceProvider.startLocalHiveServiceIfNeeded(executionContext.getHoodieTestSuiteWriter().getConfiguration());
    TypedProperties properties = new TypedProperties();
    properties.putAll(executionContext.getHoodieTestSuiteWriter().getDeltaStreamerWrapper()
        .getDeltaSyncService().getDeltaSync().getProps());
    properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), executionContext.getHoodieTestSuiteWriter().getDeltaStreamerWrapper()
        .getDeltaSyncService().getDeltaSync().getCfg().targetBasePath);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key(), executionContext.getHoodieTestSuiteWriter().getDeltaStreamerWrapper()
        .getDeltaSyncService().getDeltaSync().getCfg().baseFileFormat);
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig(properties);
    this.hiveServiceProvider.syncToLocalHiveIfNeeded(executionContext.getHoodieTestSuiteWriter());
    try (Connection con = DriverManager.getConnection(hiveSyncConfig.getString(HIVE_URL),
        hiveSyncConfig.getString(HIVE_USER), hiveSyncConfig.getString(HIVE_PASS))) {
      Statement stmt = con.createStatement();
      stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
      setSessionProperties(this.config.getHiveProperties(), stmt);
      executeAndValidateQueries(this.config.getHiveQueries(), stmt);
      stmt.close();
      this.hiveServiceProvider.stopLocalHiveServiceIfNeeded();
    }
    catch (Exception e) {
      throw new HoodieValidationException("Hive query validation failed due to " + e.getMessage(), e);
    }
  }
}
