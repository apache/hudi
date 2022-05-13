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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class PrestoQueryNode extends BaseQueryNode {

  public PrestoQueryNode(DeltaConfig.Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext context, int curItrCount) throws Exception {
    log.info("Executing presto query node {}", this.getName());
    String url = context.getHoodieTestSuiteWriter().getCfg().prestoJdbcUrl;
    if (StringUtils.isNullOrEmpty(url)) {
      throw new IllegalArgumentException("Presto JDBC connection url not provided. Please set --presto-jdbc-url.");
    }
    String user = context.getHoodieTestSuiteWriter().getCfg().prestoUsername;
    String pass = context.getHoodieTestSuiteWriter().getCfg().prestoPassword;
    try (Connection connection = DriverManager.getConnection(url, user, pass)) {
      Statement stmt = connection.createStatement();
      setSessionProperties(this.config.getPrestoProperties(), stmt);
      executeAndValidateQueries(this.config.getPrestoQueries(), stmt);
      stmt.close();
    }
    catch (Exception e) {
      throw new HoodieValidationException("Presto query validation failed due to " + e.getMessage(), e);
    }
  }
}
