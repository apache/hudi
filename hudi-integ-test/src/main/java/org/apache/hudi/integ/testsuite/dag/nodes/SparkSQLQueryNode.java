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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testsuite.helpers.HiveServiceProvider;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A SparkSQL query node in the DAG of operations for a workflow.
 */
public class SparkSQLQueryNode extends DagNode<Boolean> {

  HiveServiceProvider hiveServiceProvider;

  public SparkSQLQueryNode(Config config) {
    this.config = config;
    this.hiveServiceProvider = new HiveServiceProvider(config);
  }

  /**
   * Method helps to execute a sparkSql query from a hive table.
   *
   * @param executionContext Execution context to perform this query.
   * @param curItrCount current iteration count.
   * @throws Exception will be thrown if ant error occurred
   */
  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    log.info("Executing spark sql query node");
    this.hiveServiceProvider.startLocalHiveServiceIfNeeded(executionContext.getHoodieTestSuiteWriter().getConfiguration());
    this.hiveServiceProvider.syncToLocalHiveIfNeeded(executionContext.getHoodieTestSuiteWriter());
    SparkSession session = SparkSession.builder().sparkContext(executionContext.getJsc().sc()).getOrCreate();
    for (String hiveProperty : this.config.getHiveProperties()) {
      session.sql(hiveProperty).count();
    }
    for (Pair<String, Integer> queryAndResult : this.config.getHiveQueries()) {
      log.info("Running {}", queryAndResult.getLeft());
      Dataset<Row> res = session.sql(queryAndResult.getLeft());
      if (res.count() == 0) {
        assert 0 == queryAndResult.getRight();
      } else {
        assert ((Row[]) res.collect())[0].getInt(0) == queryAndResult.getRight();
      }
      log.info("Successfully validated query!");
    }
    this.hiveServiceProvider.stopLocalHiveServiceIfNeeded();
    this.result = true;
  }

}
