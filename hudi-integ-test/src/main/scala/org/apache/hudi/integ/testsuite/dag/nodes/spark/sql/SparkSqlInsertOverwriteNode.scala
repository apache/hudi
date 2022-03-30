/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.integ.testsuite.dag.nodes.spark.sql

import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode
import org.apache.hudi.integ.testsuite.utils.SparkSqlUtils

/**
 * DAG node of insert overwrite using Spark SQL.
 *
 * @param dagNodeConfig DAG node configurations.
 */
class SparkSqlInsertOverwriteNode(dagNodeConfig: Config) extends BaseSparkSqlNode(dagNodeConfig) {

  config = dagNodeConfig

  /**
   * Returns the Spark SQL query to execute for this {@link DagNode}.
   *
   * @param config  DAG node configurations.
   * @param context The context needed for an execution of a node.
   * @return the query String.
   */
  override def queryToRun(config: Config, context: ExecutionContext): String = {
    val targetTableName = context.getWriterContext.getCfg.targetTableName
    SparkSqlUtils.constructInsertQuery(
      "overwrite", targetTableName,
      SparkSqlUtils.getTableSchema(context.getWriterContext.getSparkSession, targetTableName),
      getTempTableName())
  }
}
