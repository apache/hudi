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

package org.apache.hudi.integ.testsuite.dag.nodes


import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.integ.testsuite.schema.SchemaUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Spark datasource based insert node
 *
 * @param dagNodeConfig DAG node configurations.
 */
class SparkDeletePartitionNode(dagNodeConfig: Config) extends DagNode[RDD[WriteStatus]] {

  private val log = LoggerFactory.getLogger(getClass)
  config = dagNodeConfig

  /**
   * Execute the {@link DagNode}.
   *
   * @param context     The context needed for an execution of a node.
   * @param curItrCount iteration count for executing the node.
   * @throws Exception Thrown if the execution failed.
   */
  override def execute(context: ExecutionContext, curItrCount: Int): Unit = {
    println("Generating input data for node {}", this.getName)

    context.getWriterContext.getSparkSession.emptyDataFrame.write.format("hudi")
      .options(DataSourceWriteOptions.mayBeDerivePartitionPath(context.getWriterContext.getProps.asScala.toMap))
        .option(DataSourceWriteOptions.ORDERING_FIELDS.key(), SchemaUtils.SOURCE_ORDERING_FIELD)
      .option(DataSourceWriteOptions.TABLE_NAME.key, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, context.getHoodieTestSuiteWriter.getCfg.tableType)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .option(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key, config.getPartitionsToDelete)
      .mode(SaveMode.Append)
      .save(context.getHoodieTestSuiteWriter.getWriteConfig.getBasePath)
  }
}
