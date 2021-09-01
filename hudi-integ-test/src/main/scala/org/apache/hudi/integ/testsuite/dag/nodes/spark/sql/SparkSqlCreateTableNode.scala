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

import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode
import org.apache.hudi.integ.testsuite.utils.SparkSqlUtils
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

/**
 * DAG node of create table using Spark SQL.
 *
 * @param config1 DAG node configurations.
 */
class SparkSqlCreateTableNode(config1: Config) extends DagNode[RDD[WriteStatus]] {

  val LOG: Logger = LoggerFactory.getLogger(classOf[SparkSqlCreateTableNode])
  val TEMP_TABLE_NAME: String = "_spark_sql_temp_table"

  config = config1

  /**
   * Execute the {@link DagNode}.
   *
   * @param context     The context needed for an execution of a node.
   * @param curItrCount iteration count for executing the node.
   * @throws Exception Thrown if the execution failed.
   */
  override def execute(context: ExecutionContext, curItrCount: Int): Unit = {
    LOG.info("Creating table in Spark SQL ...")
    val sparkSession = context.getWriterContext.getSparkSession
    val targetTableName = context.getWriterContext.getCfg.targetTableName

    var inputCount = 0L
    if (config.shouldUseCtas()) {
      if (!config.isDisableGenerate) {
        context.getDeltaGenerator().writeRecords(context.getDeltaGenerator().generateInserts(config)).count()
      }
      val nextBatch = context.getWriterContext.getHoodieTestSuiteWriter.getNextBatch
      val sparkSession = context.getWriterContext.getSparkSession
      val inputDF = AvroConversionUtils.createDataFrame(nextBatch,
        context.getWriterContext.getHoodieTestSuiteWriter.getSchema,
        sparkSession)
      inputDF.createOrReplaceTempView(TEMP_TABLE_NAME)
      inputCount = inputDF.count()
    }

    sparkSession.sql("drop table if exists " + targetTableName)
    val createTableQuery = SparkSqlUtils.constructCreateTableQuery(
      config, targetTableName, context.getWriterContext.getHoodieTestSuiteWriter.getSchema, TEMP_TABLE_NAME)
    SparkSqlUtils.logQuery(LOG, createTableQuery)
    sparkSession.sql(createTableQuery)
    val targetTableCount = sparkSession.sql("select * from " + targetTableName)
    if (config.shouldUseCtas()) {
      assert(targetTableCount.count() == inputCount)
    } else {
      assert(targetTableCount.count() == 0)
    }
    LOG.info("Finish create table in Spark SQL.")
  }
}
