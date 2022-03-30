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

import org.apache.avro.generic.GenericRecord
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode
import org.apache.hudi.integ.testsuite.utils.SparkSqlUtils
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

/**
 * Abstract class for DAG node of running Spark SQL.
 *
 * @param dagNodeConfig DAG node configurations.
 */
abstract class BaseSparkSqlNode(dagNodeConfig: Config) extends DagNode[RDD[WriteStatus]] {

  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
  val TEMP_TABLE_NAME = "_spark_sql_temp_table"
  config = dagNodeConfig

  /**
   * Returns the Spark SQL query to execute for this {@link DagNode}.
   *
   * @param config  DAG node configurations.
   * @param context The context needed for an execution of a node.
   * @return the query String.
   */
  def queryToRun(config: Config, context: ExecutionContext): String

  /**
   * Prepares the data for the Spark write operation.
   *
   * @param context The context needed for an execution of a node.
   * @return Records in {@link RDD}.
   */
  def prepareData(context: ExecutionContext): RDD[GenericRecord] = {
    if (!config.isDisableGenerate) {
      context.getDeltaGenerator().writeRecords(context.getDeltaGenerator().generateInserts(config)).getValue().count()
    }
    context.getWriterContext.getHoodieTestSuiteWriter.getNextBatch
  }

  /**
   * @return Name of the temp table containing the input data.
   */
  def getTempTableName(): String = {
    TEMP_TABLE_NAME
  }

  /**
   * Execute the {@link DagNode}.
   *
   * @param context     The context needed for an execution of a node.
   * @param curItrCount iteration count for executing the node.
   * @throws Exception Thrown if the execution failed.
   */
  override def execute(context: ExecutionContext, curItrCount: Int): Unit = {
    LOG.info("Run query in Spark SQL ...")
    val nextBatch = prepareData(context)
    val sparkSession = context.getWriterContext.getSparkSession
    val inputDF = AvroConversionUtils.createDataFrame(nextBatch,
      context.getWriterContext.getHoodieTestSuiteWriter.getSchema,
      sparkSession)
    inputDF.createOrReplaceTempView(TEMP_TABLE_NAME)

    val query = queryToRun(config, context)
    SparkSqlUtils.logQuery(LOG, query)
    sparkSession.sql(query)
    LOG.info("Finish run query in Spark SQL.")
  }
}
