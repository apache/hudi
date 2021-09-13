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

package org.apache.hudi.integ.testsuite.dag.nodes

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.{AvroConversionUtils, DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

import scala.collection.JavaConverters._

/**
 * Spark datasource based upsert node
 *
 * @param dagNodeConfig DAG node configurations.
 */
class SparkDeleteNode(dagNodeConfig: Config) extends DagNode[RDD[WriteStatus]] {

  private val log = LogManager.getLogger(getClass)
  config = dagNodeConfig

  /**
   * Execute the {@link DagNode}.
   *
   * @param context     The context needed for an execution of a node.
   * @param curItrCount iteration count for executing the node.
   * @throws Exception Thrown if the execution failed.
   */
  override def execute(context: ExecutionContext, curItrCount: Int): Unit = {
    // Deletes can't be fetched using getNextBatch() bcoz, getInsert(schema) from payload will return empty for delete
    // records
    val genRecsRDD = generateRecordsForDelete(config, context)
    val inputDF = AvroConversionUtils.createDataFrame(genRecsRDD,
      context.getWriterContext.getHoodieTestSuiteWriter.getSchema,
      context.getWriterContext.getSparkSession)
    inputDF.write.format("hudi")
      .options(DataSourceWriteOptions.translateSqlOptions(context.getWriterContext.getProps.asScala.toMap))
      .option(DataSourceWriteOptions.TABLE_NAME.key, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, context.getHoodieTestSuiteWriter.getCfg.tableType)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.COMMIT_METADATA_KEYPREFIX.key, "deltastreamer.checkpoint.key")
      .option("deltastreamer.checkpoint.key", context.getWriterContext.getHoodieTestSuiteWriter.getLastCheckpoint.orElse(""))
      .option(HoodieWriteConfig.TBL_NAME.key, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .mode(SaveMode.Append)
      .save(context.getHoodieTestSuiteWriter.getWriteConfig.getBasePath)
  }

  /**
   * Generates records for delete operations in Spark.
   *
   * @param config  Node configs.
   * @param context The context needed for an execution of a node.
   * @return Records in {@link RDD}.
   */
  private def generateRecordsForDelete(config: Config, context: ExecutionContext): RDD[GenericRecord] = {
    if (!config.isDisableGenerate) {
      context.getDeltaGenerator().writeRecords(context.getDeltaGenerator().generateDeletes(config)).count()
    }

    context.getWriterContext.getHoodieTestSuiteWriter.getNextBatchForDeletes()
    val pathToRead = context.getWriterContext.getCfg.inputBasePath + "/" + context.getWriterContext.getHoodieTestSuiteWriter.getLastCheckpoint.orElse("")

    val avroDf = context.getWriterContext.getSparkSession.read.format("avro").load(pathToRead)
    HoodieSparkUtils.createRdd(avroDf, "testStructName", "testNamespace", false,
      org.apache.hudi.common.util.Option.of(new Schema.Parser().parse(context.getWriterContext.getHoodieTestSuiteWriter.getSchema)))
  }
}
