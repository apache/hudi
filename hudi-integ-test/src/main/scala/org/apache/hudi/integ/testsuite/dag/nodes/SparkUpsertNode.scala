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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats

import org.apache.spark.api.java.JavaRDD
import org.slf4j.LoggerFactory

/**
 * Spark datasource based upsert node
 *
 * @param dagNodeConfig DAG node configurations.
 */
class SparkUpsertNode(dagNodeConfig: Config) extends SparkInsertNode(dagNodeConfig) {

  private val log = LoggerFactory.getLogger(getClass)

  override def getOperation(): String = {
    DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
  }

  override def writeRecords(context: ExecutionContext): Pair[Integer, JavaRDD[DeltaWriteStats]] = {
    context.getDeltaGenerator().writeRecords(context.getDeltaGenerator().generateUpdates(config))
  }

  /**
   * Execute the {@link DagNode}.
   *
   * @param context     The context needed for an execution of a node.
   * @param curItrCount iteration count for executing the node.
   * @throws Exception Thrown if the execution failed.
   */
  /*override def execute(context: ExecutionContext, curItrCount: Int): Unit = {
    println("Generating input data for node {}", this.getName)

    val batchIdRecords = writeRecords(context)
    batchIdRecords.getValue().count()

    val pathToRead = context.getWriterContext.getCfg.inputBasePath + "/" + batchIdRecords.getKey()
    val avroDf = context.getWriterContext.getSparkSession.read.format("avro").load(pathToRead)
    val genRecsRDD = HoodieSparkUtils.createRdd(avroDf, "testStructName", "testNamespace", false,
      org.apache.hudi.common.util.Option.of(new Schema.Parser().parse(context.getWriterContext.getHoodieTestSuiteWriter.getSchema)))

    val inputDF = AvroConversionUtils.createDataFrame(genRecsRDD,
      context.getWriterContext.getHoodieTestSuiteWriter.getSchema,
      context.getWriterContext.getSparkSession)

    inputDF.write.format("hudi")
      .options(DataSourceWriteOptions.translateSqlOptions(context.getWriterContext.getProps.asScala.toMap))
      .option(HoodieTableConfig.ORDERING_FIELDS.key(), "test_suite_source_ordering_field")
      .option(DataSourceWriteOptions.TABLE_NAME.key, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, context.getHoodieTestSuiteWriter.getCfg.tableType)
      .option(DataSourceWriteOptions.OPERATION.key, getOperation())
      .option(HoodieWriteConfig.TBL_NAME.key, context.getHoodieTestSuiteWriter.getCfg.targetTableName)
      .mode(SaveMode.Append)
      .save(context.getHoodieTestSuiteWriter.getWriteConfig.getBasePath)
  }*/
}
