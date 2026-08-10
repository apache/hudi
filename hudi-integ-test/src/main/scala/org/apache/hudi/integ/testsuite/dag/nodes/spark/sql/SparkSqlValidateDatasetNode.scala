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

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.hudi.integ.testsuite.dag.nodes.BaseValidateDatasetNode
import org.apache.hudi.integ.testsuite.utils.SparkSqlUtils

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

/**
 * This validation node uses Spark SQL to get data for comparison purposes.
 */
class SparkSqlValidateDatasetNode(dagNodeConfig: Config) extends BaseValidateDatasetNode(dagNodeConfig) {

  val LOG: Logger = LoggerFactory.getLogger(classOf[SparkSqlValidateDatasetNode])

  config = dagNodeConfig

  /**
   * @return {@link Logger} instance to use.
   */
  override def getLogger: Logger = LOG

  /**
   * @param session     {@link SparkSession} instance to use.
   * @param context     {@link ExecutionContext} instance to use.
   * @param inputSchema input schema in {@link StructType}
   * @return data in {@link Dataset< Row >} to validate.
   */
  override def getDatasetToValidate(session: SparkSession, context: ExecutionContext,
                                    inputSchema: StructType): Dataset[Row] = {
    val tableName = context.getWriterContext.getCfg.targetTableName
    LOG.info("Validate data in table " + tableName)
    val sortedInputFieldNames = inputSchema.fieldNames.sorted
    val tableSchema = session.table(tableName).schema
    val sortedTableFieldNames = tableSchema.fieldNames
      .filter(field => !HoodieRecord.HOODIE_META_COLUMNS.contains(field)).sorted
    if (!(sortedInputFieldNames sameElements sortedTableFieldNames)) {
      LOG.error("Input schema: ")
      inputSchema.printTreeString()
      LOG.error("Table schema: ")
      tableSchema.printTreeString()
      throw new AssertionError("Data set validation failed.  The schema does not match.")
    }
    session.sql(SparkSqlUtils.constructSelectQuery(inputSchema, tableName))
  }
}
