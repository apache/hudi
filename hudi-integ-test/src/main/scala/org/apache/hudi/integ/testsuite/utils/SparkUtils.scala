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

package org.apache.hudi.integ.testsuite.utils

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config
import org.apache.hudi.integ.testsuite.dag.ExecutionContext
import org.apache.spark.rdd.RDD

/**
 * Utils for test nodes in Spark
 */
object SparkUtils {

  /**
   * Generates records for delete operations in Spark.
   *
   * @param config  Node configs.
   * @param context The context needed for an execution of a node.
   * @return Records in {@link RDD}.
   */
  def generateRecordsForDelete(config: Config, context: ExecutionContext): RDD[GenericRecord] = {
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
