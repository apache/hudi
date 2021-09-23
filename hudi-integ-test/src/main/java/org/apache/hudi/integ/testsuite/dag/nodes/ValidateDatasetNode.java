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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This validation node uses spark datasource for comparison purposes.
 */
public class ValidateDatasetNode extends BaseValidateDatasetNode {

  private static Logger log = LoggerFactory.getLogger(ValidateDatasetNode.class);

  public ValidateDatasetNode(Config config) {
    super(config);
  }

  @Override
  public Logger getLogger() {
    return log;
  }

  @Override
  public Dataset<Row> getDatasetToValidate(SparkSession session, ExecutionContext context,
                                           StructType inputSchema) {
    String hudiPath = context.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/*/*/*";
    log.info("Validate data in target hudi path " + hudiPath);
    Dataset<Row> hudiDf = session.read().format("hudi").load(hudiPath);
    return hudiDf.drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
            .drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.FILENAME_METADATA_FIELD);
  }
}
