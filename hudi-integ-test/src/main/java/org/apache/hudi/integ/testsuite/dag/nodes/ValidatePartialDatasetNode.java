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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidatePartialDatasetNode extends DagNode<Boolean> {

  private static Logger log = LoggerFactory.getLogger(ValidateDatasetNode.class);

  public ValidatePartialDatasetNode(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext context) throws Exception {

    /*SparkConf sparkConf = new SparkConf().setAppName("ValidateApp").setMaster("local[2]");
    SparkSession spark = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();*/

    SparkSession session = SparkSession.builder().sparkContext(context.getJsc().sc()).getOrCreate();

    String inputPath = context.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/../input/*/*";
    String hudiPath = context.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/*/*/*";
    log.warn("ValidateDataset Node: Input path " + inputPath + ", hudi path " + hudiPath);
    String inputPathStr = context.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/../input/";
    FileSystem fs = new Path(inputPathStr)
        .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
    FileStatus[] fileStatuses = fs.listStatus(new Path(inputPathStr));
    for (FileStatus fileStatus : fileStatuses) {
      log.warn("Micro batch available to be validated : " + fileStatus.getPath().toString());
    }
    Dataset<Row> inputDf = session.read().format("avro").load(inputPath);
    Dataset<Row> hudiDf = session.read().format("hudi").load(hudiPath);
    Dataset<Row> trimmedDf = hudiDf.drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.FILENAME_METADATA_FIELD);

    Dataset<Row> intersectionDf = inputDf.intersect(trimmedDf);
    // the intersected df should be same as inputDf. if not, there is some mismatch.
    if (inputDf.except(intersectionDf).count() != 0) {
      log.error("Data set validation failed. Total count in hudi " + trimmedDf.count() + ", input df count " + inputDf.count());
      throw new AssertionError("Hudi contents does not match contents input data. ");
    } else {
      // clean up input data for current group of writes.
      inputPathStr = context.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/../input/";
      fs = new Path(inputPathStr)
          .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
      fileStatuses = fs.listStatus(new Path(inputPathStr));
      for (FileStatus fileStatus : fileStatuses) {
        log.warn("Micro batch to be deleted " + fileStatus.getPath().toString());
        fs.delete(fileStatus.getPath(), true);
      }
    }
  }
}
