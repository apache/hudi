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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.schema.SchemaUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

/**
 * This nodes validates contents from input path are in tact with Hudi. This nodes uses spark datasource for comparison purposes. By default no configs are required for this node. But there is an
 * optional config "delete_input_data" that you can set for this node. If set, once validation completes, contents from inputPath are deleted. This will come in handy for long running test suites.
 * README has more details under docker set up for usages of this node.
 */
public class ValidateDatasetNode extends DagNode<Boolean> {

  private static Logger log = LoggerFactory.getLogger(ValidateDatasetNode.class);

  public ValidateDatasetNode(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext context, int curItrCount) throws Exception {

    SparkSession session = SparkSession.builder().sparkContext(context.getJsc().sc()).getOrCreate();

    // todo: Fix partitioning schemes. For now, assumes data based partitioning.
    String inputPath = context.getHoodieTestSuiteWriter().getCfg().inputBasePath + "/*/*";
    String hudiPath = context.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/*/*/*";
    log.warn("ValidateDataset Node: Input path " + inputPath + ", hudi path " + hudiPath);
    // listing batches to be validated
    String inputPathStr = context.getHoodieTestSuiteWriter().getCfg().inputBasePath;
    FileSystem fs = new Path(inputPathStr)
        .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
    FileStatus[] fileStatuses = fs.listStatus(new Path(inputPathStr));
    for (FileStatus fileStatus : fileStatuses) {
      log.debug("Listing all Micro batches to be validated :: " + fileStatus.getPath().toString());
    }

    String recordKeyField = context.getWriterContext().getProps().getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY());
    String partitionPathField = context.getWriterContext().getProps().getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY());
    // todo: fix hard coded fields from configs.
    // read input and resolve insert, updates, etc.
    Dataset<Row> inputDf = session.read().format("avro").load(inputPath);
    ExpressionEncoder encoder = getEncoder(inputDf.schema());
    Dataset<Row> inputSnapshotDf = inputDf.groupByKey(
        (MapFunction<Row, String>) value -> partitionPathField + "+" + recordKeyField, Encoders.STRING())
        .reduceGroups((ReduceFunction<Row>) (v1, v2) -> {
          int ts1 = v1.getAs(SchemaUtils.SOURCE_ORDERING_FIELD);
          int ts2 = v2.getAs(SchemaUtils.SOURCE_ORDERING_FIELD);
          if (ts1 > ts2) {
            return v1;
          } else {
            return v2;
          }
        })
        .map((MapFunction<Tuple2<String, Row>, Row>) value -> value._2, encoder)
        .filter("_hoodie_is_deleted is NULL");

    // read from hudi and remove meta columns.
    Dataset<Row> hudiDf = session.read().format("hudi").load(hudiPath);
    Dataset<Row> trimmedDf = hudiDf.drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.FILENAME_METADATA_FIELD);

    Dataset<Row> intersectionDf = inputSnapshotDf.intersect(trimmedDf);
    // the intersected df should be same as inputDf. if not, there is some mismatch.
    if (inputSnapshotDf.except(intersectionDf).count() != 0) {
      log.error("Data set validation failed. Total count in hudi " + trimmedDf.count() + ", input df count " + inputSnapshotDf.count());
      throw new AssertionError("Hudi contents does not match contents input data. ");
    }

    if (config.isValidateHive()) {
      String database = context.getWriterContext().getProps().getString(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY());
      String tableName = context.getWriterContext().getProps().getString(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY());
      log.warn("Validating hive table with db : " + database + " and table : " + tableName);
      Dataset<Row> cowDf = session.sql("SELECT * FROM " + database + "." + tableName);
      Dataset<Row> trimmedCowDf = cowDf.drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
          .drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.FILENAME_METADATA_FIELD);
      intersectionDf = inputSnapshotDf.intersect(trimmedDf);
      // the intersected df should be same as inputDf. if not, there is some mismatch.
      if (inputSnapshotDf.except(intersectionDf).count() != 0) {
        log.error("Data set validation failed for COW hive table. Total count in hudi " + trimmedCowDf.count() + ", input df count " + inputSnapshotDf.count());
        throw new AssertionError("Hudi hive table contents does not match contents input data. ");
      }
    }

    // if delete input data is enabled, erase input data.
    if (config.isDeleteInputData()) {
      // clean up input data for current group of writes.
      inputPathStr = context.getHoodieTestSuiteWriter().getCfg().inputBasePath;
      fs = new Path(inputPathStr)
          .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
      fileStatuses = fs.listStatus(new Path(inputPathStr));
      for (FileStatus fileStatus : fileStatuses) {
        log.debug("Micro batch to be deleted " + fileStatus.getPath().toString());
        fs.delete(fileStatus.getPath(), true);
      }
    }
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }
}
