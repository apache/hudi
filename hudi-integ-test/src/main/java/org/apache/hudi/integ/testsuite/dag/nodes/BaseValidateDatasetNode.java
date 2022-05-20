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

package org.apache.hudi.integ.testsuite.dag.nodes;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
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

import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

/**
 * This nodes validates contents from input path are in tact with Hudi. By default no configs are required for this node. But there is an
 * optional config "delete_input_data" that you can set for this node. If set, once validation completes, contents from inputPath are deleted. This will come in handy for long running test suites.
 * README has more details under docker set up for usages of this node.
 */
public abstract class BaseValidateDatasetNode extends DagNode<Boolean> {

  public BaseValidateDatasetNode(DeltaConfig.Config config) {
    this.config = config;
  }

  /**
   * @return {@link Logger} instance to use.
   */
  public abstract Logger getLogger();

  /**
   * @param session     {@link SparkSession} instance to use.
   * @param context     {@link ExecutionContext} instance to use.
   * @param inputSchema input schema in {@link StructType}
   * @return data in {@link Dataset<Row>} to validate.
   */
  public abstract Dataset<Row> getDatasetToValidate(SparkSession session, ExecutionContext context,
                                                    StructType inputSchema);

  @Override
  public void execute(ExecutionContext context, int curItrCount) throws Exception {
    int validateOnceEveryItr = config.validateOnceEveryIteration();
    int itrCountToExecute = config.getIterationCountToExecute();
    if ((itrCountToExecute != -1 && itrCountToExecute == curItrCount) ||
        (itrCountToExecute == -1 && ((curItrCount % validateOnceEveryItr) == 0))) {
      SparkSession session = SparkSession.builder().sparkContext(context.getJsc().sc()).getOrCreate();
      // todo: Fix partitioning schemes. For now, assumes data based partitioning.
      String inputPath = context.getHoodieTestSuiteWriter().getCfg().inputBasePath + "/*/*";
      log.info("Validation using data from input path " + inputPath);
      // listing batches to be validated
      String inputPathStr = context.getHoodieTestSuiteWriter().getCfg().inputBasePath;
      if (log.isDebugEnabled()) {
        FileSystem fs = new Path(inputPathStr)
            .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
        FileStatus[] fileStatuses = fs.listStatus(new Path(inputPathStr));
        log.info("fileStatuses length: " + fileStatuses.length);
        for (FileStatus fileStatus : fileStatuses) {
          log.debug("Listing all Micro batches to be validated :: " + fileStatus.getPath().toString());
        }
      }

      Dataset<Row> inputSnapshotDf = getInputDf(context, session, inputPath);

      // read from hudi and remove meta columns.
      Dataset<Row> trimmedHudiDf = getDatasetToValidate(session, context, inputSnapshotDf.schema());
      if (config.isValidateFullData()) {
        log.debug("Validating full dataset");
        Dataset<Row> exceptInputDf = inputSnapshotDf.except(trimmedHudiDf);
        Dataset<Row> exceptHudiDf = trimmedHudiDf.except(inputSnapshotDf);
        long exceptInputCount = exceptInputDf.count();
        long exceptHudiCount = exceptHudiDf.count();
        log.debug("Except input df count " + exceptInputDf + ", except hudi count " + exceptHudiCount);
        if (exceptInputCount != 0 || exceptHudiCount != 0) {
          log.error("Data set validation failed. Total count in hudi " + trimmedHudiDf.count() + ", input df count " + inputSnapshotDf.count()
              + ". InputDf except hudi df = " + exceptInputCount + ", Hudi df except Input df " + exceptHudiCount);
          throw new AssertionError("Hudi contents does not match contents input data. ");
        }
      } else {
        Dataset<Row> intersectionDf = inputSnapshotDf.intersect(trimmedHudiDf);
        long inputCount = inputSnapshotDf.count();
        long outputCount = trimmedHudiDf.count();
        log.debug("Input count: " + inputCount + "; output count: " + outputCount);
        // the intersected df should be same as inputDf. if not, there is some mismatch.
        if (outputCount == 0 || inputCount == 0 || inputSnapshotDf.except(intersectionDf).count() != 0) {
          log.error("Data set validation failed. Total count in hudi " + outputCount + ", input df count " + inputCount);
          throw new AssertionError("Hudi contents does not match contents input data. ");
        }

        if (config.isValidateHive()) {
          String database = context.getWriterContext().getProps().getString(DataSourceWriteOptions.HIVE_DATABASE().key());
          String tableName = context.getWriterContext().getProps().getString(DataSourceWriteOptions.HIVE_TABLE().key());
          log.warn("Validating hive table with db : " + database + " and table : " + tableName);
          session.sql("REFRESH TABLE " + database + "." + tableName);
          Dataset<Row> cowDf = session.sql("SELECT _row_key, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare, _hoodie_is_deleted, " +
              "test_suite_source_ordering_field FROM " + database + "." + tableName);
          Dataset<Row> reorderedInputDf = inputSnapshotDf.select("_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare",
              "_hoodie_is_deleted", "test_suite_source_ordering_field");

          Dataset<Row> intersectedHiveDf = reorderedInputDf.intersect(cowDf);
          outputCount = trimmedHudiDf.count();
          log.warn("Input count: " + inputCount + "; output count: " + outputCount);
          // the intersected df should be same as inputDf. if not, there is some mismatch.
          if (outputCount == 0 || reorderedInputDf.except(intersectedHiveDf).count() != 0) {
            log.error("Data set validation failed for COW hive table. Total count in hudi " + outputCount + ", input df count " + inputCount);
            throw new AssertionError("Hudi hive table contents does not match contents input data. ");
          }
        }

        // if delete input data is enabled, erase input data.
        if (config.isDeleteInputData()) {
          // clean up input data for current group of writes.
          inputPathStr = context.getHoodieTestSuiteWriter().getCfg().inputBasePath;
          FileSystem fs = new Path(inputPathStr)
              .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
          FileStatus[] fileStatuses = fs.listStatus(new Path(inputPathStr));
          for (FileStatus fileStatus : fileStatuses) {
            log.debug("Micro batch to be deleted " + fileStatus.getPath().toString());
            fs.delete(fileStatus.getPath(), true);
          }
        }
      }
    }
  }

  private Dataset<Row> getInputDf(ExecutionContext context, SparkSession session, String inputPath) {
    String recordKeyField = context.getWriterContext().getProps().getString(DataSourceWriteOptions.RECORDKEY_FIELD().key());
    String partitionPathField = context.getWriterContext().getProps().getString(DataSourceWriteOptions.PARTITIONPATH_FIELD().key());
    // todo: fix hard coded fields from configs.
    // read input and resolve insert, updates, etc.
    Dataset<Row> inputDf = session.read().format("avro").load(inputPath);
    Dataset<Row> trimmedDf = inputDf;
    if (!config.inputPartitonsToSkipWithValidate().isEmpty()) {
      trimmedDf = inputDf.filter("instr("+partitionPathField+", \'"+ config.inputPartitonsToSkipWithValidate() +"\') != 1");
    }

    ExpressionEncoder encoder = getEncoder(inputDf.schema());
    return trimmedDf.groupByKey(
            (MapFunction<Row, String>) value ->
                (partitionPathField.isEmpty() ? value.getAs(recordKeyField) : (value.getAs(partitionPathField) + "+" + value.getAs(recordKeyField))), Encoders.STRING())
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
        .filter("_hoodie_is_deleted != true");
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }
}
