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
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.schema.SchemaUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import scala.Tuple2;

import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;

/**
 * This nodes validates contents from input path are in tact with Hudi. By default no configs are required for this node. But there is an
 * optional config "delete_input_data" that you can set for this node. If set, once validation completes, contents from inputPath are deleted. This will come in handy for long running test suites.
 * README has more details under docker set up for usages of this node.
 */
@Slf4j
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
    if ((itrCountToExecute != -1 && itrCountToExecute == curItrCount)
        || (itrCountToExecute == -1 && ((curItrCount % validateOnceEveryItr) == 0))) {
      FileSystem fs = new Path(context.getHoodieTestSuiteWriter().getCfg().inputBasePath)
          .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
      if (context.getHoodieTestSuiteWriter().getCfg().testContinuousMode) {
        awaitUntilDeltaStreamerCaughtUp(context, context.getHoodieTestSuiteWriter().getCfg().targetBasePath, fs,
            context.getHoodieTestSuiteWriter().getCfg().inputBasePath);
      }
      SparkSession session = SparkSession.builder().sparkContext(context.getJsc().sc()).getOrCreate();
      // todo: Fix partitioning schemes. For now, assumes data based partitioning.
      String inputPath = context.getHoodieTestSuiteWriter().getCfg().inputBasePath + "/*/*";
      log.info("Validation using data from input path {}", inputPath);
      // listing batches to be validated
      String inputPathStr = context.getHoodieTestSuiteWriter().getCfg().inputBasePath;
      if (log.isDebugEnabled()) {
        FileStatus[] fileStatuses = fs.listStatus(new Path(inputPathStr));
        log.info("fileStatuses length: {}", fileStatuses.length);
        for (FileStatus fileStatus : fileStatuses) {
          log.debug("Listing all Micro batches to be validated :: {}", fileStatus.getPath().toString());
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
        log.debug("Except input df count {}, except hudi count {}", exceptInputDf, exceptHudiCount);
        if (exceptInputCount != 0 || exceptHudiCount != 0) {
          log.error("Data set validation failed. Total count in hudi {}, input df count {}. InputDf except hudi df = {}, Hudi df except Input df {}", trimmedHudiDf.count(), inputSnapshotDf.count(),
              exceptInputCount, exceptHudiCount);
          throw new AssertionError("Hudi contents does not match contents input data. ");
        }
      } else {
        Dataset<Row> intersectionDf = inputSnapshotDf.intersect(trimmedHudiDf);
        long inputCount = inputSnapshotDf.count();
        long outputCount = trimmedHudiDf.count();
        log.debug("Input count: {}; output count: {}", inputCount, outputCount);
        // the intersected df should be same as inputDf. if not, there is some mismatch.
        if (outputCount == 0 || inputCount == 0 || inputSnapshotDf.except(intersectionDf).count() != 0) {
          log.error("Data set validation failed. Total count in hudi {}, input df count {}", outputCount, inputCount);
          throw new AssertionError("Hudi contents does not match contents input data. ");
        }

        if (config.isValidateHive()) {
          String database = context.getWriterContext().getProps().getString(DataSourceWriteOptions.HIVE_DATABASE().key());
          String tableName = context.getWriterContext().getProps().getString(DataSourceWriteOptions.HIVE_TABLE().key());
          log.warn("Validating hive table with db : {} and table : {}", database, tableName);
          session.sql("REFRESH TABLE " + database + "." + tableName);
          Dataset<Row> cowDf = session.sql("SELECT _row_key, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare, _hoodie_is_deleted, "
              + "test_suite_source_ordering_field FROM " + database + "." + tableName);
          Dataset<Row> reorderedInputDf = inputSnapshotDf.select("_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare",
              "_hoodie_is_deleted", "test_suite_source_ordering_field");

          Dataset<Row> intersectedHiveDf = reorderedInputDf.intersect(cowDf);
          outputCount = trimmedHudiDf.count();
          log.warn("Input count: {}; output count: {}", inputCount, outputCount);
          // the intersected df should be same as inputDf. if not, there is some mismatch.
          if (outputCount == 0 || reorderedInputDf.except(intersectedHiveDf).count() != 0) {
            log.error("Data set validation failed for COW hive table. Total count in hudi {}, input df count {}", outputCount, inputCount);
            throw new AssertionError("Hudi hive table contents does not match contents input data. ");
          }
        }

        // if delete input data is enabled, erase input data.
        if (config.isDeleteInputData()) {
          // clean up input data for current group of writes.
          inputPathStr = context.getHoodieTestSuiteWriter().getCfg().inputBasePath;
          FileStatus[] fileStatuses = fs.listStatus(new Path(inputPathStr));
          for (FileStatus fileStatus : fileStatuses) {
            log.debug("Micro batch to be deleted {}", fileStatus.getPath());
            fs.delete(fileStatus.getPath(), true);
          }
        }
      }
    }
  }

  private void awaitUntilDeltaStreamerCaughtUp(ExecutionContext context, String hudiTablePath, FileSystem fs, String inputPath) throws IOException, InterruptedException {
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(fs.getConf()))
        .setBasePath(hudiTablePath).build();
    HoodieTimeline commitTimeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    Option<String> latestCheckpoint = getLatestCheckpoint(commitTimeline);
    FileStatus[] subDirs = fs.listStatus(new Path(inputPath));
    List<FileStatus> subDirList = Arrays.asList(subDirs);
    subDirList.sort(Comparator.comparingLong(entry -> Long.parseLong(entry.getPath().getName())));
    String latestSubDir = subDirList.get(subDirList.size() - 1).getPath().getName();
    log.info("Latest sub directory in input path {}, latest checkpoint from deltastreamer {}", latestSubDir, latestCheckpoint.isPresent() ? latestCheckpoint.get() : "none");
    long maxWaitTime = config.maxWaitTimeForDeltastreamerToCatchupMs();
    long waitedSoFar = 0;
    while (!(latestCheckpoint.isPresent() && latestCheckpoint.get().equals(latestSubDir))) {
      log.warn("Sleeping for 20 secs awaiting for deltastreamer to catch up with ingested data");
      Thread.sleep(20000);
      meta.reloadActiveTimeline();
      commitTimeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      latestCheckpoint = getLatestCheckpoint(commitTimeline);
      waitedSoFar += 20000;
      if (waitedSoFar >= maxWaitTime) {
        throw new AssertionError("DeltaStreamer has not caught up after 5 mins of wait time. Last known checkpoint "
            + (latestCheckpoint.isPresent() ? latestCheckpoint.get() : "none") + ", expected checkpoint to have caught up " + latestSubDir);
      }
      log.info("Latest sub directory in input path {}, latest checkpoint from deltastreamer {}", latestSubDir, latestCheckpoint.isPresent() ? latestCheckpoint.get() : "none");
    }
  }

  private Option<String> getLatestCheckpoint(HoodieTimeline timeline) {
    return (Option<String>) timeline.getReverseOrderedInstants().map(instant -> {
      try {
        HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(instant);
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY))) {
          return Option.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
        } else {
          return Option.empty();
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse HoodieCommitMetadata for " + instant.toString(), e);
      }
    }).filter(Option::isPresent).findFirst().orElse(Option.empty());
  }

  private Dataset<Row> getInputDf(ExecutionContext context, SparkSession session, String inputPath) {
    String recordKeyField = context.getWriterContext().getProps().getString(DataSourceWriteOptions.RECORDKEY_FIELD().key());
    String partitionPathField = context.getWriterContext().getProps().getString(DataSourceWriteOptions.PARTITIONPATH_FIELD().key());
    // todo: fix hard coded fields from configs.
    // read input and resolve insert, updates, etc.
    Dataset<Row> inputDf = session.read().format("avro").load(inputPath);
    Dataset<Row> trimmedDf = inputDf;
    if (!config.inputPartitionsToSkipWithValidate().isEmpty()) {
      trimmedDf = inputDf.filter("instr(" + partitionPathField + ", \'" + config.inputPartitionsToSkipWithValidate() + "\') != 1");
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
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }
}
