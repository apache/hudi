/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.examples.quickstart;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.examples.quickstart.factory.CollectSinkTableFactory;
import org.apache.hudi.examples.quickstart.utils.QuickstartConfigurations;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.examples.quickstart.utils.QuickstartConfigurations.sql;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HoodieFlinkQuickstart {
  private EnvironmentSettings settings = null;
  @Getter
  private TableEnvironment tableEnvironment = null;

  private String tableName;

  public static HoodieFlinkQuickstart instance() {
    return new HoodieFlinkQuickstart();
  }

  /**
   * Entry point.
   *
   * <p>Usage: {@code HoodieFlinkQuickstart <tablePath> <tableName> <tableType> [useSourceV2]}
   *
   * <p>When {@code useSourceV2} is {@code true} (default: {@code false}), the Hudi table is
   * registered with {@code read.source-v2.enabled = true}, which activates the FLIP-27
   * {@link org.apache.hudi.source.HoodieSource} for both streaming and batch reads.
   */
  public static void main(String[] args) throws TableNotExistException, InterruptedException {
    if (args.length < 3) {
      System.err.println("Usage: HoodieFlinkQuickstart <tablePath> <tableName> <tableType> [useSourceV2]");
      System.exit(1);
    }
    String tablePath = args[0];
    String tableName = args[1];
    HoodieTableType tableType = HoodieTableType.valueOf(args[2]);
    boolean useSourceV2 = args.length > 3 && Boolean.parseBoolean(args[3]);

    HoodieFlinkQuickstart flinkQuickstart = instance();
    flinkQuickstart.initEnv();

    // create filesystem table named source
    flinkQuickstart.createFileSource();

    // create hudi table
    flinkQuickstart.createHudiTable(tablePath, tableName, tableType, useSourceV2);

    // insert data
    flinkQuickstart.insertData();

    // streaming query (continuous read)
    flinkQuickstart.queryData();

    // update data
    flinkQuickstart.updateData();
  }

  public void initEnv() {
    if (tableEnvironment == null) {
      settings = EnvironmentSettings.newInstance().build();
      TableEnvironment streamTableEnv = TableEnvironmentImpl.create(settings);
      streamTableEnv.getConfig().getConfiguration()
          .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
      Configuration execConf = streamTableEnv.getConfig().getConfiguration();
      execConf.setString("execution.checkpointing.interval", "2s");
      // configure not to retry after failure
      execConf.setString("restart-strategy", "fixed-delay");
      execConf.setString("restart-strategy.fixed-delay.attempts", "0");
      this.tableEnvironment = streamTableEnv;
    }
  }

  public TableEnvironment getBatchTableEnv() {
    Configuration conf = new Configuration();
    // for batch upsert use cases: current suggestion is to disable these 2 options,
    // from 1.14, flink runtime execution mode has switched from streaming
    // to batch for batch execution mode(before that, both streaming and batch use streaming execution mode),
    // current batch execution mode has these limitations:
    //
    // 1. the keyed stream default to always sort the inputs by key;
    // 2. the batch state-backend requires the inputs sort by state key
    //
    // For our hudi batch pipeline upsert case, we rely on the consuming sequence for index records and data records,
    // the index records must be loaded first before data records for BucketAssignFunction to keep upsert semantics correct,
    // so we suggest disabling these 2 options to use streaming state-backend for batch execution mode
    // to keep the strategy before 1.14.
    conf.setString("execution.sorted-inputs.enabled", "false");
    conf.setString("execution.batch-state-backend.enabled", "false");
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment batchTableEnv = StreamTableEnvironment.create(execEnv, settings);
    batchTableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    return batchTableEnv;
  }

  /**
   * Creates a Hudi streaming table, optionally enabling the FLIP-27 Source V2 reader.
   *
   * <p>When {@code useSourceV2} is {@code true}, {@code read.source-v2.enabled} is added to the
   * table DDL, routing reads through {@link org.apache.hudi.source.HoodieSource} (split-enumerator
   * / reader architecture) instead of the legacy SourceFunction path.  The table is created with
   * {@code READ_AS_STREAMING = true} so the Source V2 enumerator runs in continuous-unbounded mode
   * ({@link org.apache.hudi.source.enumerator.HoodieContinuousSplitEnumerator}).
   *
   * @param tablePath   storage path for the Hudi table
   * @param tableName   logical table name used in SQL
   * @param tableType   COPY_ON_WRITE or MERGE_ON_READ
   * @param useSourceV2 whether to enable the FLIP-27 Source V2 reader
   */
  public void createHudiTable(String tablePath, String tableName,
                              HoodieTableType tableType, boolean useSourceV2) {
    this.tableName = tableName;

    QuickstartConfigurations.Sql sqlBuilder = sql(tableName)
        .option(FlinkOptions.PATH, tablePath)
        .option(FlinkOptions.RECORD_KEY_FIELD, "uuid")
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
        .option(FlinkOptions.READ_AS_STREAMING, true)
        .option(FlinkOptions.TABLE_TYPE, tableType)
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, useSourceV2)
        .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false);

    tableEnvironment.executeSql(sqlBuilder.end());
  }

  public void createFileSource() {
    // create filesystem table named source
    String createSource = QuickstartConfigurations.getFileSourceDDL("source");
    tableEnvironment.executeSql(createSource);
  }

  @Nonnull List<Row> insertData() throws InterruptedException, TableNotExistException {
    // insert data
    String insertInto = String.format("insert into %s select * from source", tableName);
    execInsertSql(tableEnvironment, insertInto);
    return queryData();
  }

  List<Row> queryData() throws InterruptedException, TableNotExistException {
    // query data
    // reading from the latest commit instance.
    return execSelectSql(tableEnvironment, String.format("select * from %s", tableName), 10);
  }

  @Nonnull List<Row> updateData() throws InterruptedException, TableNotExistException {
    // update data
    String insertInto = String.format("insert into %s select * from source", tableName);
    execInsertSql(tableEnvironment, insertInto);
    return queryData();
  }

  public static void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.await();
    } catch (InterruptedException | ExecutionException ex) {
      // ignored
    }
  }

  public static List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout)
      throws InterruptedException, TableNotExistException {
    return execSelectSql(tEnv, select, timeout, null);
  }

  public static List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout, String sourceTable)
      throws InterruptedException, TableNotExistException {
    final String sinkDDL;
    if (sourceTable != null) {
      // use the source table schema as the sink schema if the source table was specified, .
      ObjectPath objectPath = new ObjectPath(tEnv.getCurrentDatabase(), sourceTable);
      String currentCatalog = tEnv.getCurrentCatalog();
      Catalog catalog = tEnv.getCatalog(currentCatalog).get();
      ResolvedCatalogTable table = (ResolvedCatalogTable) catalog.getTable(objectPath);
      ResolvedSchema schema = table.getResolvedSchema();
      sinkDDL = QuickstartConfigurations.getCollectSinkDDL("sink", schema);
    } else {
      sinkDDL = QuickstartConfigurations.getCollectSinkDDL("sink");
    }
    return execSelectSql(tEnv, select, sinkDDL, timeout);
  }

  public static List<Row> execSelectSql(TableEnvironment tEnv, String select, String sinkDDL, long timeout)
      throws InterruptedException {
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    tEnv.executeSql(sinkDDL);
    TableResult tableResult = tEnv.executeSql("insert into sink " + select);
    // wait for the timeout then cancels the job
    TimeUnit.SECONDS.sleep(timeout);
    tableResult.getJobClient().ifPresent(JobClient::cancel);
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    return CollectSinkTableFactory.RESULT.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
