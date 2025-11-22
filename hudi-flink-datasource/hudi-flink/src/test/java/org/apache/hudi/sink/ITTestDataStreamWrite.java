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

package org.apache.hudi.sink;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.exception.MissingSchemaFieldException;
import org.apache.hudi.sink.transform.ChainedTransformer;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.catalog.TableOptionProperties;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.hudi.util.JsonDeserializationFunction;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;
import static org.apache.hudi.config.HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE;
import static org.apache.hudi.config.HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP;
import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for Flink Hoodie stream sink.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestDataStreamWrite extends TestLogger {

  private static final Map<String, List<String>> EXPECTED = new HashMap<>();
  private static final Map<String, List<String>> EXPECTED_TRANSFORMER = new HashMap<>();
  private static final Map<String, List<String>> EXPECTED_CHAINED_TRANSFORMER = new HashMap<>();

  static {
    EXPECTED.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));

    EXPECTED_TRANSFORMER.put("par1", Arrays.asList("id1,par1,id1,Danny,24,1000,par1", "id2,par1,id2,Stephen,34,2000,par1"));
    EXPECTED_TRANSFORMER.put("par2", Arrays.asList("id3,par2,id3,Julian,54,3000,par2", "id4,par2,id4,Fabian,32,4000,par2"));
    EXPECTED_TRANSFORMER.put("par3", Arrays.asList("id5,par3,id5,Sophia,19,5000,par3", "id6,par3,id6,Emma,21,6000,par3"));
    EXPECTED_TRANSFORMER.put("par4", Arrays.asList("id7,par4,id7,Bob,45,7000,par4", "id8,par4,id8,Han,57,8000,par4"));

    EXPECTED_CHAINED_TRANSFORMER.put("par1", Arrays.asList("id1,par1,id1,Danny,25,1000,par1", "id2,par1,id2,Stephen,35,2000,par1"));
    EXPECTED_CHAINED_TRANSFORMER.put("par2", Arrays.asList("id3,par2,id3,Julian,55,3000,par2", "id4,par2,id4,Fabian,33,4000,par2"));
    EXPECTED_CHAINED_TRANSFORMER.put("par3", Arrays.asList("id5,par3,id5,Sophia,20,5000,par3", "id6,par3,id6,Emma,22,6000,par3"));
    EXPECTED_CHAINED_TRANSFORMER.put("par4", Arrays.asList("id7,par4,id7,Bob,46,7000,par4", "id8,par4,id8,Han,58,8000,par4"));
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testWriteCopyOnWrite(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, indexType);
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 1);
    conf.set(FlinkOptions.PRE_COMBINE, true);

    defaultWriteAndCheckExpected(conf, "cow_write", 2);
  }

  @Test
  public void testWriteCopyOnWriteWithTransformer() throws Exception {
    Transformer transformer = (ds) -> ds.map((rowdata) -> {
      if (rowdata instanceof GenericRowData) {
        GenericRowData genericRD = (GenericRowData) rowdata;
        //update age field to age + 1
        genericRD.setField(2, genericRD.getInt(2) + 1);
        return genericRD;
      } else {
        throw new RuntimeException("Unrecognized row type information: " + rowdata.getClass().getSimpleName());
      }
    });

    writeWithTransformerAndCheckExpected(transformer, "cow_write_with_transformer", EXPECTED_TRANSFORMER);
  }

  @Test
  public void testWriteCopyOnWriteWithChainedTransformer() throws Exception {
    Transformer t1 = (ds) -> ds.map(rowData -> {
      if (rowData instanceof GenericRowData) {
        GenericRowData genericRD = (GenericRowData) rowData;
        //update age field to age + 1
        genericRD.setField(2, genericRD.getInt(2) + 1);
        return genericRD;
      } else {
        throw new RuntimeException("Unrecognized row type : " + rowData.getClass().getSimpleName());
      }
    });

    ChainedTransformer chainedTransformer = new ChainedTransformer(Arrays.asList(t1, t1));

    writeWithTransformerAndCheckExpected(chainedTransformer, "cow_write_with_chained_transformer", EXPECTED_CHAINED_TRANSFORMER);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testWriteMergeOnReadWithCompaction(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, indexType);
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    // use synchronized compaction to ensure flink job finishing with compaction completed.
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    defaultWriteAndCheckExpected(conf, "mor_write_with_compact", 1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteCopyOnWriteWithClustering(boolean sortClusteringEnabled) throws Exception {
    String basePath = tempFile.toURI().toString();
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);
    conf.set(FlinkOptions.CLUSTERING_DELTA_COMMITS, 1);
    conf.set(FlinkOptions.OPERATION, "insert");
    if (sortClusteringEnabled) {
      conf.set(FlinkOptions.CLUSTERING_SORT_COLUMNS, "uuid");
    }

    writeWithClusterAndCheckExpected(conf, "cow_write_with_cluster", 1, EXPECTED);
    if (sortClusteringEnabled) {
      HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf));
      HoodieTableMetaClient metaClient =
          HoodieTestUtils.createMetaClient(storageConf, basePath);
      HoodieInstant clusteringInstant = metaClient.getActiveTimeline().getLastClusteringInstant().get();
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
          metaClient, clusteringInstant);
      assertTrue(clusteringPlanOption.isPresent());
      HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();
      Map<String, String> strategyParams = clusteringPlan.getStrategy().getStrategyParams();
      // could be used in spark MultipleSparkJobExecutionStrategy
      Option<String[]> orderByColumnsOpt =
          Option.ofNullable(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()))
              .map(listStr -> listStr.split(","));
      assertTrue(orderByColumnsOpt.isPresent());
      assertTrue(orderByColumnsOpt.get()[0].equalsIgnoreCase("uuid"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testStreamWriteWithIndexBootstrap(String tableType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    // use synchronized compaction to avoid sleeping for async compact.
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.set(FlinkOptions.TABLE_TYPE, tableType);

    writeAndCheckExpected(
        conf,
        Option.empty(),
        tableType + "_index_bootstrap",
        2,
        true,
        EXPECTED);

    // check that there is no exceptions during the same with enabled index bootstrap
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    writeAndCheckExpected(
        conf,
        Option.empty(),
        tableType + "_index_bootstrap",
        2,
        true,
        EXPECTED);
  }

  private void writeWithTransformerAndCheckExpected(
      Transformer transformer,
      String jobName,
      Map<String, List<String>> expected) throws Exception {
    writeAndCheckExpected(
        TestConfigurations.getDefaultConf(tempFile.toURI().toString()),
        Option.of(transformer),
        jobName,
        2,
        true,
        expected);
  }

  private void defaultWriteAndCheckExpected(
      Configuration conf,
      String jobName,
      int checkpoints) throws Exception {
    writeAndCheckExpected(
        conf,
        Option.empty(),
        jobName,
        checkpoints,
        true,
        EXPECTED);
  }

  private void writeAndCheckExpected(
      Configuration conf,
      Option<Transformer> transformer,
      String jobName,
      int checkpoints,
      boolean restartJob,
      Map<String, List<String>> expected) throws Exception {

    Configuration envConf = new Configuration();
    if (!restartJob) {
      envConf.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
    }

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment(envConf);
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    boolean isMor = conf.get(FlinkOptions.TABLE_TYPE).equals(HoodieTableType.MERGE_ON_READ.name());

    DataStream<RowData> dataStream;

    dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(JsonDeserializationFunction.getInstance(rowType))
        .setParallelism(4);

    if (transformer.isPresent()) {
      dataStream = transformer.get().apply(dataStream);
    }

    OptionsInference.setupSinkTasks(conf, execEnv.getParallelism());
    DataStream<HoodieFlinkInternalRow> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, rowType, dataStream);
    DataStream<RowData> pipeline = Pipelines.hoodieStreamWrite(conf, rowType, hoodieRecordDataStream);
    execEnv.addOperator(pipeline.getTransformation());

    if (isMor) {
      Pipelines.compact(conf, pipeline);
    }

    execute(execEnv, isMor, jobName);
    TestData.checkWrittenDataCOW(tempFile, expected);
  }

  private void writeWithClusterAndCheckExpected(
      Configuration conf,
      String jobName,
      int checkpoints,
      Map<String, List<String>> expected) throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    DataStream<RowData> dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(JsonDeserializationFunction.getInstance(rowType))
        .setParallelism(4);

    OptionsInference.setupSinkTasks(conf, execEnv.getParallelism());
    DataStream<RowData> pipeline = Pipelines.append(conf, rowType, rowType, dataStream);
    execEnv.addOperator(pipeline.getTransformation());

    Pipelines.cluster(conf, rowType, pipeline);
    execute(execEnv, false, jobName);

    TestData.checkWrittenDataCOW(tempFile, expected);
  }

  public void execute(StreamExecutionEnvironment execEnv, boolean isMor, String jobName) throws Exception {
    // wait for the streaming job to finish
    execEnv.execute(jobName);
  }

  @Test
  public void testHoodiePipelineBuilderSource() throws Exception {
    //create a StreamExecutionEnvironment instance.
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(1);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.TABLE_NAME, "t1");
    conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    // write 3 batches of data set
    TestData.writeData(TestData.dataSetInsert(1, 2), conf);
    TestData.writeData(TestData.dataSetInsert(3, 4), conf);
    TestData.writeData(TestData.dataSetInsert(5, 6), conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.toURI().toString());

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.READ_START_COMMIT.key(), latestCommit);

    //read a hoodie table use low-level source api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_source")
        .column("uuid string not null")
        .column("name string")
        .column("age int")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    DataStream<RowData> rowDataDataStream = builder.source(execEnv);
    List<RowData> result = new ArrayList<>();
    rowDataDataStream.executeAndCollect().forEachRemaining(result::add);
    TimeUnit.SECONDS.sleep(2);//sleep 2 second for collect data
    TestData.assertRowDataEquals(result, TestData.dataSetInsert(5, 6));
  }

  @Test
  public void testHoodiePipelineBuilderSink() throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, String> options = new HashMap<>();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(), Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    Configuration conf = Configuration.fromMap(options);

    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    DataStream dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(JsonDeserializationFunction.getInstance(conf))
        .setParallelism(4);

    //sink to hoodie table use low-level sink api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("name string")
        .column("age int")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);

    builder.sink(dataStream, false);

    execute(execEnv, false, "Api_Sink_Test");
    TestData.checkWrittenDataCOW(tempFile, EXPECTED);
  }

  @Test
  public void testHoodiePipelineBuilderSourceWithSchemaSet() throws Exception {
    //create a StreamExecutionEnvironment instance.
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(1);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // create table dir
    final String dbName = DEFAULT_DATABASE.defaultValue();
    final String tableName = "t1";
    File testTable = new File(tempFile, dbName + StoragePath.SEPARATOR + tableName);
    testTable.mkdir();

    Configuration conf = TestConfigurations.getDefaultConf(testTable.toURI().toString());
    conf.set(FlinkOptions.TABLE_NAME, tableName);
    conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    // write 3 batches of data set
    TestData.writeData(TestData.dataSetInsert(1, 2), conf);
    TestData.writeData(TestData.dataSetInsert(3, 4), conf);
    TestData.writeData(TestData.dataSetInsert(5, 6), conf);

    String latestCommit = TestUtils.getLastCompleteInstant(testTable.toURI().toString());

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), testTable.toURI().toString());
    options.put(FlinkOptions.READ_START_COMMIT.key(), latestCommit);

    // create hoodie catalog, in order to get the table schema
    Configuration catalogConf = new Configuration();
    catalogConf.setString(CATALOG_PATH.key(), tempFile.toURI().toString());
    catalogConf.setString(DEFAULT_DATABASE.key(), DEFAULT_DATABASE.defaultValue());
    HoodieCatalog catalog = new HoodieCatalog("hudi", catalogConf);
    catalog.open();
    // get hoodieTable
    ObjectPath tablePath = new ObjectPath(dbName, tableName);
    TableOptionProperties.createProperties(testTable.toURI().toString(), HadoopConfigurations.getHadoopConf(catalogConf), options);
    CatalogBaseTable hoodieTable = catalog.getTable(tablePath);

    //read a hoodie table use low-level source api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_source")
        .schema(hoodieTable.getUnresolvedSchema())
        .pk("uuid")
        .partition("partition")
        .options(options);
    DataStream<RowData> rowDataDataStream = builder.source(execEnv);
    List<RowData> result = new ArrayList<>();
    rowDataDataStream.executeAndCollect().forEachRemaining(result::add);
    TimeUnit.SECONDS.sleep(2);//sleep 2 second for collect data
    TestData.assertRowDataEquals(result, TestData.dataSetInsert(5, 6));
  }

  @Test
  public void testHoodiePipelineBuilderSinkWithSchemaSet() throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, String> options = new HashMap<>();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(), Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    Configuration conf = Configuration.fromMap(options);

    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    DataStream dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(JsonDeserializationFunction.getInstance(conf))
        .setParallelism(4);

    Schema schema =
        Schema.newBuilder()
            .column("uuid", DataTypes.STRING().notNull())
            .column("name", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .column("ts", DataTypes.TIMESTAMP(3))
            .column("partition", DataTypes.STRING())
            .primaryKey("uuid")
            .build();

    //sink to hoodie table use low-level sink api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_sink")
        .schema(schema)
        .partition("partition")
        .options(options);

    builder.sink(dataStream, false);

    execute(execEnv, false, "Api_Sink_Test");
    TestData.checkWrittenDataCOW(tempFile, EXPECTED);
  }

  @Test
  public void testColumnDroppingIsNotAllowed() throws Exception {
    // Write cols: uuid, name, age, ts, partition
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    defaultWriteAndCheckExpected(conf, "initial write", 1);

    // Write cols: uuid, name, ts, partition
    conf.setString(AVRO_SCHEMA_VALIDATE_ENABLE.key(), "false");
    conf.setString(SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key(), "false");
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA_PATH,
        Objects.requireNonNull(Thread.currentThread()
            .getContextClassLoader()
            .getResource("test_read_schema_dropped_age.avsc")
        ).toString()
    );

    // assert job failure with schema compatibility exception
    try {
      writeAndCheckExpected(conf, Option.empty(), "failing job", 1, false, Collections.emptyMap());
    } catch (JobExecutionException e) {
      Throwable actualException = e;
      while (actualException != null) {
        if (actualException.getClass() == MissingSchemaFieldException.class) {
          // test is passed
          return;
        }
        actualException = actualException.getCause();
      }
    }
    throw new AssertionError(String.format("Excepted exception %s is not found", MissingSchemaFieldException.class));
  }
}
