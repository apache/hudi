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

package org.apache.hudi.integ.testsuite.job;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteJob;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.integ.testsuite.dag.ComplexDagGenerator;
import org.apache.hudi.integ.testsuite.dag.HiveSyncDagGenerator;
import org.apache.hudi.integ.testsuite.dag.HiveSyncDagGeneratorMOR;
import org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator;
import org.apache.hudi.integ.testsuite.helpers.DFSTestSuitePathSelector;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.schema.SchemaUtils;
import org.apache.hudi.integ.testsuite.schema.TestSuiteFileBasedSchemaProvider;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.utilities.sources.AvroDFSSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test against {@link HoodieTestSuiteJob}.
 */
@Disabled
// TODO(HUDI-3668): Fix this test
public class TestHoodieTestSuiteJob extends UtilitiesTestBase {

  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with useDeltaStreamer={0}, tableType={1}";
  private static final String BASE_PROPERTIES_DOCKER_DEMO_RELATIVE_PATH = "/docker/demo/config/test-suite/base"
      + ".properties";
  private static final String SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH = "/docker/demo/config/test-suite/source.avsc";
  private static final String TARGET_SCHEMA_DOCKER_DEMO_RELATIVE_PATH = "/docker/demo/config/test-suite/target.avsc";

  private static final String COW_DAG_FILE_NAME = "unit-test-cow-dag.yaml";
  private static final String COW_DAG_SOURCE_PATH = "/hudi-integ-test/src/test/resources/" + COW_DAG_FILE_NAME;

  private static final String MOR_DAG_FILE_NAME = "unit-test-mor-dag.yaml";
  private static final String MOR_DAG_SOURCE_PATH = "/hudi-integ-test/src/test/resources/" + MOR_DAG_FILE_NAME;

  private static final String COW_DAG_FILE_NAME_SPARK_DATASOURCE_NODES = "unit-test-cow-dag-spark-datasource.yaml";
  private static final String COW_DAG_SPARK_DATASOURCE_NODES_RELATIVE_PATH = "/hudi-integ-test/src/test/resources/unit-test-cow-dag-spark-datasource.yaml";

  private static final String SPARK_SQL_DAG_FILE_NAME = "unit-test-spark-sql-dag.yaml";
  private static final String SPARK_SQL_DAG_SOURCE_PATH = "/hudi-integ-test/src/test/resources/" + SPARK_SQL_DAG_FILE_NAME;

  public static Stream<Arguments> configParams() {
    Object[][] data =
        new Object[][] {{false, "COPY_ON_WRITE"}};
    return Stream.of(data).map(Arguments::of);
  }

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
    // prepare the configs.
    UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(System.getProperty("user.dir") + "/.."
        + BASE_PROPERTIES_DOCKER_DEMO_RELATIVE_PATH, dfs, dfsBasePath + "/base.properties");
    UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(System.getProperty("user.dir") + "/.."
        + SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH, dfs, dfsBasePath + "/source.avsc");
    UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(System.getProperty("user.dir") + "/.."
        + TARGET_SCHEMA_DOCKER_DEMO_RELATIVE_PATH, dfs, dfsBasePath + "/target.avsc");

    UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(System.getProperty("user.dir") + "/.."
        + COW_DAG_SOURCE_PATH, dfs, dfsBasePath + "/" + COW_DAG_FILE_NAME);
    UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(System.getProperty("user.dir") + "/.."
        + MOR_DAG_SOURCE_PATH, dfs, dfsBasePath + "/" + MOR_DAG_FILE_NAME);

    TypedProperties props = getProperties();
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/test-source"
        + ".properties");

    UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(System.getProperty("user.dir") + "/.."
        + COW_DAG_SPARK_DATASOURCE_NODES_RELATIVE_PATH, dfs, dfsBasePath + "/" + COW_DAG_FILE_NAME_SPARK_DATASOURCE_NODES);
    UtilitiesTestBase.Helpers.savePropsToDFS(getProperties(), dfs, dfsBasePath + "/test-source"
        + ".properties");
    UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(System.getProperty("user.dir") + "/.."
        + SPARK_SQL_DAG_SOURCE_PATH, dfs, dfsBasePath + "/" + SPARK_SQL_DAG_FILE_NAME);

    // Properties used for the delta-streamer which incrementally pulls from upstream DFS Avro source and
    // writes to downstream hudi table
    TypedProperties downstreamProps = new TypedProperties();
    downstreamProps.setProperty("include", "base.properties");
    downstreamProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    downstreamProps.setProperty("hoodie.datasource.write.partitionpath.field", "timestamp");

    // Source schema is the target schema of upstream table
    downstreamProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    downstreamProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/source.avsc");
    UtilitiesTestBase.Helpers.savePropsToDFS(downstreamProps, dfs,
        dfsBasePath + "/test-downstream-source.properties");
    // these tests cause a lot of log verbosity from spark, turning it down
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  private void cleanDFSDirs() throws Exception {
    dfs.delete(new Path(dfsBasePath + "/input"), true);
    dfs.delete(new Path(dfsBasePath + "/result"), true);
  }

  private static TypedProperties getProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "timestamp");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyy/MM/dd");
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.source.dfs.root", dfsBasePath + "/input");
    props.setProperty("hoodie.datasource.hive_sync.assume_date_partitioning", "true");
    props.setProperty("hoodie.datasource.hive_sync.skip_ro_suffix", "true");
    props.setProperty("hoodie.datasource.write.keytranslator.class", "org.apache.hudi"
        + ".DayBasedPartitionPathKeyTranslator");
    props.setProperty("hoodie.compact.inline.max.delta.commits", "3");
    props.setProperty("hoodie.parquet.max.file.size", "1024000");
    props.setProperty("hoodie.compact.inline.max.delta.commits", "0");
    props.setProperty("hoodie.index.type", HoodieIndex.IndexType.GLOBAL_SIMPLE.name());
    props.setProperty("hoodie.global.simple.index.parallelism", "2");
    // Reduce shuffle parallelism, spark hangs when numPartitions >> numRecords to process
    props.setProperty("hoodie.insert.shuffle.parallelism", "10");
    props.setProperty("hoodie.upsert.shuffle.parallelism", "10");
    props.setProperty("hoodie.bulkinsert.shuffle.parallelism", "10");
    props.setProperty("hoodie.compact.inline.max.delta.commits", "0");
    // Make path selection test suite specific
    props.setProperty("hoodie.deltastreamer.source.input.selector", DFSTestSuitePathSelector.class.getName());
    // Hive Configs
    props.setProperty(HiveSyncConfig.HIVE_URL.key(), "jdbc:hive2://127.0.0.1:9999/");
    props.setProperty(HiveSyncConfig.META_SYNC_DATABASE_NAME.key(), "testdb1");
    props.setProperty(HiveSyncConfig.META_SYNC_TABLE_NAME.key(), "table1");
    props.setProperty(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "datestr");
    props.setProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), TimestampBasedKeyGenerator.class.getName());

    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider");
    props.setProperty("hoodie.write.lock.hivemetastore.database", "testdb1");
    props.setProperty("hoodie.write.lock.hivemetastore.table", "table1");
    props.setProperty("hoodie.write.lock.zookeeper.url", "127.0.0.1");
    props.setProperty("hoodie.write.lock.zookeeper.port", "2828");
    props.setProperty("hoodie.write.lock.wait_time_ms", "1200000");
    props.setProperty("hoodie.write.lock.num_retries", "10");
    props.setProperty("hoodie.write.lock.zookeeper.lock_key", "test_table");
    props.setProperty("hoodie.write.lock.zookeeper.zk_base_path", "/test");
    return props;
  }

  // Tests in this class add to the test build time significantly. Since this is a Integration Test (end to end), we
  // would like to run this as a nightly build which is a TODO.
  // TODO : Clean up input / result paths after each test
  @MethodSource("configParams")
  public void testDagWithInsertUpsertAndValidate(boolean useDeltaStreamer, String tableType) throws Exception {
    this.cleanDFSDirs();
    String inputBasePath = dfsBasePath + "/input/" + UUID.randomUUID().toString();
    String outputBasePath = dfsBasePath + "/result/" + UUID.randomUUID().toString();
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath, useDeltaStreamer, tableType);
    cfg.workloadDagGenerator = ComplexDagGenerator.class.getName();
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(new Configuration()).setBasePath(cfg.targetBasePath).build();
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 2);
  }

  @Test
  public void testHiveSync() throws Exception {
    boolean useDeltaStreamer = false;
    String tableType = "COPY_ON_WRITE";
    this.cleanDFSDirs();
    String inputBasePath = dfsBasePath + "/input";
    String outputBasePath = dfsBasePath + "/result";
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath, useDeltaStreamer, tableType);
    if (tableType == HoodieTableType.COPY_ON_WRITE.name()) {
      cfg.workloadDagGenerator = HiveSyncDagGenerator.class.getName();
    } else {
      cfg.workloadDagGenerator = HiveSyncDagGeneratorMOR.class.getName();
    }
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(new Configuration()).setBasePath(cfg.targetBasePath).build();
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 1);
  }

  @Test
  public void testCOWFullDagFromYaml() throws Exception {
    boolean useDeltaStreamer = false;
    this.cleanDFSDirs();
    String inputBasePath = dfsBasePath + "/input";
    String outputBasePath = dfsBasePath + "/result";
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath, useDeltaStreamer, HoodieTableType
        .COPY_ON_WRITE.name());
    cfg.workloadYamlPath = dfsBasePath + "/" + COW_DAG_FILE_NAME;
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(new Configuration()).setBasePath(cfg.targetBasePath).build();
    //assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 5);
  }

  @Test
  public void testMORFullDagFromYaml() throws Exception {
    boolean useDeltaStreamer = false;
    this.cleanDFSDirs();
    String inputBasePath = dfsBasePath + "/input";
    String outputBasePath = dfsBasePath + "/result";
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath, useDeltaStreamer, HoodieTableType
        .MERGE_ON_READ.name());
    cfg.workloadYamlPath = dfsBasePath + "/" + MOR_DAG_FILE_NAME;
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(new Configuration()).setBasePath(cfg.targetBasePath).build();
    //assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 7);
  }

  @Test
  public void testSparkDataSourceNodesDagWithLock() throws Exception {
    boolean useDeltaStreamer = false;
    this.cleanDFSDirs();

    TypedProperties props = getProperties();
    props.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
    props.setProperty("hoodie.failed.writes.cleaner.policy", "LAZY");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/test-source"
        + ".properties");
    String inputBasePath = dfsBasePath + "/input";
    String outputBasePath = dfsBasePath + "/result";
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath, useDeltaStreamer, HoodieTableType
        .COPY_ON_WRITE.name());
    cfg.workloadYamlPath = dfsBasePath + "/" + COW_DAG_FILE_NAME_SPARK_DATASOURCE_NODES;
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(new Configuration()).setBasePath(cfg.targetBasePath).build();
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 3);
  }

  @Test
  public void testSparkSqlDag() throws Exception {
    boolean useDeltaStreamer = false;
    this.cleanDFSDirs();
    String inputBasePath = dfsBasePath + "/input";
    String outputBasePath = dfsBasePath + "/result";
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath, useDeltaStreamer, HoodieTableType
        .COPY_ON_WRITE.name());
    cfg.workloadYamlPath = dfsBasePath + "/" + SPARK_SQL_DAG_FILE_NAME;
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
  }

  protected HoodieTestSuiteConfig makeConfig(String inputBasePath, String outputBasePath, boolean useDeltaStream,
                                             String tableType) {
    HoodieTestSuiteConfig cfg = new HoodieTestSuiteConfig();
    cfg.targetBasePath = outputBasePath;
    cfg.inputBasePath = inputBasePath;
    cfg.targetTableName = "table1";
    cfg.tableType = tableType;
    cfg.sourceClassName = AvroDFSSource.class.getName();
    cfg.sourceOrderingField = SchemaUtils.SOURCE_ORDERING_FIELD;
    cfg.propsFilePath = dfsBasePath + "/test-source.properties";
    cfg.outputTypeName = DeltaOutputMode.DFS.name();
    cfg.inputFormatName = DeltaInputType.AVRO.name();
    cfg.limitFileSize = 1024 * 1024L;
    cfg.sourceLimit = 20000000;
    cfg.workloadDagGenerator = WorkflowDagGenerator.class.getName();
    cfg.schemaProviderClassName = TestSuiteFileBasedSchemaProvider.class.getName();
    cfg.useDeltaStreamer = useDeltaStream;
    return cfg;
  }

}
