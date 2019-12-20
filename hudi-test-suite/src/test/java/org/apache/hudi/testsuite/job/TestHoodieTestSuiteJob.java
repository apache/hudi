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

package org.apache.hudi.testsuite.job;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.testsuite.DeltaInputFormat;
import org.apache.hudi.testsuite.DeltaOutputType;
import org.apache.hudi.testsuite.dag.HiveSyncDagGenerator;
import org.apache.hudi.testsuite.dag.InsertOnlyDagGenerator;
import org.apache.hudi.testsuite.dag.InsertUpsertDagGenerator;
import org.apache.hudi.testsuite.dag.WorkflowDagGenerator;
import org.apache.hudi.testsuite.job.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.hudi.utilities.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.AvroDFSSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestHoodieTestSuiteJob extends UtilitiesTestBase {

  String tableType;
  boolean useDeltaStream;

  public TestHoodieTestSuiteJob(String tableType, boolean useDeltaStream) {
    this.tableType = tableType;
    this.useDeltaStream = useDeltaStream;
  }

  @Parameterized.Parameters(name = "TableType")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{"COPY_ON_WRITE", false}});
  }

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
    // prepare the configs.
    UtilitiesTestBase.Helpers.copyToDFS(UtilitiesTestBase.Helpers.class.getClassLoader(), "hudi-test-suite-config/base"
            + ".properties", dfs, dfsBasePath + "/base.properties");
    UtilitiesTestBase.Helpers.copyToDFS(UtilitiesTestBase.Helpers.class.getClassLoader(), "hudi-test-suite-config/source"
        + ".avsc", dfs, dfsBasePath + "/source.avsc");
    UtilitiesTestBase.Helpers.copyToDFS(UtilitiesTestBase.Helpers.class.getClassLoader(), "hudi-test-suite-config/target"
        + ".avsc", dfs, dfsBasePath + "/target.avsc");

    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "timestamp");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyy/MM/dd");
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.source.dfs.root", dfsBasePath + "/input");
    props.setProperty("hoodie.datasource.hive_sync.assume_date_partitioning", "true");
    props.setProperty("hoodie.datasource.write.keytranslator.class", "org.apache.hudi"
        + ".DayBasedPartitionPathKeyTranslator");
    // Hive Configs
    props.setProperty(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), "jdbc:hive2://127.0.0.1:9999/");
    props.setProperty(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), "testdb1");
    props.setProperty(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), "hive_trips");
    props.setProperty(DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY(), "false");
    props.setProperty(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "datestr");
    props.setProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), TimestampBasedKeyGenerator.class.getName());
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/test-source"
        + ".properties");

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
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
  }

  // TODO : Clean up input / result paths after each test
  @Test
  public void testSimpleInsert() throws Exception {
    dfs.delete(new Path(dfsBasePath + "/input"), true);
    dfs.delete(new Path(dfsBasePath + "/result"), true);
    String inputBasePath = dfsBasePath + "/input";
    String outputBasePath = dfsBasePath + "/result";
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath);
    cfg.workloadDagGenerator = InsertOnlyDagGenerator.class.getName();
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(new Configuration(), cfg.targetBasePath);
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 2);
  }

  @Test
  public void testSimpleInsertUpdate() throws Exception {
    dfs.delete(new Path(dfsBasePath + "/input"), true);
    dfs.delete(new Path(dfsBasePath + "/result"), true);
    String inputBasePath = dfsBasePath + "/input/" + UUID.randomUUID().toString();
    String outputBasePath = dfsBasePath + "/result/" + UUID.randomUUID().toString();
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath);
    cfg.workloadDagGenerator = InsertUpsertDagGenerator.class.getName();
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(new Configuration(), cfg.targetBasePath);
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 2);
  }

  @Test
  public void testComplexDag() throws Exception {
    dfs.delete(new Path(dfsBasePath + "/input"), true);
    dfs.delete(new Path(dfsBasePath + "/result"), true);
    String inputBasePath = dfsBasePath + "/input/" + UUID.randomUUID().toString();
    String outputBasePath = dfsBasePath + "/result/" + UUID.randomUUID().toString();
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath);
    cfg.workloadDagGenerator = WorkflowDagGenerator.class.getName();
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(new Configuration(), cfg.targetBasePath);
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 3);
  }

  @Test
  public void testHiveSync() throws Exception {
    dfs.delete(new Path(dfsBasePath + "/input"), true);
    dfs.delete(new Path(dfsBasePath + "/result"), true);
    String inputBasePath = dfsBasePath + "/input";
    String outputBasePath = dfsBasePath + "/result";
    HoodieTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath);
    cfg.workloadDagGenerator = HiveSyncDagGenerator.class.getName();
    HoodieTestSuiteJob hoodieTestSuiteJob = new HoodieTestSuiteJob(cfg, jsc);
    hoodieTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(new Configuration(), cfg.targetBasePath);
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 1);
  }

  protected HoodieTestSuiteConfig makeConfig(String inputBasePath, String outputBasePath) {
    HoodieTestSuiteConfig cfg = new HoodieTestSuiteConfig();
    cfg.targetBasePath = outputBasePath;
    cfg.inputBasePath = inputBasePath;
    cfg.targetTableName = "hoodie_trips";
    cfg.storageType = this.tableType;
    cfg.sourceClassName = AvroDFSSource.class.getName();
    cfg.sourceOrderingField = "timestamp";
    cfg.propsFilePath = dfsBasePath + "/test-source.properties";
    cfg.outputTypeName = DeltaOutputType.DFS.name();
    cfg.inputFormatName = DeltaInputFormat.AVRO.name();
    cfg.limitFileSize = 1024 * 1024L;
    cfg.sourceLimit = 20000000;
    cfg.workloadDagGenerator = WorkflowDagGenerator.class.getName();
    cfg.schemaProviderClassName = FilebasedSchemaProvider.class.getName();
    cfg.useDeltaStreamer = this.useDeltaStream;
    return cfg;
  }

}
