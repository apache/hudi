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

package org.apache.hudi.integ.testsuite;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.DagUtils;
import org.apache.hudi.integ.testsuite.dag.WorkflowDag;
import org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator;
import org.apache.hudi.integ.testsuite.dag.WriterContext;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.RollbackNode;
import org.apache.hudi.integ.testsuite.dag.scheduler.DagScheduler;
import org.apache.hudi.integ.testsuite.dag.scheduler.SaferSchemaDagScheduler;
import org.apache.hudi.integ.testsuite.helpers.HiveServiceProvider;
import org.apache.hudi.integ.testsuite.helpers.ZookeeperServiceProvider;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_HISTORY_PATH;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * This is the entry point for running a Hudi Test Suite. Although this class has similarities with {@link HoodieDeltaStreamer} this class does not extend it since do not want to create a dependency
 * on the changes in DeltaStreamer.
 */
public class HoodieTestSuiteJob {

  private static volatile Logger log = LoggerFactory.getLogger(HoodieTestSuiteJob.class);

  private final HoodieTestSuiteConfig cfg;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;
  /**
   * Filesystem used.
   */
  private transient FileSystem fs;
  /**
   * Spark context.
   */
  private transient JavaSparkContext jsc;
  /**
   * Spark Session.
   */
  private transient SparkSession sparkSession;
  /**
   * Hive Config.
   */
  private transient HiveConf hiveConf;

  private boolean stopJsc = true;
  private BuiltinKeyGenerator keyGenerator;
  private transient HoodieTableMetaClient metaClient;

  public HoodieTestSuiteJob(HoodieTestSuiteConfig cfg, JavaSparkContext jsc) throws IOException {
    this(cfg, jsc, true);
  }

  public HoodieTestSuiteJob(HoodieTestSuiteConfig cfg, JavaSparkContext jsc, boolean stopJsc) throws IOException {
    log.warn("Running spark job w/ app id " + jsc.sc().applicationId());
    this.cfg = cfg;
    this.jsc = jsc;
    this.stopJsc = stopJsc;
    cfg.propsFilePath = HadoopFSUtils.addSchemeIfLocalPath(cfg.propsFilePath).toString();
    this.sparkSession =
        SparkSession.builder().config(jsc.getConf()).enableHiveSupport().getOrCreate();
    this.fs = HadoopFSUtils.getFs(cfg.inputBasePath, jsc.hadoopConfiguration());
    this.props =
        UtilHelpers.readConfig(fs.getConf(), new Path(cfg.propsFilePath), cfg.configs).getProps();
    log.info("Creating workload generator with configs : {}", props.toString());
    this.hiveConf = getDefaultHiveConf(jsc.hadoopConfiguration());
    this.keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    if (!fs.exists(new Path(cfg.targetBasePath))) {
      metaClient = HoodieTableMetaClient.newTableBuilder()
          .setTableType(cfg.tableType)
          .setTableName(cfg.targetTableName)
          .setRecordKeyFields(this.props.getString(DataSourceWriteOptions.RECORDKEY_FIELD().key()))
          .setArchiveLogFolder(TIMELINE_HISTORY_PATH.defaultValue())
          .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), cfg.targetBasePath);
    } else {
      metaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
          .setBasePath(cfg.targetBasePath).build();
    }

    if (cfg.cleanInput) {
      Path inputPath = new Path(cfg.inputBasePath);
      if (fs.exists(inputPath)) {
        fs.delete(inputPath, true);
      }
    }

    if (cfg.cleanOutput) {
      Path outputPath = new Path(cfg.targetBasePath);
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }
    }
  }

  int getSchemaVersionFromCommit(int nthCommit) throws Exception {
    int version = 0;
    try {
      HoodieTimeline timeline = new ActiveTimelineV2(metaClient).getCommitsTimeline();
      // Pickup the schema version from nth commit from last (most recent insert/upsert will be rolled back).
      HoodieInstant prevInstant = timeline.nthFromLastInstant(nthCommit).get();
      HoodieCommitMetadata commit = timeline.readCommitMetadata(prevInstant);
      Map<String, String> extraMetadata = commit.getExtraMetadata();
      String avroSchemaStr = extraMetadata.get(HoodieCommitMetadata.SCHEMA_KEY);
      Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
      version = Integer.parseInt(avroSchema.getObjectProp("schemaVersion").toString());
      // DAG will generate & ingest data for 2 versions (n-th version being validated, n-1).
      log.info(String.format("Last used schemaVersion from latest commit file was %d. Optimizing the DAG.", version));
    } catch (Exception e) {
      // failed to open the commit to read schema version.
      // continue executing the DAG without any changes.
      log.info("Last used schemaVersion could not be validated from commit file.  Skipping SaferSchema Optimization.");
    }
    return version;
  }

  private static HiveConf getDefaultHiveConf(Configuration cfg) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(cfg);
    return hiveConf;
  }

  public static void main(String[] args) throws Exception {
    final HoodieTestSuiteConfig cfg = new HoodieTestSuiteConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("workload-generator-" + cfg.outputTypeName
        + "-" + cfg.inputFormatName, cfg.sparkMaster);
    new HoodieTestSuiteJob(cfg, jssc, true).runTestSuite();
  }

  public WorkflowDag createWorkflowDag() throws IOException {
    WorkflowDag workflowDag = this.cfg.workloadYamlPath == null ? ((WorkflowDagGenerator) ReflectionUtils
        .loadClass((this.cfg).workloadDagGenerator)).build()
        : DagUtils.convertYamlPathToDag(
        HadoopFSUtils.getFs(this.cfg.workloadYamlPath, jsc.hadoopConfiguration(), true),
        this.cfg.workloadYamlPath);
    return workflowDag;
  }

  public void runTestSuite() {
    WriterContext writerContext = null;
    try {
      WorkflowDag workflowDag = createWorkflowDag();
      log.info("Workflow Dag => " + DagUtils.convertDagToYaml(workflowDag));
      long startTime = System.currentTimeMillis();
      writerContext = new WriterContext(jsc, props, cfg, keyGenerator, sparkSession);
      writerContext.initContext(jsc);
      startOtherServicesIfNeeded(writerContext);
      if (this.cfg.saferSchemaEvolution) {
        int numRollbacks = 2; // rollback most recent upsert/insert, by default.
        // if root is RollbackNode, get num_rollbacks
        List<DagNode> root = workflowDag.getNodeList();
        if (!root.isEmpty() && root.get(0) instanceof RollbackNode) {
          numRollbacks = root.get(0).getConfig().getNumRollbacks();
        }

        int version = getSchemaVersionFromCommit(numRollbacks - 1);
        SaferSchemaDagScheduler dagScheduler = new SaferSchemaDagScheduler(workflowDag, writerContext, jsc, version);
        dagScheduler.schedule();
      } else {
        DagScheduler dagScheduler = new DagScheduler(workflowDag, writerContext, jsc);
        dagScheduler.schedule();
      }
      log.info("Finished scheduling all tasks, Time taken {}", System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      log.error("Failed to run Test Suite ", e);
      throw new HoodieException("Failed to run Test Suite ", e);
    } finally {
      if (writerContext != null) {
        writerContext.shutdownResources();
      }
      if (stopJsc) {
        stopQuietly();
      }
    }
  }

  protected void stopQuietly() {
    try {
      sparkSession.stop();
      jsc.stop();
    } catch (Exception e) {
      log.error("Unable to stop spark session", e);
    }
  }

  private void startOtherServicesIfNeeded(WriterContext writerContext) throws Exception {
    if (cfg.startHiveMetastore) {
      HiveServiceProvider hiveServiceProvider = new HiveServiceProvider(
          Config.newBuilder().withHiveLocal(true).build());
      hiveServiceProvider.startLocalHiveServiceIfNeeded(writerContext.getHoodieTestSuiteWriter().getConfiguration());
      hiveServiceProvider.syncToLocalHiveIfNeeded(writerContext.getHoodieTestSuiteWriter());
    }

    if (cfg.startZookeeper) {
      ZookeeperServiceProvider zookeeperServiceProvider = new ZookeeperServiceProvider(Config.newBuilder().withHiveLocal(true).build(),
          writerContext.getHoodieTestSuiteWriter().getConfiguration());
      zookeeperServiceProvider.startLocalZookeeperIfNeeded();
    }
  }

  /**
   * The Hudi test suite uses {@link HoodieDeltaStreamer} to run some operations hence extend delta streamer config.
   */
  public static class HoodieTestSuiteConfig extends HoodieDeltaStreamer.Config {

    @Parameter(names = {"--input-base-path"}, description = "base path for input data"
        + "(Will be created if did not exist first time around. If exists, more data will be added to that path)",
        required = true)
    public String inputBasePath;

    @Parameter(names = {
        "--workload-generator-classname"}, description = "WorkflowDag of operations to generate the workload")
    public String workloadDagGenerator = WorkflowDagGenerator.class.getName();

    @Parameter(names = {
        "--workload-yaml-path"}, description = "Workflow Dag yaml path to generate the workload")
    public String workloadYamlPath;

    @Parameter(names = {"--delta-output-type"}, description = "Subclass of "
        + "org.apache.hudi.testsuite.workload.DeltaOutputMode to readAvro data.")
    public String outputTypeName = DeltaOutputMode.DFS.name();

    @Parameter(names = {"--delta-input-format"}, description = "Subclass of "
        + "org.apache.hudi.testsuite.workload.DeltaOutputMode to read avro data.")
    public String inputFormatName = DeltaInputType.AVRO.name();

    @Parameter(names = {"--input-file-size"}, description = "The min/max size of the input files to generate",
        required = true)
    public Long limitFileSize = 1024 * 1024 * 120L;

    @Parameter(names = {"--input-parallelism"}, description = "Parallelism to use when generation input files",
        required = false)
    public Integer inputParallelism = 0;

    @Parameter(names = {"--delete-old-input"}, description = "Delete older input files once they have been ingested",
        required = false)
    public Boolean deleteOldInput = false;

    @Parameter(names = {"--use-deltastreamer"}, description = "Choose whether to use HoodieDeltaStreamer to "
        + "perform ingestion. If set to false, HoodieWriteClient will be used")
    public Boolean useDeltaStreamer = false;

    @Parameter(names = {"--clean-input"}, description = "Clean the input folders and delete all files within it "
        + "before starting the Job")
    public Boolean cleanInput = false;

    @Parameter(names = {"--clean-output"}, description = "Clean the output folders and delete all files within it "
        + "before starting the Job")
    public Boolean cleanOutput = false;

    @Parameter(names = {"--saferSchemaEvolution"}, description = "Optimize the DAG for safer schema evolution."
        + "(If not provided, assumed to be false.)",
        required = false)
    public Boolean saferSchemaEvolution = false;

    @Parameter(names = {"--start-zookeeper"}, description = "Start Zookeeper instance to use for optimistic lock ")
    public Boolean startZookeeper = false;

    @Parameter(names = {"--start-hive-metastore"}, description = "Start Hive Metastore to use for optimistic lock ")
    public Boolean startHiveMetastore = false;

    @Parameter(names = {"--use-hudi-data-to-generate-updates"}, description = "Use data from hudi to generate updates for new batches ")
    public Boolean useHudiToGenerateUpdates = false;

    @Parameter(names = {"--test-continuous-mode"}, description = "Tests continuous mode in deltastreamer.")
    public Boolean testContinuousMode = false;

    @Parameter(names = {"--enable-presto-validation"}, description = "Enables presto validation")
    public Boolean enablePrestoValidation = false;

    @Parameter(names = {"--presto-jdbc-url"}, description = "Presto JDBC URL in the format jdbc:presto://<host>:<port>/<catalog>/<schema>  "
        + "e.g. URL to connect to Presto running on localhost port 8080 with the catalog `hive` and the schema `sales`: "
        + "jdbc:presto://localhost:8080/hive/sales")
    public String prestoJdbcUrl = EMPTY_STRING;

    @Parameter(names = {"--presto-jdbc-username"}, description = "Username to use for authentication")
    public String prestoUsername = "test";

    @Parameter(names = {"--presto-jdbc-password"}, description = "Password corresponding to the username to use for authentication")
    public String prestoPassword;

    @Parameter(names = {"--trino-jdbc-url"}, description = "Trino JDBC URL in the format jdbc:trino://<host>:<port>/<catalog>/<schema>  "
        + "e.g. URL to connect to Trino running on localhost port 8080 with the catalog `hive` and the schema `sales`: "
        + "jdbc:trino://localhost:8080/hive/sales")
    public String trinoJdbcUrl = EMPTY_STRING;

    @Parameter(names = {"--trino-jdbc-username"}, description = "Username to use for authentication")
    public String trinoUsername = "test";

    @Parameter(names = {"--trino-jdbc-password"}, description = "Password corresponding to the username to use for authentication")
    public String trinoPassword;

    @Parameter(names = {"--index-type"}, description = "Index type to use for writes")
    public String indexType = "SIMPLE";

    @Parameter(names = {"--enable-metadata-on-read"}, description = "Enables metadata for queries")
    public Boolean enableMetadataOnRead = HoodieMetadataConfig.ENABLE.defaultValue();
  }
}
