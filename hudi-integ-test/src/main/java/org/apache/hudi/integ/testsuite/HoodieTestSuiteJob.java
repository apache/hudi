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

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
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

import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;

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

  private BuiltinKeyGenerator keyGenerator;
  private transient HoodieTableMetaClient metaClient;

  public HoodieTestSuiteJob(HoodieTestSuiteConfig cfg, JavaSparkContext jsc) throws IOException {
    log.warn("Running spark job w/ app id " + jsc.sc().applicationId());
    this.cfg = cfg;
    this.jsc = jsc;
    cfg.propsFilePath = FSUtils.addSchemeIfLocalPath(cfg.propsFilePath).toString();
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).enableHiveSupport().getOrCreate();
    this.fs = FSUtils.getFs(cfg.inputBasePath, jsc.hadoopConfiguration());
    this.props = UtilHelpers.readConfig(fs.getConf(), new Path(cfg.propsFilePath), cfg.configs).getProps();
    log.info("Creating workload generator with configs : {}", props.toString());
    this.hiveConf = getDefaultHiveConf(jsc.hadoopConfiguration());
    this.keyGenerator = (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    metaClient = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(cfg.tableType)
        .setTableName(cfg.targetTableName)
        .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
        .initTable(jsc.hadoopConfiguration(), cfg.targetBasePath);

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
      HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitsTimeline();
      // Pickup the schema version from nth commit from last (most recent insert/upsert will be rolled back).
      HoodieInstant prevInstant = timeline.nthFromLastInstant(nthCommit).get();
      HoodieCommitMetadata commit = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(prevInstant).get(),
          HoodieCommitMetadata.class);
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
    new HoodieTestSuiteJob(cfg, jssc).runTestSuite();
  }

  public WorkflowDag createWorkflowDag() throws IOException {
    WorkflowDag workflowDag = this.cfg.workloadYamlPath == null ? ((WorkflowDagGenerator) ReflectionUtils
        .loadClass((this.cfg).workloadDagGenerator)).build()
        : DagUtils.convertYamlPathToDag(
            FSUtils.getFs(this.cfg.workloadYamlPath, jsc.hadoopConfiguration(), true),
            this.cfg.workloadYamlPath);
    return workflowDag;
  }

  public void runTestSuite() {
    try {
      WorkflowDag workflowDag = createWorkflowDag();
      log.info("Workflow Dag => " + DagUtils.convertDagToYaml(workflowDag));
      long startTime = System.currentTimeMillis();
      WriterContext writerContext = new WriterContext(jsc, props, cfg, keyGenerator, sparkSession);
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
      stopQuietly();
    }
  }

  private void stopQuietly() {
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
  }
}
