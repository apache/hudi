/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.bench.job;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.uber.hoodie.ComplexKeyGenerator;
import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.bench.DeltaInputFormat;
import com.uber.hoodie.bench.DeltaSinkType;
import com.uber.hoodie.bench.configuration.DFSDeltaConfig;
import com.uber.hoodie.bench.dag.DagUtils;
import com.uber.hoodie.bench.dag.WorkflowDag;
import com.uber.hoodie.bench.dag.WorkflowDagGenerator;
import com.uber.hoodie.bench.dag.scheduler.DagScheduler;
import com.uber.hoodie.bench.generator.DeltaGenerator;
import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.utilities.UtilHelpers;
import com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * This is the entry point for running a Hudi Test Suite. Although this class has similarities with
 * {@link HoodieDeltaStreamer} this class does not extend it since do not want to create a dependency on the changes in
 * DeltaStreamer.
 */
public class HudiTestSuiteJob {

  private static volatile Logger log = LogManager.getLogger(HudiTestSuiteJob.class);

  private final HudiTestSuiteConfig cfg;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;
  /**
   * Schema provider that supplies the command for writing out the generated payloads
   */
  private transient SchemaProvider schemaProvider;
  /**
   * Filesystem used
   */
  private transient FileSystem fs;
  /**
   * Spark context
   */
  private transient JavaSparkContext jsc;
  /**
   * Spark Session
   */
  private transient SparkSession sparkSession;
  /**
   * Hive Config
   */
  private transient HiveConf hiveConf;

  private ComplexKeyGenerator keyGenerator;

  public HudiTestSuiteJob(HudiTestSuiteConfig cfg, JavaSparkContext jsc) throws IOException {
    this.cfg = cfg;
    this.jsc = jsc;
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
    this.fs = FSUtils.getFs(cfg.inputBasePath, jsc.hadoopConfiguration());
    this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    log.info("Creating workload generator with configs : " + props.toString());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jsc);
    this.hiveConf = getDefaultHiveConf(jsc.hadoopConfiguration());
    this.keyGenerator = (ComplexKeyGenerator) DataSourceUtils.createKeyGenerator(props);
    if (!fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient.initTableType(jsc.hadoopConfiguration(), cfg.targetBasePath,
          cfg.storageType, cfg.targetTableName, "archived");
    }
  }

  private static HiveConf getDefaultHiveConf(Configuration cfg) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(cfg);
    return hiveConf;
  }

  public static void main(String[] args) throws Exception {
    final HudiTestSuiteConfig cfg = new HudiTestSuiteConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("workload-generator-" + cfg.sinkTypeName
        + "-" + cfg.sinkFormatName, cfg.sparkMaster);
    new HudiTestSuiteJob(cfg, jssc).runTestSuite();
  }

  public void runTestSuite() throws Exception {
    try {
      WorkflowDag workflowDag = this.cfg.workloadYamlPath == null ? ((WorkflowDagGenerator) ReflectionUtils
          .loadClass((this.cfg).workloadDagGenerator)).build()
          : DagUtils.convertYamlPathToDag(this.fs, this.cfg.workloadYamlPath);
      String schemaStr = schemaProvider.getSourceSchema().toString();
      final DeltaWriter writer = new DeltaWriter(jsc, props, cfg, schemaStr);
      final DeltaGenerator deltaGenerator = new DeltaGenerator(
          new DFSDeltaConfig(DeltaSinkType.valueOf(cfg.sinkTypeName), DeltaInputFormat.valueOf(cfg.sinkFormatName),
              new SerializableConfiguration(jsc.hadoopConfiguration()), cfg.inputBasePath, cfg.targetBasePath,
              schemaStr, cfg.limitFileSize), jsc, sparkSession, schemaStr, keyGenerator);
      DagScheduler dagScheduler = new DagScheduler(workflowDag, writer, deltaGenerator);
      dagScheduler.schedule();
      log.info("Finished scheduling all tasks");
    } catch (Exception e) {
      log.error("Failed to run Test Suite ", e);
      throw new HoodieException("Failed to run Test Suite ", e);
    } finally {
      jsc.stop();
    }
  }

  /**
   * The Hudi test suite uses {@link HoodieDeltaStreamer} to run some operations hence extend delta streamer config
   */
  public static class HudiTestSuiteConfig extends HoodieDeltaStreamer.Config {

    @Parameter(names = {"--input-base-path"}, description = "base path for input data"
        + "(Will be created if did not exist first time around. If exists, more data will be added to that path)",
        required = true)
    public String inputBasePath;

    @Parameter(names = {
        "--workload-generator-classname"}, description = "WorkflowDag of operations to generate the workload",
        required = true)
    public String workloadDagGenerator = WorkflowDagGenerator.class.getName();

    @Parameter(names = {
        "--workload-yaml-path"}, description = "Workflow Dag yaml path to generate the workload")
    public String workloadYamlPath;

    @Parameter(names = {"--sink-type"}, description = "Subclass of "
        + "com.uber.hoodie.bench.workload.DeltaSinkType to readAvro data.")
    public String sinkTypeName = DeltaSinkType.DFS.name();

    @Parameter(names = {"--sink-format"}, description = "Subclass of "
        + "com.uber.hoodie.bench.workload.DeltaSinkType to readAvro data.")
    public String sinkFormatName = DeltaInputFormat.AVRO.name();

    @Parameter(names = {"--input-file-size"}, description = "The min/max size of the input files to generate",
        required = true)
    public Long limitFileSize = 1024 * 1024 * 120L;

    @Parameter(names = {"--deltastreamer-ingest"}, description = "Choose whether to use HoodieDeltaStreamer to perform"
        + " ingestion. If set to false, HoodieWriteClient will be used")
    public Boolean useDeltaStreamer = false;

  }
}
