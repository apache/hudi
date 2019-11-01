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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.integ.testsuite.configuration.DFSDeltaConfig;
import org.apache.hudi.integ.testsuite.dag.DagUtils;
import org.apache.hudi.integ.testsuite.dag.WorkflowDag;
import org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator;
import org.apache.hudi.integ.testsuite.dag.scheduler.DagScheduler;
import org.apache.hudi.integ.testsuite.generator.DeltaGenerator;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the entry point for running a Hudi Test Suite. Although this class has similarities with
 * {@link HoodieDeltaStreamer} this class does not extend it since do not want to create a dependency on the changes in
 * DeltaStreamer.
 */
public class HoodieTestSuiteJob {

  private static volatile Logger log = LoggerFactory.getLogger(HoodieTestSuiteJob.class);

  private final HoodieTestSuiteConfig cfg;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;
  /**
   * Schema provider that supplies the command for writing out the generated payloads.
   */
  private transient SchemaProvider schemaProvider;
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

  private KeyGenerator keyGenerator;

  public HoodieTestSuiteJob(HoodieTestSuiteConfig cfg, JavaSparkContext jsc) throws IOException {
    this.cfg = cfg;
    this.jsc = jsc;
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
    this.fs = FSUtils.getFs(cfg.inputBasePath, jsc.hadoopConfiguration());
    this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    log.info("Creating workload generator with configs : {}", props.toString());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jsc);
    this.hiveConf = getDefaultHiveConf(jsc.hadoopConfiguration());
    this.keyGenerator = DataSourceUtils.createKeyGenerator(props);
    if (!fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient.initTableType(jsc.hadoopConfiguration(), cfg.targetBasePath,
          HoodieTableType.valueOf(cfg.tableType), cfg.targetTableName, "archived");
    }
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

  public void runTestSuite() {
    try {
      WorkflowDag workflowDag = this.cfg.workloadYamlPath == null ? ((WorkflowDagGenerator) ReflectionUtils
          .loadClass((this.cfg).workloadDagGenerator)).build()
          : DagUtils.convertYamlPathToDag(this.fs, this.cfg.workloadYamlPath);
      log.info("Workflow Dag => " + DagUtils.convertDagToYaml(workflowDag));
      long startTime = System.currentTimeMillis();
      String schemaStr = schemaProvider.getSourceSchema().toString();
      final HoodieTestSuiteWriter writer = new HoodieTestSuiteWriter(jsc, props, cfg, schemaStr);
      final DeltaGenerator deltaGenerator = new DeltaGenerator(
          new DFSDeltaConfig(DeltaOutputMode.valueOf(cfg.outputTypeName), DeltaInputType.valueOf(cfg.inputFormatName),
              new SerializableConfiguration(jsc.hadoopConfiguration()), cfg.inputBasePath, cfg.targetBasePath,
              schemaStr, cfg.limitFileSize), jsc, sparkSession, schemaStr, keyGenerator);
      DagScheduler dagScheduler = new DagScheduler(workflowDag, writer, deltaGenerator);
      dagScheduler.schedule();
      log.info("Finished scheduling all tasks, Time taken {}", System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      log.error("Failed to run Test Suite ", e);
      throw new HoodieException("Failed to run Test Suite ", e);
    } finally {
      jsc.stop();
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
        "--workload-generator-classname"}, description = "WorkflowDag of operations to generate the workload",
        required = true)
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

    @Parameter(names = {"--use-deltastreamer"}, description = "Choose whether to use HoodieDeltaStreamer to "
        + "perform"
        + " ingestion. If set to false, HoodieWriteClient will be used")
    public Boolean useDeltaStreamer = false;

  }
}
