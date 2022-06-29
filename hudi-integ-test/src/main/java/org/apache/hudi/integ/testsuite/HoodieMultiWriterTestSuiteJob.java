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

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi write test suite job to assist in testing multi-writer scenarios. This test spins up one thread per writer as per configurations.
 * Three params are of interest to this job in addition to regular HoodieTestsuiteJob.
 * --input-base-paths "base_path/input1,base_path/input2"
 * --props-paths "file:props_path/multi-writer-1.properties,file:/props_path/multi-writer-2.properties"
 * --workload-yaml-paths "file:some_path/multi-writer-1-ds.yaml,file:/some_path/multi-writer-2-sds.yaml"
 *
 * Each of these should have same number of comma separated entries.
 * Each writer will generate data in the corresponding input-base-path.
 * and each writer will take in its own properties path and the respective yaml file as well.
 *
 * Common tests:
 * Writer 1 DeltaStreamer ingesting data into partitions 0 to 10, Writer 2 Spark datasource ingesting data into partitions 100 to 110.
 * Multiple spark datasource writers, each writing to exclusive set of partitions.
 *
 * Example comamnd
 * spark-submit
 * --packages org.apache.spark:spark-avro_2.11:2.4.0
 * --conf spark.task.cpus=3
 * --conf spark.executor.cores=3
 * --conf spark.task.maxFailures=100
 * --conf spark.memory.fraction=0.4
 * --conf spark.rdd.compress=true
 * --conf spark.kryoserializer.buffer.max=2000m
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
 * --conf spark.memory.storageFraction=0.1
 * --conf spark.shuffle.service.enabled=true
 * --conf spark.sql.hive.convertMetastoreParquet=false
 * --conf spark.driver.maxResultSize=12g
 * --conf spark.executor.heartbeatInterval=120s
 * --conf spark.network.timeout=600s
 * --conf spark.yarn.max.executor.failures=10
 * --conf spark.sql.catalogImplementation=hive
 * --conf spark.driver.extraClassPath=/var/demo/jars/*
 * --conf spark.executor.extraClassPath=/var/demo/jars/*
 * --class org.apache.hudi.integ.testsuite.HoodieMultiWriterTestSuiteJob /opt/hudi-integ-test-bundle-0.11.0-SNAPSHOT.jar
 * --source-ordering-field test_suite_source_ordering_field
 * --use-deltastreamer
 * --target-base-path /user/hive/warehouse/hudi-integ-test-suite/output
 * --input-base-paths "/user/hive/warehouse/hudi-integ-test-suite/input1,/user/hive/warehouse/hudi-integ-test-suite/input2"
 * --target-table hudi_table
 * --props-paths "multi-writer-1.properties,multi-writer-2.properties"
 * --schemaprovider-class org.apache.hudi.integ.testsuite.schema.TestSuiteFileBasedSchemaProvider
 * --source-class org.apache.hudi.utilities.sources.AvroDFSSource --input-file-size 125829120
 * --workload-yaml-paths "file:/opt/multi-writer-1-ds.yaml,file:/opt/multi-writer-2-sds.yaml"
 * --workload-generator-classname org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator
 * --table-type COPY_ON_WRITE --compact-scheduling-minshare 1
 * --input-base-path "dummyValue"
 * --workload-yaml-path "dummyValue"
 * --props "dummyValue"
 * --use-hudi-data-to-generate-updates
 *
 * Example command that works w/ docker.
 *
 */
public class HoodieMultiWriterTestSuiteJob {

  private static final Logger LOG = LogManager.getLogger(HoodieMultiWriterTestSuiteJob.class);

  public static void main(String[] args) throws Exception {
    final HoodieMultiWriterTestSuiteConfig cfg = new HoodieMultiWriterTestSuiteConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("multi-writer-test-run-" + cfg.outputTypeName
        + "-" + cfg.inputFormatName, cfg.sparkMaster);

    String[] inputPaths = cfg.inputBasePaths.split(",");
    String[] yamls = cfg.workloadYamlPaths.split(",");
    String[] propsFiles = cfg.propsFilePaths.split(",");

    if (inputPaths.length != yamls.length || yamls.length != propsFiles.length) {
      throw new HoodieException("Input paths, property file and yaml file counts does not match ");
    }

    ExecutorService executor = Executors.newFixedThreadPool(inputPaths.length);
    Random random = new Random();

    List<HoodieTestSuiteJob.HoodieTestSuiteConfig> testSuiteConfigList = new ArrayList<>();
    int jobIndex = 0;
    for (String inputPath : inputPaths) {
      HoodieMultiWriterTestSuiteConfig testSuiteConfig = new HoodieMultiWriterTestSuiteConfig();
      deepCopyConfigs(cfg, testSuiteConfig);
      testSuiteConfig.inputBasePath = inputPath;
      testSuiteConfig.workloadYamlPath = yamls[jobIndex];
      testSuiteConfig.propsFilePath = propsFiles[jobIndex];
      testSuiteConfigList.add(testSuiteConfig);
      jobIndex++;
    }

    AtomicBoolean jobFailed = new AtomicBoolean(false);
    AtomicInteger counter = new AtomicInteger(0);
    List<Long> waitTimes = new ArrayList<>();
    for (int i = 0;i < jobIndex ;i++) {
      if (i == 0) {
        waitTimes.add(0L);
      } else {
        // every job after 1st, will start after 1 min + some delta.
        waitTimes.add(60000L + random.nextInt(10000));
      }
    }
    List<CompletableFuture<Boolean>> completableFutureList = new ArrayList<>();
    testSuiteConfigList.forEach(hoodieTestSuiteConfig -> {
      try {
        // start each job at 20 seconds interval so that metaClient instantiation does not overstep
        Thread.sleep(waitTimes.get(counter.get()));
        LOG.info("Starting job " + hoodieTestSuiteConfig.toString());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      completableFutureList.add(CompletableFuture.supplyAsync(() -> {
        boolean toReturn = true;
        try {
          new HoodieTestSuiteJob(hoodieTestSuiteConfig, jssc, false).runTestSuite();
          LOG.info("Job completed successfully");
        } catch (Exception e) {
          if (!jobFailed.getAndSet(true)) {
            LOG.error("Exception thrown " + e.getMessage() + ", cause : " + e.getCause());
            throw new RuntimeException("HoodieTestSuiteJob Failed " + e.getCause() + ", and msg " + e.getMessage(), e);
          } else {
            LOG.info("Already a job failed. so, not throwing any exception ");
          }
        }
        return toReturn;
      }, executor));
      counter.getAndIncrement();
    });

    LOG.info("Going to await until all jobs complete");
    try {
      CompletableFuture completableFuture = allOfTerminateOnFailure(completableFutureList);
      completableFuture.get();
    } finally {
      executor.shutdownNow();
      if (jssc != null) {
        LOG.info("Completed and shutting down spark context ");
        LOG.info("Shutting down spark session and JavaSparkContext");
        SparkSession.builder().config(jssc.getConf()).enableHiveSupport().getOrCreate().stop();
        jssc.close();
      }
    }
  }

  public static CompletableFuture allOfTerminateOnFailure(List<CompletableFuture<Boolean>> futures) {
    CompletableFuture<?> failure = new CompletableFuture();
    AtomicBoolean jobFailed = new AtomicBoolean(false);
    for (CompletableFuture<?> f : futures) {
      f.exceptionally(ex -> {
        if (!jobFailed.getAndSet(true)) {
          System.out.println("One of the job failed. Cancelling all other futures. " + ex.getCause() + ", " + ex.getMessage());
          futures.forEach(future -> future.cancel(true));
        }
        return null;
      });
    }
    return CompletableFuture.anyOf(failure, CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])));
  }

  static void deepCopyConfigs(HoodieMultiWriterTestSuiteConfig globalConfig, HoodieMultiWriterTestSuiteConfig tableConfig) {
    tableConfig.enableHiveSync = globalConfig.enableHiveSync;
    tableConfig.enableMetaSync = globalConfig.enableMetaSync;
    tableConfig.schemaProviderClassName = globalConfig.schemaProviderClassName;
    tableConfig.sourceOrderingField = globalConfig.sourceOrderingField;
    tableConfig.sourceClassName = globalConfig.sourceClassName;
    tableConfig.tableType = globalConfig.tableType;
    tableConfig.targetTableName = globalConfig.targetTableName;
    tableConfig.operation = globalConfig.operation;
    tableConfig.sourceLimit = globalConfig.sourceLimit;
    tableConfig.checkpoint = globalConfig.checkpoint;
    tableConfig.continuousMode = globalConfig.continuousMode;
    tableConfig.filterDupes = globalConfig.filterDupes;
    tableConfig.payloadClassName = globalConfig.payloadClassName;
    tableConfig.forceDisableCompaction = globalConfig.forceDisableCompaction;
    tableConfig.maxPendingCompactions = globalConfig.maxPendingCompactions;
    tableConfig.maxPendingClustering = globalConfig.maxPendingClustering;
    tableConfig.minSyncIntervalSeconds = globalConfig.minSyncIntervalSeconds;
    tableConfig.transformerClassNames = globalConfig.transformerClassNames;
    tableConfig.commitOnErrors = globalConfig.commitOnErrors;
    tableConfig.compactSchedulingMinShare = globalConfig.compactSchedulingMinShare;
    tableConfig.compactSchedulingWeight = globalConfig.compactSchedulingWeight;
    tableConfig.deltaSyncSchedulingMinShare = globalConfig.deltaSyncSchedulingMinShare;
    tableConfig.deltaSyncSchedulingWeight = globalConfig.deltaSyncSchedulingWeight;
    tableConfig.sparkMaster = globalConfig.sparkMaster;
    tableConfig.workloadDagGenerator = globalConfig.workloadDagGenerator;
    tableConfig.outputTypeName = globalConfig.outputTypeName;
    tableConfig.inputFormatName = globalConfig.inputFormatName;
    tableConfig.inputParallelism = globalConfig.inputParallelism;
    tableConfig.useDeltaStreamer = globalConfig.useDeltaStreamer;
    tableConfig.cleanInput = globalConfig.cleanInput;
    tableConfig.cleanOutput = globalConfig.cleanOutput;
    tableConfig.targetBasePath = globalConfig.targetBasePath;
  }

  public static class HoodieMultiWriterTestSuiteConfig extends HoodieTestSuiteJob.HoodieTestSuiteConfig {

    @Parameter(names = {"--input-base-paths"}, description = "base paths for input data"
        + "(Will be created if did not exist first time around. If exists, more data will be added to that path)",
        required = true)
    public String inputBasePaths;

    @Parameter(names = {
        "--workload-yaml-paths"}, description = "Workflow Dag yaml path to generate the workload")
    public String workloadYamlPaths;

    @Parameter(names = {
        "--props-paths"}, description = "Workflow Dag yaml path to generate the workload")
    public String propsFilePaths;
  }
}
