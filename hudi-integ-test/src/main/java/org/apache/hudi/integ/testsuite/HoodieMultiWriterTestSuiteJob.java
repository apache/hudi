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

import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class HoodieMultiWriterTestSuiteJob {

  private static final Logger LOG = LogManager.getLogger(HoodieMultiWriterTestSuiteJob.class);

  public static void main(String[] args) throws Exception {
    final HoodieMultiWriterTestSuiteConfig cfg = new HoodieMultiWriterTestSuiteConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("workload-generator-" + cfg.outputTypeName
        + "-" + cfg.inputFormatName, cfg.sparkMaster);

    String[] inputPaths = cfg.inputBasePaths.split(",");
    String[] yamls = cfg.workloadYamlPaths.split(",");
    String[] propsFiles = cfg.propsFilePaths.split(",");

    ExecutorService executor = Executors.newFixedThreadPool(inputPaths.length);

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
    List<CompletableFuture<Boolean>> completableFutureList = new ArrayList<>();
    testSuiteConfigList.forEach(hoodieTestSuiteConfig -> {
      try {
        // start each job at 20 seconds interval so that metaClient instantiation does not overstep
        Thread.sleep(counter.get() * 20000);
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
