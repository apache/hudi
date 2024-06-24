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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import scala.collection.JavaConverters;

@ShellComponent
public class ClusteringCommand {

  /**
   * Schedule clustering table service.
   * <p>
   * Example:
   * > connect --path {path to hudi table}
   * > clustering schedule --sparkMaster local --sparkMemory 2g
   */
  @ShellMethod(key = "clustering schedule", value = "Schedule Clustering")
  public String scheduleClustering(
      @ShellOption(value = "--sparkMaster", defaultValue = SparkUtil.DEFAULT_SPARK_MASTER, help = "Spark master") final String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "1g", help = "Spark executor memory") final String sparkMemory,
      @ShellOption(value = "--propsFilePath", help = "path to properties file on localfs or dfs with configurations "
          + "for hoodie client for clustering", defaultValue = "") final String propsFilePath,
      @ShellOption(value = "--hoodieConfigs", help = "Any configuration that can be set in the properties file can "
          + "be passed here in the form of an array", defaultValue = "") final String[] configs) throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);

    // First get a clustering instant time and pass it to spark launcher for scheduling clustering
    String clusteringInstantTime = HoodieActiveTimeline.createNewInstantTime();

    sparkLauncher.addAppArgs(SparkCommand.CLUSTERING_SCHEDULE.toString(), master, sparkMemory,
        HoodieCLI.basePath, client.getTableConfig().getTableName(), clusteringInstantTime, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to schedule clustering for " + clusteringInstantTime;
    }
    return "Succeeded to schedule clustering for " + clusteringInstantTime;
  }

  /**
   * Run clustering table service.
   * <p>
   * Example:
   * > connect --path {path to hudi table}
   * > clustering schedule --sparkMaster local --sparkMemory 2g
   * > clustering run --sparkMaster local --sparkMemory 2g --clusteringInstant  20211124005208
   */
  @ShellMethod(key = "clustering run", value = "Run Clustering")
  public String runClustering(
      @ShellOption(value = "--sparkMaster", defaultValue = SparkUtil.DEFAULT_SPARK_MASTER, help = "Spark master") final String master,
      @ShellOption(value = "--sparkMemory", help = "Spark executor memory", defaultValue = "4g") final String sparkMemory,
      @ShellOption(value = "--parallelism", help = "Parallelism for hoodie clustering", defaultValue = "1") final String parallelism,
      @ShellOption(value = "--retry", help = "Number of retries", defaultValue = "1") final String retry,
      @ShellOption(value = "--clusteringInstant", help = "Clustering instant time",
          defaultValue = ShellOption.NULL) final String clusteringInstantTime,
      @ShellOption(value = "--propsFilePath", help = "path to properties file on localfs or dfs with configurations for "
          + "hoodie client for compacting", defaultValue = "") final String propsFilePath,
      @ShellOption(value = "--hoodieConfigs", help = "Any configuration that can be set in the properties file can be "
          + "passed here in the form of an array", defaultValue = "") final String[] configs) throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkCommand.CLUSTERING_RUN.toString(), master, sparkMemory,
        HoodieCLI.basePath, client.getTableConfig().getTableName(), clusteringInstantTime,
        parallelism, retry, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to run clustering for " + clusteringInstantTime;
    }
    return "Succeeded to run clustering for " + clusteringInstantTime;
  }

  /**
   * Run clustering table service.
   * <p>
   * Example:
   * > connect --path {path to hudi table}
   * > clustering scheduleAndExecute --sparkMaster local --sparkMemory 2g
   */
  @ShellMethod(key = "clustering scheduleAndExecute", value = "Run Clustering. Make a cluster plan first and execute that plan immediately")
  public String runClustering(
      @ShellOption(value = "--sparkMaster", defaultValue = SparkUtil.DEFAULT_SPARK_MASTER, help = "Spark master") final String master,
      @ShellOption(value = "--sparkMemory", help = "Spark executor memory", defaultValue = "4g") final String sparkMemory,
      @ShellOption(value = "--parallelism", help = "Parallelism for hoodie clustering", defaultValue = "1") final String parallelism,
      @ShellOption(value = "--retry", help = "Number of retries", defaultValue = "1") final String retry,
      @ShellOption(value = "--propsFilePath", help = "path to properties file on localfs or dfs with configurations for "
          + "hoodie client for compacting", defaultValue = "") final String propsFilePath,
      @ShellOption(value = "--hoodieConfigs", help = "Any configuration that can be set in the properties file can be "
          + "passed here in the form of an array", defaultValue = "") final String[] configs) throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkCommand.CLUSTERING_SCHEDULE_AND_EXECUTE.toString(), master, sparkMemory,
        HoodieCLI.basePath, client.getTableConfig().getTableName(), parallelism, retry, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to run clustering for scheduleAndExecute.";
    }
    return "Succeeded to run clustering for scheduleAndExecute";
  }
}
