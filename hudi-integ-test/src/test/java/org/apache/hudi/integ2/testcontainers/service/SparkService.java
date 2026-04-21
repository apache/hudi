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

package org.apache.hudi.integ2.testcontainers.service;

import org.apache.hudi.integ2.testcontainers.ContainerProvider;
import org.apache.hudi.integ2.testcontainers.command.CommandExecutor;
import org.apache.hudi.integ2.testcontainers.command.CommandResult;

/**
 * A service wrapper for the Spark container.
 * This class is responsible for all interactions with the Spark service,
 * including executing spark-shell commands.
 */
public class SparkService {

  // NOTE: Testcontainers renames and adds suffix to defined container names
  // Container name
  public static final String ADHOC_1 = "adhoc-1-1";
  public static final String ADHOC_2 = "adhoc-2-1";

  private static final String HADOOP_CONF_DIR = "/etc/hadoop";
  private static final String HUDI_SPARK_BUNDLE =
      "/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";

  private final CommandExecutor executor;

  public SparkService(ContainerProvider provider, String containerName) {
    this.executor = new CommandExecutor(provider.getContainer(containerName));
  }

  /**
   * Execute a Spark SQL command file.
   */
  public CommandResult executeSQLFile(String commandFile) throws Exception {
    String sparkShellCmd = new StringBuilder()
        .append("spark-shell --jars ").append(HUDI_SPARK_BUNDLE)
        .append(" --master local[2] --driver-class-path ").append(HADOOP_CONF_DIR)
        .append(" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer")
        .append(" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .append(" --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .append(" --deploy-mode client --driver-memory 1G --executor-memory 1G --num-executors 1")
        // Pipe via stdin instead of `-i`. With `-i`, Spark 4's REPL boots in
        // dumb-terminal mode (`WARN jline: Unable to create a system terminal,
        // creating a dumb terminal`) and silently runs the script — neither
        // input echo nor exception traces reach stdout, so success markers and
        // failures alike are invisible to assertStdOutContains.
        .append(" < ").append(commandFile)
        .toString();

    return executor.executeCommandString(sparkShellCmd);
  }

  /**
   * Copy a file from the host to the Spark container.
   */
  public void copyFile(String fromFile, String remotePath) {
    executor.copyFileToContainer(fromFile, remotePath);
  }

  /**
   * Executes an arbitrary shell command inside the service's container.
   * This is useful for hdfs commands, hudi-cli, etc.
   *
   * @param command The shell command to execute.
   * @return The result of the command execution.
   */
  public CommandResult executeShellCommand(String command) throws Exception {
    return executor.executeCommandString(command);
  }
}
