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

package org.apache.hudi.integ;

import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.collection.Pair;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.DockerCmdExecFactory;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public abstract class ITTestBase {

  public static final Logger LOG = LogManager.getLogger(ITTestBase.class);
  protected static final String SPARK_WORKER_CONTAINER = "/spark-worker-1";
  protected static final String ADHOC_1_CONTAINER = "/adhoc-1";
  protected static final String ADHOC_2_CONTAINER = "/adhoc-2";
  protected static final String HIVESERVER = "/hiveserver";
  protected static final String PRESTO_COORDINATOR = "/presto-coordinator-1";
  protected static final String HOODIE_WS_ROOT = "/var/hoodie/ws";
  protected static final String HOODIE_JAVA_APP = HOODIE_WS_ROOT + "/hudi-spark/run_hoodie_app.sh";
  protected static final String HUDI_HADOOP_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hadoop-mr-bundle.jar";
  protected static final String HUDI_HIVE_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hive-bundle.jar";
  protected static final String HUDI_SPARK_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";
  protected static final String HUDI_UTILITIES_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-utilities.jar";
  protected static final String HIVE_SERVER_JDBC_URL = "jdbc:hive2://hiveserver:10000";
  protected static final String PRESTO_COORDINATOR_URL = "presto-coordinator-1:8090";
  protected static final String HADOOP_CONF_DIR = "/etc/hadoop";

  // Skip these lines when capturing output from hive
  private static final String DEFAULT_DOCKER_HOST = "unix:///var/run/docker.sock";
  private static final String OVERRIDDEN_DOCKER_HOST = System.getenv("DOCKER_HOST");
  protected DockerClient dockerClient;
  protected Map<String, Container> runningContainers;

  static String[] getHiveConsoleCommand(String rawCommand) {
    String jarCommand = "add jar " + HUDI_HADOOP_BUNDLE + ";";
    String fullCommand = jarCommand + rawCommand;

    List<String> cmd = new ArrayList<>();
    cmd.add("hive");
    cmd.add("--hiveconf");
    cmd.add("hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
    cmd.add("--hiveconf");
    cmd.add("hive.stats.autogather=false");
    cmd.add("-e");
    cmd.add("\"" + fullCommand + "\"");
    return cmd.stream().toArray(String[]::new);
  }

  private static String getHiveConsoleCommandFile(String commandFile, String additionalVar) {
    StringBuilder builder = new StringBuilder().append("beeline -u " + HIVE_SERVER_JDBC_URL)
        .append(" --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat ")
        .append(" --hiveconf hive.stats.autogather=false ")
        .append(" --hivevar hudi.hadoop.bundle=" + HUDI_HADOOP_BUNDLE);

    if (additionalVar != null) {
      builder.append(" --hivevar " + additionalVar + " ");
    }
    return builder.append(" -f ").append(commandFile).toString();
  }

  static String getSparkShellCommand(String commandFile) {
    return new StringBuilder().append("spark-shell --jars ").append(HUDI_SPARK_BUNDLE)
        .append(" --master local[2] --driver-class-path ").append(HADOOP_CONF_DIR)
        .append(
            " --conf spark.sql.hive.convertMetastoreParquet=false --deploy-mode client  --driver-memory 1G --executor-memory 1G --num-executors 1 ")
        .append(" --packages com.databricks:spark-avro_2.11:4.0.0 ").append(" -i ").append(commandFile).toString();
  }

  static String getPrestoConsoleCommand(String commandFile) {
    StringBuilder builder = new StringBuilder().append("presto --server " + PRESTO_COORDINATOR_URL)
        .append(" --catalog hive --schema default")
        .append(" -f " + commandFile );
    System.out.println("Presto comamnd " + builder.toString());
    return builder.toString();
  }

  @Before
  public void init() {
    String dockerHost = (OVERRIDDEN_DOCKER_HOST != null) ? OVERRIDDEN_DOCKER_HOST : DEFAULT_DOCKER_HOST;
    // Assuming insecure docker engine
    DockerClientConfig config =
        DefaultDockerClientConfig.createDefaultConfigBuilder().withDockerHost(dockerHost).build();
    // using jaxrs/jersey implementation here (netty impl is also available)
    DockerCmdExecFactory dockerCmdExecFactory = new JerseyDockerCmdExecFactory().withConnectTimeout(1000)
        .withMaxTotalConnections(100).withMaxPerRouteConnections(10);
    dockerClient = DockerClientBuilder.getInstance(config).withDockerCmdExecFactory(dockerCmdExecFactory).build();
    await().atMost(60, SECONDS).until(this::servicesUp);
  }

  private boolean servicesUp() {
    List<Container> containerList = dockerClient.listContainersCmd().exec();
    for (Container c : containerList) {
      if (!c.getState().equalsIgnoreCase("running")) {
        LOG.info("Container : " + Arrays.toString(c.getNames()) + "not in running state,  Curr State :" + c.getState());
        return false;
      }
    }
    runningContainers = containerList.stream().map(c -> Pair.of(c.getNames()[0], c))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    return true;
  }

  private String singleSpace(String str) {
    return str.replaceAll("[\\s]+", " ");
  }

  private TestExecStartResultCallback executeCommandInDocker(String containerName, String[] command,
      boolean expectedToSucceed) throws Exception {
    Container sparkWorkerContainer = runningContainers.get(containerName);
    ExecCreateCmd cmd = dockerClient.execCreateCmd(sparkWorkerContainer.getId()).withCmd(command).withAttachStdout(true)
        .withAttachStderr(true);

    ExecCreateCmdResponse createCmdResponse = cmd.exec();
    TestExecStartResultCallback callback =
        new TestExecStartResultCallback(new ByteArrayOutputStream(), new ByteArrayOutputStream());
    dockerClient.execStartCmd(createCmdResponse.getId()).withDetach(false).withTty(false).exec(callback)
        .awaitCompletion();
    int exitCode = dockerClient.inspectExecCmd(createCmdResponse.getId()).exec().getExitCode();
    LOG.info("Exit code for command : " + exitCode);
    LOG.error("\n\n ###### Stdout #######\n" + callback.getStdout().toString());
    LOG.error("\n\n ###### Stderr #######\n" + callback.getStderr().toString());

    if (expectedToSucceed) {
      Assert.assertTrue("Command (" + Arrays.toString(command) + ") expected to succeed. Exit (" + exitCode + ")",
          exitCode == 0);
    } else {
      Assert.assertTrue("Command (" + Arrays.toString(command) + ") expected to fail. Exit (" + exitCode + ")",
          exitCode != 0);
    }
    cmd.close();
    return callback;
  }

  void executeCommandStringsInDocker(String containerName, List<String> commands) throws Exception {
    for (String cmd : commands) {
      executeCommandStringInDocker(containerName, cmd, true);
    }
  }

  TestExecStartResultCallback executeCommandStringInDocker(String containerName, String cmd, boolean expectedToSucceed)
      throws Exception {
    LOG.info("\n\n#################################################################################################");
    LOG.info("Container : " + containerName + ", Running command :" + cmd);
    LOG.info("\n#################################################################################################");

    String[] cmdSplits = singleSpace(cmd).split(" ");
    return executeCommandInDocker(containerName, cmdSplits, expectedToSucceed);
  }

  Pair<String, String> executeHiveCommand(String hiveCommand) throws Exception {

    LOG.info("\n\n#################################################################################################");
    LOG.info("Running hive command :" + hiveCommand);
    LOG.info("\n#################################################################################################");

    String[] hiveCmd = getHiveConsoleCommand(hiveCommand);
    TestExecStartResultCallback callback = executeCommandInDocker(HIVESERVER, hiveCmd, true);
    return Pair.of(callback.getStdout().toString().trim(), callback.getStderr().toString().trim());
  }

  Pair<String, String> executeHiveCommandFile(String commandFile) throws Exception {
    return executeHiveCommandFile(commandFile, null);
  }

  Pair<String, String> executeHiveCommandFile(String commandFile, String additionalVar) throws Exception {
    String hiveCmd = getHiveConsoleCommandFile(commandFile, additionalVar);
    TestExecStartResultCallback callback = executeCommandStringInDocker(HIVESERVER, hiveCmd, true);
    return Pair.of(callback.getStdout().toString().trim(), callback.getStderr().toString().trim());
  }

  Pair<String, String> executeSparkSQLCommand(String commandFile, boolean expectedToSucceed) throws Exception {
    String sparkShellCmd = getSparkShellCommand(commandFile);
    TestExecStartResultCallback callback =
        executeCommandStringInDocker(ADHOC_1_CONTAINER, sparkShellCmd, expectedToSucceed);
    return Pair.of(callback.getStdout().toString(), callback.getStderr().toString());
  }

  Pair<String, String> executePrestoCommandFile(String commandFile) throws Exception {
    String prestoCmd = getPrestoConsoleCommand(commandFile);
    TestExecStartResultCallback callback = executeCommandStringInDocker(PRESTO_COORDINATOR, prestoCmd, true);
    return Pair.of(callback.getStdout().toString().trim(), callback.getStderr().toString().trim());
  }

  void executePrestoCopyCommand(String fromFile, String remotePath){
    Container sparkWorkerContainer = runningContainers.get(PRESTO_COORDINATOR);
    dockerClient.copyArchiveToContainerCmd(sparkWorkerContainer.getId())
        .withHostResource(fromFile)
        .withRemotePath(remotePath)
        .exec();
  }

  private void saveUpLogs() {
    try {
      // save up the Hive log files for introspection
      String hiveLogStr =
          executeCommandStringInDocker(HIVESERVER, "cat /tmp/root/hive.log", true).getStdout().toString();
      String filePath = System.getProperty("java.io.tmpdir") + "/" + System.currentTimeMillis() + "-hive.log";
      FileIOUtils.writeStringToFile(hiveLogStr, filePath);
      LOG.info("Hive log saved up at  : " + filePath);
    } catch (Exception e) {
      LOG.error("Unable to save up logs..", e);
    }
  }

  void assertStdOutContains(Pair<String, String> stdOutErr, String expectedOutput) {
    assertStdOutContains(stdOutErr, expectedOutput, 1);
  }

  void assertStdOutContains(Pair<String, String> stdOutErr, String expectedOutput, int times) {
    // this is so that changes in padding don't affect comparison
    String stdOutSingleSpaced = singleSpace(stdOutErr.getLeft()).replaceAll(" ", "");
    expectedOutput = singleSpace(expectedOutput).replaceAll(" ", "");

    int lastIndex = 0;
    int count = 0;
    while (lastIndex != -1) {
      lastIndex = stdOutSingleSpaced.indexOf(expectedOutput, lastIndex);
      if (lastIndex != -1) {
        count++;
        lastIndex += expectedOutput.length();
      }
    }

    if (times != count) {
      saveUpLogs();
    }

    Assert.assertEquals("Did not find output the expected number of times", times, count);
  }

  public class TestExecStartResultCallback extends ExecStartResultCallback {

    // Storing the reference in subclass to expose to clients
    private final ByteArrayOutputStream stdout;
    private final ByteArrayOutputStream stderr;

    public TestExecStartResultCallback(ByteArrayOutputStream stdout, ByteArrayOutputStream stderr) {
      super(stdout, stderr);
      this.stdout = stdout;
      this.stderr = stderr;
    }

    @Override
    public void onComplete() {
      super.onComplete();
      LOG.info("onComplete called");
      try {
        stderr.flush();
        stdout.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public ByteArrayOutputStream getStdout() {
      return stdout;
    }

    public ByteArrayOutputStream getStderr() {
      return stderr;
    }
  }
}
