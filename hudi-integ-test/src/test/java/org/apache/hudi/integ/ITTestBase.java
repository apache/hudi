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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.util.FileIOUtils;

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
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Base test class for IT Test helps to run command and generate data.
 */
public abstract class ITTestBase {

  public static final Logger LOG = LoggerFactory.getLogger(ITTestBase.class);
  protected static final String SPARK_WORKER_CONTAINER = "/spark-worker-1";
  protected static final String ADHOC_1_CONTAINER = "/adhoc-1";
  protected static final String ADHOC_2_CONTAINER = "/adhoc-2";
  protected static final String HIVESERVER = "/hiveserver";
  protected static final String PRESTO_COORDINATOR = "/presto-coordinator-1";
  protected static final String TRINO_COORDINATOR = "/trino-coordinator-1";
  protected static final String HOODIE_WS_ROOT = "/var/hoodie/ws";
  protected static final String HOODIE_JAVA_APP = HOODIE_WS_ROOT + "/hudi-spark-datasource/hudi-spark/run_hoodie_app.sh";
  protected static final String HOODIE_GENERATE_APP = HOODIE_WS_ROOT + "/hudi-spark-datasource/hudi-spark/run_hoodie_generate_app.sh";
  protected static final String HOODIE_JAVA_STREAMING_APP = HOODIE_WS_ROOT + "/hudi-spark-datasource/hudi-spark/run_hoodie_streaming_app.sh";
  protected static final String HUDI_HADOOP_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hadoop-mr-bundle.jar";
  protected static final String HUDI_HIVE_SYNC_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hive-sync-bundle.jar";
  protected static final String HUDI_SPARK_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";
  protected static final String HUDI_UTILITIES_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-utilities.jar";
  protected static final String HIVE_SERVER_JDBC_URL = "jdbc:hive2://hiveserver:10000";
  protected static final String PRESTO_COORDINATOR_URL = "presto-coordinator-1:8090";
  protected static final String TRINO_COORDINATOR_URL = "trino-coordinator-1:8091";
  protected static final String HADOOP_CONF_DIR = "/etc/hadoop";

  // Skip these lines when capturing output from hive
  private static final String DEFAULT_DOCKER_HOST = "unix:///var/run/docker.sock";
  private static final String OVERRIDDEN_DOCKER_HOST = System.getenv("DOCKER_HOST");
  protected DockerClient dockerClient;
  protected Map<String, Container> runningContainers;

  static String[] getHiveConsoleCommand(String hiveExpr) {
    List<String> cmd = new ArrayList<>();
    cmd.add("hive");
    cmd.add("--hiveconf");
    cmd.add("hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
    cmd.add("--hiveconf");
    cmd.add("hive.stats.autogather=false");
    cmd.add("-e");
    cmd.add("\"" + hiveExpr + "\"");
    return cmd.toArray(new String[0]);
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
        .append(" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer")
        .append(" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .append(" --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .append(" --deploy-mode client  --driver-memory 1G --executor-memory 1G --num-executors 1")
        .append(" -i ").append(commandFile).toString();
  }

  static String getPrestoConsoleCommand(String commandFile) {
    return new StringBuilder().append("presto --server " + PRESTO_COORDINATOR_URL)
        .append(" --catalog hive --schema default")
        .append(" -f " + commandFile).toString();
  }

  static String getTrinoConsoleCommand(String commandFile) {
    return new StringBuilder().append("trino --server " + TRINO_COORDINATOR_URL)
        .append(" --catalog hive --schema default")
        .append(" -f " + commandFile).toString();
  }

  @BeforeEach
  public void init() {
    String dockerHost = (OVERRIDDEN_DOCKER_HOST != null) ? OVERRIDDEN_DOCKER_HOST : DEFAULT_DOCKER_HOST;
    // Assuming insecure docker engine
    DockerClientConfig config =
        DefaultDockerClientConfig.createDefaultConfigBuilder().withDockerHost(dockerHost).build();
    // using jaxrs/jersey implementation here (netty impl is also available)
    DockerCmdExecFactory dockerCmdExecFactory = new JerseyDockerCmdExecFactory().withConnectTimeout(10000)
        .withMaxTotalConnections(100).withMaxPerRouteConnections(50);
    dockerClient = DockerClientBuilder.getInstance(config).withDockerCmdExecFactory(dockerCmdExecFactory).build();
    LOG.info("Start waiting for all the containers and services to be ready");
    long currTs = System.currentTimeMillis();
    await().atMost(300, SECONDS).until(this::servicesUp);
    LOG.info(String.format("Waiting for all the containers and services finishes in %d ms",
        System.currentTimeMillis() - currTs));
  }

  private boolean servicesUp() {
    List<Container> containerList = dockerClient.listContainersCmd().exec();
    for (Container c : containerList) {
      if (!c.getState().equalsIgnoreCase("running")) {
        LOG.info("Container : " + Arrays.toString(c.getNames()) + "not in running state,  Curr State :" + c.getState());
        return false;
      }
    }

    if (runningContainers == null) {
      runningContainers = containerList.stream().map(c -> Pair.of(c.getNames()[0], c))
          .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    return checkHealth(ADHOC_1_CONTAINER, "namenode", 8020);
  }

  private boolean checkHealth(String fromContainerName, String hostname, int port) {
    try {
      String command = String.format("nc -z -v %s %d", hostname, port);
      TestExecStartResultCallback resultCallback =
          executeCommandStringInDocker(fromContainerName, command, false, true);
      String stderrString = resultCallback.getStderr().toString().trim();
      if (!stderrString.contains("succeeded")) {
        Thread.sleep(1000);
        return false;
      }
    } catch (Exception e) {
      throw new HoodieException(String.format("Exception thrown while checking health from %s for %s:%d",
          fromContainerName, hostname, port), e);
    }
    return true;
  }

  private String singleSpace(String str) {
    return str.replaceAll("[\\s]+", " ");
  }

  private TestExecStartResultCallback executeCommandInDocker(
      String containerName, String[] command, boolean expectedToSucceed) throws Exception {
    return executeCommandInDocker(containerName, command, true, expectedToSucceed, Collections.emptyMap());
  }

  private TestExecStartResultCallback executeCommandInDocker(String containerName,
                                                             String[] command,
                                                             boolean checkIfSucceed,
                                                             boolean expectedToSucceed) throws Exception {
    return executeCommandInDocker(containerName, command, checkIfSucceed, expectedToSucceed, Collections.emptyMap());
  }

  private TestExecStartResultCallback executeCommandInDocker(String containerName,
                                                             String[] command,
                                                             boolean checkIfSucceed,
                                                             boolean expectedToSucceed,
                                                             Map<String, String> env) throws Exception {
    Container targetContainer = runningContainers.get(containerName);

    List<String> dockerEnv = env.entrySet()
        .stream()
        .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
        .collect(Collectors.toList());

    ExecCreateCmd cmd = dockerClient.execCreateCmd(targetContainer.getId())
        .withEnv(dockerEnv)
        .withCmd(command)
        .withAttachStdout(true)
        .withAttachStderr(true);

    ExecCreateCmdResponse createCmdResponse = cmd.exec();
    TestExecStartResultCallback callback =
        new TestExecStartResultCallback(new ByteArrayOutputStream(), new ByteArrayOutputStream());
    // Each execution of command(s) in docker should not be more than 15 mins. Otherwise, it is deemed stuck. We will
    // try to capture stdout and stderr of the stuck process.

    boolean completed =
        dockerClient.execStartCmd(createCmdResponse.getId()).withDetach(false).withTty(false).exec(callback)
            .awaitCompletion(540, SECONDS);
    if (!completed) {
      callback.getStderr().flush();
      callback.getStdout().flush();
      LOG.error("\n\n ###### Timed Out Command : " + Arrays.asList(command));
      LOG.error("\n\n ###### Stderr of timed-out command #######\n" + callback.getStderr().toString());
      LOG.error("\n\n ###### stdout of timed-out command #######\n" + callback.getStdout().toString());
      throw new TimeoutException("Command " + command + " has been running for more than 9 minutes. "
          + "Killing and failing !!");
    }
    int exitCode = dockerClient.inspectExecCmd(createCmdResponse.getId()).exec().getExitCode();
    LOG.info("Exit code for command : " + exitCode);

    if (exitCode != 0) {
      LOG.error("\n\n ###### Stdout #######\n" + callback.getStdout().toString());
    }
    LOG.error("\n\n ###### Stderr #######\n" + callback.getStderr().toString());

    if (checkIfSucceed) {
      if (expectedToSucceed) {
        assertEquals(0, exitCode, "Command (" + Arrays.toString(command) + ") expected to succeed. Exit (" + exitCode + ")");
      } else {
        assertNotEquals(0, exitCode, "Command (" + Arrays.toString(command) + ") expected to fail. Exit (" + exitCode + ")");
      }
    }
    cmd.close();
    return callback;
  }

  void executeCommandStringsInDocker(String containerName, List<String> commands) throws Exception {
    for (String cmd : commands) {
      executeCommandStringInDocker(containerName, cmd, true);
    }
  }

  protected TestExecStartResultCallback executeCommandStringInDocker(
      String containerName, String cmd, boolean expectedToSucceed) throws Exception {
    return executeCommandStringInDocker(containerName, cmd, true, expectedToSucceed);
  }

  protected TestExecStartResultCallback executeCommandStringInDocker(
      String containerName, String cmd, boolean checkIfSucceed, boolean expectedToSucceed)
      throws Exception {
    LOG.info("\n\n#################################################################################################");
    LOG.info("Container : " + containerName + ", Running command :" + cmd);
    LOG.info("\n#################################################################################################");

    String[] cmdSplits = singleSpace(cmd).split(" ");
    return executeCommandInDocker(containerName, cmdSplits, checkIfSucceed, expectedToSucceed);
  }

  protected Pair<String, String> executeHiveCommand(String hiveCommand) throws Exception {

    LOG.info("\n\n#################################################################################################");
    LOG.info("Running hive command :" + hiveCommand);
    LOG.info("\n#################################################################################################");

    String[] hiveCmd = getHiveConsoleCommand(hiveCommand);
    Map<String, String> env = Collections.singletonMap("AUX_CLASSPATH", "file://" + HUDI_HADOOP_BUNDLE);
    TestExecStartResultCallback callback =
        executeCommandInDocker(HIVESERVER, hiveCmd, true, true, env);
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

  void executePrestoCopyCommand(String fromFile, String remotePath) {
    Container sparkWorkerContainer = runningContainers.get(PRESTO_COORDINATOR);
    dockerClient.copyArchiveToContainerCmd(sparkWorkerContainer.getId())
        .withHostResource(fromFile)
        .withRemotePath(remotePath)
        .exec();
  }

  Pair<String, String> executeTrinoCommandFile(String commandFile) throws Exception {
    String trinoCmd = getTrinoConsoleCommand(commandFile);
    TestExecStartResultCallback callback = executeCommandStringInDocker(ADHOC_1_CONTAINER, trinoCmd, true);
    return Pair.of(callback.getStdout().toString().trim(), callback.getStderr().toString().trim());
  }

  void executeTrinoCopyCommand(String fromFile, String remotePath) {
    Container adhocContainer = runningContainers.get(ADHOC_1_CONTAINER);
    dockerClient.copyArchiveToContainerCmd(adhocContainer.getId())
        .withHostResource(fromFile)
        .withRemotePath(remotePath)
        .exec();
  }

  private void saveUpLogs() {
    try {
      // save up the Hive log files for introspection
      String hiveLogStr =
          executeCommandStringInDocker(HIVESERVER, "cat /tmp/root/hive.log |  grep -i exception -A 10 -B 5", false).getStdout().toString();
      String filePath = System.getProperty("java.io.tmpdir") + "/" + System.currentTimeMillis() + "-hive.log";
      FileIOUtils.writeStringToFile(hiveLogStr, filePath);
      LOG.info("Hive log saved up at  : " + filePath);
      LOG.info("<===========  Full hive log ===============>\n"
          + "\n" + hiveLogStr
          + "\n <==========================================>");
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
      // TODO(HUDI-8268): fix the command with pipe
      // saveUpLogs();
    }

    assertEquals(times, count, "Did not find output the expected number of times.");
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
