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

package com.uber.hoodie.integ;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

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
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;

public abstract class ITTestBase {

  public static final Logger LOG = LogManager.getLogger(ITTestBase.class);
  protected static final String SPARK_WORKER_CONTAINER = "/spark-worker-1";
  protected static final String ADHOC_1_CONTAINER = "/adhoc-1";
  protected static final String ADHOC_2_CONTAINER = "/adhoc-2";
  protected static final String HIVESERVER = "/hiveserver";
  protected static final String HOODIE_WS_ROOT = "/var/hoodie/ws";
  protected static final String HOODIE_JAVA_APP = HOODIE_WS_ROOT + "/hoodie-spark/run_hoodie_app.sh";
  protected static final String HUDI_HADOOP_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hadoop-mr-bundle.jar";
  protected static final String HUDI_HIVE_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hive-bundle.jar";
  protected static final String HUDI_SPARK_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";
  protected static final String HIVE_SERVER_JDBC_URL = "jdbc:hive2://hiveserver:10000";
  // Skip these lines when capturing output from hive
  protected static final Integer SLF4J_WARNING_LINE_COUNT_IN_HIVE_CMD = 9;
  private static final String DEFAULT_DOCKER_HOST = "unix:///var/run/docker.sock";
  private static final String OVERRIDDEN_DOCKER_HOST = System.getenv("DOCKER_HOST");
  protected DockerClient dockerClient;
  protected Map<String, Container> runningContainers;

  protected static String[] getHiveConsoleCommand(String rawCommand) {
    String jarCommand = "add jar " + HUDI_HADOOP_BUNDLE + ";";
    String fullCommand = jarCommand + rawCommand;

    List<String> cmd = new ImmutableList.Builder().add("hive")
        .add("--hiveconf")
        .add("hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat")
        .add("--hiveconf")
        .add("hive.stats.autogather=false")
        .add("-e")
        .add("\"" + fullCommand + "\"")
        .build();
    return cmd.stream().toArray(String[]::new);
  }

  @Before
  public void init() throws IOException {
    String dockerHost = (OVERRIDDEN_DOCKER_HOST != null) ? OVERRIDDEN_DOCKER_HOST : DEFAULT_DOCKER_HOST;
    //Assuming insecure docker engine
    DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
        .withDockerHost(dockerHost)
        .build();
    // using jaxrs/jersey implementation here (netty impl is also available)
    DockerCmdExecFactory dockerCmdExecFactory = new JerseyDockerCmdExecFactory()
        .withConnectTimeout(1000)
        .withMaxTotalConnections(100)
        .withMaxPerRouteConnections(10);
    dockerClient = DockerClientBuilder.getInstance(config)
        .withDockerCmdExecFactory(dockerCmdExecFactory)
        .build();
    await().atMost(60, SECONDS).until(this::servicesUp);
  }

  private boolean servicesUp() {
    List<Container> containerList = dockerClient.listContainersCmd().exec();
    for (Container c : containerList) {
      if (!c.getState().equalsIgnoreCase("running")) {
        System.out.println("Container : " + Arrays.toString(c.getNames())
            + "not in running state,  Curr State :" + c.getState());
        return false;
      }
    }
    runningContainers = containerList.stream().map(c -> Pair.of(c.getNames()[0], c))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    return true;
  }

  protected TestExecStartResultCallback executeCommandInDocker(String containerName, String[] command,
      boolean expectedToSucceed)
      throws Exception {
    LOG.info("Executing command (" + Arrays.toString(command) + ") in container " + containerName);
    Container sparkWorkerContainer = runningContainers.get(containerName);
    ExecCreateCmd cmd = dockerClient.execCreateCmd(sparkWorkerContainer.getId())
        .withCmd(command).withAttachStdout(true).withAttachStderr(true);

    ExecCreateCmdResponse createCmdResponse = cmd.exec();
    TestExecStartResultCallback callback = new TestExecStartResultCallback(new ByteArrayOutputStream(),
        new ByteArrayOutputStream());
    dockerClient.execStartCmd(createCmdResponse.getId()).withDetach(false).withTty(false)
        .exec(callback).awaitCompletion();
    int exitCode = dockerClient.inspectExecCmd(createCmdResponse.getId()).exec().getExitCode();
    LOG.info("Exit code for command (" + Arrays.toString(command) + ") is " + exitCode);
    if (exitCode != 0) {
      LOG.error("Command (" + Arrays.toString(command) + ") failed.");
      LOG.error("Stdout is :" + callback.getStdout().toString());
      LOG.error("Stderr is :" + callback.getStderr().toString());
    }
    if (expectedToSucceed) {
      Assert.assertTrue("Command (" + Arrays.toString(command)
          + ") expected to succeed. Exit (" + exitCode + ")", exitCode == 0);
    } else {
      Assert.assertTrue("Command (" + Arrays.toString(command)
          + ") expected to fail. Exit (" + exitCode + ")", exitCode != 0);
    }
    cmd.close();
    return callback;
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
