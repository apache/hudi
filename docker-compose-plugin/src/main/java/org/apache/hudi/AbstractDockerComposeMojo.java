/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.codehaus.plexus.util.IOUtil;
import org.codehaus.plexus.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

abstract class AbstractDockerComposeMojo extends AbstractMojo {

  /**
   * Specify an alternate project name
   */
  @Parameter(property = "dockerCompose.projectName")
  private String projectName;

  /**
   * Docker host to interact with
   */
  @Parameter(property = "dockerCompose.host")
  private String host;

  /**
   * Remove volumes on down
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.removeVolumes")
  boolean removeVolumes;

  /**
   * Remove images on down
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.removeImages")
  boolean removeImages;

  /**
   * 'type' of images to remove on down
   */
  @Parameter(defaultValue = "all", property = "dockerCompose.removeImages.type")
  String removeImagesType;

  /**
   * Run in detached mode
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.detached")
  protected boolean detachedMode;

  /**
   * Build containers before run
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.build")
  protected boolean build;

  /**
   * Arguments for the {@link DockerComposeBuildMojo}
   */
  @Parameter(property = "dockerCompose.buildArgs")
  protected BuildArguments buildArgs;

  /**
   * The location of the Compose file. Value of this property is ignored if {@link #composeFiles} is set and non-empty.
   */
  @Parameter(defaultValue = "${project.basedir}/src/main/resources/docker-compose.yml", property = "dockerCompose.file")
  private String composeFile;

  /**
   * Location of the Compose files. If this property is set and non-empty then {@link #composeFile} is ignored.
   */
  @Parameter(property = "dockerCompose.composeFiles")
  private List<String> composeFiles;

  /**
   * Names of services. If this property is set only the configured services will be controlled.
   */
  @Parameter(property = "dockerCompose.services")
  protected List<String> services;

  /**
   * The Compose Api Version
   */
  @Parameter(property = "dockerCompose.apiVersion")
  private String apiVersion;

  /**
   * Verbose
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.verbose")
  private boolean verbose;

  /**
   * Skip
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.skip")
  boolean skip;

  /**
   * Remove containers for services not defined in the Compose file
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.removeOrphans")
  boolean removeOrphans;

  /**
   * Properties file from which docker environment variables are set
   */
  @Parameter(property = "dockerCompose.envFile")
  private String envFile;

  /**
   * Environment variables defined directly in the POM, overriding envFile
   */
  @Parameter(property = "dockerCompose.envVars")
  private Map<String, String> envVars;

  /**
   * Cmd to run and wait for exit status 0
   */
  @Parameter(property = "dockerCompose.awaitCmd")
  String awaitCmd;

  /**
   * Arguments to awaitCmd (comma separated)
   */
  @Parameter(property = "dockerCompose.awaitCmdArgs")
  String awaitCmdArgs;

  /**
   * Timeout for await (seconds)
   */
  @Parameter(property = "dockerCompose.awaitTimeout", defaultValue = "30")
  int awaitTimeout;

  /**
   * Ignore pull failures
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.ignorePullFailures")
  boolean ignorePullFailures;

  void execute(List<String> args) throws MojoExecutionException {

    ProcessBuilder pb = buildProcess(args);

    getLog().info("Running: " + StringUtils.join(pb.command().iterator(), " "));

    try {
      Process p = pb.start();

      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));

      String line;

      while ((line = br.readLine()) != null)
        getLog().info(line);

      int ec = p.waitFor();

      if (ec != 0)
        throw new DockerComposeException(IOUtil.toString(p.getErrorStream()));

    } catch (Exception e) {
      throw new MojoExecutionException(e.getMessage());
    }
  }

  private ProcessBuilder buildProcess(List<String> args) throws MojoExecutionException {

    List<String> command = buildCmd(args);

    ProcessBuilder pb = new ProcessBuilder(command).inheritIO();

    setEnvironment(pb);

    return pb;
  }

  private List<String> buildCmd(List<String> args) {
    List<String> composeFilePaths = new ArrayList<>();

    if (composeFiles != null && !composeFiles.isEmpty()) {
      composeFiles.stream()
          .map(Paths::get)
          .map(Path::toString)
          .forEachOrdered(composeFilePaths::add);
    } else {
      composeFilePaths.add(Paths.get(this.composeFile).toString());
    }

    getLog().info("Docker Compose Files: " + String.join(", ", composeFilePaths));

    final List<String> cmd = new ArrayList<>();

    cmd.add("docker compose");

    composeFilePaths.forEach(composeFilePath -> {
      cmd.add("-f");
      cmd.add(composeFilePath);
    });

    if (verbose)
      cmd.add("--verbose");

    if (host != null) {
      cmd.add("-H");
      cmd.add(host);
    }

    if (projectName != null) {
      cmd.add("-p");
      cmd.add(projectName);
    }

    cmd.addAll(args);

    return cmd;
  }

  private void setEnvironment(ProcessBuilder processBuilder) throws MojoExecutionException {
    Map<String, String> environment = processBuilder.environment();

    if (apiVersion != null) {
      getLog().info("COMPOSE_API_VERSION: " + apiVersion);
      environment.put("COMPOSE_API_VERSION", apiVersion);
    }

    if (envVars != null && !envVars.isEmpty()) {
      envVars.forEach(environment::put);
    }

    if (envFile != null) {
      final Properties properties = new Properties();

      try {
        properties.load(new FileInputStream(envFile));
        properties.forEach((k, v) -> environment.put(k.toString(), v.toString()));
      } catch (final IOException e) {
        throw new MojoExecutionException(e.getMessage());
      }
    }

    if (null != envVars) {
      envVars.forEach((name, value) -> {
        getLog().info(String.format("%s: %s", name, value));
        environment.put(name, value);
      });
    }
  }

  enum Command {
    UP("up"),
    DOWN("down"),
    STOP("stop"),
    PULL("pull"),
    PUSH("push"),
    BUILD("build"),
    RESTART("restart");

    @SuppressWarnings("unused")
    private String value;

    Command(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }
}
