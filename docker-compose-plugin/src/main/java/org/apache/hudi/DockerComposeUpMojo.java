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

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * docker-compose up
 */
@SuppressWarnings("unused")
@Mojo(name = "up", threadSafe = true)
public class DockerComposeUpMojo extends AbstractDockerComposeMojo {

  public void execute() throws MojoExecutionException {

    if (skip) {
      getLog().info("Skipping execution");
      return;
    }

    List<String> args = new ArrayList<>();
    args.add(Command.UP.getValue());

    if (detachedMode) {
      getLog().info("Running in detached mode");
      args.add("-d");
    }

    if (build) {
      getLog().info("Building images");
      args.add("--build");
    }

    args.add("--no-color");

    if (services != null && !services.isEmpty())
      args.addAll(services);

    super.execute(args);

    if (awaitCmd != null) {
      await(buildCmd(awaitCmd, awaitCmdArgs));
    }
  }

  private List<String> buildCmd(String cmd, String args) {
    String[] cmdArgs = null;
    if (args != null) {
      cmdArgs = args.split(",");
    }
    List<String> answer = new ArrayList<>();
    answer.add(cmd);
    if (cmdArgs != null) {
      answer.addAll(Arrays.asList(cmdArgs));
    }
    return answer;
  }

  private void await(List<String> awaitCmd) {
    Integer exitCode = null;
    ProcessBuilder pb = new ProcessBuilder(awaitCmd).inheritIO();
    long start = System.currentTimeMillis();
    try {
      boolean stillWaiting;
      do {
        Process process = null;
        if (exitCode != null) {
          Thread.sleep(1000);
        }
        exitCode = pb.start().waitFor();
        stillWaiting = (System.currentTimeMillis() - start) < (awaitTimeout * 1000);
      } while ((exitCode != 0) && stillWaiting);

      if (exitCode != 0) {
        throw new RuntimeException("await failed after " + awaitTimeout + " seconds");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
