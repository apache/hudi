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
import java.util.List;

/**
 * docker-compose build
 */
@SuppressWarnings("unused")
@Mojo(name = "build", threadSafe = true)
public class DockerComposeBuildMojo extends AbstractDockerComposeMojo {

  public void execute() throws MojoExecutionException {

    if (skip) {
      getLog().info("Skipping execution");
      return;
    }

    final List<String> args = new ArrayList<>();

    args.add(Command.BUILD.getValue());


    if (buildArgs.forceRm) {
      getLog().info("Always remove intermediate containers.");
      args.add("--force-rm");
    }


    if (buildArgs.noCache) {
      getLog().info("Do not use cache when building the image.");
      args.add("--no-cache");
    }

    if (buildArgs.alwaysPull) {
      getLog().info("Always attempt to pull a newer version of the image.");
      args.add("--pull");
    }

    if (buildArgs.args != null && !buildArgs.args.isEmpty()) {
      getLog().info("Adding build args");
      buildArgs.args.forEach((key, value) -> {
        args.add("--build-arg");
        args.add(key + "=" + value);
      });
    }

    if (services != null && !services.isEmpty()) {
      args.addAll(services);
    }


    super.execute(args);
  }

}
