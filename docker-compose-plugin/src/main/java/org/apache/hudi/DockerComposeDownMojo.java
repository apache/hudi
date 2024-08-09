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

@Mojo(name = "down", threadSafe = true)
public class DockerComposeDownMojo extends AbstractDockerComposeMojo {

  public void execute() throws MojoExecutionException {

    if (skip) {
      getLog().info("Skipping execution");
      return;
    }

    List<String> args = new ArrayList<>();

    args.add(Command.DOWN.getValue());

    if (removeVolumes) {
      getLog().info("Removing volumes");
      args.add("-v");
    }

    if (removeImages) {
      getLog().info("Removing images");
      args.add("--rmi");
      args.add(removeImagesType);
    }

    if (removeOrphans) {
      getLog().info("Removing orphans");
      args.add("--remove-orphans");
    }

    super.execute(args);
  }
}
