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

import org.apache.maven.plugins.annotations.Parameter;

import java.util.Map;

/**
 * Arguments which are for the {@link DockerComposeBuildMojo}
 */
public class BuildArguments {

  /**
   * Always remove intermediate containers.
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.buildArgs.forceRm")
  protected boolean forceRm;

  /**
   * Do not use cache when building the image.
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.buildArgs.noCache")
  protected boolean noCache;

  /**
   * Always attempt to pull a newer version of the image.
   */
  @Parameter(defaultValue = "false", property = "dockerCompose.buildArgs.alwaysPull")
  protected boolean alwaysPull;

  /**
   * Always attempt to pull a newer version of the image.
   */
  @Parameter(property = "dockerCompose.buildArgs.args")
  protected Map<String, String> args;

}
