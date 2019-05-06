/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.configs;

import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.List;

public class HoodieCleanerJobConfig extends AbstractJobConfig {

  @Parameter(names = {"--target-base-path"}, description = "base path for the hoodie dataset to be cleaner.",
      required = true)
  public String basePath;

  @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
      + "hoodie client for cleaning")
  public String propsFilePath =
      "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

  @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
      + "(using the CLI parameter \"--propsFilePath\") can also be passed command line using this parameter")
  public List<String> configs = new ArrayList<>();

  @Parameter(names = {"--spark-master"}, description = "spark master to use.")
  public String sparkMaster = "local[2]";
}
