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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

/**
 * DFS Path Selector Configs
 */
@Immutable
@ConfigClassProperty(name = "DFS Path Selector Configs",
    groupName = ConfigGroups.Names.DELTA_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of path selector for DFS source in Deltastreamer.")
public class DFSPathSelectorConfig extends HoodieConfig {

  public static final ConfigProperty<String> SOURCE_INPUT_SELECTOR = ConfigProperty
      .key("hoodie.deltastreamer.source.input.selector")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Source input selector");

  public static final ConfigProperty<String> ROOT_INPUT_PATH = ConfigProperty
      .key("hoodie.deltastreamer.source.dfs.root")
      .noDefaultValue()
      .withDocumentation("Root path of the source on DFS");
}
