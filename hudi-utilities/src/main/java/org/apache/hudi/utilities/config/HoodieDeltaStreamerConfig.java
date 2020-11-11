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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.utilities.sources.selector.DFSPathSelector;

import javax.annotation.concurrent.Immutable;
import java.util.Properties;

@Immutable
public class HoodieDeltaStreamerConfig extends DefaultHoodieConfig {

  private static final long serialVersionUID = 0L;

  public static final String ROOT_INPUT_PATH_PROP = "hoodie.deltastreamer.source.dfs.root";
  public static final String SOURCE_INPUT_SELECTOR = "hoodie.deltastreamer.source.input.selector";
  public static final String DEFAULT_SOURCE_INPUT_SELECTOR = DFSPathSelector.class.getName();
  public static final String SOURCE_FULL_OVERWRITE = "hoodie.deltastreamer.source.dfs.full.overwrite";
  public static final String DEFAULT_SOURCE_FULL_OVERWRITE = "false";

  public HoodieDeltaStreamerConfig(Properties props) {
    super(props);
  }

  public String getInputSelector() {
    return props.getOrDefault(SOURCE_INPUT_SELECTOR, DEFAULT_SOURCE_INPUT_SELECTOR).toString();
  }

  public Boolean getFullOverwrite() {
    return Boolean.valueOf(props.getOrDefault(SOURCE_FULL_OVERWRITE, DEFAULT_SOURCE_FULL_OVERWRITE).toString());
  }
}
