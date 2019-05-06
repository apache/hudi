/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.configs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.Serializable;
import java.util.Map;

public abstract class AbstractJobConfig implements Serializable {

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public void parseJobConfig(String[] args) {
    parseJobConfig(args, false);
  }

  public void parseJobConfig(String[] args, boolean showHelpIfArgsLengthZero) {
    JCommander cmd = JCommander.newBuilder().addObject(this).build();
    cmd.parse(args);

    if (this.help || (showHelpIfArgsLengthZero && args.length == 0)) {
      cmd.usage();
      System.exit(1);
    }
  }

  public static AbstractJobConfig parseJobConfig(String[] args, Map<String, AbstractJobConfig> commandConfigMap) {
    JCommander cmd = JCommander.newBuilder().build();
    for (Map.Entry<String, AbstractJobConfig> entry : commandConfigMap.entrySet()) {
      cmd.addCommand(entry.getKey(), entry.getValue());
    }
    cmd.parse(args);

    AbstractJobConfig config = commandConfigMap.get(args[0]);
    if (config.help || args.length == 1) {
      cmd.usage();
      System.exit(1);
    }

    return config;
  }
}
