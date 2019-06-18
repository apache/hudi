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

package com.uber.hoodie.utilities.config;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.uber.hoodie.utilities.exception.InvalidCommandConfigException;
import java.io.Serializable;
import java.lang.reflect.Field;

public abstract class AbstractCommandConfig implements Serializable {

  private static final String SPACE = " ";
  private String commandConfigs;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public String[] getCommandConfigsAsStringArray(String prefix) {
    StringBuilder result = new StringBuilder();
    Field[] fields = this.getClass().getDeclaredFields();

    if (prefix != null) {
      result.append(prefix);
      result.append(SPACE);
    }

    for (Field field : fields) {
      try {
        if (field.isSynthetic()) {
          continue;
        }
        Object value = field.get(this);
        Parameter param = field.getAnnotation(com.beust.jcommander.Parameter.class);
        if (value != null && param != null) {
          result.append(param.names()[0]);
          result.append(SPACE);
          result.append(value);
          result.append(SPACE);
        }
      } catch (Throwable e) {
        throw new InvalidCommandConfigException("Failed to convert job configs to string array.", e);
      }
    }
    return result.toString().trim().split(SPACE);
  }

  @Override
  public String toString() {
    return commandConfigs;
  }

  public boolean parseCommandConfig(String[] args) {
    return parseCommandConfig(args, false);
  }

  public boolean parseCommandConfig(String[] args, boolean showHelpIfArgsLengthZero) {
    return parseCommandConfig(args, showHelpIfArgsLengthZero, false);
  }

  public boolean parseCommandConfig(String[] args, boolean showHelpIfArgsLengthZero, boolean exitJobOnHelp) {
    JCommander cmd = JCommander.newBuilder().addObject(this).build();
    return parse(cmd, args, showHelpIfArgsLengthZero, exitJobOnHelp);
  }

  private boolean parse(JCommander cmd, String[] args, boolean showHelpIfArgsLengthZero, boolean exitJob) {
    boolean output = false;
    try {
      cmd.parse(args);

      if (this.help || (showHelpIfArgsLengthZero && args.length == 0)) {
        printUsage(cmd, this);
        if (exitJob) {
          System.exit(1);
        }
      } else {
        output = true;
      }
    } catch (Throwable e) {
      cmd.usage();
      throw new InvalidCommandConfigException("Failed to parse command configs: " + e.getMessage());
    }
    buildCommandConfigs(cmd);
    return output;
  }

  private void buildCommandConfigs(JCommander cmd) {
    StringBuilder commandConfigsBuilder = new StringBuilder();
    for (ParameterDescription param : cmd.getParameters()) {
      Object value = param.getParameterized().get(this);
      if (value != null) {
        commandConfigsBuilder.append(param.getLongestName());
        commandConfigsBuilder.append(SPACE);
        commandConfigsBuilder.append(value.toString());
        commandConfigsBuilder.append(SPACE);
      }
    }
    commandConfigs = commandConfigsBuilder.toString();
  }

  private void printUsage(JCommander cmd, AbstractCommandConfig config) {
    if (config.help) {
      cmd.usage();
    }
  }
}
