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
import com.uber.hoodie.exception.InvalidJobConfigException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;

public abstract class AbstractJobConfig implements Serializable {

  private static final String SPACE = " ";

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public String[] getJobConfigsAsCommandOption(String prefix) {
    return getJobConfigsAsCommandOption(prefix, null);
  }

  public String[] getJobConfigsAsCommandOption(String prefix, String suffix) {
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
        if (value != null) {
          result.append(field.getAnnotation(com.beust.jcommander.Parameter.class).names()[0]);
          result.append(SPACE);
          result.append(value);
          result.append(SPACE);
        }
      } catch (Throwable e) {
        throw new InvalidJobConfigException("Failed to convert job config to command string ", e);
      }
    }

    if (suffix != null) {
      result.append(suffix);
    }

    return result.toString().trim().split(SPACE);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    Field[] fields = this.getClass().getDeclaredFields();

    result.append(this.getClass().getSimpleName());
    result.append('{');
    for (Field field : fields) {
      if (field.isSynthetic()) {
        continue;
      }
      try {
        Object value = field.get(this);
        result.append(field.getName());
        result.append("='");
        result.append(value);
        result.append("', ");
      }  catch (Throwable e) {
        throw new InvalidJobConfigException("Failed in execution of 'toString()' " + e.getMessage());
      }
    }
    result.append("help=");
    result.append(help);
    result.append('}');
    return result.toString();
  }

  public boolean parseJobConfig(String[] args) {
    return parseJobConfig(args, false);
  }

  public boolean parseJobConfig(String[] args, boolean showHelpIfArgsLengthZero) {
    JCommander cmd = JCommander.newBuilder().addObject(this).build();
    return parse(cmd, args, showHelpIfArgsLengthZero, false);
  }

  public boolean parseJobConfig(String[] args, boolean showHelpIfArgsLengthZero, boolean exitJobOnHelp) {
    JCommander cmd = JCommander.newBuilder().addObject(this).build();
    return parse(cmd, args, showHelpIfArgsLengthZero, exitJobOnHelp);
  }

  public boolean parseJobConfig(String[] args, Map<String, AbstractJobConfig> commandConfigMap) {
    JCommander cmd = JCommander.newBuilder().build();
    for (Map.Entry<String, AbstractJobConfig> entry : commandConfigMap.entrySet()) {
      cmd.addCommand(entry.getKey(), entry.getValue());
    }

    boolean isParsed = parse(cmd, args, false, false);
    if (isParsed && commandConfigMap.get(args[0]).help) {
      printUsage(cmd.getCommands().get(args[0]), commandConfigMap.get(args[0]));
    }
    return true;
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
      throw new InvalidJobConfigException("Failed to parse to job config: " + e.getMessage());
    }
    return output;
  }

  private void printUsage(JCommander cmd, AbstractJobConfig config) {
    if (config.help) {
      cmd.usage();
    }
  }
}
