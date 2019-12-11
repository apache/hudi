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

package org.apache.hudi.cli.commands;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * CLI command to set env.
 */
@Component
public class SetSparkEnvCommand implements CommandMarker {

  public static Map env = new HashMap<String, String>();

  @CliCommand(value = "set", help = "Set spark launcher env to cli")
  public void setEnv(@CliOption(key = {"conf"}, help = "Env config to be set") final String confMap)
      throws IllegalArgumentException {
    String[] map = confMap.split("=");
    if (map.length != 2) {
      throw new IllegalArgumentException("Illegal set parameter, please use like [set SPARK_HOME=/usr/etc/spark]'");
    }
    env.put(map[0].trim(), map[1].trim());
  }
}
