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

import org.apache.hudi.common.util.StringUtils;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * CLI command to display utils.
 */
@Component
public class UtilsCommand implements CommandMarker {

  @CliCommand(value = "utils loadClass", help = "Load a class")
  public String loadClass(@CliOption(key = {"class"}, help = "Check mode") final String clazz) {
    if (StringUtils.isNullOrEmpty(clazz)) {
      return "Class to be loaded can not be null!";
    }
    try {
      Class klass = Class.forName(clazz);
      return klass.getProtectionDomain().getCodeSource().getLocation().toExternalForm();
    } catch (ClassNotFoundException e) {
      return String.format("Class %s not found!", clazz);
    }
  }
}
