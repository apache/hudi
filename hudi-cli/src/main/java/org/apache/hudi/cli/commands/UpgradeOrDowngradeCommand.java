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

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.StringUtils;

import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

/**
 * CLI command to assist in upgrading/downgrading Hoodie table to a different version.
 */
@ShellComponent
public class UpgradeOrDowngradeCommand {

  @ShellMethod(key = "upgrade table", value = "Upgrades a table")
  public String upgradeHoodieTable(
      @ShellOption(value = {"--toVersion"}, help = "To version of Hoodie table to be upgraded/downgraded to", defaultValue = "") final String toVersion,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") final String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    String toVersionName = getHoodieTableVersionName(toVersion, true);
    SparkMain.addAppArgs(sparkLauncher, SparkCommand.UPGRADE, master, sparkMemory, HoodieCLI.basePath, toVersionName);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    HoodieCLI.refreshTableMetadata();
    if (exitCode != 0) {
      return String.format("Failed: Could not Upgrade/Downgrade Hoodie table to \"%s\".", toVersionName);
    }
    return String.format("Hoodie table upgraded/downgraded to %s", toVersionName);
  }

  @ShellMethod(key = "downgrade table", value = "Downgrades a table")
  public String downgradeHoodieTable(
      @ShellOption(value = {"--toVersion"}, help = "To version of Hoodie table to be upgraded/downgraded to", defaultValue = "") final String toVersion,
      @ShellOption(value = {"--sparkProperties"}, help = "Spark Properties File Path",
          defaultValue = "") final String sparkPropertiesPath,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", defaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    String toVersionName = getHoodieTableVersionName(toVersion, false);
    SparkMain.addAppArgs(sparkLauncher, SparkCommand.DOWNGRADE, master, sparkMemory, HoodieCLI.basePath, toVersionName);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    HoodieCLI.refreshTableMetadata();
    if (exitCode != 0) {
      return String.format("Failed: Could not Upgrade/Downgrade Hoodie table to \"%s\".", toVersionName);
    }
    return String.format("Hoodie table upgraded/downgraded to %s", toVersionName);
  }

  static String getHoodieTableVersionName(String versionOption, boolean overrideWithDefault) {
    if (StringUtils.isNullOrEmpty(versionOption) && overrideWithDefault) {
      return HoodieTableVersion.current().name();
    }

    try {
      int versionCode = Integer.parseInt(versionOption);
      return HoodieTableVersion.fromVersionCode(versionCode).name();
    } catch (NumberFormatException e) {
      // The version option from the CLI is not a number, returns the original String
      return versionOption;
    }
  }
}
