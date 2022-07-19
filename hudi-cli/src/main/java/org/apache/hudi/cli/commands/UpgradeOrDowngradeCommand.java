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
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * CLI command to assist in upgrading/downgrading Hoodie table to a different version.
 */
@Component
public class UpgradeOrDowngradeCommand implements CommandMarker {

  @CliCommand(value = "upgrade table", help = "Upgrades a table")
  public String upgradeHoodieTable(
      @CliOption(key = {"toVersion"}, help = "To version of Hoodie table to be upgraded/downgraded to", unspecifiedDefaultValue = "") final String toVersion,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkCommand.UPGRADE.toString(), master, sparkMemory, metaClient.getBasePath(), toVersion);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    HoodieCLI.refreshTableMetadata();
    if (exitCode != 0) {
      return String.format("Failed: Could not Upgrade/Downgrade Hoodie table to \"%s\".", toVersion);
    }
    return String.format("Hoodie table upgraded/downgraded to %s", toVersion);
  }

  @CliCommand(value = "downgrade table", help = "Downgrades a table")
  public String downgradeHoodieTable(
      @CliOption(key = {"toVersion"}, help = "To version of Hoodie table to be upgraded/downgraded to", unspecifiedDefaultValue = "") final String toVersion,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properties File Path") final String sparkPropertiesPath,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory)
      throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkCommand.DOWNGRADE.toString(), master, sparkMemory, metaClient.getBasePath(), toVersion);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    HoodieCLI.refreshTableMetadata();
    if (exitCode != 0) {
      return String.format("Failed: Could not Upgrade/Downgrade Hoodie table to \"%s\".", toVersion);
    }
    return String.format("Hoodie table upgraded/downgraded to %s", toVersion);
  }
}
