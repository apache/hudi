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

import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.utilities.HDFSParquetImporter.FormatValidator;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import scala.collection.JavaConverters;

/**
 * CLI command for importing parquet table to hudi table.
 *
 * @see HoodieDeltaStreamer
 * @deprecated This utility is deprecated in 0.10.0 and will be removed in 0.11.0. Use {@link HoodieDeltaStreamer.Config#runBootstrap} instead.
 */
@ShellComponent
public class HDFSParquetImportCommand {

  @ShellMethod(key = "hdfsparquetimport", value = "Imports Parquet table to a hoodie table")
  public String convert(
      @ShellOption(value = "--upsert", defaultValue = "false",
          help = "Uses upsert API instead of the default insert API of WriteClient") boolean useUpsert,
      @ShellOption(value = "--srcPath", help = "Base path for the input table") final String srcPath,
      @ShellOption(value = "--targetPath",
          help = "Base path for the target hoodie table") final String targetPath,
      @ShellOption(value = "--tableName", help = "Table name") final String tableName,
      @ShellOption(value = "--tableType", help = "Table type") final String tableType,
      @ShellOption(value = "--rowKeyField", help = "Row key field name") final String rowKeyField,
      @ShellOption(value = "--partitionPathField", defaultValue = "",
          help = "Partition path field name") final String partitionPathField,
      @ShellOption(value = {"--parallelism"},
          help = "Parallelism for hoodie insert") final String parallelism,
      @ShellOption(value = "--schemaFilePath",
          help = "Path for Avro schema file") final String schemaFilePath,
      @ShellOption(value = "--format", help = "Format for the input data") final String format,
      @ShellOption(value = "--sparkMaster", defaultValue = "", help = "Spark Master") String master,
      @ShellOption(value = "--sparkMemory", help = "Spark executor memory") final String sparkMemory,
      @ShellOption(value = "--retry", help = "Number of retries") final String retry,
      @ShellOption(value = "--propsFilePath", help = "path to properties file on localfs or dfs with configurations for hoodie client for importing",
          defaultValue = "") final String propsFilePath,
      @ShellOption(value = "--hoodieConfigs", help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
          defaultValue = "") final String[] configs) throws Exception {

    (new FormatValidator()).validate("format", format);

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);

    String cmd = SparkCommand.IMPORT.toString();
    if (useUpsert) {
      cmd = SparkCommand.UPSERT.toString();
    }

    sparkLauncher.addAppArgs(cmd, master, sparkMemory, srcPath, targetPath, tableName, tableType, rowKeyField,
        partitionPathField, parallelism, schemaFilePath, retry, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to import table to hoodie format";
    }
    return "Table imported to hoodie format";
  }
}
