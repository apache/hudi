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
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import scala.collection.JavaConverters;

/**
 * CLI command for importing parquet table to hudi table.
 *
 * @deprecated This utility is deprecated in 0.10.0 and will be removed in 0.11.0. Use {@link HoodieDeltaStreamer.Config#runBootstrap} instead.
 * @see HoodieDeltaStreamer
 */
@Component
public class HDFSParquetImportCommand implements CommandMarker {

  @CliCommand(value = "hdfsparquetimport", help = "Imports Parquet table to a hoodie table")
  public String convert(
      @CliOption(key = "upsert", unspecifiedDefaultValue = "false",
          help = "Uses upsert API instead of the default insert API of WriteClient") boolean useUpsert,
      @CliOption(key = "srcPath", mandatory = true, help = "Base path for the input table") final String srcPath,
      @CliOption(key = "targetPath", mandatory = true,
          help = "Base path for the target hoodie table") final String targetPath,
      @CliOption(key = "tableName", mandatory = true, help = "Table name") final String tableName,
      @CliOption(key = "tableType", mandatory = true, help = "Table type") final String tableType,
      @CliOption(key = "rowKeyField", mandatory = true, help = "Row key field name") final String rowKeyField,
      @CliOption(key = "partitionPathField", mandatory = true,
          help = "Partition path field name") final String partitionPathField,
      @CliOption(key = {"parallelism"}, mandatory = true,
          help = "Parallelism for hoodie insert") final String parallelism,
      @CliOption(key = "schemaFilePath", mandatory = true,
          help = "Path for Avro schema file") final String schemaFilePath,
      @CliOption(key = "format", mandatory = true, help = "Format for the input data") final String format,
      @CliOption(key = "sparkMaster", unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "retry", mandatory = true, help = "Number of retries") final String retry,
      @CliOption(key = "propsFilePath", help = "path to properties file on localfs or dfs with configurations for hoodie client for importing",
        unspecifiedDefaultValue = "") final String propsFilePath,
      @CliOption(key = "hoodieConfigs", help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
        unspecifiedDefaultValue = "") final String[] configs) throws Exception {

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
