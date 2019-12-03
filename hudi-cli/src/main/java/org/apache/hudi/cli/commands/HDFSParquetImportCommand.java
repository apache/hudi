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
import org.apache.hudi.utilities.HDFSParquetImporter.FormatValidator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import scala.collection.JavaConverters;

/**
 * CLI command for importing parquet dataset to hudi dataset.
 */
@Component
public class HDFSParquetImportCommand implements CommandMarker {

  private static Logger log = LogManager.getLogger(HDFSParquetImportCommand.class);

  @CliCommand(value = "hdfsparquetimport", help = "Imports Parquet dataset to a hoodie dataset")
  public String convert(
      @CliOption(key = "upsert", mandatory = false, unspecifiedDefaultValue = "false",
          help = "Uses upsert API instead of the default insert API of WriteClient") boolean useUpsert,
      @CliOption(key = "srcPath", mandatory = true, help = "Base path for the input dataset") final String srcPath,
      @CliOption(key = "targetPath", mandatory = true,
          help = "Base path for the target hoodie dataset") final String targetPath,
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
      @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "retry", mandatory = true, help = "Number of retries") final String retry) throws Exception {

    (new FormatValidator()).validate("format", format);

    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);
    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);

    String cmd = SparkCommand.IMPORT.toString();
    if (useUpsert) {
      cmd = SparkCommand.UPSERT.toString();
    }

    sparkLauncher.addAppArgs(cmd, srcPath, targetPath, tableName, tableType, rowKeyField, partitionPathField,
        parallelism, schemaFilePath, sparkMemory, retry);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to import dataset to hoodie format";
    }
    return "Dataset imported to hoodie format";
  }
}
