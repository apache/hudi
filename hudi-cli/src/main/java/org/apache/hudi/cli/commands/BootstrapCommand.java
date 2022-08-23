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
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

/**
 * CLI command to perform bootstrap action & display bootstrap index.
 */
@Component
public class BootstrapCommand implements CommandMarker {

  @CliCommand(value = "bootstrap run", help = "Run a bootstrap action for current Hudi table")
  public String bootstrap(
      @CliOption(key = {"srcPath"}, mandatory = true, help = "Bootstrap source data path of the table") final String srcPath,
      @CliOption(key = {"targetPath"}, mandatory = true,
          help = "Base path for the target hoodie table") final String targetPath,
      @CliOption(key = {"tableName"}, mandatory = true, help = "Hoodie table name") final String tableName,
      @CliOption(key = {"tableType"}, mandatory = true, help = "Hoodie table type") final String tableType,
      @CliOption(key = {"rowKeyField"}, mandatory = true, help = "Record key columns for bootstrap data") final String rowKeyField,
      @CliOption(key = {"partitionPathField"}, unspecifiedDefaultValue = "",
          help = "Partition fields for bootstrap source data") final String partitionPathField,
      @CliOption(key = {"bootstrapIndexClass"}, unspecifiedDefaultValue = "org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex",
          help = "Bootstrap Index Class") final String bootstrapIndexClass,
      @CliOption(key = {"selectorClass"}, unspecifiedDefaultValue = "org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector",
          help = "Selector class for bootstrap") final String selectorClass,
      @CliOption(key = {"keyGeneratorClass"}, unspecifiedDefaultValue = "org.apache.hudi.keygen.SimpleKeyGenerator",
          help = "Key generator class for bootstrap") final String keyGeneratorClass,
      @CliOption(key = {"fullBootstrapInputProvider"}, unspecifiedDefaultValue = "org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider",
          help = "Class for Full bootstrap input provider") final String fullBootstrapInputProvider,
      @CliOption(key = {"schemaProviderClass"}, unspecifiedDefaultValue = "",
          help = "SchemaProvider to attach schemas to bootstrap source data") final String schemaProviderClass,
      @CliOption(key = {"payloadClass"}, unspecifiedDefaultValue = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
          help = "Payload Class") final String payloadClass,
      @CliOption(key = {"parallelism"}, unspecifiedDefaultValue = "1500", help = "Bootstrap writer parallelism") final int parallelism,
      @CliOption(key = {"sparkMaster"}, unspecifiedDefaultValue = "", help = "Spark Master") String master,
      @CliOption(key = {"sparkMemory"}, unspecifiedDefaultValue = "4G", help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = {"enableHiveSync"}, unspecifiedDefaultValue = "false", help = "Enable Hive sync") final Boolean enableHiveSync,
      @CliOption(key = {"propsFilePath"}, help = "path to properties file on localfs or dfs with configurations for hoodie client for importing",
          unspecifiedDefaultValue = "") final String propsFilePath,
      @CliOption(key = {"hoodieConfigs"}, help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
          unspecifiedDefaultValue = "") final String[] configs)
      throws IOException, InterruptedException, URISyntaxException {

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);

    String cmd = SparkCommand.BOOTSTRAP.toString();

    sparkLauncher.addAppArgs(cmd, master, sparkMemory, tableName, tableType, targetPath, srcPath, rowKeyField,
        partitionPathField, String.valueOf(parallelism), schemaProviderClass, bootstrapIndexClass, selectorClass,
        keyGeneratorClass, fullBootstrapInputProvider, payloadClass, String.valueOf(enableHiveSync), propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to bootstrap source data to Hudi dataset";
    }
    return "Bootstrapped source data as Hudi dataset";
  }

  @CliCommand(value = "bootstrap index showmapping", help = "Show bootstrap index mapping")
  public String showBootstrapIndexMapping(
      @CliOption(key = {"partitionPath"}, unspecifiedDefaultValue = "", help = "A valid partition path") String partitionPath,
      @CliOption(key = {"fileIds"}, unspecifiedDefaultValue = "", help = "Valid fileIds split by comma") String fileIds,
      @CliOption(key = {"limit"}, unspecifiedDefaultValue = "-1", help = "Limit rows to be displayed") Integer limit,
      @CliOption(key = {"sortBy"}, unspecifiedDefaultValue = "", help = "Sorting Field") final String sortByField,
      @CliOption(key = {"desc"}, unspecifiedDefaultValue = "false", help = "Ordering") final boolean descending,
      @CliOption(key = {"headeronly"}, unspecifiedDefaultValue = "false", help = "Print Header Only") final boolean headerOnly) {

    if (partitionPath.isEmpty() && !fileIds.isEmpty()) {
      throw new IllegalStateException("PartitionPath is mandatory when passing fileIds.");
    }

    BootstrapIndex.IndexReader indexReader = createBootstrapIndexReader();
    List<String> indexedPartitions = indexReader.getIndexedPartitionPaths();

    if (!partitionPath.isEmpty() && !indexedPartitions.contains(partitionPath)) {
      return partitionPath + " is not an valid indexed partition";
    }

    List<BootstrapFileMapping> mappingList = new ArrayList<>();
    if (!fileIds.isEmpty()) {
      List<HoodieFileGroupId> fileGroupIds = Arrays.stream(fileIds.split(","))
          .map(fileId -> new HoodieFileGroupId(partitionPath, fileId)).collect(Collectors.toList());
      mappingList.addAll(indexReader.getSourceFileMappingForFileIds(fileGroupIds).values());
    } else if (!partitionPath.isEmpty()) {
      mappingList.addAll(indexReader.getSourceFileMappingForPartition(partitionPath));
    } else {
      for (String part : indexedPartitions) {
        mappingList.addAll(indexReader.getSourceFileMappingForPartition(part));
      }
    }

    final List<Comparable[]> rows = convertBootstrapSourceFileMapping(mappingList);
    final TableHeader header = new TableHeader()
        .addTableHeaderField("Hudi Partition")
        .addTableHeaderField("FileId")
        .addTableHeaderField("Source File Base Path")
        .addTableHeaderField("Source File Partition")
        .addTableHeaderField("Source File Path");

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending,
        limit, headerOnly, rows);
  }

  @CliCommand(value = "bootstrap index showpartitions", help = "Show bootstrap indexed partitions")
  public String showBootstrapIndexPartitions() {

    BootstrapIndex.IndexReader indexReader = createBootstrapIndexReader();
    List<String> indexedPartitions = indexReader.getIndexedPartitionPaths();

    String[] header = new String[] {"Indexed partitions"};
    String[][] rows = new String[indexedPartitions.size()][1];
    for (int i = 0; i < indexedPartitions.size(); i++) {
      rows[i][0] = indexedPartitions.get(i);
    }
    return HoodiePrintHelper.print(header, rows);
  }

  private BootstrapIndex.IndexReader createBootstrapIndexReader() {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    if (!index.useIndex()) {
      throw new HoodieException("This is not a bootstrapped Hudi table. Don't have any index info");
    }
    return index.createReader();
  }

  private List<Comparable[]> convertBootstrapSourceFileMapping(List<BootstrapFileMapping> mappingList) {
    final List<Comparable[]> rows = new ArrayList<>();
    for (BootstrapFileMapping mapping : mappingList) {
      rows.add(new Comparable[] {mapping.getPartitionPath(), mapping.getFileId(),
          mapping.getBootstrapBasePath(), mapping.getBootstrapPartitionPath(), mapping.getBootstrapFileStatus().getPath().getUri()});
    }
    return rows;
  }
}
