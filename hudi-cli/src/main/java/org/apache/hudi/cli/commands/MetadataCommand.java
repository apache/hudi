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
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CLI commands to operate on the Metadata Table.
 * <p>
 * <p>
 * Example:
 * The default spark.master conf is set to yarn. If you are running on a local deployment,
 * we can set the spark master to local using set conf command.
 * > set --conf SPARK_MASTER=local
 * <p>
 * Connect to the table
 * > connect --path {path to hudi table}
 * <p>
 * Run metadata commands
 * > metadata list-partitions
 */
@Component
public class MetadataCommand implements CommandMarker {

  private static final Logger LOG = LogManager.getLogger(MetadataCommand.class);
  private static String metadataBaseDirectory;
  private JavaSparkContext jsc;

  /**
   * Sets the directory to store/read Metadata Table.
   * <p>
   * This can be used to store the metadata table away from the dataset directory.
   * - Useful for testing as well as for using via the HUDI CLI so that the actual dataset is not written to.
   * - Useful for testing Metadata Table performance and operations on existing datasets before enabling.
   */
  public static void setMetadataBaseDirectory(String metadataDir) {
    ValidationUtils.checkState(metadataBaseDirectory == null,
        "metadataBaseDirectory is already set to " + metadataBaseDirectory);
    metadataBaseDirectory = metadataDir;
  }

  public static String getMetadataTableBasePath(String tableBasePath) {
    if (metadataBaseDirectory != null) {
      return metadataBaseDirectory;
    }
    return HoodieTableMetadata.getMetadataTableBasePath(tableBasePath);
  }

  @CliCommand(value = "metadata set", help = "Set options for Metadata Table")
  public String set(@CliOption(key = {"metadataDir"},
      help = "Directory to read/write metadata table (can be different from dataset)", unspecifiedDefaultValue = "") final String metadataDir) {
    if (!metadataDir.isEmpty()) {
      setMetadataBaseDirectory(metadataDir);
    }

    return "Ok";
  }

  @CliCommand(value = "metadata create", help = "Create the Metadata Table if it does not exist")
  public String create() throws IOException {
    HoodieCLI.getTableMetaClient();
    Path metadataPath = new Path(getMetadataTableBasePath(HoodieCLI.basePath));
    try {
      FileStatus[] statuses = HoodieCLI.fs.listStatus(metadataPath);
      if (statuses.length > 0) {
        throw new RuntimeException("Metadata directory (" + metadataPath.toString() + ") not empty.");
      }
    } catch (FileNotFoundException e) {
      // Metadata directory does not exist yet
      HoodieCLI.fs.mkdirs(metadataPath);
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    HoodieWriteConfig writeConfig = getWriteConfig();
    initJavaSparkContext();
    SparkHoodieBackedTableMetadataWriter.create(HoodieCLI.conf, writeConfig, new HoodieSparkEngineContext(jsc));
    return String.format("Created Metadata Table in %s (duration=%.2f secs)", metadataPath, timer.endTimer() / 1000.0);
  }

  @CliCommand(value = "metadata delete", help = "Remove the Metadata Table")
  public String delete(@CliOption(key = {"backup"}, help = "Backup the metadata table before delete", mandatory = true) final boolean backup) throws Exception {
    HoodieTableMetaClient dataMetaClient = HoodieCLI.getTableMetaClient();
    HoodieTableMetadataUtil.deleteMetadataTable(dataMetaClient, new HoodieSparkEngineContext(jsc), backup);
    return "Metadata Table has been deleted from " + getMetadataTableBasePath(HoodieCLI.basePath);
  }

  @CliCommand(value = "metadata stats", help = "Print stats about the metadata")
  public String stats() throws IOException {
    HoodieCLI.getTableMetaClient();
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(new HoodieLocalEngineContext(HoodieCLI.conf),
        config, HoodieCLI.basePath);
    Map<String, String> stats = metadata.stats();

    final List<Comparable[]> rows = new ArrayList<>();
    for (Map.Entry<String, String> entry : stats.entrySet()) {
      Comparable[] row = new Comparable[2];
      row[0] = entry.getKey();
      row[1] = entry.getValue();
      rows.add(row);
    }

    TableHeader header = new TableHeader()
        .addTableHeaderField("stat key")
        .addTableHeaderField("stat value");
    return HoodiePrintHelper.print(header, new HashMap<>(), "",
        false, Integer.MAX_VALUE, false, rows);
  }

  @CliCommand(value = "metadata list-partitions", help = "List all partitions from metadata")
  public String listPartitions() throws IOException {
    HoodieCLI.getTableMetaClient();
    initJavaSparkContext();
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(new HoodieSparkEngineContext(jsc), config,
        HoodieCLI.basePath);

    if (!metadata.enabled()) {
      return "[ERROR] Metadata Table not enabled/initialized\n\n";
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    List<String> partitions = metadata.getAllPartitionPaths();
    LOG.debug("Took " + timer.endTimer() + " ms");

    final List<Comparable[]> rows = new ArrayList<>();
    partitions.stream().sorted(Comparator.reverseOrder()).forEach(p -> {
      Comparable[] row = new Comparable[1];
      row[0] = p;
      rows.add(row);
    });

    TableHeader header = new TableHeader().addTableHeaderField("partition");
    return HoodiePrintHelper.print(header, new HashMap<>(), "",
        false, Integer.MAX_VALUE, false, rows);
  }

  @CliCommand(value = "metadata list-files", help = "Print a list of all files in a partition from the metadata")
  public String listFiles(
      @CliOption(key = {"partition"}, help = "Name of the partition to list files", mandatory = true) final String partition) throws IOException {
    HoodieCLI.getTableMetaClient();
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    HoodieBackedTableMetadata metaReader = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(HoodieCLI.conf), config, HoodieCLI.basePath);

    if (!metaReader.enabled()) {
      return "[ERROR] Metadata Table not enabled/initialized\n\n";
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    FileStatus[] statuses = metaReader.getAllFilesInPartition(new Path(HoodieCLI.basePath, partition));
    LOG.debug("Took " + timer.endTimer() + " ms");

    final List<Comparable[]> rows = new ArrayList<>();
    Arrays.stream(statuses).sorted((p1, p2) -> p2.getPath().getName().compareTo(p1.getPath().getName())).forEach(f -> {
      Comparable[] row = new Comparable[1];
      row[0] = f;
      rows.add(row);
    });

    TableHeader header = new TableHeader().addTableHeaderField("file path");
    return HoodiePrintHelper.print(header, new HashMap<>(), "",
        false, Integer.MAX_VALUE, false, rows);
  }

  @CliCommand(value = "metadata validate-files", help = "Validate all files in all partitions from the metadata")
  public String validateFiles(
      @CliOption(key = {"verbose"}, help = "Print all file details", unspecifiedDefaultValue = "false") final boolean verbose) throws IOException {
    HoodieCLI.getTableMetaClient();
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    HoodieBackedTableMetadata metadataReader = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(HoodieCLI.conf), config, HoodieCLI.basePath);

    if (!metadataReader.enabled()) {
      return "[ERROR] Metadata Table not enabled/initialized\n\n";
    }

    HoodieMetadataConfig fsConfig = HoodieMetadataConfig.newBuilder().enable(false).build();
    HoodieBackedTableMetadata fsMetaReader = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(HoodieCLI.conf), fsConfig, HoodieCLI.basePath);

    HoodieTimer timer = new HoodieTimer().startTimer();
    List<String> metadataPartitions = metadataReader.getAllPartitionPaths();
    LOG.debug("Listing partitions Took " + timer.endTimer() + " ms");
    List<String> fsPartitions = fsMetaReader.getAllPartitionPaths();
    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    Set<String> allPartitions = new HashSet<>();
    allPartitions.addAll(fsPartitions);
    allPartitions.addAll(metadataPartitions);

    if (!fsPartitions.equals(metadataPartitions)) {
      LOG.error("FS partition listing is not matching with metadata partition listing!");
      LOG.error("All FS partitions: " + Arrays.toString(fsPartitions.toArray()));
      LOG.error("All Metadata partitions: " + Arrays.toString(metadataPartitions.toArray()));
    }

    final List<Comparable[]> rows = new ArrayList<>();
    for (String partition : allPartitions) {
      Map<String, FileStatus> fileStatusMap = new HashMap<>();
      Map<String, FileStatus> metadataFileStatusMap = new HashMap<>();
      FileStatus[] metadataStatuses = metadataReader.getAllFilesInPartition(new Path(HoodieCLI.basePath, partition));
      Arrays.stream(metadataStatuses).forEach(entry -> metadataFileStatusMap.put(entry.getPath().getName(), entry));
      FileStatus[] fsStatuses = fsMetaReader.getAllFilesInPartition(new Path(HoodieCLI.basePath, partition));
      Arrays.stream(fsStatuses).forEach(entry -> fileStatusMap.put(entry.getPath().getName(), entry));

      Set<String> allFiles = new HashSet<>();
      allFiles.addAll(fileStatusMap.keySet());
      allFiles.addAll(metadataFileStatusMap.keySet());

      for (String file : allFiles) {
        Comparable[] row = new Comparable[6];
        row[0] = partition;
        FileStatus fsFileStatus = fileStatusMap.get(file);
        FileStatus metaFileStatus = metadataFileStatusMap.get(file);
        row[1] = file;
        row[2] = fsFileStatus != null;
        row[3] = metaFileStatus != null;
        row[4] = (fsFileStatus != null) ? fsFileStatus.getLen() : 0;
        row[5] = (metaFileStatus != null) ? metaFileStatus.getLen() : 0;
        rows.add(row);
      }

      if (metadataStatuses.length != fsStatuses.length) {
        LOG.error(" FS and metadata files count not matching for " + partition + ". FS files count " + fsStatuses.length + ", metadata base files count "
            + metadataStatuses.length);
      }

      for (Map.Entry<String, FileStatus> entry : fileStatusMap.entrySet()) {
        if (!metadataFileStatusMap.containsKey(entry.getKey())) {
          LOG.error("FS file not found in metadata " + entry.getKey());
        } else {
          if (entry.getValue().getLen() != metadataFileStatusMap.get(entry.getKey()).getLen()) {
            LOG.error(" FS file size mismatch " + entry.getKey() + ", size equality "
                + (entry.getValue().getLen() == metadataFileStatusMap.get(entry.getKey()).getLen())
                + ". FS size " + entry.getValue().getLen() + ", metadata size "
                + metadataFileStatusMap.get(entry.getKey()).getLen());
          }
        }
      }
      for (Map.Entry<String, FileStatus> entry : metadataFileStatusMap.entrySet()) {
        if (!fileStatusMap.containsKey(entry.getKey())) {
          LOG.error("Metadata file not found in FS " + entry.getKey());
        } else {
          if (entry.getValue().getLen() != fileStatusMap.get(entry.getKey()).getLen()) {
            LOG.error(" Metadata file size mismatch " + entry.getKey() + ", size equality "
                + (entry.getValue().getLen() == fileStatusMap.get(entry.getKey()).getLen())
                + ". Metadata size " + entry.getValue().getLen() + ", FS size "
                + metadataFileStatusMap.get(entry.getKey()).getLen());
          }
        }
      }
    }
    TableHeader header = new TableHeader().addTableHeaderField("Partition")
        .addTableHeaderField("File Name")
        .addTableHeaderField(" IsPresent in FS ")
        .addTableHeaderField(" IsPresent in Metadata")
        .addTableHeaderField(" FS size")
        .addTableHeaderField(" Metadata size");
    return HoodiePrintHelper.print(header, new HashMap<>(), "", false, Integer.MAX_VALUE, false, rows);
  }

  private HoodieWriteConfig getWriteConfig() {
    return HoodieWriteConfig.newBuilder().withPath(HoodieCLI.basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).build();
  }

  private void initJavaSparkContext() {
    if (jsc == null) {
      jsc = SparkUtil.initJavaSparkConf(SparkUtil.getDefaultConf("HoodieCLI", Option.empty()));
    }
  }
}