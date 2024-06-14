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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

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
 * > set --conf SPARK_MASTER=local[2]
 * <p>
 * Connect to the table
 * > connect --path {path to hudi table}
 * <p>
 * Run metadata commands
 * > metadata list-partitions
 */
@ShellComponent
public class MetadataCommand {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataCommand.class);
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

  @ShellMethod(key = "metadata set", value = "Set options for Metadata Table")
  public String set(@ShellOption(value = {"--metadataDir"},
      help = "Directory to read/write metadata table (can be different from dataset)", defaultValue = "") final String metadataDir) {
    if (!metadataDir.isEmpty()) {
      setMetadataBaseDirectory(metadataDir);
    }

    return "Ok";
  }

  @ShellMethod(key = "metadata create", value = "Create the Metadata Table if it does not exist")
  public String create(
      @ShellOption(value = "--sparkMaster", defaultValue = SparkUtil.DEFAULT_SPARK_MASTER, help = "Spark master") final String master
  ) throws Exception {
    HoodieCLI.getTableMetaClient();
    StoragePath metadataPath = new StoragePath(getMetadataTableBasePath(HoodieCLI.basePath));
    try {
      List<StoragePathInfo> pathInfoList = HoodieCLI.storage.listDirectEntries(metadataPath);
      if (pathInfoList.size() > 0) {
        throw new RuntimeException("Metadata directory (" + metadataPath + ") not empty.");
      }
    } catch (FileNotFoundException e) {
      // Metadata directory does not exist yet
      HoodieCLI.storage.createDirectory(metadataPath);
    }

    HoodieTimer timer = HoodieTimer.start();
    HoodieWriteConfig writeConfig = getWriteConfig();
    initJavaSparkContext(Option.of(master));
    try (HoodieTableMetadataWriter writer = SparkHoodieBackedTableMetadataWriter.create(HoodieCLI.conf, writeConfig, new HoodieSparkEngineContext(jsc))) {
      return String.format("Created Metadata Table in %s (duration=%.2f secs)", metadataPath, timer.endTimer() / 1000.0);
    }
  }

  @ShellMethod(key = "metadata delete", value = "Remove the Metadata Table")
  public String delete(@ShellOption(value = "--backup", help = "Backup the metadata table before delete", defaultValue = "true", arity = 1) final boolean backup) throws Exception {
    HoodieTableMetaClient dataMetaClient = HoodieCLI.getTableMetaClient();
    String backupPath = HoodieTableMetadataUtil.deleteMetadataTable(dataMetaClient, new HoodieSparkEngineContext(jsc), backup);
    if (backup) {
      return "Metadata Table has been deleted and backed up to " + backupPath;
    } else {
      return "Metadata Table has been deleted from " + getMetadataTableBasePath(HoodieCLI.basePath);
    }
  }

  @ShellMethod(key = "metadata delete-record-index", value = "Delete the record index from Metadata Table")
  public String deleteRecordIndex(@ShellOption(value = "--backup", help = "Backup the record index before delete", defaultValue = "true", arity = 1) final boolean backup) throws Exception {
    HoodieTableMetaClient dataMetaClient = HoodieCLI.getTableMetaClient();
    String backupPath = HoodieTableMetadataUtil.deleteMetadataTablePartition(dataMetaClient, new HoodieSparkEngineContext(jsc),
        MetadataPartitionType.RECORD_INDEX, backup);
    if (backup) {
      return "Record Index has been deleted from the Metadata Table and backed up to " + backupPath;
    } else {
      return "Record Index has been deleted from the Metadata Table";
    }
  }

  @ShellMethod(key = "metadata init", value = "Update the metadata table from commits since the creation")
  public String init(@ShellOption(value = "--sparkMaster", defaultValue = SparkUtil.DEFAULT_SPARK_MASTER, help = "Spark master") final String master,
                     @ShellOption(value = {"--readonly"}, defaultValue = "false",
                         help = "Open in read-only mode") final boolean readOnly) throws Exception {
    HoodieCLI.getTableMetaClient();
    StoragePath metadataPath = new StoragePath(getMetadataTableBasePath(HoodieCLI.basePath));
    try {
      HoodieCLI.storage.listDirectEntries(metadataPath);
    } catch (FileNotFoundException e) {
      // Metadata directory does not exist
      throw new RuntimeException("Metadata directory (" + metadataPath + ") does not exist.");
    }

    HoodieTimer timer = HoodieTimer.start();
    if (!readOnly) {
      HoodieWriteConfig writeConfig = getWriteConfig();
      initJavaSparkContext(Option.of(master));
      try (HoodieTableMetadataWriter writer = SparkHoodieBackedTableMetadataWriter.create(HoodieCLI.conf, writeConfig, new HoodieSparkEngineContext(jsc))) {
        // Empty
      }
    }

    String action = readOnly ? "Opened" : "Initialized";
    return String.format(action + " Metadata Table in %s (duration=%.2fsec)", metadataPath, (timer.endTimer()) / 1000.0);
  }

  @ShellMethod(key = "metadata stats", value = "Print stats about the metadata")
  public String stats() throws IOException {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(HoodieCLI.conf), metaClient.getStorage(), config, HoodieCLI.basePath)) {
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
      return HoodiePrintHelper.print(header, new HashMap<>(), "", false, Integer.MAX_VALUE, false, rows);
    }
  }

  @ShellMethod(key = "metadata list-partitions", value = "List all partitions from metadata")
  public String listPartitions(
      @ShellOption(value = "--sparkMaster", defaultValue = SparkUtil.DEFAULT_SPARK_MASTER, help = "Spark master") final String master
  ) throws IOException {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    initJavaSparkContext(Option.of(master));
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        new HoodieSparkEngineContext(jsc), metaClient.getStorage(), config, HoodieCLI.basePath)) {

      if (!metadata.enabled()) {
        return "[ERROR] Metadata Table not enabled/initialized\n\n";
      }

      HoodieTimer timer = HoodieTimer.start();
      List<String> partitions = metadata.getAllPartitionPaths();
      LOG.debug("Metadata Partition listing took " + timer.endTimer() + " ms");

      final List<Comparable[]> rows = new ArrayList<>();
      partitions.stream().sorted(Comparator.reverseOrder()).forEach(p -> {
        Comparable[] row = new Comparable[1];
        row[0] = p;
        rows.add(row);
      });

      TableHeader header = new TableHeader().addTableHeaderField("partition");
      return HoodiePrintHelper.print(header, new HashMap<>(), "", false, Integer.MAX_VALUE, false, rows);
    }
  }

  @ShellMethod(key = "metadata list-files", value = "Print a list of all files in a partition from the metadata")
  public String listFiles(
      @ShellOption(value = {"--partition"}, help = "Name of the partition to list files", defaultValue = "") final String partition) throws IOException {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieBackedTableMetadata metaReader = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(HoodieCLI.conf), metaClient.getStorage(), config, HoodieCLI.basePath)) {

      if (!metaReader.enabled()) {
        return "[ERROR] Metadata Table not enabled/initialized\n\n";
      }

      StoragePath partitionPath = new StoragePath(HoodieCLI.basePath);
      if (!StringUtils.isNullOrEmpty(partition)) {
        partitionPath = new StoragePath(HoodieCLI.basePath, partition);
      }

      HoodieTimer timer = HoodieTimer.start();
      List<StoragePathInfo> pathInfoList = metaReader.getAllFilesInPartition(partitionPath);
      LOG.debug("Took " + timer.endTimer() + " ms");

      final List<Comparable[]> rows = new ArrayList<>();
      pathInfoList.stream()
          .sorted((p1, p2) -> p2.getPath().getName().compareTo(p1.getPath().getName()))
          .forEach(f -> {
            Comparable[] row = new Comparable[1];
            row[0] = f;
            rows.add(row);
          });

      TableHeader header = new TableHeader().addTableHeaderField("file path");
      return HoodiePrintHelper.print(header, new HashMap<>(), "", false, Integer.MAX_VALUE, false,
          rows);
    }
  }

  @ShellMethod(key = "metadata validate-files", value = "Validate all files in all partitions from the metadata")
  public String validateFiles(
          @ShellOption(value = {"--verbose"}, help = "Print all file details", defaultValue = "false") final boolean verbose)
        throws IOException {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().enable(true).build();
    HoodieBackedTableMetadata metadataReader = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(HoodieCLI.conf), metaClient.getStorage(), config, HoodieCLI.basePath);

    if (!metadataReader.enabled()) {
      return "[ERROR] Metadata Table not enabled/initialized\n\n";
    }

    FileSystemBackedTableMetadata fsMetaReader = new FileSystemBackedTableMetadata(new HoodieLocalEngineContext(HoodieCLI.conf),
            HoodieCLI.getTableMetaClient().getTableConfig(), metaClient.getStorage(),
        HoodieCLI.basePath, false);
    HoodieMetadataConfig fsConfig = HoodieMetadataConfig.newBuilder().enable(false).build();

    HoodieTimer timer = HoodieTimer.start();
    List<String> metadataPartitions = metadataReader.getAllPartitionPaths();
    LOG.debug("Metadata Listing partitions Took " + timer.endTimer() + " ms");
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
      Map<String, StoragePathInfo> pathInfoMap = new HashMap<>();
      Map<String, StoragePathInfo> metadataPathInfoMap = new HashMap<>();
      List<StoragePathInfo> metadataPathInfoList = metadataReader.getAllFilesInPartition(
          new StoragePath(HoodieCLI.basePath, partition));
      metadataPathInfoList.forEach(entry -> metadataPathInfoMap.put(
          entry.getPath().getName(), entry));
      List<StoragePathInfo> pathInfoList =
          fsMetaReader.getAllFilesInPartition(new StoragePath(HoodieCLI.basePath, partition));
      pathInfoList.forEach(entry -> pathInfoMap.put(entry.getPath().getName(), entry));

      Set<String> allFiles = new HashSet<>();
      allFiles.addAll(pathInfoMap.keySet());
      allFiles.addAll(metadataPathInfoMap.keySet());

      for (String file : allFiles) {
        Comparable[] row = new Comparable[6];
        row[0] = partition;
        StoragePathInfo pathInfo = pathInfoMap.get(file);
        StoragePathInfo metaPathInfo = metadataPathInfoMap.get(file);
        boolean doesFsFileExists = pathInfo != null;
        boolean doesMetadataFileExists = metaPathInfo != null;
        long fsFileLength = doesFsFileExists ? pathInfo.getLength() : 0;
        long metadataFileLength = doesMetadataFileExists ? metaPathInfo.getLength() : 0;
        row[1] = file;
        row[2] = doesFsFileExists;
        row[3] = doesMetadataFileExists;
        row[4] = fsFileLength;
        row[5] = metadataFileLength;
        if (verbose) { // if verbose print all files
          rows.add(row);
        } else if ((doesFsFileExists != doesMetadataFileExists)
            || (fsFileLength != metadataFileLength)) {
          // if non verbose, print only non matching files
          rows.add(row);
        }
      }

      if (metadataPathInfoList.size() != pathInfoList.size()) {
        LOG.error(" FS and metadata files count not matching for " + partition
            + ". FS files count " + pathInfoList.size()
            + ", metadata base files count " + metadataPathInfoList.size());
      }

      for (Map.Entry<String, StoragePathInfo> entry : pathInfoMap.entrySet()) {
        if (!metadataPathInfoMap.containsKey(entry.getKey())) {
          LOG.error("FS file not found in metadata " + entry.getKey());
        } else {
          if (entry.getValue().getLength()
              != metadataPathInfoMap.get(entry.getKey()).getLength()) {
            LOG.error(" FS file size mismatch " + entry.getKey() + ", size equality "
                + (entry.getValue().getLength()
                == metadataPathInfoMap.get(entry.getKey()).getLength())
                + ". FS size " + entry.getValue().getLength()
                + ", metadata size " + metadataPathInfoMap.get(entry.getKey()).getLength());
          }
        }
      }
      for (Map.Entry<String, StoragePathInfo> entry : metadataPathInfoMap.entrySet()) {
        if (!pathInfoMap.containsKey(entry.getKey())) {
          LOG.error("Metadata file not found in FS " + entry.getKey());
        } else {
          if (entry.getValue().getLength() != pathInfoMap.get(entry.getKey()).getLength()) {
            LOG.error(" Metadata file size mismatch " + entry.getKey() + ", size equality "
                + (entry.getValue().getLength() == pathInfoMap.get(entry.getKey()).getLength())
                + ". Metadata size " + entry.getValue().getLength() + ", FS size "
                + metadataPathInfoMap.get(entry.getKey()).getLength());
          }
        }
      }
    }
    TableHeader header = new TableHeader().addTableHeaderField("Partition")
        .addTableHeaderField("File Name")
        .addTableHeaderField(" Is Present in FS ")
        .addTableHeaderField(" Is Present in Metadata")
        .addTableHeaderField(" FS size")
        .addTableHeaderField(" Metadata size");
    return HoodiePrintHelper.print(header, new HashMap<>(), "", false, Integer.MAX_VALUE, false, rows);
  }

  private HoodieWriteConfig getWriteConfig() {
    return HoodieWriteConfig.newBuilder().withPath(HoodieCLI.basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).build();
  }

  private void initJavaSparkContext(Option<String> userDefinedMaster) {
    if (jsc == null) {
      jsc = SparkUtil.initJavaSparkContext(SparkUtil.getDefaultConf("HoodieCLI", userDefinedMaster));
    }
  }
}
