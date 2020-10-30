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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieMetadataConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataReader;
import org.apache.hudi.metadata.HoodieMetadataWriter;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * CLI commands to operate on the Metadata Table.
 */
@Component
public class MetadataCommand implements CommandMarker {
  private JavaSparkContext jsc;

  @CliCommand(value = "metadata set", help = "Set options for Metadata Table")
  public String set(@CliOption(key = {"metadataDir"},
      help = "Directory to read/write metadata table (can be different from dataset)", unspecifiedDefaultValue = "")
      final String metadataDir) {
    if (!metadataDir.isEmpty()) {
      HoodieMetadataReader.setMetadataBaseDirectory(metadataDir);
    }

    return String.format("Ok");
  }

  @CliCommand(value = "metadata create", help = "Create the Metadata Table if it does not exist")
  public String create() throws IOException {
    HoodieCLI.getTableMetaClient();
    Path metadataPath = new Path(HoodieMetadataReader.getMetadataTableBasePath(HoodieCLI.basePath));
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
    HoodieMetadataWriter.create(HoodieCLI.conf, writeConfig).initialize(jsc);
    return String.format("Created Metadata Table in %s (duration=%.2f secs)", metadataPath, timer.endTimer() / 1000.0);
  }

  @CliCommand(value = "metadata delete", help = "Remove the Metadata Table")
  public String delete() throws Exception {
    HoodieCLI.getTableMetaClient();
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    Path metadataPath = new Path(HoodieMetadataReader.getMetadataTableBasePath(HoodieCLI.basePath));
    try {
      FileStatus[] statuses = HoodieCLI.fs.listStatus(metadataPath);
      if (statuses.length > 0) {
        HoodieCLI.fs.delete(metadataPath, true);
      }
    } catch (FileNotFoundException e) {
      // Metadata directory does not exist
    }

    return String.format("Removed Metdata Table from %s", metadataPath);
  }

  @CliCommand(value = "metadata init", help = "Update the metadata table from commits since the creation")
  public String init(@CliOption(key = {"readonly"}, unspecifiedDefaultValue = "false",
      help = "Open in read-only mode") final boolean readOnly) throws Exception {
    HoodieCLI.getTableMetaClient();
    Path metadataPath = new Path(HoodieMetadataReader.getMetadataTableBasePath(HoodieCLI.basePath));
    try {
      HoodieCLI.fs.listStatus(metadataPath);
    } catch (FileNotFoundException e) {
      // Metadata directory does not exist
      throw new RuntimeException("Metadata directory (" + metadataPath.toString() + ") does not exist.");
    }

    long t1 = System.currentTimeMillis();
    if (readOnly) {
      //HoodieMetadata.init(HoodieCLI.conf, HoodieCLI.basePath);
    } else {
      HoodieWriteConfig writeConfig = getWriteConfig();
      initJavaSparkContext();
      HoodieMetadataWriter.create(HoodieCLI.conf, writeConfig).initialize(jsc);
    }
    long t2 = System.currentTimeMillis();

    String action = readOnly ? "Opened" : "Initialized";
    return String.format(action + " Metadata Table in %s (duration=%.2fsec)", metadataPath, (t2 - t1) / 1000.0);
  }

  @CliCommand(value = "metadata stats", help = "Print stats about the metadata")
  public String stats() throws IOException {
    HoodieCLI.getTableMetaClient();
    HoodieMetadataReader metaReader = new HoodieMetadataReader(HoodieCLI.conf, HoodieCLI.basePath, "/tmp", true, false);
    Map<String, String> stats = metaReader.getStats(true);

    StringBuffer out = new StringBuffer("\n");
    out.append(String.format("Base path: %s\n", HoodieMetadataReader.getMetadataTableBasePath(HoodieCLI.basePath)));
    for (Map.Entry<String, String> entry : stats.entrySet()) {
      out.append(String.format("%s: %s\n", entry.getKey(), entry.getValue()));
    }

    return out.toString();
  }

  @CliCommand(value = "metadata list-partitions", help = "Print a list of all partitions from the metadata")
  public String listPartitions() throws IOException {
    HoodieCLI.getTableMetaClient();
    HoodieMetadataReader metaReader = new HoodieMetadataReader(HoodieCLI.conf, HoodieCLI.basePath, "/tmp", true, false);

    StringBuffer out = new StringBuffer("\n");
    if (!metaReader.enabled()) {
      out.append("=== Metadata Table not initilized. Using file listing to get list of partitions. ===\n\n");
    }

    long t1 = System.currentTimeMillis();
    List<String> partitions = metaReader.getAllPartitionPaths(HoodieCLI.fs, HoodieCLI.basePath, false);
    long t2 = System.currentTimeMillis();

    int[] count = {0};
    partitions.stream().sorted((p1, p2) -> p2.compareTo(p1)).forEach(p -> {
      out.append(p);
      if (++count[0] % 15 == 0) {
        out.append("\n");
      } else {
        out.append(", ");
      }
    });

    out.append(String.format("\n\n=== List of partitions retrieved in %.2fsec ===", (t2 - t1) / 1000.0));

    return out.toString();
  }

  @CliCommand(value = "metadata list-files", help = "Print a list of all files in a partition from the metadata")
  public String listFiles(
      @CliOption(key = {"partition"}, help = "Name of the partition to list files", mandatory = true)
      final String partition) throws IOException {
    HoodieCLI.getTableMetaClient();
    HoodieMetadataReader metaReader = new HoodieMetadataReader(HoodieCLI.conf, HoodieCLI.basePath, "/tmp", true, false);

    StringBuffer out = new StringBuffer("\n");
    if (!metaReader.enabled()) {
      out.append("=== Metadata Table not initilized. Using file listing to get list of files in partition. ===\n\n");
    }

    long t1 = System.currentTimeMillis();
    FileStatus[] statuses = metaReader.getAllFilesInPartition(HoodieCLI.conf, HoodieCLI.basePath,
        new Path(HoodieCLI.basePath, partition));
    long t2 = System.currentTimeMillis();

    Arrays.stream(statuses).sorted((p1, p2) -> p2.getPath().getName().compareTo(p1.getPath().getName())).forEach(p -> {
      out.append("\t" + p.getPath().getName());
      out.append("\n");
    });

    out.append(String.format("\n=== Files in partition retrieved in %.2fsec ===", (t2 - t1) / 1000.0));

    return out.toString();
  }

  private HoodieWriteConfig getWriteConfig() {
    return HoodieWriteConfig.newBuilder().withPath(HoodieCLI.basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).build();
  }

  private void initJavaSparkContext() {
    if (jsc == null) {
      jsc = SparkUtil.initJavaSparkConf("HoodieClI");
    }
  }
}