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
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.TableNotFoundException;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * CLI command to display hudi table options.
 */
@Component
public class TableCommand implements CommandMarker {

  static {
    System.out.println("Table command getting loaded");
  }

  @CliCommand(value = "connect", help = "Connect to a hoodie table")
  public String connect(
      @CliOption(key = {"path"}, mandatory = true, help = "Base Path of the table") final String path,
      @CliOption(key = {"layoutVersion"}, help = "Timeline Layout version") Integer layoutVersion,
      @CliOption(key = {"eventuallyConsistent"}, unspecifiedDefaultValue = "false",
          help = "Enable eventual consistency") final boolean eventuallyConsistent,
      @CliOption(key = {"initialCheckIntervalMs"}, unspecifiedDefaultValue = "2000",
          help = "Initial wait time for eventual consistency") final Integer initialConsistencyIntervalMs,
      @CliOption(key = {"maxWaitIntervalMs"}, unspecifiedDefaultValue = "300000",
          help = "Max wait time for eventual consistency") final Integer maxConsistencyIntervalMs,
      @CliOption(key = {"maxCheckIntervalMs"}, unspecifiedDefaultValue = "7",
          help = "Max checks for eventual consistency") final Integer maxConsistencyChecks)
      throws IOException {
    HoodieCLI
        .setConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(eventuallyConsistent)
            .withInitialConsistencyCheckIntervalMs(initialConsistencyIntervalMs)
            .withMaxConsistencyCheckIntervalMs(maxConsistencyIntervalMs).withMaxConsistencyChecks(maxConsistencyChecks)
            .build());
    HoodieCLI.initConf();
    HoodieCLI.connectTo(path, layoutVersion);
    HoodieCLI.initFS(true);
    HoodieCLI.state = HoodieCLI.CLIState.TABLE;
    return "Metadata for table " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " loaded";
  }

  /**
   * Create a Hoodie Table if it does not exist.
   *
   * @param path Base Path
   * @param name Hoodie Table Name
   * @param tableTypeStr Hoodie Table Type
   * @param payloadClass Payload Class
   */
  @CliCommand(value = "create", help = "Create a hoodie table if not present")
  public String createTable(
      @CliOption(key = {"path"}, mandatory = true, help = "Base Path of the table") final String path,
      @CliOption(key = {"tableName"}, mandatory = true, help = "Hoodie Table Name") final String name,
      @CliOption(key = {"tableType"}, unspecifiedDefaultValue = "COPY_ON_WRITE",
          help = "Hoodie Table Type. Must be one of : COPY_ON_WRITE or MERGE_ON_READ") final String tableTypeStr,
      @CliOption(key = {"archiveLogFolder"}, help = "Folder Name for storing archived timeline") String archiveFolder,
      @CliOption(key = {"layoutVersion"}, help = "Specific Layout Version to use") Integer layoutVersion,
      @CliOption(key = {"payloadClass"}, unspecifiedDefaultValue = "org.apache.hudi.common.model.HoodieAvroPayload",
          help = "Payload Class") final String payloadClass) throws IOException {

    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    boolean existing = false;
    try {
      HoodieTableMetaClient.builder().setConf(HoodieCLI.conf).setBasePath(path).build();
      existing = true;
    } catch (TableNotFoundException dfe) {
      // expected
    }

    // Do not touch table that already exist
    if (existing) {
      throw new IllegalStateException("Table already existing in path : " + path);
    }

    HoodieTableMetaClient.withPropertyBuilder()
      .setTableType(tableTypeStr)
      .setTableName(name)
      .setArchiveLogFolder(archiveFolder)
      .setPayloadClassName(payloadClass)
      .setTimelineLayoutVersion(layoutVersion)
      .initTable(HoodieCLI.conf, path);
    // Now connect to ensure loading works
    return connect(path, layoutVersion, false, 0, 0, 0);
  }

  /**
   * Describes table properties.
   */
  @CliCommand(value = "desc", help = "Describe Hoodie Table properties")
  public String descTable() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    TableHeader header = new TableHeader().addTableHeaderField("Property").addTableHeaderField("Value");
    List<Comparable[]> rows = new ArrayList<>();
    rows.add(new Comparable[] {"basePath", client.getBasePath()});
    rows.add(new Comparable[] {"metaPath", client.getMetaPath()});
    rows.add(new Comparable[] {"fileSystem", client.getFs().getScheme()});
    client.getTableConfig().getProps().entrySet().forEach(e -> {
      rows.add(new Comparable[] {e.getKey(), e.getValue()});
    });
    return HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
  }

  /**
   * Refresh table metadata.
   */
  @CliCommand(value = {"refresh", "metadata refresh", "commits refresh", "cleans refresh", "savepoints refresh"},
      help = "Refresh table metadata")
  public String refreshMetadata() {
    HoodieCLI.refreshTableMetadata();
    return "Metadata for table " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " refreshed.";
  }
}
