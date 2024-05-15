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
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.exception.TableNotFoundException;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * CLI command to display hudi table options.
 */
@ShellComponent
public class TableCommand {

  private static final Logger LOG = LoggerFactory.getLogger(TableCommand.class);

  static {
    System.out.println("Table command getting loaded");
  }

  @ShellMethod(key = "connect", value = "Connect to a hoodie table")
  public String connect(
      @ShellOption(value = {"--path"}, help = "Base Path of the table") final String path,
      @ShellOption(value = {"--layoutVersion"}, help = "Timeline Layout version", defaultValue = ShellOption.NULL) Integer layoutVersion,
      @ShellOption(value = {"--eventuallyConsistent"}, defaultValue = "false",
          help = "Enable eventual consistency") final boolean eventuallyConsistent,
      @ShellOption(value = {"--initialCheckIntervalMs"}, defaultValue = "2000",
          help = "Initial wait time for eventual consistency") final Integer initialConsistencyIntervalMs,
      @ShellOption(value = {"--maxWaitIntervalMs"}, defaultValue = "300000",
          help = "Max wait time for eventual consistency") final Integer maxConsistencyIntervalMs,
      @ShellOption(value = {"--maxCheckIntervalMs"}, defaultValue = "7",
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
   * @param path         Base Path
   * @param name         Hoodie Table Name
   * @param tableTypeStr Hoodie Table Type
   * @param payloadClass Payload Class
   */
  @ShellMethod(key = "create", value = "Create a hoodie table if not present")
  public String createTable(
      @ShellOption(value = {"--path"}, help = "Base Path of the table") final String path,
      @ShellOption(value = {"--tableName"}, help = "Hoodie Table Name") final String name,
      @ShellOption(value = {"--tableType"}, defaultValue = "COPY_ON_WRITE",
          help = "Hoodie Table Type. Must be one of : COPY_ON_WRITE or MERGE_ON_READ") final String tableTypeStr,
      @ShellOption(value = {"--archiveLogFolder"}, help = "Folder Name for storing archived timeline",
          defaultValue = ShellOption.NULL) String archiveFolder,
      @ShellOption(value = {"--layoutVersion"}, help = "Specific Layout Version to use",
          defaultValue = ShellOption.NULL) Integer layoutVersion,
      @ShellOption(value = {"--payloadClass"}, defaultValue = "org.apache.hudi.common.model.HoodieAvroPayload",
          help = "Payload Class") final String payloadClass) throws IOException {

    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    boolean existing = false;
    try {
      HoodieTableMetaClient.builder().setConf(HoodieCLI.conf.newInstance()).setBasePath(path).build();
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
        .initTable(HoodieCLI.conf.newInstance(), path);
    // Now connect to ensure loading works
    return connect(path, layoutVersion, false, 0, 0, 0);
  }

  /**
   * Describes table properties.
   */
  @ShellMethod(key = "desc", value = "Describe Hoodie Table properties")
  public String descTable() {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    TableHeader header = new TableHeader().addTableHeaderField("Property").addTableHeaderField("Value");
    List<Comparable[]> rows = new ArrayList<>();
    rows.add(new Comparable[] {"basePath", client.getBasePath()});
    rows.add(new Comparable[] {"metaPath", client.getMetaPath()});
    rows.add(new Comparable[] {"fileSystem", client.getStorage().getScheme()});
    client.getTableConfig().propsMap().entrySet().forEach(e -> {
      rows.add(new Comparable[] {e.getKey(), e.getValue()});
    });
    return HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
  }

  /**
   * Refresh table metadata.
   */
  @ShellMethod(key = {"refresh", "metadata refresh", "commits refresh", "cleans refresh", "savepoints refresh"},
      value = "Refresh table metadata")
  public String refreshMetadata() {
    HoodieCLI.refreshTableMetadata();
    return "Metadata for table " + HoodieCLI.getTableMetaClient().getTableConfig().getTableName() + " refreshed.";
  }

  /**
   * Fetches table schema in avro format.
   */
  @ShellMethod(key = "fetch table schema", value = "Fetches latest table schema")
  public String fetchTableSchema(
      @ShellOption(value = {"--outputFilePath"}, defaultValue = ShellOption.NULL,
              help = "File path to write schema") final String outputFilePath) throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(client);
    Schema schema = tableSchemaResolver.getTableAvroSchema();
    if (outputFilePath != null) {
      LOG.info("Latest table schema : " + schema.toString(true));
      writeToFile(outputFilePath, schema.toString(true));
      return String.format("Latest table schema written to %s", outputFilePath);
    } else {
      return String.format("Latest table schema %s", schema.toString(true));
    }
  }

  @ShellMethod(key = "table recover-configs", value = "Recover table configs, from update/delete that failed midway.")
  public String recoverTableConfig() throws IOException {
    HoodieCLI.refreshTableMetadata();
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    HoodieTableConfig.recover(client.getStorage(), client.getMetaPath());
    return descTable();
  }

  @ShellMethod(key = "table update-configs", value = "Update the table configs with configs with provided file.")
  public String updateTableConfig(
      @ShellOption(value = {"--props-file"}, help = "Path to a properties file on local filesystem")
      final String updatePropsFilePath) throws IOException {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    Map<String, String> oldProps = client.getTableConfig().propsMap();

    Properties updatedProps = new Properties();
    try (FileInputStream fileInputStream = new FileInputStream(updatePropsFilePath)) {
      updatedProps.load(fileInputStream);
    }
    HoodieTableConfig.update(client.getStorage(), client.getMetaPath(), updatedProps);

    HoodieCLI.refreshTableMetadata();
    Map<String, String> newProps = HoodieCLI.getTableMetaClient().getTableConfig().propsMap();
    return renderOldNewProps(newProps, oldProps);
  }

  @ShellMethod(key = "table delete-configs", value = "Delete the supplied table configs from the table.")
  public String deleteTableConfig(
      @ShellOption(value = {"--comma-separated-configs"},
              help = "Comma separated list of configs to delete.") final String csConfigs) {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    Map<String, String> oldProps = client.getTableConfig().propsMap();

    Set<String> deleteConfigs = Arrays.stream(csConfigs.split(",")).collect(Collectors.toSet());
    HoodieTableConfig.delete(client.getStorage(), client.getMetaPath(), deleteConfigs);

    HoodieCLI.refreshTableMetadata();
    Map<String, String> newProps = HoodieCLI.getTableMetaClient().getTableConfig().propsMap();
    return renderOldNewProps(newProps, oldProps);
  }

  private static String renderOldNewProps(Map<String, String> newProps, Map<String, String> oldProps) {
    TreeSet<String> allPropKeys = new TreeSet<>();
    allPropKeys.addAll(newProps.keySet().stream().map(Object::toString).collect(Collectors.toSet()));
    allPropKeys.addAll(oldProps.keySet());

    String[][] rows = new String[allPropKeys.size()][];
    int ind = 0;
    for (String propKey : allPropKeys) {
      String[] row = new String[] {
          propKey,
          oldProps.getOrDefault(propKey, "null"),
          newProps.getOrDefault(propKey, "null")
      };
      rows[ind++] = row;
    }
    return HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_HOODIE_PROPERTY,
        HoodieTableHeaderFields.HEADER_OLD_VALUE, HoodieTableHeaderFields.HEADER_NEW_VALUE}, rows);
  }

  /**
   * Use Streams when you are dealing with raw data.
   *
   * @param filePath output file path.
   * @param data     to be written to file.
   */
  private static void writeToFile(String filePath, String data) throws IOException {
    File outFile = new File(filePath);
    if (outFile.exists()) {
      outFile.delete();
    }
    try (OutputStream os = new FileOutputStream(outFile)) {
      os.write(getUTF8Bytes(data), 0, data.length());
    }
  }
}
