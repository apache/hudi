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

package com.uber.hoodie.cli.commands;

import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.cli.TableHeader;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.exception.DatasetNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class DatasetsCommand implements CommandMarker {

  @CliCommand(value = "connect", help = "Connect to a hoodie dataset")
  public String connect(
      @CliOption(key = {"path"}, mandatory = true, help = "Base Path of the dataset") final String path)
      throws IOException {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);
    HoodieCLI.setTableMetadata(new HoodieTableMetaClient(HoodieCLI.conf, path));
    HoodieCLI.state = HoodieCLI.CLIState.DATASET;
    return "Metadata for table " + HoodieCLI.tableMetadata.getTableConfig().getTableName() + " loaded";
  }

  /**
   * Create a Hoodie Table if it does not exist
   *
   * @param path         Base Path
   * @param name         Hoodie Table Name
   * @param tableTypeStr Hoodie Table Type
   * @param payloadClass Payload Class
   */
  @CliCommand(value = "create", help = "Create a hoodie table if not present")
  public String createTable(
      @CliOption(key = {"path"}, mandatory = true, help = "Base Path of the dataset") final String path,
      @CliOption(key = {"tableName"}, mandatory = true, help = "Hoodie Table Name") final String name,
      @CliOption(key = {"tableType"}, unspecifiedDefaultValue = "COPY_ON_WRITE",
          help = "Hoodie Table Type. Must be one of : COPY_ON_WRITE or MERGE_ON_READ") final String tableTypeStr,
      @CliOption(key = {"payloadClass"}, unspecifiedDefaultValue = "com.uber.hoodie.common.model.HoodieAvroPayload",
          help = "Payload Class") final String payloadClass) throws IOException {

    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    boolean existing = false;
    try {
      new HoodieTableMetaClient(HoodieCLI.conf, path);
      existing = true;
    } catch (DatasetNotFoundException dfe) {
      // expected
    }

    // Do not touch table that already exist
    if (existing) {
      throw new IllegalStateException("Dataset already existing in path : " + path);
    }

    final HoodieTableType tableType = HoodieTableType.valueOf(tableTypeStr);
    HoodieTableMetaClient.initTableType(HoodieCLI.conf, path, tableType, name, payloadClass);

    // Now connect to ensure loading works
    return connect(path);
  }

  @CliAvailabilityIndicator({"desc"})
  public boolean isDescAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  /**
   * Describes table properties
   */
  @CliCommand(value = "desc", help = "Describle Hoodie Table properties")
  public String descTable() {
    TableHeader header = new TableHeader()
        .addTableHeaderField("Property")
        .addTableHeaderField("Value");
    List<Comparable[]> rows = new ArrayList<>();
    rows.add(new Comparable[]{"basePath", HoodieCLI.tableMetadata.getBasePath()});
    rows.add(new Comparable[]{"metaPath", HoodieCLI.tableMetadata.getMetaPath()});
    rows.add(new Comparable[]{"fileSystem", HoodieCLI.tableMetadata.getFs().getScheme()});
    HoodieCLI.tableMetadata.getTableConfig().getProps().entrySet().forEach(e -> {
      rows.add(new Comparable[]{e.getKey(), e.getValue()});
    });
    return HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
  }
}
