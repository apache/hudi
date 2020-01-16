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

package org.apache.hudi.utilities.util;

import org.apache.hudi.utilities.model.TableConfig;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Used for parsing custom files having TableConfig objects.
 */
public class DFSTablePropertiesConfiguration {

  private static volatile Logger log = LogManager.getLogger(DFSTablePropertiesConfiguration.class);

  private final FileSystem fs;
  private List<TableConfig> configs;

  public DFSTablePropertiesConfiguration(FileSystem fs, Path rootFile) {
    this.fs = fs;
    this.configs = new ArrayList<>();
    visitFile(rootFile);
  }

  private void visitFile(Path file) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
      addProperties(reader);
    } catch (IOException ioe) {
      log.error("Error reading in properies from dfs", ioe);
      throw new IllegalArgumentException("Cannot read properties from dfs", ioe);
    }
  }

  private void addProperties(BufferedReader reader) throws IOException {
    try {
      String line;
      TableConfig config = null;
      while ((line = reader.readLine()) != null) {
        if (line.trim().startsWith("[") || line.trim().startsWith("]") || line.equals("") || line.trim().startsWith("#")) {
          continue;
        } else if (line.trim().startsWith("}")) {
          if (config == null) {
            throw new IllegalArgumentException("Custom props file is not formatted properly!");
          }
          configs.add(config);
          config = null;
          continue;
        } else if (line.trim().startsWith("{")) {
          config = new TableConfig();
          continue;
        } else if (!line.contains(":")) {
          continue;
        }

        splitAndPopulateProp(line, config);
      }
    } finally {
      reader.close();
    }
  }

  private void splitAndPopulateProp(String line, TableConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Custom props file is not formatted properly!");
    }
    int ind = line.indexOf(':');
    String key = line.substring(0, ind).trim();
    key = unwrapStringFromQuotes(key);
    String value = line.substring(ind + 1).trim();
    if (value.charAt(value.length() - 1) == ',') {
      value = value.substring(0, value.length() - 1);
    }
    value = unwrapStringFromQuotes(value);
    populateProp(config, key, value);
  }

  private String unwrapStringFromQuotes(String str) {
    if (str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"') {
      return str.substring(1, str.length() - 1);
    }
    return str;
  }

  /**
   * This function lists all the topic level overrides from TableConfig object and
   * matches them with the supplied key to populate TableConfig object.
   * @param config
   * @param key
   * @param value
   */
  private void populateProp(TableConfig config, String key, String value) {
    switch (key) {
      case "database":
        config.setDatabase(value);
        break;
      case "table_name":
        config.setTableName(value);
        break;
      case "primary_key_field":
        config.setPrimaryKeyField(value);
        break;
      case "partition_key_field":
        config.setPartitionKeyField(value);
        break;
      case "kafka_topic":
        config.setTopic(value);
        break;
      case "timestamp_type":
        config.setPartitionTimestampType(value);
        break;
      case "partition_input_format":
        config.setPartitionInputFormat(value);
        break;
      case "hive_sync_db":
        config.setHiveSyncDatabase(value);
        break;
      case "hive_sync_table":
        config.setHiveSyncTable(value);
        break;
      case "key_generator_class":
        config.setKeyGeneratorClassName(value);
        break;
      case "use_pre_apache_input_format":
        config.setUsePreApacheInputFormatForHiveSync(Boolean.valueOf(value));
        break;
      case "assume_date_partitioning":
        config.setAssumeDatePartitioningForHiveSync(Boolean.valueOf(value));
        break;
      default:
        log.error("Incorrect key encountered: " + key);
    }
  }

  public List<TableConfig> getConfigs() {
    return this.configs;
  }
}
