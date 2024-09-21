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

package org.apache.hudi.common.table;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.ColumnFamilyDefinition;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieConfig.MAX_READ_RETRIES;
import static org.apache.hudi.common.config.HoodieConfig.READ_RETRY_DELAY_MSEC;
import static org.apache.hudi.common.util.ConfigUtils.fetchConfigs;
import static org.apache.hudi.common.util.ConfigUtils.recoverIfNeeded;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Helper class for all operations on column family definitions (create, modify, validate)
 */
public class ColumnFamilyDefinitionHelper {

  public static final String COLUMN_FAMILIES_FILE_NAME = "column-families.properties";
  public static final String COLUMN_FAMILIES_BACKUP_NAME = "column-families.backup";

  private final HoodieTableMetaClient metaClient;
  private final HoodieStorage storage;
  private final StoragePath metaPath;

  public ColumnFamilyDefinitionHelper(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    this.storage = metaClient.getStorage();
    this.metaPath = metaClient.getMetaPath();
  }

  /**
   * Builds column family definitions and writes to file
   *
   * @throws IOException
   */
  public void persistColumnFamilyDefinitions(Map<String, String> conf) throws Exception {
    StoragePath cfdPath = new StoragePath(metaPath, COLUMN_FAMILIES_FILE_NAME);
    checkState(!storage.exists(cfdPath), "Column families are already defined in " + cfdPath);
    Map<String, ColumnFamilyDefinition> cfDefinitions = validateInitialDefinitions(conf);
    if (!cfDefinitions.isEmpty()) {
      Properties properties = new Properties();
      cfDefinitions.forEach((name, def) -> properties.put(name, def.toConfigValue()));
      saveToFile(cfdPath, properties, true);
    }
  }

  private Map<String, ColumnFamilyDefinition> validateInitialDefinitions(Map<String, String> conf) throws Exception {
    Map<String, ColumnFamilyDefinition> cfDefinitions = HoodieTableConfig.getColumnFamilyDefinitions(conf);
    if (!cfDefinitions.isEmpty()) {
      String primaryKey = conf.get(HoodieTableConfig.RECORDKEY_FIELDS.key());
      List<String> primaryKeys = Collections.emptyList();
      if (!StringUtils.isNullOrEmpty(primaryKey)) {
        primaryKeys = Arrays.asList(primaryKey.split(HoodieConfig.CONFIG_VALUES_DELIMITER));
      }

      Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
      Set<String> usedColumns = new HashSet<>();
      Set<String> unUsedColumns = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());

      for (ColumnFamilyDefinition cfDef : cfDefinitions.values()) {
        if (!cfDef.getColumns().containsAll(primaryKeys)) {
          throw new HoodieValidationException("Column family '" + cfDef.getName()
              + "' doesn't contain primary key [" + String.join(",", primaryKeys) + "]");
        }
        for (String colName : cfDef.getColumns()) {
          if (schema.getField(colName) == null) {
            throw new HoodieValidationException("Unknown column: " + colName);
          } else {
            if (!primaryKeys.contains(colName)) {
              if (usedColumns.contains(colName)) {
                throw new HoodieValidationException("Column '" + colName + "' defined in multiple families");
              } else {
                usedColumns.add(colName);
              }
            }
            unUsedColumns.remove(colName);
          }
        }
      }

      String partitionFields = conf.get(HoodieTableConfig.PARTITION_FIELDS.key());
      if (!StringUtils.isNullOrEmpty(partitionFields)) {
        Arrays.asList(partitionFields.split(HoodieConfig.CONFIG_VALUES_DELIMITER)).forEach(unUsedColumns::remove);
      }
      if (!unUsedColumns.isEmpty()) {
        throw new HoodieValidationException("Some columns are not used in families: " + String.join(", ", unUsedColumns));
      }
    }
    return cfDefinitions;
  }

  /**
   * Returns Option with Map of column definitions
   *
   * @throws IOException
   */
  public Option<Map<String, ColumnFamilyDefinition>> fetchColumnFamilyDefinitions() throws IOException {
    StoragePath cfdPath = new StoragePath(metaPath, COLUMN_FAMILIES_FILE_NAME);
    StoragePath cfdBackupPath = new StoragePath(metaPath, COLUMN_FAMILIES_BACKUP_NAME);
    if (storage.exists(cfdPath) || storage.exists(cfdBackupPath)) {
      Properties cfProps = fetchConfigs(storage, metaPath, COLUMN_FAMILIES_FILE_NAME, COLUMN_FAMILIES_BACKUP_NAME, MAX_READ_RETRIES, READ_RETRY_DELAY_MSEC);
      Map<String, ColumnFamilyDefinition> cfd = new HashMap<>();
      cfProps.forEach((key, value) -> {
        String cfName = String.valueOf(key);
        cfd.put(cfName, ColumnFamilyDefinition.fromConfig(cfName, String.valueOf(value)));
      });
      return Option.of(cfd);
    } else {
      return Option.empty();
    }
  }

  /**
   * Merges existing column family definitions with provided column family's changes and saves results to file
   *
   * @param newCfConf only changes in column families definitions
   * @throws IOException
   */
  public void updateColumnFamilyDefinitions(Map<String, String> newCfConf) throws Exception {
    Map<String, ColumnFamilyDefinition> cfdUpdates = HoodieTableConfig.getColumnFamilyDefinitions(newCfConf);
    if (!cfdUpdates.isEmpty()) {
      Option<Map<String, ColumnFamilyDefinition>> cfdOpt = fetchColumnFamilyDefinitions();
      if (cfdOpt.isPresent()) {
        // validate changes and get resulting map of column family definitions
        Map<String, ColumnFamilyDefinition> cfDefinitions = validateColumnFamilyDefinitions(cfdOpt.get(), cfdUpdates);

        if (!cfDefinitions.isEmpty()) {
          StoragePath cfdPath = new StoragePath(metaPath, COLUMN_FAMILIES_FILE_NAME);
          StoragePath cfdBackupPath = new StoragePath(metaPath, COLUMN_FAMILIES_BACKUP_NAME);
          try {
            // do any recovery from prior attempts
            recoverIfNeeded(storage, cfdPath, cfdBackupPath);
            // read the existing column family properties
            Properties currentProps = fetchConfigs(storage, metaPath, COLUMN_FAMILIES_FILE_NAME, COLUMN_FAMILIES_BACKUP_NAME,
                MAX_READ_RETRIES, READ_RETRY_DELAY_MSEC);
            // backup the existing properties
            saveToFile(cfdBackupPath, currentProps, false);
            // delete the properties file, reads will go to the backup, until we are done
            storage.deleteFile(cfdPath);
            // save new column family definitions
            Properties newProps = new Properties();
            for (Map.Entry<String, ColumnFamilyDefinition> entry : cfDefinitions.entrySet()) {
              newProps.put(entry.getKey(), entry.getValue().toConfigValue());
            }
            saveToFile(cfdPath, newProps, true);
            // delete the backup
            storage.deleteFile(cfdBackupPath);
          } catch (IOException e) {
            throw new HoodieIOException("Could not save Column Family definitions to " + cfdPath);
          }
        }
      }
    }
  }

  private Map<String, ColumnFamilyDefinition> validateColumnFamilyDefinitions(Map<String, ColumnFamilyDefinition> cfdCurrent,
                                                                              Map<String, ColumnFamilyDefinition> cfdUpdates) throws Exception {
    Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    // todo
    for (Map.Entry<String, ColumnFamilyDefinition> cfdUpdate : cfdUpdates.entrySet()) {
      if (cfdUpdate.getValue() == null) {
        if (cfdCurrent.containsKey(cfdUpdate.getKey())) {
          cfdCurrent.remove(cfdUpdate.getKey());
        } else {
          throw new HoodieValidationException("Column family '" + cfdUpdate.getKey() + "' doesn't exist, unable to delete");
        }
      } else {
        cfdCurrent.put(cfdUpdate.getKey(), cfdUpdate.getValue());
      }
    }
    if (cfdCurrent.isEmpty()) {
      throw new HoodieValidationException("Removing all column families is not allowed");
    }
    return cfdCurrent;
  }

  private void saveToFile(StoragePath cfdPath, Properties properties, boolean overwrite) {
    try (OutputStream out = storage.create(cfdPath, overwrite)) {
      properties.store(out, "Column family definitions saved on " + Instant.now());
    } catch (IOException e) {
      throw new HoodieIOException("Could not save Column Family definitions to " + cfdPath);
    }
  }

}
