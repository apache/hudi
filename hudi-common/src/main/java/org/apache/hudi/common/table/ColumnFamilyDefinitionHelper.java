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
import org.apache.hudi.common.model.ColumnFamilyDefinition;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

  private final HoodieStorage storage;
  private final StoragePath metaPath;

  public ColumnFamilyDefinitionHelper(HoodieStorage storage, StoragePath metaPath) {
    this.storage = storage;
    this.metaPath = metaPath;
  }

  /**
   * Builds column family definitions and writes to file
   *
   * @throws IOException
   */
  public void persistColumnFamilyDefinitions(Schema schema, Map<String, String> conf) throws IOException {
    StoragePath cfdPath = new StoragePath(metaPath, COLUMN_FAMILIES_FILE_NAME);
    checkState(!storage.exists(cfdPath), "Column families are already defined in " + cfdPath);
    Map<String, ColumnFamilyDefinition> cfDefinitions = validateInitialDefinitions(schema, conf);
    if (!cfDefinitions.isEmpty()) {
      Properties properties = new Properties();
      for (Map.Entry<String, ColumnFamilyDefinition> entry : cfDefinitions.entrySet()) {
        properties.put(entry.getKey(), entry.getValue().toConfigValue());
      }
      saveToFile(cfdPath, properties, true);
    }
  }

  private Map<String, ColumnFamilyDefinition> validateInitialDefinitions(Schema schema, Map<String, String> conf) {
    Map<String, ColumnFamilyDefinition> cfDefinitions = HoodieTableConfig.getColumnFamilyDefinitions(conf);
    // todo

    return cfDefinitions;
  }

  /**
   * Returns Option with Map of column definitions
   *
   * @throws IOException
   */
  public Option<Map<String, ColumnFamilyDefinition>> getColumnFamilyDefinitions() throws IOException {
    StoragePath cfdPath = new StoragePath(metaPath, COLUMN_FAMILIES_FILE_NAME);
    StoragePath cfdBackupPath = new StoragePath(metaPath, COLUMN_FAMILIES_BACKUP_NAME);
    if (storage.exists(cfdPath) || storage.exists(cfdBackupPath)) {
      Properties cfProps = fetchConfigs(storage, metaPath, COLUMN_FAMILIES_FILE_NAME, COLUMN_FAMILIES_BACKUP_NAME, MAX_READ_RETRIES, READ_RETRY_DELAY_MSEC);
      Map<String, ColumnFamilyDefinition> cfd = new HashMap<>();
      for (Map.Entry<Object, Object> entry : cfProps.entrySet()) {
        String cfName = String.valueOf(entry.getKey());
        cfd.put(cfName, ColumnFamilyDefinition.fromConfig(cfName, String.valueOf(entry.getValue())));
      }
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
  public void updateColumnFamilyDefinitions(Schema schema, Map<String, String> newCfConf) throws IOException {
    if (!newCfConf.isEmpty()) {
      Option<Map<String, ColumnFamilyDefinition>> cfdOpt = getColumnFamilyDefinitions();
      if (cfdOpt.isPresent()) {
        // validate changes and get resulting map of column family definitions
        Map<String, ColumnFamilyDefinition> cfDefinitions = validateColumnFamilyDefinitions(schema, cfdOpt.get(), newCfConf);

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

  private Map<String, ColumnFamilyDefinition> validateColumnFamilyDefinitions(Schema schema, Map<String, ColumnFamilyDefinition> currentDefinitions, Map<String, String> conf) {
    Map<String, ColumnFamilyDefinition> cfdUpdates = HoodieTableConfig.getColumnFamilyDefinitions(conf);
    // todo

    for (Map.Entry<String, ColumnFamilyDefinition> cfdUpdate: cfdUpdates.entrySet()) {
      if (cfdUpdate.getValue() == null) {
        currentDefinitions.remove(cfdUpdate.getKey());
      } else {
        currentDefinitions.put(cfdUpdate.getKey(), cfdUpdate.getValue());
      }
    }
    return currentDefinitions;
  }

  private void saveToFile(StoragePath cfdPath, Properties properties, boolean overwrite) {
    try (OutputStream out = storage.create(cfdPath, overwrite)) {
      properties.store(out, "Column family definitions saved on " + Instant.now());
    } catch (IOException e) {
      throw new HoodieIOException("Could not save Column Family definitions to " + cfdPath);
    }
  }

}
