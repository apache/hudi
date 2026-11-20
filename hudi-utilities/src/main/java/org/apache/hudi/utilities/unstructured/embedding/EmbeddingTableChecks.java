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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;

import java.io.IOException;

/** The embedding table's start-up guards, read from its table config: whether it exists and its table version. */
public final class EmbeddingTableChecks {

  /** Lowest table version the embedding write path supports (its pinned merge configuration). */
  public static final HoodieTableVersion MIN_TABLE_VERSION = HoodieTableVersion.NINE;

  private EmbeddingTableChecks() {
  }

  /**
   * Whether the embedding table exists: its {@code hoodie.properties}, or the backup Hudi reads it from while a table
   * config update has moved it away ({@link HoodieTableConfig#update} deletes the file before rewriting it; a crash in
   * that window leaves only the backup). A table in that state is not new.
   */
  public static boolean tableExists(String basePath, StorageConfiguration<?> storageConf) {
    try {
      StoragePath metaFolder = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
      HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, storageConf);
      return storage.exists(new StoragePath(metaFolder, HoodieTableConfig.HOODIE_PROPERTIES_FILE))
          || storage.exists(new StoragePath(metaFolder, HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP));
    } catch (IOException e) {
      throw new HoodieIOException("Failed checking embedding table at " + basePath, e);
    }
  }

  /**
   * Fails fast, before any work, when the embedding table would be written below table version 9: the job's
   * resolved {@code hoodie.write.table.version} (its default included), or an existing table whose
   * {@code hoodie.properties} carries a lower {@code hoodie.table.version}. The embedding write path writes VECTOR columns and
   * creates its tables at version 9 or above; an existing table below 9 is refused even with {@code hoodie.write.auto.upgrade}
   * on: it was not built by the embedding write path, and upgrading it in place is not supported.
   *
   * @throws EmbeddingTableVersionException naming the version key, its value and the table, also for a value that is
   *     no number
   */
  public static void checkTableVersion(String basePath, StorageConfiguration<?> storageConf,
                                       EmbeddingTableConfig config) {
    int writeTableVersion;
    try {
      writeTableVersion = ConfigUtils.getIntWithAltKeys(config.getProps(), HoodieWriteConfig.WRITE_TABLE_VERSION);
    } catch (NumberFormatException e) {
      throw new EmbeddingTableVersionException("Embedding table " + basePath + " refuses "
          + HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=" + ConfigUtils.getStringWithAltKeys(config.getProps(),
          HoodieWriteConfig.WRITE_TABLE_VERSION, (String) null) + ": not a table version; unset it or set "
          + MIN_TABLE_VERSION.versionCode() + " or above. Nothing was written");
    }
    if (writeTableVersion < MIN_TABLE_VERSION.versionCode()) {
      throw new EmbeddingTableVersionException("Embedding table " + basePath + " refuses "
          + HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=" + writeTableVersion + ": the embedding write path requires "
          + "table version " + MIN_TABLE_VERSION.versionCode() + " or above; unset it or set "
          + MIN_TABLE_VERSION.versionCode() + ". Nothing was written");
    }
    if (!tableExists(basePath, storageConf)) {
      return;
    }
    HoodieTableConfig table = HoodieTableMetaClient.builder().setConf(storageConf.newInstance())
        .setBasePath(basePath).build().getTableConfig();
    HoodieTableVersion tableVersion = table.getTableVersion();
    if (!tableVersion.greaterThanOrEquals(MIN_TABLE_VERSION)) {
      throw new EmbeddingTableVersionException("Embedding table " + basePath + " is at "
          + HoodieTableConfig.VERSION.key() + "=" + tableVersion.versionCode() + " but the embedding write path requires "
          + "table version " + MIN_TABLE_VERSION.versionCode() + " or above; build a NEW embedding table. Nothing was "
          + "written");
    }
    UnstructuredSourceContract.embeddingTablePins("Embedding prep", config.getProps()).checkTable(basePath, table);
  }
}
