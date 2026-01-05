/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH;

/**
 * Upgrade handler to upgrade Hudi's table version from 4 to 5.
 */
@Slf4j
public class FourToFiveUpgradeHandler implements UpgradeHandler {

  @Override
  public UpgradeDowngrade.TableConfigChangeSet upgrade(HoodieWriteConfig config,
                                                                         HoodieEngineContext context,
                                                                         String instantTime,
                                                                         SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    try {
      HoodieTable table = upgradeDowngradeHelper.getTable(config, context);

      if (!config.doSkipDefaultPartitionValidation() && hasDefaultPartitionPath(config, table)) {
        log.error(String.format("\"%s\" partition detected. From 0.12, we are changing the default partition in hudi to \"%s\"."
                + " Please read and write back the data in \"%s\" partition in hudi to new partition path \"%s\". \"\n"
                + "Sample spark command to use to re-write the data: \n\n"
                + "val df = spark.read.format(\"hudi\").load(HUDI_TABLE_PATH).filter(col(\"PARTITION_PATH_COLUMN\") === \"%s\"); \t \n\n"
                + "df.drop(\"_hoodie_commit_time\").drop(\"_hoodie_commit_seqno\").drop(\"_hoodie_record_key\")\n"
                + " .drop(\"_hoodie_partition_path\").drop(\"_hoodie_file_name\").withColumn(PARTITION_PATH_COLUMN,\"%s\")\n"
                + " .write.format(\"hudi\").options(writeOptions).mode(Append).save(HUDI_TABLE_PATH);\n\n"
                + "Please fix values for PARTITION_PATH_COLUMN, HUDI_TABLE_PATH and set all write configs in above command before running. "
                + "Also do delete the records in old partition once above command succeeds. "
                + "Sample spark command to delete old partition records: \n\n"
                + "val df = spark.read.format(\"hudi\").load(HUDI_TABLE_PATH).filter(col(\"PARTITION_PATH_COLUMN\") === \"%s\");\n\n"
                + "df.write.format(\"hudi\").option(\"hoodie.datasource.write.operation\",\"delete\").options(writeOptions).mode(Append).save(HUDI_TABLE_PATH);\t\n\n",
            DEPRECATED_DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH, DEPRECATED_DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH,
            DEPRECATED_DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH, DEPRECATED_DEFAULT_PARTITION_PATH));
        throw new HoodieException(String.format("Old deprecated \"%s\" partition found in hudi table. This needs a migration step before we can upgrade ",
            DEPRECATED_DEFAULT_PARTITION_PATH));
      }
      return new UpgradeDowngrade.TableConfigChangeSet();
    } catch (IOException e) {
      log.error("Fetching file system instance failed", e);
      throw new HoodieException("Fetching FileSystem instance failed ", e);
    }
  }

  private boolean hasDefaultPartitionPath(HoodieWriteConfig config, HoodieTable table) throws IOException {
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    if (!tableConfig.isTablePartitioned()) {
      return false;
    }
    String checkPartitionPath = DEPRECATED_DEFAULT_PARTITION_PATH;
    boolean hiveStylePartitioningEnable = Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable());
    // dt=default/ht=default, only need check dt=default
    if (hiveStylePartitioningEnable) {
      String[] partitions = tableConfig.getPartitionFields().get();
      checkPartitionPath = partitions[0] + "=" + DEPRECATED_DEFAULT_PARTITION_PATH;
    }

    return table.getStorage().exists(new StoragePath(config.getBasePath() + "/" + checkPartitionPath));
  }
}
