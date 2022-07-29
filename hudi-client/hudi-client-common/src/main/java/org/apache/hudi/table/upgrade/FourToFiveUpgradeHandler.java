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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.keygen.KeyGenUtils.OLD_DEPRECATED_DEFAULT_PARTITION_PATH;

/**
 * Upgrade handler to upgrade Hudi's table version from 4 to 5.
 */
public class FourToFiveUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LogManager.getLogger(FourToFiveUpgradeHandler.class);

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    if (FSUtils.getAllPartitionPaths(context, HoodieMetadataConfig.newBuilder().enable(false).build(), config.getBasePath())
        .contains(OLD_DEPRECATED_DEFAULT_PARTITION_PATH)) {
      LOG.error("\"default\" partition detected. From 0.12, we are changing the default partition in hudi to \"__HIVE_DEFAULT_PARTITION__\". "
          + " Please read and write back the data in \"default\" partition in hudi to new partition path \"__HIVE_DEFAULT_PARTITION__\". "
          + "Sample spark command to use: \n"
          + "val df = spark.read.format(\"hudi\").load(HUDI_TABLE_PATH); \t \n"
          + "df.drop(\"_hoodie_commit_time\").drop(\"_hoodie_commit_seqno\").drop(\"_hoodie_record_key\")"
          + ".drop(\"_hoodie_partition_path\").drop(\"_hoodie_file_name\").withColumn(PARTITION_PATH_COLUMN,\"__HIVE_DEFAULT_PARTITION__\")"
          + ".write.options(writeOptions).mode(Append).save(HUDI_TABLE_PATH);\t\n"
          + "Please fix values for PARTITION_PATH_COLUMN, HUDI_TABLE_PATH and set all write configs in above command before running ");
      throw new HoodieException("Old deprecated \"default\" partition found in hudi table. This needs a migration step before we can upgrade ");
    }
    return new HashMap<>();
  }
}
