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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseOneToTwoUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    tablePropsToAdd.put(HoodieTableConfig.HOODIE_TABLE_NAME_PROP, config.getTableName());
    tablePropsToAdd.put(HoodieTableConfig.HOODIE_TABLE_PARTITION_FIELDS_PROP, getPartitionColumns(config));
    tablePropsToAdd.put(HoodieTableConfig.HOODIE_TABLE_RECORDKEY_FIELDS, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD.key()));
    tablePropsToAdd.put(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP, config.getString(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP));
    return tablePropsToAdd;
  }

  abstract String getPartitionColumns(HoodieWriteConfig config);
}
