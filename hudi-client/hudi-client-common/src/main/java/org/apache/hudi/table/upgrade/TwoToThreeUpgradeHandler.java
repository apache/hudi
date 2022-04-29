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
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.Hashtable;
import java.util.Map;

/**
 * UpgradeHandler to assist in upgrading {@link org.apache.hudi.table.HoodieTable} from version 2 to 3.
 */
public class TwoToThreeUpgradeHandler implements UpgradeHandler {
  public static final String SPARK_SIMPLE_KEY_GENERATOR = "org.apache.hudi.keygen.SimpleKeyGenerator";

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    if (config.isMetadataTableEnabled()) {
      // Metadata Table in version 2 is asynchronous and in version 3 is synchronous. Synchronous table will not
      // sync any instants not already synced. So its simpler to re-bootstrap the table. Also, the schema for the
      // table has been updated and is not backward compatible.
      HoodieTableMetadataUtil.deleteMetadataTable(config.getBasePath(), context);
    }
    Map<ConfigProperty, String> tablePropsToAdd = new Hashtable<>();
    tablePropsToAdd.put(HoodieTableConfig.URL_ENCODE_PARTITIONING, config.getStringOrDefault(HoodieTableConfig.URL_ENCODE_PARTITIONING));
    tablePropsToAdd.put(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE, config.getStringOrDefault(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE));
    String keyGenClassName = Option.ofNullable(config.getString(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME))
        .orElse(config.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME));
    if (keyGenClassName == null && config.getEngineType() == EngineType.SPARK) {
      // For Spark, if the key generator class is not configured by user,
      // set it to SimpleKeyGenerator as default
      keyGenClassName = SPARK_SIMPLE_KEY_GENERATOR;
    }
    ValidationUtils.checkState(keyGenClassName != null, String.format("Missing config: %s or %s",
        HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, HoodieWriteConfig.KEYGENERATOR_CLASS_NAME));
    tablePropsToAdd.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, keyGenClassName);
    return tablePropsToAdd;
  }
}
