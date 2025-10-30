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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;

/**
 * Flink upgrade and downgrade helper.
 */
public class FlinkUpgradeDowngradeHelper implements SupportsUpgradeDowngrade {

  private static final FlinkUpgradeDowngradeHelper SINGLETON_INSTANCE =
      new FlinkUpgradeDowngradeHelper();

  private FlinkUpgradeDowngradeHelper() {
  }

  public static FlinkUpgradeDowngradeHelper getInstance() {
    return SINGLETON_INSTANCE;
  }

  @Override
  public HoodieTable getTable(HoodieWriteConfig config, HoodieEngineContext context) {
    return HoodieFlinkTable.create(config, context);
  }

  @Override
  public String getPartitionColumns(HoodieWriteConfig config) {
    return config.getProps().getProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
  }

  @Override
  public BaseHoodieWriteClient getWriteClient(HoodieWriteConfig config, HoodieEngineContext context) {
    // Because flink enables reusing of embedded timeline service by default, disable the embedded time service to avoid closing of the reused timeline server.
    // The write config inherits from the info of the currently running timeline server started in coordinator, even though the flag is disabled, it still can
    // access the remote timeline server started before.
    config.setValue(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key(), "false");
    return new HoodieFlinkWriteClient(context, config, false);
  }
}
