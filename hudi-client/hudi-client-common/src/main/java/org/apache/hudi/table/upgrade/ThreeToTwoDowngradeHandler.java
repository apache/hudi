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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.Collections;
import java.util.Map;

/**
 * Downgrade handler to assist in downgrading hoodie table from version 3 to 2.
 */
public class ThreeToTwoDowngradeHandler implements DowngradeHandler {

  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    if (config.isMetadataTableEnabled()) {
      // Metadata Table in version 3 is synchronous and in version 2 is asynchronous. Downgrading to asynchronous
      // removes the checks in code to decide whether to use a LogBlock or not. Also, the schema for the
      // table has been updated and is not forward compatible. Hence, we need to delete the table.
      HoodieTableMetadataUtil.deleteMetadataTable(config.getBasePath(), context);
    }
    return Collections.emptyMap();
  }
}
