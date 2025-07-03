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
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES;

public class EightToNineUpgradeHandler implements UpgradeHandler {
  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config,
                                             HoodieEngineContext context,
                                             String instantTime,
                                             SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    String payloadClass = tableConfig.getPayloadClass();

    String partialUpdateProperties = tableConfig.getPartialUpdateProperties();
    if (!StringUtils.isNullOrEmpty(payloadClass)) {
      if (payloadClass.equals(AWSDmsAvroPayload.class.getName())) {
        String propertiesToAdd = DELETE_KEY + "=Op," + DELETE_MARKER + "=D";
        partialUpdateProperties = StringUtils.isNullOrEmpty(partialUpdateProperties)
            ? propertiesToAdd : partialUpdateProperties + "," + partialUpdateProperties;
      } else if (payloadClass.equals(PostgresDebeziumAvroPayload.class.getName())) {
        String propertiesToAdd =
            PARTIAL_UPDATE_CUSTOM_MARKER + "=" + DEBEZIUM_UNAVAILABLE_VALUE;
        partialUpdateProperties = StringUtils.isNullOrEmpty(partialUpdateProperties)
            ? propertiesToAdd : partialUpdateProperties + "," + partialUpdateProperties;
      }
    }

    tablePropsToAdd.put(PARTIAL_UPDATE_PROPERTIES, partialUpdateProperties);
    return tablePropsToAdd;
  }
}
