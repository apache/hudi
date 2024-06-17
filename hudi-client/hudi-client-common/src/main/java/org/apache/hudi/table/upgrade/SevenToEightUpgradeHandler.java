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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

/**
 * Version 7 is going to be placeholder version for bridge release 0.16.0.
 * Version 8 is the placeholder version to track 1.x.
 */
public class SevenToEightUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context,
                                             String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTableConfig tableConfig = upgradeDowngradeHelper.getTable(config, context).getMetaClient().getTableConfig();

    if (tableConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ)) {
      // Record merge mode is required to dictate the merging behavior in version 8,
      // playing the same role as the payload class config in version 7.
      // Inferring of record merge mode from payload class here.
      String payloadClassName = tableConfig.getPayloadClass();
      String propToAdd;
      if (null != payloadClassName) {
        if (payloadClassName.equals(OverwriteWithLatestAvroPayload.class.getName())) {
          propToAdd = RecordMergeMode.OVERWRITE_WITH_LATEST.toString();
        } else if (payloadClassName.equals(DefaultHoodieRecordPayload.class.getName())) {
          propToAdd = RecordMergeMode.EVENT_TIME_ORDERING.toString();
        } else {
          propToAdd = RecordMergeMode.CUSTOM.toString();
        }
      } else {
        propToAdd =  RecordMergeMode.CUSTOM.toString();
      }
      ValidationUtils.checkState(null != propToAdd, String.format("Couldn't infer (%s) from (%s) class name",
          HoodieTableConfig.RECORD_MERGE_MODE.key(), payloadClassName));

      Map<ConfigProperty, String> tablePropsToAdd = new Hashtable<>();
      tablePropsToAdd.put(HoodieTableConfig.RECORD_MERGE_MODE, propToAdd);

      return tablePropsToAdd;
    } else {
      return Collections.emptyMap();
    }
  }
}
