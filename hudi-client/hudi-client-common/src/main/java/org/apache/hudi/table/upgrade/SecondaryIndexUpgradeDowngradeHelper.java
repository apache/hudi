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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.index.HoodieIndexUtils.dropMDTPartitions;

/**
 * Helper class to handle secondary index operations during upgrade/downgrade.
 */
public class SecondaryIndexUpgradeDowngradeHelper {
  private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexUpgradeDowngradeHelper.class);

  /**
   * Drops secondary index partitions from metadata table.
   *
   * @param config Write config
   * @param context Engine context
   * @param table Hoodie table
   * @param operationType Type of operation (upgrade/downgrade)
   */
  public static void dropSecondaryIndexPartitions(HoodieWriteConfig config, HoodieEngineContext context,
      HoodieTable table, String operationType) {
    dropMDTPartitions(MetadataPartitionType.SECONDARY_INDEX, config, context, table, operationType);
  }
}
