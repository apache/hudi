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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;

/**
 * Interface for engine-specific logic needed for upgrade and downgrade actions.
 */
public interface SupportsUpgradeDowngrade extends Serializable {
  /**
   * @param config  Write config.
   * @param context {@link HoodieEngineContext} instance to use.
   * @return A new Hudi table for upgrade and downgrade actions.
   */
  HoodieTable getTable(HoodieWriteConfig config, HoodieEngineContext context);

  /**
   * @param config Write config.
   * @return partition columns in String.
   */
  String getPartitionColumns(HoodieWriteConfig config);

  BaseHoodieWriteClient getWriteClient(HoodieWriteConfig config, HoodieEngineContext context);

  default HoodieTable addTxnManager(HoodieTable table) {
    // FIME-vc: This method is deprecated - table should be created with TransactionManager passed directly
    throw new UnsupportedOperationException("Use getTable(config, context) instead which creates table with TransactionManager");
  }
}
