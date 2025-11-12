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

/**
 * There is no format change between table version 6 and 7.
 * Table version 7 is meant for required bridge release upgrade before upgrading to 1.0.
 * Version 7 is going to be placeholder version for bridge release 0.16.0.
 * Version 8 is the placeholder version to track 1.x.
 */
public class SixToSevenUpgradeHandler implements UpgradeHandler {

  @Override
  public UpgradeDowngrade.TableConfigChangeSet upgrade(HoodieWriteConfig config,
                                                                         HoodieEngineContext context,
                                                                         String instantTime,
                                                                         SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    return new UpgradeDowngrade.TableConfigChangeSet();
  }
}
