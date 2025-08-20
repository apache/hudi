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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;

public class DowngradeStrategy implements UpgradeDowngradeStrategy {
  private HoodieTableMetaClient metaClient;

  public DowngradeStrategy(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  @Override
  public boolean requiresMigration(HoodieTableVersion toWriteVersion) {
    HoodieTableVersion fromTableVersion = metaClient.getTableConfig().getTableVersion();
    if (toWriteVersion.lesserThan(HoodieTableVersion.SIX)) {
      throw new HoodieUpgradeDowngradeException(
          String.format("Hudi 1.x release only supports table version greater than "
                  + "version 6 or above. Please downgrade table from version %s to %s "
                  + "using a Hudi release prior to 1.0.0",
              HoodieTableVersion.SIX.versionCode(), toWriteVersion.versionCode()));
    }
    return toWriteVersion.lesserThan(fromTableVersion);
  }
}
