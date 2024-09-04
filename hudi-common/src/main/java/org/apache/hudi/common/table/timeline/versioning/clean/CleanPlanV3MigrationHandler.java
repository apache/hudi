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

package org.apache.hudi.common.table.timeline.versioning.clean;

import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.AbstractMigratorBase;

import java.util.Collections;

/**
 * Migration handler for clean plan in version 3.
 */
public class CleanPlanV3MigrationHandler extends AbstractMigratorBase<HoodieCleanerPlan> {

  public static final Integer VERSION = 3;

  public CleanPlanV3MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCleanerPlan upgradeFrom(HoodieCleanerPlan input) {
    if (input.getVersion() == 1) {
      return CleanPlanV2MigrationHandler.upgradeFromV2(input, metaClient, VERSION);
    }
    return new HoodieCleanerPlan(input.getEarliestInstantToRetain(), input.getLastCompletedCommitTimestamp(),
        input.getPolicy(), input.getFilesToBeDeletedPerPartition(), VERSION, input.getFilePathsToBeDeletedPerPartition(),
        input.getPartitionsToBeDeleted(), input.getExtraMetadata() == null ? Collections.emptyMap() : input.getExtraMetadata());
  }

  @Override
  public HoodieCleanerPlan downgradeFrom(HoodieCleanerPlan input) {
    throw new IllegalArgumentException(
        "This is the current highest version. Plan cannot be any higher version");
  }
}
