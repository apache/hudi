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

package org.apache.hudi.common.table.timeline.versioning.clean;

import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.AbstractMigratorBase;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Migration handler for clean plan in version 1.
 */
public class CleanPlanV1MigrationHandler extends AbstractMigratorBase<HoodieCleanerPlan> {

  public static final Integer VERSION = 1;

  public CleanPlanV1MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCleanerPlan upgradeFrom(HoodieCleanerPlan plan) {
    throw new IllegalArgumentException(
        "This is the lowest version. Plan cannot be any lower version");
  }

  @Override
  public HoodieCleanerPlan downgradeFrom(HoodieCleanerPlan plan) {
    if (metaClient.getTableConfig().getBootstrapBasePath().isPresent()) {
      throw new IllegalArgumentException(
          "This version do not support METADATA_ONLY bootstrapped tables. Failed to downgrade.");
    }
    Map<String, List<String>> filesPerPartition = plan.getFilePathsToBeDeletedPerPartition().entrySet().stream()
        .map(e -> Pair.of(e.getKey(), e.getValue().stream().map(v -> new StoragePath(v.getFilePath()).getName())
            .collect(Collectors.toList()))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    return new HoodieCleanerPlan(plan.getEarliestInstantToRetain(), plan.getLastCompletedCommitTimestamp(),
        plan.getPolicy(), filesPerPartition, VERSION, new HashMap<>(), new ArrayList<>(), Collections.EMPTY_MAP);
  }
}
