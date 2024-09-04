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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.AbstractMigratorBase;

import java.util.Collections;

/**
 * Migration handler for clean metadata in version 3.
 */
public class CleanMetadataV3MigrationHandler extends AbstractMigratorBase<HoodieCleanMetadata> {

  public static final Integer VERSION = 3;

  public CleanMetadataV3MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCleanMetadata upgradeFrom(HoodieCleanMetadata input) {
    if (input.getVersion() == 1) {
      return CleanMetadataV2MigrationHandler.upgradeFromV2(input, metaClient, VERSION);
    }
    return HoodieCleanMetadata.newBuilder()
        .setEarliestCommitToRetain(input.getEarliestCommitToRetain())
        .setLastCompletedCommitTimestamp(input.getLastCompletedCommitTimestamp())
        .setStartCleanTime(input.getStartCleanTime())
        .setTimeTakenInMillis(input.getTimeTakenInMillis())
        .setTotalFilesDeleted(input.getTotalFilesDeleted())
        .setPartitionMetadata(input.getPartitionMetadata())
        .setExtraMetadata(input.getExtraMetadata() == null ? Collections.emptyMap() : input.getExtraMetadata())
        .setVersion(VERSION).build();
  }

  @Override
  public HoodieCleanMetadata downgradeFrom(HoodieCleanMetadata input) {
    throw new IllegalArgumentException(
        "This is the current highest version. Input cannot be any higher version");
  }
}
