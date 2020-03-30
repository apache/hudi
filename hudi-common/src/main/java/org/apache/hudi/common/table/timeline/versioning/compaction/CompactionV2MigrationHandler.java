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

package org.apache.hudi.common.table.timeline.versioning.compaction;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.AbstractMigratorBase;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * With version 2 of compaction plan, paths are no longer absolute.
 */
public class CompactionV2MigrationHandler extends AbstractMigratorBase<HoodieCompactionPlan> {

  public static final Integer VERSION = 2;

  public CompactionV2MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCompactionPlan upgradeFrom(HoodieCompactionPlan input) {
    ValidationUtils.checkArgument(input.getVersion() == 1, "Input version is " + input.getVersion() + ". Must be 1");
    HoodieCompactionPlan compactionPlan = new HoodieCompactionPlan();
    List<HoodieCompactionOperation> v2CompactionOperationList = new ArrayList<>();
    if (null != input.getOperations()) {
      v2CompactionOperationList = input.getOperations().stream().map(inp ->
        HoodieCompactionOperation.newBuilder().setBaseInstantTime(inp.getBaseInstantTime())
            .setFileId(inp.getFileId()).setPartitionPath(inp.getPartitionPath()).setMetrics(inp.getMetrics())
            .setDataFilePath(inp.getDataFilePath() == null ? null : new Path(inp.getDataFilePath()).getName()).setDeltaFilePaths(
                inp.getDeltaFilePaths().stream().map(s -> new Path(s).getName()).collect(Collectors.toList()))
        .build()).collect(Collectors.toList());
    }
    compactionPlan.setOperations(v2CompactionOperationList);
    compactionPlan.setExtraMetadata(input.getExtraMetadata());
    compactionPlan.setVersion(getManagedVersion());
    return compactionPlan;
  }

  @Override
  public HoodieCompactionPlan downgradeFrom(HoodieCompactionPlan input) {
    throw new IllegalArgumentException("This is the current highest version. Input cannot be any higher version");
  }
}
