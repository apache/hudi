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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.AbstractMigratorBase;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.storage.StoragePath;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compaction V1 has absolute paths as part of compaction operations.
 */
public class CompactionV1MigrationHandler extends AbstractMigratorBase<HoodieCompactionPlan> {

  public static final Integer VERSION = 1;

  public CompactionV1MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCompactionPlan upgradeFrom(HoodieCompactionPlan input) {
    throw new IllegalArgumentException("This is the lowest version. Input cannot be any lower version");
  }

  @Override
  public HoodieCompactionPlan downgradeFrom(HoodieCompactionPlan input) {
    ValidationUtils.checkArgument(input.getVersion() == 2, "Input version is " + input.getVersion() + ". Must be 2");
    HoodieCompactionPlan compactionPlan = new HoodieCompactionPlan();
    final StoragePath basePath = metaClient.getBasePath();
    List<HoodieCompactionOperation> v1CompactionOperationList = new ArrayList<>();
    if (null != input.getOperations()) {
      v1CompactionOperationList = input.getOperations().stream().map(inp ->
        HoodieCompactionOperation.newBuilder().setBaseInstantTime(inp.getBaseInstantTime())
            .setFileId(inp.getFileId()).setPartitionPath(inp.getPartitionPath()).setMetrics(inp.getMetrics())
            .setDataFilePath(convertToV1Path(basePath, inp.getPartitionPath(), inp.getDataFilePath()))
            .setDeltaFilePaths(inp.getDeltaFilePaths().stream()
                .map(s -> convertToV1Path(basePath, inp.getPartitionPath(), s)).collect(Collectors.toList()))
        .build()).collect(Collectors.toList());
    }
    compactionPlan.setOperations(v1CompactionOperationList);
    compactionPlan.setExtraMetadata(input.getExtraMetadata());
    compactionPlan.setVersion(getManagedVersion());
    return compactionPlan;
  }

  private static String convertToV1Path(StoragePath basePath, String partitionPath, String fileName) {
    if ((fileName == null) || (fileName.isEmpty())) {
      return fileName;
    }

    return new StoragePath(FSUtils.constructAbsolutePath(basePath, partitionPath), fileName).toString();
  }
}
