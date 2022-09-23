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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.AbstractMigratorBase;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CleanMetadataV2MigrationHandler extends AbstractMigratorBase<HoodieCleanMetadata> {

  public static final Integer VERSION = 2;

  public CleanMetadataV2MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCleanMetadata upgradeFrom(HoodieCleanMetadata input) {
    ValidationUtils.checkArgument(input.getVersion() == 1,
        "Input version is " + input.getVersion() + ". Must be 1");

    Map<String, HoodieCleanPartitionMetadata> partitionMetadataMap = input.getPartitionMetadata()
        .entrySet()
        .stream().map(entry -> {
          final String partitionPath = entry.getKey();
          final HoodieCleanPartitionMetadata partitionMetadata = entry.getValue();

          final List<String> deletePathPatterns = convertToV2Path(
              partitionMetadata.getDeletePathPatterns());
          final List<String> successDeleteFiles = convertToV2Path(
              partitionMetadata.getSuccessDeleteFiles());
          final List<String> failedDeleteFiles = convertToV2Path(
              partitionMetadata.getFailedDeleteFiles());

          final HoodieCleanPartitionMetadata cleanPartitionMetadata = HoodieCleanPartitionMetadata
              .newBuilder().setPolicy(partitionMetadata.getPolicy())
              .setPartitionPath(partitionMetadata.getPartitionPath())
              .setDeletePathPatterns(deletePathPatterns)
              .setSuccessDeleteFiles(successDeleteFiles)
              .setFailedDeleteFiles(failedDeleteFiles).build();

          return Pair.of(partitionPath, cleanPartitionMetadata);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    return HoodieCleanMetadata.newBuilder()
        .setEarliestCommitToRetain(input.getEarliestCommitToRetain())
        .setLastCompletedCommitTimestamp(input.getLastCompletedCommitTimestamp())
        .setStartCleanTime(input.getStartCleanTime())
        .setTimeTakenInMillis(input.getTimeTakenInMillis())
        .setTotalFilesDeleted(input.getTotalFilesDeleted())
        .setPartitionMetadata(partitionMetadataMap).setVersion(getManagedVersion()).build();
  }

  @Override
  public HoodieCleanMetadata downgradeFrom(HoodieCleanMetadata input) {
    throw new IllegalArgumentException(
        "This is the current highest version. Input cannot be any higher version");
  }

  private List<String> convertToV2Path(List<String> paths) {
    return paths.stream().map(path -> new Path(path).getName())
        .collect(Collectors.toList());
  }
}
