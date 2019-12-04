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

package org.apache.hudi.common.versioning.clean;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.versioning.AbstractMigratorBase;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.stream.Collectors;

public class CleanV1MigrationHandler extends AbstractMigratorBase<HoodieCleanMetadata> {

  public static final Integer VERSION = 1;

  public CleanV1MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCleanMetadata upgradeFrom(HoodieCleanMetadata input) {
    throw new IllegalArgumentException(
        "This is the lowest version. Input cannot be any lower version");
  }

  @Override
  public HoodieCleanMetadata downgradeFrom(HoodieCleanMetadata input) {
    Preconditions.checkArgument(input.getVersion() == 2,
        "Input version is " + input.getVersion() + ". Must be 2");
    final Path basePath = new Path(metaClient.getBasePath());

    final Map<String, HoodieCleanPartitionMetadata> partitionMetadataMap = input
        .getPartitionMetadata()
        .entrySet().stream().map(entry -> {
          final String partitionPath = entry.getKey();
          final HoodieCleanPartitionMetadata partitionMetadata = entry.getValue();

          HoodieCleanPartitionMetadata cleanPartitionMetadata = HoodieCleanPartitionMetadata
              .newBuilder()
              .setDeletePathPatterns(partitionMetadata.getDeletePathPatterns().stream()
                  .map(
                      path -> convertToV1Path(basePath, partitionMetadata.getPartitionPath(), path))
                  .collect(Collectors.toList()))
              .setSuccessDeleteFiles(partitionMetadata.getSuccessDeleteFiles().stream()
                  .map(
                      path -> convertToV1Path(basePath, partitionMetadata.getPartitionPath(), path))
                  .collect(Collectors.toList())).setPartitionPath(partitionPath)
              .setFailedDeleteFiles(partitionMetadata.getFailedDeleteFiles().stream()
                  .map(
                      path -> convertToV1Path(basePath, partitionMetadata.getPartitionPath(), path))
                  .collect(Collectors.toList()))
              .setPolicy(partitionMetadata.getPolicy()).setPartitionPath(partitionPath)
              .build();
          return Pair.of(partitionPath, cleanPartitionMetadata);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    HoodieCleanMetadata metadata = HoodieCleanMetadata.newBuilder()
        .setEarliestCommitToRetain(input.getEarliestCommitToRetain())
        .setStartCleanTime(input.getStartCleanTime())
        .setTimeTakenInMillis(input.getTimeTakenInMillis())
        .setTotalFilesDeleted(input.getTotalFilesDeleted())
        .setPartitionMetadata(partitionMetadataMap)
        .setVersion(getManagedVersion()).build();

    return metadata;

  }

  private static String convertToV1Path(Path basePath, String partitionPath, String fileName) {
    if ((fileName == null) || (fileName.isEmpty())) {
      return fileName;
    }

    return new Path(FSUtils.getPartitionPath(basePath, partitionPath), fileName).toString();
  }
}
