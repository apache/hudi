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

package org.apache.hudi.common.table.timeline.versioning;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Migrates a specific metadata type stored in .hoodie folder to latest version.
 * 
 * @param <T>
 */
public class MetadataMigrator<T> {

  private final Map<Integer, VersionMigrator<T>> migrators;
  private final Integer latestVersion;
  private final Integer oldestVersion;

  public MetadataMigrator(HoodieTableMetaClient metaClient, List<VersionMigrator<T>> migratorList) {
    migrators = migratorList.stream().map(m -> Pair.of(m.getManagedVersion(), m))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    latestVersion = migrators.keySet().stream().reduce((x, y) -> x > y ? x : y).get();
    oldestVersion = migrators.keySet().stream().reduce((x, y) -> x < y ? x : y).get();
  }

  /**
   * Upgrade Metadata version to its latest.
   * 
   * @param metadata Metadata
   * @param metadataVersion Current version of metadata
   * @return Metadata conforming to the latest version of this metadata
   */
  public T upgradeToLatest(T metadata, int metadataVersion) {
    if (metadataVersion == latestVersion) {
      return metadata;
    }

    int newVersion = metadataVersion + 1;
    while (newVersion <= latestVersion) {
      VersionMigrator<T> upgrader = migrators.get(newVersion);
      metadata = upgrader.upgradeFrom(metadata);
      newVersion += 1;
    }
    return metadata;
  }

  /**
   * Migrate metadata to a specific version.
   * 
   * @param metadata Hoodie Table Meta Client
   * @param metadataVersion Metadata Version
   * @param targetVersion Target Version
   * @return Metadata conforming to the target version
   */
  public T migrateToVersion(T metadata, int metadataVersion, int targetVersion) {
    ValidationUtils.checkArgument(targetVersion >= oldestVersion);
    ValidationUtils.checkArgument(targetVersion <= latestVersion);
    if (metadataVersion == targetVersion) {
      return metadata;
    } else if (metadataVersion > targetVersion) {
      return downgradeToVersion(metadata, metadataVersion, targetVersion);
    } else {
      return upgradeToVersion(metadata, metadataVersion, targetVersion);
    }
  }

  private T upgradeToVersion(T metadata, int metadataVersion, int targetVersion) {
    int newVersion = metadataVersion + 1;
    while (newVersion <= targetVersion) {
      VersionMigrator<T> upgrader = migrators.get(newVersion);
      metadata = upgrader.upgradeFrom(metadata);
      newVersion += 1;
    }
    return metadata;
  }

  private T downgradeToVersion(T metadata, int metadataVersion, int targetVersion) {
    int newVersion = metadataVersion - 1;
    while (newVersion >= targetVersion) {
      VersionMigrator<T> downgrader = migrators.get(newVersion);
      metadata = downgrader.downgradeFrom(metadata);
      newVersion -= 1;
    }
    return metadata;
  }

}
