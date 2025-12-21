/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.profile;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for {@link WriteProfile}.
 */
public class WriteProfiles {
  private static final Logger LOG = LoggerFactory.getLogger(WriteProfiles.class);

  private static final Map<String, WriteProfile> PROFILES = new HashMap<>();

  private WriteProfiles() {
  }

  public static synchronized WriteProfile singleton(
      boolean ignoreSmallFiles,
      boolean delta,
      HoodieWriteConfig config,
      HoodieFlinkEngineContext context) {
    return PROFILES.computeIfAbsent(config.getBasePath(),
        k -> getWriteProfile(ignoreSmallFiles, delta, config, context));
  }

  private static WriteProfile getWriteProfile(
      boolean ignoreSmallFiles,
      boolean delta,
      HoodieWriteConfig config,
      HoodieFlinkEngineContext context) {
    if (ignoreSmallFiles) {
      return new EmptyWriteProfile(config, context);
    } else if (delta) {
      return new DeltaWriteProfile(config, context);
    } else {
      return new WriteProfile(config, context);
    }
  }

  public static void clean(String path) {
    PROFILES.remove(path);
  }

  /**
   * Returns all the incremental write file statuses with the given commits metadata.
   * Only existing files are included.
   *
   * @param basePath     Table base path
   * @param hadoopConf   The hadoop conf
   * @param metadataList The commit metadata list (should in ascending order)
   * @param tableType    The table type
   * @return the file status array
   */
  public static List<StoragePathInfo> getFilesFromMetadata(
      Path basePath,
      Configuration hadoopConf,
      List<HoodieCommitMetadata> metadataList,
      HoodieTableType tableType) {
    return getFilesFromMetadata(basePath, hadoopConf, metadataList, tableType, true);
  }

  /**
   * Returns all the incremental write file statuses with the given commits metadata.
   *
   * @param basePath           Table base path
   * @param hadoopConf         The hadoop conf
   * @param metadataList       The commit metadata list (should in ascending order)
   * @param tableType          The table type
   * @param ignoreMissingFiles Whether to ignore the missing files from filesystem
   * @return the file status array or null if any file is missing with ignoreMissingFiles as false
   */
  @Nullable
  public static List<StoragePathInfo> getFilesFromMetadata(
      Path basePath,
      Configuration hadoopConf,
      List<HoodieCommitMetadata> metadataList,
      HoodieTableType tableType,
      boolean ignoreMissingFiles) {
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath.toString(), HadoopFSUtils.getStorageConf(hadoopConf));
    Map<String, StoragePathInfo> uniqueIdToInfoMap = new HashMap<>();
    // If a file has been touched multiple times in the given commits, the return value should keep the one
    // from the latest commit, so here we traverse in reverse order
    for (int i = metadataList.size() - 1; i >= 0; i--) {
      for (Map.Entry<String, StoragePathInfo> entry : getFilesToRead(hadoopConf, metadataList.get(i),
          basePath.toString(), tableType).entrySet()) {
        if (StreamerUtil.isValidFile(entry.getValue())
            && !uniqueIdToInfoMap.containsKey(entry.getKey())) {
          if (StreamerUtil.fileExists(storage, entry.getValue().getPath())) {
            uniqueIdToInfoMap.put(entry.getKey(), entry.getValue());
          } else if (!ignoreMissingFiles) {
            return null;
          }
        }
      }
    }
    return new ArrayList<>(uniqueIdToInfoMap.values());
  }

  private static Map<String, StoragePathInfo> getFilesToRead(
      Configuration hadoopConf,
      HoodieCommitMetadata metadata,
      String basePath,
      HoodieTableType tableType
  ) {
    switch (tableType) {
      case COPY_ON_WRITE:
        return metadata.getFileIdToInfo(basePath);
      case MERGE_ON_READ:
        return metadata.getFullPathToInfo(HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(hadoopConf)), basePath);
      default:
        throw new AssertionError();
    }
  }

  /**
   * Returns the commit metadata of the given instant safely.
   *
   * @param tableName The table name
   * @param basePath  The table base path
   * @param instant   The hoodie instant
   * @param timeline  The timeline
   * @return the commit metadata or empty if any error occurs
   */
  public static Option<HoodieCommitMetadata> getCommitMetadataSafely(
      String tableName,
      Path basePath,
      HoodieInstant instant,
      HoodieTimeline timeline) {
    try {
      return Option.of(timeline.readCommitMetadata(instant));
    } catch (FileNotFoundException fe) {
      // make this fail safe.
      LOG.warn("Instant {} was deleted by the cleaner, ignore", instant.requestedTime());
      return Option.empty();
    } catch (Throwable throwable) {
      LOG.error("Get write metadata for table {} with instant {} and path: {} error",
          tableName, instant.requestedTime(), basePath);
      return Option.empty();
    }
  }

  /**
   * Returns the commit metadata of the given instant.
   *
   * @param tableName The table name
   * @param basePath  The table base path
   * @param instant   The hoodie instant
   * @param timeline  The timeline
   * @return the commit metadata
   */
  public static HoodieCommitMetadata getCommitMetadata(
      String tableName,
      Path basePath,
      HoodieInstant instant,
      HoodieTimeline timeline) {
    try {
      return TimelineUtils.getCommitMetadata(instant, timeline);
    } catch (IOException e) {
      LOG.error("Get write metadata for table {} with instant {} and path: {} error",
          tableName, instant.requestedTime(), basePath);
      throw new HoodieException(e);
    }
  }
}
