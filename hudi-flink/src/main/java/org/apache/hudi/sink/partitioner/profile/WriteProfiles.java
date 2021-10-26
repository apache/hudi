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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
   *
   * @param basePath     Table base path
   * @param hadoopConf   The hadoop conf
   * @param metadataList The commits metadata
   * @param tableType    The table type
   * @return the file status array
   */
  public static FileStatus[] getWritePathsOfInstants(
      Path basePath,
      Configuration hadoopConf,
      List<HoodieCommitMetadata> metadataList,
      HoodieTableType tableType) {
    FileSystem fs = FSUtils.getFs(basePath.toString(), hadoopConf);
    Map<String, FileStatus> uniqueIdToFileStatus = new HashMap<>();
    metadataList.forEach(metadata ->
            uniqueIdToFileStatus.putAll(getFilesToReadOfInstant(basePath, metadata, fs, tableType)));
    return uniqueIdToFileStatus.values().toArray(new FileStatus[0]);
  }

  /**
   * Returns the commit file status info with given metadata.
   *
   * @param basePath  Table base path
   * @param metadata  The metadata
   * @param fs        The filesystem
   * @param tableType The table type
   * @return the commit file status info grouping by specific ID
   */
  private static Map<String, FileStatus> getFilesToReadOfInstant(
      Path basePath,
      HoodieCommitMetadata metadata,
      FileSystem fs,
      HoodieTableType tableType) {
    return getFilesToRead(metadata, basePath.toString(), tableType).entrySet().stream()
        // filter out the file paths that does not exist, some files may be cleaned by
        // the cleaner.
        .filter(entry -> {
          try {
            return fs.exists(entry.getValue().getPath());
          } catch (IOException e) {
            LOG.error("Checking exists of path: {} error", entry.getValue().getPath());
            throw new HoodieException(e);
          }
        })
        .filter(entry -> StreamerUtil.isValidFile(entry.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Map<String, FileStatus> getFilesToRead(
      HoodieCommitMetadata metadata,
      String basePath,
      HoodieTableType tableType) {
    switch (tableType) {
      case COPY_ON_WRITE:
        return metadata.getFileIdToFileStatus(basePath);
      case MERGE_ON_READ:
        return metadata.getFullPathToFileStatus(basePath);
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
      byte[] data = timeline.getInstantDetails(instant).get();
      return Option.of(HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class));
    } catch (FileNotFoundException fe) {
      // make this fail safe.
      LOG.warn("Instant {} was deleted by the cleaner, ignore", instant.getTimestamp());
      return Option.empty();
    } catch (Throwable throwable) {
      LOG.error("Get write metadata for table {} with instant {} and path: {} error",
          tableName, instant.getTimestamp(), basePath);
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
      return HoodieInputFormatUtils.getCommitMetadata(instant, timeline);
    } catch (IOException e) {
      LOG.error("Get write metadata for table {} with instant {} and path: {} error",
          tableName, instant.getTimestamp(), basePath);
      throw new HoodieException(e);
    }
  }
}
