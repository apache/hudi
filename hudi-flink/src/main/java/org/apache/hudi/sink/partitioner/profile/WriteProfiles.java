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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
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

  private WriteProfiles() {}

  public static synchronized  WriteProfile singleton(
      boolean overwrite,
      boolean delta,
      HoodieWriteConfig config,
      HoodieFlinkEngineContext context) {
    return PROFILES.computeIfAbsent(config.getBasePath(),
        k -> getWriteProfile(overwrite, delta, config, context));
  }

  private static WriteProfile getWriteProfile(
      boolean overwrite,
      boolean delta,
      HoodieWriteConfig config,
      HoodieFlinkEngineContext context) {
    if (overwrite) {
      return new OverwriteWriteProfile(config, context);
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
   * Returns all the incremental write file path statuses with the given commits metadata.
   *
   * @param basePath     Table base path
   * @param hadoopConf   The hadoop conf
   * @param metadataList The commits metadata
   * @return the file statuses array
   */
  public static FileStatus[] getWritePathsOfInstants(
      Path basePath,
      Configuration hadoopConf,
      List<HoodieCommitMetadata> metadataList) {
    FileSystem fs = FSUtils.getFs(basePath.toString(), hadoopConf);
    return metadataList.stream().map(metadata -> getWritePathsOfInstant(basePath, metadata, fs))
        .flatMap(Collection::stream).toArray(FileStatus[]::new);
  }

  /**
   * Returns the commit file paths with given metadata.
   *
   * @param basePath Table base path
   * @param metadata The metadata
   * @param fs       The filesystem
   *
   * @return the commit file status list
   */
  private static List<FileStatus> getWritePathsOfInstant(Path basePath, HoodieCommitMetadata metadata, FileSystem fs) {
    return metadata.getFileIdAndFullPaths(basePath.toString()).values().stream()
        .map(org.apache.hadoop.fs.Path::new)
        // filter out the file paths that does not exist, some files may be cleaned by
        // the cleaner.
        .filter(path -> {
          try {
            return fs.exists(path);
          } catch (IOException e) {
            LOG.error("Checking exists of path: {} error", path);
            throw new HoodieException(e);
          }
        }).map(path -> {
          try {
            return fs.getFileStatus(path);
          } catch (IOException e) {
            LOG.error("Get write status of path: {} error", path);
            throw new HoodieException(e);
          }
        })
        // filter out crushed files
        .filter(fileStatus -> fileStatus.getLen() > 0)
        .collect(Collectors.toList());
  }

  /**
   * Returns the commit metadata of the given instant.
   *
   * @param tableName The table name
   * @param basePath  The table base path
   * @param instant   The hoodie instant
   * @param timeline  The timeline
   *
   * @return the commit metadata
   */
  public static HoodieCommitMetadata getCommitMetadata(
      String tableName,
      Path basePath,
      HoodieInstant instant,
      HoodieTimeline timeline) {
    byte[] data = timeline.getInstantDetails(instant).get();
    try {
      return HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
    } catch (IOException e) {
      LOG.error("Get write metadata for table {} with instant {} and path: {} error",
          tableName, instant.getTimestamp(), basePath);
      throw new HoodieException(e);
    }
  }
}
