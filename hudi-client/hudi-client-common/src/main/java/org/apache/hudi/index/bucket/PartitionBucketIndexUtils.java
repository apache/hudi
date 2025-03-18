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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class PartitionBucketIndexUtils {
  public static final String INITIAL_HASHING_CONFIG_INSTANT = HoodieTimeline.INIT_INSTANT_TS;

  private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketIndexUtils.class);

  public static boolean isPartitionSimpleBucketIndex(Configuration conf) {
    StoragePath storagePath = new StoragePath(HoodieTableMetaClient.PARTITION_BUCKET_INDEX_HASHING_FOLDER);
    try (HoodieHadoopStorage storage = new HoodieHadoopStorage(storagePath, HadoopFSUtils.getStorageConf(conf))) {
      return storage.exists(storagePath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to list PARTITION_BUCKET_INDEX_HASHING_FOLDER folder ", e);
    }
  }

  public static boolean initHashingConfig(HoodieTableMetaClient metaClient,
                                          String expressions,
                                          String rule,
                                          int defaultBucketNumber,
                                          String instant) {
    if (StringUtils.isNullOrEmpty(expressions)) {
      return false;
    }
    String hashingInstant = StringUtils.isNullOrEmpty(instant) ? INITIAL_HASHING_CONFIG_INSTANT : instant;
    HoodieStorage storage = metaClient.getStorage();
    PartitionBucketIndexHashingConfig hashingConfig =
        new PartitionBucketIndexHashingConfig(expressions, defaultBucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, hashingInstant);
    StoragePath hashingConfigPath = new StoragePath(metaClient.getHashingMetadataConfigPath(), hashingConfig.getFilename());

    try {
      Option<byte []> content = Option.of(hashingConfig.toJsonString().getBytes(StandardCharsets.UTF_8));
      storage.createImmutableFileInPath(hashingConfigPath, content.map(HoodieInstantWriter::convertByteArrayToWriter));
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to initHashingConfig ", ioe);
    }
    return true;
  }

  public static Option<PartitionBucketIndexHashingConfig> loadHashingConfig(HoodieStorage storage, StoragePathInfo hashingConfig) {
    return loadHashingConfig(storage, hashingConfig.getPath());
  }

  public static Option<PartitionBucketIndexHashingConfig> loadHashingConfig(HoodieStorage storage, StoragePath hashingConfig) {
    if (hashingConfig == null) {
      return Option.empty();
    }
    try (InputStream is = storage.open(hashingConfig)) {
      byte[] content = FileIOUtils.readAsByteArray(is);
      return Option.of(PartitionBucketIndexHashingConfig.fromBytes(content));
    } catch (IOException e) {
      LOG.error("Error when loading hashing config, for path: " + hashingConfig.getName(), e);
      throw new HoodieIOException("Error while loading hashing config", e);
    }
  }

  public static String getHashingConfigInstantToLoad(HoodieTableMetaClient metaClient) {
    List<String> instants = metaClient.getActiveTimeline().getCompletedReplaceTimeline()
        .getInstants().stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
    Option<HoodieInstant> earliestInstant = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().firstInstant();
    String instantToLoad = "";
    try {
      // 按时间降序排列
      List<String> hashingConfigInstants = metaClient.getStorage()
          .listDirectEntries(new StoragePath(metaClient.getHashingMetadataConfigPath())).stream().map(info -> {
            String instant = getHashingConfigInstant(info.getPath().getName());
            if (StringUtils.isNullOrEmpty(instant)) {
              throw new HoodieException("Failed to get hashing config instant to load.");
            }
            return instant;
          }).sorted().collect(Collectors.toList());

      for (String instant : hashingConfigInstants) {
        if (instants.contains(instant)) {
          instantToLoad = instant;
          break;
        } else if (earliestInstant.isPresent() && instant.compareTo(earliestInstant.get().requestedTime()) < 0){
          instantToLoad = instant;
        }
      }

      if (StringUtils.isNullOrEmpty(instantToLoad)) {
        throw new HoodieException("Failed to get hashing config instant to load.");
      }

      return instantToLoad;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get hashing config instant to load.", e);
    }
  }

  public static String getHashingConfigInstant(String hashingConfig) {
    int lastIndex = hashingConfig.lastIndexOf('/');
    if (lastIndex == -1) {
      return null;
    }
    String fileName = hashingConfig.substring(lastIndex + 1);
    int dotIndex = fileName.indexOf('.');
    if (dotIndex == -1) {
      return null;
    }
    return fileName.substring(0, dotIndex);
  }
}
