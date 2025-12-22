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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionBucketIndexHashingConfig implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketIndexHashingConfig.class);
  public static final String INITIAL_HASHING_CONFIG_INSTANT = HoodieTimeline.INIT_INSTANT_TS;
  public static final String HASHING_CONFIG_FILE_SUFFIX = ".hashing_config";
  public static final Integer CURRENT_VERSION = 1;
  private final String expressions;
  private final int defaultBucketNumber;
  private final String rule;
  private final int version;
  private final String instant;

  @JsonCreator
  public PartitionBucketIndexHashingConfig(@JsonProperty("expressions") String expressions,
                                           @JsonProperty("defaultBucketNumber") int defaultBucketNumber,
                                           @JsonProperty("rule") String rule,
                                           @JsonProperty("version") int version,
                                           @JsonProperty("instant") String instant) {
    this.expressions = expressions;
    this.defaultBucketNumber = defaultBucketNumber;
    this.rule = rule;
    this.version = version;
    this.instant = instant;
  }

  public String getFilename() {
    return instant + HASHING_CONFIG_FILE_SUFFIX;
  }

  public String toJsonString() throws IOException {
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public String getInstant() {
    return this.instant;
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or somethings bad happen).
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }

  public static PartitionBucketIndexHashingConfig fromBytes(byte[] bytes) throws IOException {
    try {
      return fromJsonString(new String(bytes, StandardCharsets.UTF_8), PartitionBucketIndexHashingConfig.class);
    } catch (Exception e) {
      throw new IOException("unable to load hashing config", e);
    }
  }

  public int getVersion() {
    return version;
  }

  public String getRule() {
    return rule;
  }

  public int getDefaultBucketNumber() {
    return defaultBucketNumber;
  }

  public String getExpressions() {
    return expressions;
  }

  /**
   * Get the absolute path of hashing config meta folder.
   */
  public static StoragePath getHashingConfigStorageFolder(String basePath) {
    StoragePath metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    return new StoragePath(metaPath, HoodieTableMetaClient.BUCKET_INDEX_METAFOLDER_CONFIG_FOLDER);
  }

  public static StoragePath getArchiveHashingConfigStorageFolder(String basePath) {
    StoragePath metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    return new StoragePath(metaPath, HoodieTableMetaClient.BUCKET_INDEX_METAFOLDER_CONFIG_ARCHIVE_FOLDER);
  }

  /**
   * Get the absolute path of the specific <instant>.hashing_config path.
   */
  public static StoragePath getHashingConfigPath(String basePath, String instant) {
    StoragePath hashingBase = getHashingConfigStorageFolder(basePath);
    return new StoragePath(hashingBase, instant + PartitionBucketIndexHashingConfig.HASHING_CONFIG_FILE_SUFFIX);
  }

  public static StoragePath getArchiveHashingConfigPath(String basePath, String instant) {
    StoragePath hashingBase = getArchiveHashingConfigStorageFolder(basePath);
    return new StoragePath(hashingBase, instant + PartitionBucketIndexHashingConfig.HASHING_CONFIG_FILE_SUFFIX);
  }

  /**
   * Create and save <instant>.hashing_config base on given expressions, rule, defaultBucketNumber and instant.
   * If given instant is null or empty, use INITIAL_HASHING_CONFIG_INSTANT.
   */
  public static boolean saveHashingConfig(HoodieTableMetaClient metaClient,
                                          String expressions,
                                          String rule,
                                          int defaultBucketNumber,
                                          String instant) {
    if (StringUtils.isNullOrEmpty(expressions)) {
      return false;
    }
    String hashingInstant = StringUtils.isNullOrEmpty(instant) ? INITIAL_HASHING_CONFIG_INSTANT : instant;
    PartitionBucketIndexHashingConfig hashingConfig =
        new PartitionBucketIndexHashingConfig(expressions, defaultBucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, hashingInstant);
    return saveHashingConfig(hashingConfig, metaClient);
  }

  /**
   * Save given hashing config.
   */

  public static boolean saveHashingConfig(PartitionBucketIndexHashingConfig hashingConfig, HoodieTableMetaClient metaClient) {
    StoragePath hashingConfigPath = new StoragePath(metaClient.getHashingMetadataConfigPath(), hashingConfig.getFilename());
    HoodieStorage storage = metaClient.getStorage();
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

  /**
   * Get Latest committed hashing config instant to load.
   * If instant is empty, then return latest hashing config instant
   */
  public static Option<StoragePath> getHashingConfigToLoad(HoodieTableMetaClient metaClient, Option<String> instant) {
    try {
      String basePath = metaClient.getBasePath().toString();
      List<String> allCommittedHashingConfig = getCommittedHashingConfigInstants(metaClient);
      if (instant.isPresent()) {
        Option<StoragePath> res = getHashingConfigInstantToLoadBeforeOrOn(allCommittedHashingConfig, instant.get()).map(i -> {
          return getHashingConfigPath(basePath, i);
        });
        // fall back to look up archived hashing config instant before return empty
        return res.isPresent() ? res : getHashingConfigInstantToLoadBeforeOrOn(getArchiveHashingConfigInstants(metaClient), instant.get()).map(i -> {
          return getArchiveHashingConfigPath(basePath, i);
        });
      } else {
        return Option.of(allCommittedHashingConfig.get(allCommittedHashingConfig.size() - 1)).map(i -> {
          return getHashingConfigPath(basePath, i);
        });
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to get hashing config instant to load.", e);
    }
  }

  public static List<PartitionBucketIndexHashingConfig> getAllHashingConfig(HoodieTableMetaClient metaClient) throws IOException {
    String basePath = metaClient.getBasePath().toString();
    List<StoragePath> allHashingConfigPaths = getCommittedHashingConfigInstants(metaClient).stream().map(instant -> {
      return getHashingConfigPath(basePath, instant);
    }).collect(Collectors.toList());

    if (metaClient.getStorage().exists(new StoragePath(metaClient.getArchiveHashingMetadataConfigPath()))) {
      allHashingConfigPaths.addAll(getArchiveHashingConfigInstants(metaClient).stream().map(instant -> {
        return getArchiveHashingConfigPath(basePath, instant);
      }).collect(Collectors.toList()));
    }
    return allHashingConfigPaths.stream().map(hashingConfigPath -> {
      return loadHashingConfig(metaClient.getStorage(), hashingConfigPath);
    }).filter(Option::isPresent).map(Option::get).collect(Collectors.toList());
  }

  private static Option<String> getHashingConfigInstantToLoadBeforeOrOn(List<String> hashingConfigInstants, String instant) {
    List<String> res = hashingConfigInstants.stream().filter(hashingConfigInstant -> {
      return hashingConfigInstant.compareTo(instant) <= 0;
    }).collect(Collectors.toList());
    return res.isEmpty() ? Option.empty() : Option.of(res.get(res.size() - 1));
  }

  public static PartitionBucketIndexHashingConfig loadingLatestHashingConfig(HoodieTableMetaClient metaClient) {
    Option<StoragePath> hashingConfigPathToLoad = getHashingConfigToLoad(metaClient, Option.empty());
    ValidationUtils.checkArgument(hashingConfigPathToLoad.isPresent(), "Can not load latest hashing config " + hashingConfigPathToLoad);
    Option<PartitionBucketIndexHashingConfig> latestHashingConfig = loadHashingConfig(metaClient.getStorage(), hashingConfigPathToLoad.get());
    ValidationUtils.checkArgument(latestHashingConfig.isPresent(), "Can not load latest hashing config " + hashingConfigPathToLoad);

    return latestHashingConfig.get();
  }

  public static Option<PartitionBucketIndexHashingConfig> loadingLatestHashingConfigBeforeOrOn(HoodieTableMetaClient metaClient, String instant) {
    Option<StoragePath> hashingConfigPathToLoad = getHashingConfigToLoad(metaClient, Option.of(instant));
    if (hashingConfigPathToLoad.isPresent()) {
      Option<PartitionBucketIndexHashingConfig> latestHashingConfig = loadHashingConfig(metaClient.getStorage(), hashingConfigPathToLoad.get());
      ValidationUtils.checkArgument(latestHashingConfig.isPresent(), "Can not load hashing config " + hashingConfigPathToLoad + " based on " + instant);
      return latestHashingConfig;
    } else {
      return Option.empty();
    }
  }

  /**
   * Archive hashing config.
   */
  public static boolean archiveHashingConfigIfNecessary(HoodieTableMetaClient metaClient) throws IOException {
    List<String> hashingConfigToArchive = getHashingConfigToArchive(metaClient);
    if (hashingConfigToArchive.size() == 0) {
      LOG.info("Nothing to archive " + hashingConfigToArchive);
      return false;
    }

    LOG.info("Start to archive hashing config " + hashingConfigToArchive);
    return archiveHashingConfig(hashingConfigToArchive, metaClient);
  }

  // for now we just remove active hashing config into archive folder
  private static boolean archiveHashingConfig(List<String> hashingConfigToArchive, HoodieTableMetaClient metaClient) {
    hashingConfigToArchive.forEach(instant -> {
      StoragePath activeHashingPath = getHashingConfigPath(metaClient.getBasePath().toString(), instant);
      StoragePath archiveHashingPath = getArchiveHashingConfigPath(metaClient.getBasePath().toString(), instant);
      try {
        metaClient.getStorage().rename(activeHashingPath, archiveHashingPath);
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
    return true;
  }

  public static List<String> getArchiveHashingConfigInstants(HoodieTableMetaClient metaClient) throws IOException {
    return metaClient.getStorage()
        .listDirectEntries(new StoragePath(metaClient.getArchiveHashingMetadataConfigPath())).stream().map(info -> {
          String instant = getHashingConfigInstant(info.getPath().getName());
          if (StringUtils.isNullOrEmpty(instant)) {
            throw new HoodieException("Failed to get hashing config instant to load.");
          }
          return instant;
        }).sorted().collect(Collectors.toList());
  }

  /**
   * Get all commit hashing config.
   * During rollback we will delete hashing config first, then remove related pending instant.
   * So that Listed uncommitted hashing instant always exist in active timeline.
   * **Ascending order**  like 20250325091919474, 20250325091923956, 20250325091927529
   */
  public static List<String> getCommittedHashingConfigInstants(HoodieTableMetaClient metaClient) throws IOException {
    List<String> allActiveHashingConfigInstants = metaClient.getStorage()
        .listDirectEntries(new StoragePath(metaClient.getHashingMetadataConfigPath())).stream().map(info -> {
          String instant = getHashingConfigInstant(info.getPath().getName());
          if (StringUtils.isNullOrEmpty(instant)) {
            throw new HoodieException("Failed to get hashing config instant to load.");
          }
          return instant;
        }).sorted().collect(Collectors.toList());

    HoodieTimeline pendingReplaceTimeline = metaClient.getActiveTimeline().filterPendingReplaceTimeline();
    return allActiveHashingConfigInstants.stream().filter(hashingConfigInstant -> {
      return !pendingReplaceTimeline.containsInstant(hashingConfigInstant);
    }).collect(Collectors.toList());
  }

  /**
   * get hashing config to archive
   * 1. get all committed active hashing config as list1
   * 2. get hashing configs before active timeline start instant based on list1 as list2
   * 3. try to archive list2
   *
   * Always keep at least one committed hashing config
   */
  public static List<String> getHashingConfigToArchive(HoodieTableMetaClient metaClient) throws IOException {
    Option<HoodieInstant> activeTimelineStart = metaClient.getActiveTimeline().getCommitsTimeline().firstInstant();
    if (activeTimelineStart.isPresent()) {
      String startInstant = activeTimelineStart.get().requestedTime();
      List<String> committedHashingConfig = getCommittedHashingConfigInstants(metaClient);
      int index = 0;
      for (; index < committedHashingConfig.size(); index++) {
        if (committedHashingConfig.get(index).compareTo(startInstant) >= 0) {
          break;
        }
      }
      return index == 0 ? Collections.emptyList() : committedHashingConfig.subList(0, index - 1);
    } else {
      return Collections.emptyList();
    }
  }

  public static boolean rollbackHashingConfig(HoodieInstant instant, HoodieTableMetaClient metaClient) {
    try {
      StoragePath path = getHashingConfigPath(metaClient.getBasePath().toString(), instant.requestedTime());
      HoodieStorage storage = metaClient.getStorage();
      if (storage.exists(path)) {
        boolean res = storage.deleteFile(path);
        ValidationUtils.checkArgument(res, "Failed to delete hashing_config " + path);
        LOG.info("Deleted hashing config " + path);
        return true;
      }
      LOG.info("Hashing config " + path + " doesn't exist.");
      return false;
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  public static String getHashingConfigInstant(String hashingConfigName) {
    int dotIndex = hashingConfigName.indexOf('.');
    if (dotIndex == -1) {
      return null;
    }
    return hashingConfigName.substring(0, dotIndex);
  }

  public static boolean isHashingConfigExisted(HoodieInstant instant, HoodieTableMetaClient metaClient) throws IOException {
    StoragePath path = getHashingConfigPath(metaClient.getBasePath().toString(), instant.requestedTime());
    HoodieStorage storage = metaClient.getStorage();
    return storage.exists(path);
  }

  public String toString() {
    return "PartitionBucketIndexHashingConfig{" + "expressions='" + expressions + '\''
        + ", defaultBucketNumber='" + defaultBucketNumber + '\''
        + ", rule='" + rule + '\''
        + ", version='" + version + '\''
        + ", instant=" + instant
        + '}';
  }
}
