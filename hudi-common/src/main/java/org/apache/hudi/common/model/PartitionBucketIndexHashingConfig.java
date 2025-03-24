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
import org.apache.hudi.common.util.FileIOUtils;
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

  /**
   * Get the absolute path of the specific <instant>.hashing_config path.
   */
  public static StoragePath getHashingConfigPath(String basePath, String instantToLoad) {
    StoragePath hashingBase = getHashingConfigStorageFolder(basePath);
    return new StoragePath(hashingBase, instantToLoad + PartitionBucketIndexHashingConfig.HASHING_CONFIG_FILE_SUFFIX);
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
   * TODO zhangyue19921010 need more test and details java Doc
   */
  public static String getHashingConfigInstantToLoad(HoodieTableMetaClient metaClient) {
    List<String> instants = metaClient.getActiveTimeline().getCompletedReplaceTimeline()
        .getInstants().stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
    Option<HoodieInstant> earliestInstant = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().firstInstant();
    String instantToLoad = "";
    try {
      List<String> hashingConfigInstants = metaClient.getStorage()
          .listDirectEntries(new StoragePath(metaClient.getHashingMetadataConfigPath())).stream().map(info -> {
            String instant = getHashingConfigInstant(info.getPath().getName());
            if (StringUtils.isNullOrEmpty(instant)) {
              throw new HoodieException("Failed to get hashing config instant to load.");
            }
            return instant;
          }).sorted().collect(Collectors.toList());

      for (String instant : hashingConfigInstants) {
        if (!earliestInstant.isPresent()) {
          instantToLoad = instant;
          break;
        } else if (instants.contains(instant)) {
          instantToLoad = instant;
          break;
        } else if (instant.compareTo(earliestInstant.get().requestedTime()) < 0) {
          instantToLoad = instant;
          break;
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

  public static PartitionBucketIndexHashingConfig loadingLatestHashingConfig(HoodieTableMetaClient metaClient) {
    String instantToLoad = getHashingConfigInstantToLoad(metaClient);
    Option<PartitionBucketIndexHashingConfig> latestHashingConfig = loadHashingConfig(metaClient.getStorage(), getHashingConfigPath(metaClient.getBasePath().toString(), instantToLoad));
    ValidationUtils.checkArgument(latestHashingConfig.isPresent(), "Can not load latest hashing config " + instantToLoad);

    return latestHashingConfig.get();
  }

  public static String getHashingConfigInstant(String hashingConfigName) {
    int dotIndex = hashingConfigName.indexOf('.');
    if (dotIndex == -1) {
      return null;
    }
    return hashingConfigName.substring(0, dotIndex);
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
