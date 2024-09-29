/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sync;

import ai.onehouse.api.AsyncHttpClientWithRetry;
import ai.onehouse.api.OnehouseApiClient;
import ai.onehouse.config.Config;
import ai.onehouse.config.ConfigProvider;
import ai.onehouse.config.models.common.FileSystemConfiguration;
import ai.onehouse.config.models.common.GCSConfig;
import ai.onehouse.config.models.common.OnehouseClientConfig;
import ai.onehouse.config.models.common.S3Config;
import ai.onehouse.config.models.configv1.ConfigV1;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.MetadataExtractorConfig;
import ai.onehouse.config.models.configv1.ParserConfig;
import ai.onehouse.metadata_extractor.ActiveTimelineInstantBatcher;
import ai.onehouse.metadata_extractor.HoodiePropertiesReader;
import ai.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import ai.onehouse.metadata_extractor.TableDiscoveryService;
import ai.onehouse.metadata_extractor.TableMetadataUploaderService;
import ai.onehouse.metadata_extractor.TimelineCommitInstantsUploader;
import ai.onehouse.metrics.LakeViewExtractorMetrics;
import ai.onehouse.metrics.Metrics;
import ai.onehouse.storage.AsyncStorageClient;
import ai.onehouse.storage.GCSAsyncStorageClient;
import ai.onehouse.storage.PresignedUrlFileUploader;
import ai.onehouse.storage.S3AsyncStorageClient;
import ai.onehouse.storage.StorageUtils;
import ai.onehouse.storage.providers.GcsClientProvider;
import ai.onehouse.storage.providers.S3AsyncClientProvider;
import com.google.common.annotations.VisibleForTesting;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hudi.sync.LakeviewSyncConfigHolder.LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS;
import static org.apache.hudi.sync.LakeviewSyncConfigHolder.LAKEVIEW_METADATA_EXTRACTOR_PATH_EXCLUSION_PATTERNS;

public class LakeviewSyncTool extends HoodieSyncTool implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LakeviewSyncTool.class);
  private static final Pattern LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS_PATTERN = Pattern.compile("([^.]+)\\.databases\\.([^.]+)\\.basePaths");
  // TODO: configs
  private static final int HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS = 15;
  private static final int HTTP_CLIENT_MAX_RETRIES = 3;
  private static final long HTTP_CLIENT_RETRY_DELAY_MS = 1000;

  private final boolean isLakeviewSyncToolEnabled;
  @Nullable
  private final Config config;
  @Nullable
  private final ExecutorService executorService;
  @Nullable
  private final TableDiscoveryAndUploadJob tableDiscoveryAndUploadJob;

  public LakeviewSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    this.isLakeviewSyncToolEnabled = Boolean.TRUE.toString().equalsIgnoreCase(
        props.getOrDefault(LakeviewSyncConfigHolder.LAKEVIEW_SYNC_ENABLED.key(),
            LakeviewSyncConfigHolder.LAKEVIEW_SYNC_ENABLED.defaultValue()).toString());
    if (isLakeviewSyncToolEnabled) {
      this.config = getConfig(props);
      this.executorService = Executors.newFixedThreadPool(1);
      this.tableDiscoveryAndUploadJob = getTableDiscoveryAndUploadJob(this.config, this.executorService);
    } else {
      this.config = null;
      this.executorService = null;
      this.tableDiscoveryAndUploadJob = null;
    }
  }

  private Config getConfig(Properties props) {
    MetadataExtractorConfig metadataExtractorConfig = MetadataExtractorConfig.builder()
        .parserConfig(getParserConfig())
        .pathExclusionPatterns(getPathsToExclude(props))
        .jobRunMode(MetadataExtractorConfig.JobRunMode.ONCE)
        .build();
    OnehouseClientConfig onehouseClientConfig = OnehouseClientConfig.builder()
        .projectId(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_PROJECT_ID.key()))
        .apiKey(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_API_KEY.key()))
        .apiSecret(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_API_SECRET.key()))
        .userId(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_USERID.key()))
        .build();
    FileSystemConfiguration fileSystemConfiguration = getFileSystemConfiguration(props);
    return ConfigV1.builder()
        .version(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_VERSION.key(),
            LakeviewSyncConfigHolder.LAKEVIEW_VERSION.defaultValue()))
        .metadataExtractorConfig(metadataExtractorConfig)
        .onehouseClientConfig(onehouseClientConfig)
        .fileSystemConfiguration(fileSystemConfiguration)
        .build();
  }

  private FileSystemConfiguration getFileSystemConfiguration(Properties props) {
    FileSystemConfiguration.FileSystemConfigurationBuilder fileSystemConfigurationBuilder = FileSystemConfiguration.builder();
    Optional<S3Config> s3Config = getS3Config(props);
    if (s3Config.isPresent()) {
      fileSystemConfigurationBuilder.s3Config(s3Config.get());
    } else {
      Optional<GCSConfig> gcsConfig = getGCSConfig(props);
      if (gcsConfig.isPresent()) {
        fileSystemConfigurationBuilder.gcsConfig(gcsConfig.get());
      } else {
        String errorMessage = "Couldn't find any properties related to file system";
        LOG.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      }
    }
    return fileSystemConfigurationBuilder.build();
  }

  private Optional<S3Config> getS3Config(Properties props) {
    String region = props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_S3_REGION.key());
    if (!StringUtils.isNullOrEmpty(region)) {
      return Optional.of(S3Config.builder()
          .region(region)
          .accessKey(Optional.ofNullable(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_S3_ACCESS_KEY.key())))
          .accessSecret(Optional.ofNullable(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_S3_ACCESS_SECRET.key())))
          .build());
    } else {
      return Optional.empty();
    }
  }

  private Optional<GCSConfig> getGCSConfig(Properties props) {
    String gcsProjectId = props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_GCS_PROJECT_ID.key());
    if (!StringUtils.isNullOrEmpty(gcsProjectId)) {
      return Optional.of(GCSConfig.builder()
          .projectId(Optional.of(gcsProjectId))
          .gcpServiceAccountKeyPath(Optional.ofNullable(props.getProperty(LakeviewSyncConfigHolder.LAKEVIEW_GCS_SERVICE_ACCOUNT_KEY_PATH.key())))
          .build());
    } else {
      return Optional.empty();
    }
  }

  private Optional<List<String>> getPathsToExclude(Properties props) {
    String pathsToExclude = props.getProperty(LAKEVIEW_METADATA_EXTRACTOR_PATH_EXCLUSION_PATTERNS.key());
    if (StringUtils.isNullOrEmpty(pathsToExclude)) {
      return Optional.empty();
    } else {
      return Optional.of(Arrays.stream(pathsToExclude.split(","))
          .filter(entry -> !entry.isEmpty())
          .collect(Collectors.toList()));
    }
  }

  private List<ParserConfig> getParserConfig() {
    Map<String, ParserConfig> lakeNameToParserConfig = new HashMap<>();
    props.forEach((key, value) -> {
      if (key.toString().startsWith(LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS.key())) {
        String currentKey = key.toString();
        currentKey = currentKey.substring(LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS.key().length() + 1);
        Matcher matcher = LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS_PATTERN.matcher(currentKey);
        if (matcher.find()) {
          String lakeName = matcher.group(1);
          String databaseName = matcher.group(2);
          List<String> tableBasePaths = Arrays.asList(value.toString().split(","));

          ParserConfig currentParserConfig = lakeNameToParserConfig
              .computeIfAbsent(lakeName, lake -> ParserConfig.builder().lake(lake).databases(new ArrayList<>()).build());
          Database database = Database.builder().name(databaseName).basePaths(tableBasePaths).build();
          currentParserConfig.getDatabases().add(database);
        } else {
          LOG.warn("Couldn't parse lakes/databases from {}={}", key, value);
        }
      }
    });
    return new ArrayList<>(lakeNameToParserConfig.values());
  }

  private TableDiscoveryAndUploadJob getTableDiscoveryAndUploadJob(@Nonnull Config config,
                                                                   @Nonnull ExecutorService executorService) {
    StorageUtils storageUtils = new StorageUtils();
    AsyncStorageClient asyncStorageClient = getAsyncStorageClient(config, executorService, storageUtils);
    ConfigProvider configProvider = new ConfigProvider(config);
    TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(asyncStorageClient, storageUtils,
        configProvider, executorService);
    LakeViewExtractorMetrics lakeViewExtractorMetrics = new LakeViewExtractorMetrics(Metrics.getInstance(),
        configProvider);
    HoodiePropertiesReader hoodiePropertiesReader = new HoodiePropertiesReader(asyncStorageClient,
        lakeViewExtractorMetrics);
    AsyncHttpClientWithRetry asyncHttpClientWithRetry = getAsyncHttpClientWithRetry(executorService);
    OnehouseApiClient onehouseApiClient = new OnehouseApiClient(asyncHttpClientWithRetry, config,
        lakeViewExtractorMetrics);
    PresignedUrlFileUploader presignedUrlFileUploader = new PresignedUrlFileUploader(asyncStorageClient,
        asyncHttpClientWithRetry, lakeViewExtractorMetrics);
    TimelineCommitInstantsUploader timelineCommitInstantsUploader = new TimelineCommitInstantsUploader(asyncStorageClient,
        presignedUrlFileUploader, onehouseApiClient, storageUtils, executorService, new ActiveTimelineInstantBatcher(),
        lakeViewExtractorMetrics, config);
    TableMetadataUploaderService tableMetadataUploaderService = new TableMetadataUploaderService(hoodiePropertiesReader,
        onehouseApiClient, timelineCommitInstantsUploader, lakeViewExtractorMetrics, executorService);
    return new TableDiscoveryAndUploadJob(tableDiscoveryService, tableMetadataUploaderService, lakeViewExtractorMetrics);
  }

  private AsyncStorageClient getAsyncStorageClient(@Nonnull Config config, @Nonnull ExecutorService executorService,
                                                   StorageUtils storageUtils) {
    if (config.getFileSystemConfiguration().getS3Config() != null) {
      S3AsyncClientProvider s3AsyncClientProvider = new S3AsyncClientProvider(config, executorService);
      return new S3AsyncStorageClient(s3AsyncClientProvider, storageUtils, executorService);
    } else {
      GcsClientProvider gcsClientProvider = new GcsClientProvider(config);
      return new GCSAsyncStorageClient(gcsClientProvider, storageUtils, executorService);
    }
  }

  private AsyncHttpClientWithRetry getAsyncHttpClientWithRetry(@Nonnull ExecutorService executorService) {
    Dispatcher dispatcher = new Dispatcher(executorService);
    OkHttpClient okHttpClient = new OkHttpClient.Builder()
        .readTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .writeTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .connectTimeout(HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .dispatcher(dispatcher)
        .build();
    return new AsyncHttpClientWithRetry(
        HTTP_CLIENT_MAX_RETRIES, HTTP_CLIENT_RETRY_DELAY_MS, okHttpClient);
  }

  @VisibleForTesting
  public @Nullable Config getConfig() {
    return config;
  }

  @Override
  public void syncHoodieTable() {
    if (isLakeviewSyncToolEnabled && tableDiscoveryAndUploadJob != null) {
      // TODO: handle timeout
      tableDiscoveryAndUploadJob.runOnce();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (executorService != null) {
      executorService.shutdown();
    }
  }
}
