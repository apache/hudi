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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.utilities.IdentitySplitter;

import java.util.List;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;

public class LakeviewSyncConfigHolder {

  // this class holds static config fields
  private LakeviewSyncConfigHolder() {}

  public static final ConfigProperty<Boolean> LAKEVIEW_SYNC_ENABLED = ConfigProperty
      .key("hoodie.datasource.lakeview_sync.enable")
      .defaultValue(false)
      .withDocumentation("When set to true, register/sync the table to Lakeview.");

  public static final ConfigProperty<String> LAKEVIEW_VERSION = ConfigProperty
      .key("hoodie.meta.sync.lakeview.version")
      .defaultValue("V1")
      .withDocumentation("Lakeview version");

  public static final ConfigProperty<String> LAKEVIEW_PROJECT_ID = ConfigProperty
      .key("hoodie.meta.sync.lakeview.projectId")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Project ID in lakeview");

  public static final ConfigProperty<String> LAKEVIEW_API_KEY = ConfigProperty
      .key("hoodie.meta.sync.lakeview.apiKey")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("API key to access lakeview");

  public static final ConfigProperty<String> LAKEVIEW_API_SECRET = ConfigProperty
      .key("hoodie.meta.sync.lakeview.apiSecret")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("API secret to access lakeview");

  public static final ConfigProperty<String> LAKEVIEW_USERID = ConfigProperty
      .key("hoodie.meta.sync.lakeview.userId")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("UserId used for creating API key, secret in lakeview");

  public static final ConfigProperty<String> LAKEVIEW_S3_REGION = ConfigProperty
      .key("hoodie.meta.sync.lakeview.s3.region")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("S3 region associated with the table base path");

  public static final ConfigProperty<String> LAKEVIEW_S3_ACCESS_KEY = ConfigProperty
      .key("hoodie.meta.sync.lakeview.s3.accessKey")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Access key required to access table base paths present in S3");

  public static final ConfigProperty<String> LAKEVIEW_S3_ACCESS_SECRET = ConfigProperty
      .key("hoodie.meta.sync.lakeview.s3.accessSecret")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Access secret required to access table base paths present in S3");

  public static final ConfigProperty<String> LAKEVIEW_GCS_PROJECT_ID = ConfigProperty
      .key("hoodie.meta.sync.lakeview.gcs.projectId")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("GCS Project ID the table base path belongs to");

  public static final ConfigProperty<String> LAKEVIEW_GCS_SERVICE_ACCOUNT_KEY_PATH = ConfigProperty
      .key("hoodie.meta.sync.lakeview.gcs.gcpServiceAccountKeyPath")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("GCS Service account key path to access the table base path present in GCS");

  public static final ConfigProperty<String> LAKEVIEW_METADATA_EXTRACTOR_PATH_EXCLUSION_PATTERNS = ConfigProperty
      .key("hoodie.meta.sync.lakeview.metadataExtractor.pathExclusionPatterns")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("List of pattens to be ignored by lakeview metadata extractor");

  /**
   * Eg properties:
   * <p>
   * hoodie.meta.sync.lakeview.metadataExtractor.lakes.<lake1>.databases.<database1>.basePaths=<basepath11>,<basepath12>
   * hoodie.meta.sync.lakeview.metadataExtractor.lakes.<lake1>.databases.<database2>.basePaths=<basepath13>,<basepath14>
   * <p>
   * NOTE: multiple properties with hoodie.meta.sync.lakeview.metadataExtractor.lakes prefix can be included in the properties
   */
  public static final ConfigProperty<String> LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS = ConfigProperty
      .key("hoodie.meta.sync.lakeview.metadataExtractor.lakes")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Lake name & database name that should be applied to specified list of table base paths in lakeview metadata extractor");

  public static final ConfigProperty<Integer> LAKEVIEW_HTTP_CLIENT_TIMEOUT_SECONDS = ConfigProperty
      .key("hoodie.datasource.lakeview_sync.http.client.timeout")
      .defaultValue(15)
      .withDocumentation("Timeout set to http client used by lakeview sync tool");

  public static final ConfigProperty<Integer> LAKEVIEW_HTTP_CLIENT_MAX_RETRIES = ConfigProperty
      .key("hoodie.datasource.lakeview_sync.http.client.retries")
      .defaultValue(3)
      .withDocumentation("Max retries by http client used by lakeview sync tool");

  public static final ConfigProperty<Integer> LAKEVIEW_HTTP_CLIENT_RETRY_DELAY_MS = ConfigProperty
      .key("hoodie.datasource.lakeview_sync.http.client.retry.delay.ms")
      .defaultValue(1000)
      .withDocumentation("Delay between retries of http client used by lakeview sync tool");

  public static class LakeviewSyncConfigParams {

    @ParametersDelegate()
    public final HoodieSyncConfig.HoodieSyncConfigParams hoodieSyncConfigParams = new HoodieSyncConfig.HoodieSyncConfigParams();

    @Parameter(names = {"--version"}, description = "Version of lakeview config")
    public String version;
    @Parameter(names = {"--project-id"}, description = "Lakeview project id", required = true, order = 1)
    public String projectId;
    @Parameter(names = {"--api-key"}, description = "Lakeview API Key", required = true, order = 2)
    public String apiKey;
    @Parameter(names = {"--api-secret"}, description = "Lakeview API Secret", required = true, password = true, order = 3)
    public String apiSecret;
    @Parameter(names = {"--userid"}, description = "Lakeview User ID", required = true, order = 4)
    public String userId;

    @Parameter(names = {"--s3-region"}, description = "S3 Bucket region")
    public String s3Region;
    @Parameter(names = {"--s3-access-key"}, description = "Access key to use S3 Bucket")
    public String s3AccessKey;
    @Parameter(names = {"--s3-access-secret"}, description = "Access secret to use S3 Bucket", password = true)
    public String s3AccessSecret;

    @Parameter(names = {"--gcp-project-id"}, description = "GCP Project ID")
    public String gcpProjectId;
    @Parameter(names = {"--gcp-service-account-key-path"}, description = "GCP Project Service account key path")
    public String gcpServiceAccountKeyPath;

    @Parameter(names = {"--path-exclusion-patterns"}, description = "Path exclusion patterns (comma separated)")
    public String pathExclusionPatterns;

    @Parameter(names = {"--lake-paths"}, description = "Lake/Database paths (eg: <lake1>.databases.<database1>.basePaths=<basepath11>,<basepath12>)", required = true, order = 5, splitter = IdentitySplitter.class)
    public List<String> lakePaths;

    @Parameter(names = {"--http-client-timeout"}, description = "Http client timeout")
    public int httpClientTimeout;

    @Parameter(names = {"--http-client-max-retries"}, description = "Max retries by the http client")
    public int httpClientMaxRetries;

    @Parameter(names = {"--http-client-retries-delay-ms"}, description = "Delay between retries by the http client in milliseconds")
    public int httpClientDelayBetweenRetriesInMs;

    public boolean isHelp() {
      return hoodieSyncConfigParams.isHelp();
    }

    public TypedProperties toProps() {
      final TypedProperties props = hoodieSyncConfigParams.toProps();
      props.setPropertyIfNonNull(LAKEVIEW_SYNC_ENABLED.key(), Boolean.TRUE.toString().toLowerCase());
      props.setPropertyIfNonNull(LAKEVIEW_VERSION.key(), version);
      props.setPropertyIfNonNull(LAKEVIEW_PROJECT_ID.key(), projectId);
      props.setPropertyIfNonNull(LAKEVIEW_API_KEY.key(), apiKey);
      props.setPropertyIfNonNull(LAKEVIEW_API_SECRET.key(), apiSecret);
      props.setPropertyIfNonNull(LAKEVIEW_USERID.key(), userId);
      props.setPropertyIfNonNull(BASE_PATH.key(), hoodieSyncConfigParams.basePath);

      props.setPropertyIfNonNull(LAKEVIEW_S3_REGION.key(), s3Region);
      props.setPropertyIfNonNull(LAKEVIEW_S3_ACCESS_KEY.key(), s3AccessKey);
      props.setPropertyIfNonNull(LAKEVIEW_S3_ACCESS_SECRET.key(), s3AccessSecret);

      props.setPropertyIfNonNull(LAKEVIEW_GCS_PROJECT_ID.key(), gcpProjectId);
      props.setPropertyIfNonNull(LAKEVIEW_GCS_SERVICE_ACCOUNT_KEY_PATH.key(), gcpServiceAccountKeyPath);

      props.setPropertyIfNonNull(LAKEVIEW_METADATA_EXTRACTOR_PATH_EXCLUSION_PATTERNS.key(), pathExclusionPatterns);
      for (String lakePath : lakePaths) {
        String[] fields = lakePath.split("=");
        String key = fields[0];
        String value = fields[1];
        props.setPropertyIfNonNull(LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS.key() + "." + key, value);
      }

      props.setPropertyIfNonNull(LAKEVIEW_HTTP_CLIENT_TIMEOUT_SECONDS.key(), httpClientTimeout);
      props.setPropertyIfNonNull(LAKEVIEW_HTTP_CLIENT_MAX_RETRIES.key(), httpClientMaxRetries);
      props.setPropertyIfNonNull(LAKEVIEW_HTTP_CLIENT_RETRY_DELAY_MS.key(), httpClientDelayBetweenRetriesInMs);
      return props;
    }
  }
}
