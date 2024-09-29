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

import org.apache.hudi.common.config.ConfigProperty;

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
}
