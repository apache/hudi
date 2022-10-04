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

package org.apache.hudi.sync.datahub.config;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import datahub.client.rest.RestEmitter;

import java.util.Properties;

import static org.apache.hudi.sync.datahub.config.HoodieDataHubDatasetIdentifier.DEFAULT_DATAHUB_ENV;
import static org.apache.hudi.sync.datahub.config.HoodieDataHubDatasetIdentifier.DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME;

public class DataHubSyncConfig extends HoodieSyncConfig {

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS = ConfigProperty
      .key("hoodie.meta.sync.datahub.dataset.identifier.class")
      .defaultValue(HoodieDataHubDatasetIdentifier.class.getName())
      .withDocumentation("Pluggable class to help provide info to identify a DataHub Dataset.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_EMITTER_SERVER = ConfigProperty
      .key("hoodie.meta.sync.datahub.emitter.server")
      .noDefaultValue()
      .withDocumentation("Server URL of the DataHub instance.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_EMITTER_TOKEN = ConfigProperty
      .key("hoodie.meta.sync.datahub.emitter.token")
      .noDefaultValue()
      .withDocumentation("Auth token to connect to the DataHub instance.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS = ConfigProperty
      .key("hoodie.meta.sync.datahub.emitter.supplier.class")
      .noDefaultValue()
      .withDocumentation("Pluggable class to supply a DataHub REST emitter to connect to the DataHub instance. This overwrites other emitter configs.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATAPLATFORM_NAME = ConfigProperty
          .key("hoodie.meta.sync.datahub.dataplatform.name")
          .defaultValue(DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME)
          .withDocumentation("String used to represent Hudi when creating its corresponding DataPlatform entity "
                  + "within Datahub");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATASET_ENV = ConfigProperty
          .key("hoodie.meta.sync.datahub.dataset.env")
          .defaultValue(DEFAULT_DATAHUB_ENV.name())
          .withDocumentation("Environment to use when pushing entities to Datahub");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_ENVIRONMENT_NAME = ConfigProperty
          .key("hoodie.meta.sync.datahub.env.name")
          .defaultValue(DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME)
          .withDocumentation("String used to represent Hudi when creating its corresponding DataPlatform entity "
                  + "within Datahub");

  public final HoodieDataHubDatasetIdentifier datasetIdentifier;

  public DataHubSyncConfig(Properties props) {
    super(props);
    String identifierClass = getStringOrDefault(META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS);
    datasetIdentifier = (HoodieDataHubDatasetIdentifier) ReflectionUtils.loadClass(identifierClass, new Class<?>[] {Properties.class}, props);
  }

  public RestEmitter getRestEmitter() {
    if (contains(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS)) {
      return ((DataHubEmitterSupplier) ReflectionUtils.loadClass(getString(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS))).get();
    } else if (contains(META_SYNC_DATAHUB_EMITTER_SERVER)) {
      return RestEmitter.create(b -> b.server(getString(META_SYNC_DATAHUB_EMITTER_SERVER)).token(getStringOrDefault(META_SYNC_DATAHUB_EMITTER_TOKEN, null)));
    } else {
      return RestEmitter.createWithDefaults();
    }
  }

  public static class DataHubSyncConfigParams {

    @ParametersDelegate()
    public final HoodieSyncConfigParams hoodieSyncConfigParams = new HoodieSyncConfigParams();

    @Parameter(names = {"--identifier-class"}, description = "Pluggable class to help provide info to identify a DataHub Dataset.")
    public String identifierClass;

    @Parameter(names = {"--emitter-server"}, description = "Server URL of the DataHub instance.")
    public String emitterServer;

    @Parameter(names = {"--emitter-token"}, description = "Auth token to connect to the DataHub instance.")
    public String emitterToken;

    @Parameter(names = {"--emitter-supplier-class"}, description = "Pluggable class to supply a DataHub REST emitter to connect to the DataHub instance. This overwrites other emitter configs.")
    public String emitterSupplierClass;

    @Parameter(names = {"--data-platform-name"}, description = "String used to represent Hudi when creating its " +
            "corresponding DataPlatform entity within Datahub")
    public String dataPlatformName;

    @Parameter(names = {"--dataset-env"}, description = "Which Datahub Environment to use when pushing entities")
    public String datasetEnv;

    public boolean isHelp() {
      return hoodieSyncConfigParams.isHelp();
    }

    public Properties toProps() {
      final TypedProperties props = hoodieSyncConfigParams.toProps();
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS.key(), identifierClass);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_EMITTER_SERVER.key(), emitterServer);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_EMITTER_TOKEN.key(), emitterToken);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS.key(), emitterSupplierClass);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_DATAPLATFORM_NAME.key(), dataPlatformName);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_DATASET_ENV.key(), datasetEnv);
      return props;
    }
  }
}
