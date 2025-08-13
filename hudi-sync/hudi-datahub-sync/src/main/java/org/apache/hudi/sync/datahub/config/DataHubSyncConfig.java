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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import datahub.client.rest.RestEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;

import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_WRITE_TABLE_NAME_KEY;
import static org.apache.hudi.sync.datahub.config.HoodieDataHubDatasetIdentifier.DEFAULT_DATAHUB_ENV;
import static org.apache.hudi.sync.datahub.config.HoodieDataHubDatasetIdentifier.DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME;

@Immutable
@ConfigClassProperty(name = "DataHub Sync Configs",
    groupName = ConfigGroups.Names.META_SYNC,
    description = "Configurations used by the Hudi to sync metadata to DataHub.")
public class DataHubSyncConfig extends HoodieSyncConfig {

  private static final Logger LOG = LoggerFactory.getLogger(DataHubSyncConfig.class);

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS = ConfigProperty
      .key("hoodie.meta.sync.datahub.dataset.identifier.class")
      .defaultValue(HoodieDataHubDatasetIdentifier.class.getName())
      .markAdvanced()
      .withDocumentation("Pluggable class to help provide info to identify a DataHub Dataset.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_EMITTER_SERVER = ConfigProperty
      .key("hoodie.meta.sync.datahub.emitter.server")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Server URL of the DataHub instance.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_EMITTER_TOKEN = ConfigProperty
      .key("hoodie.meta.sync.datahub.emitter.token")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Auth token to connect to the DataHub instance.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_EMITTER_SUPPLIER_CLASS = ConfigProperty
      .key("hoodie.meta.sync.datahub.emitter.supplier.class")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Pluggable class to supply a DataHub REST emitter to connect to the DataHub instance. This overwrites other emitter configs.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATAPLATFORM_NAME = ConfigProperty
      .key("hoodie.meta.sync.datahub.dataplatform.name")
      .defaultValue(DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME)
      .markAdvanced()
      .withDocumentation("String used to represent Hudi when creating its corresponding DataPlatform entity "
          + "within Datahub");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATAPLATFORM_INSTANCE_NAME = ConfigProperty
      .key("hoodie.meta.sync.datahub.dataplatform_instance.name")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("String used to represent Hudi instance when emitting Container and Dataset entities "
          + "with the corresponding DataPlatformInstance, only if given.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATASET_ENV = ConfigProperty
      .key("hoodie.meta.sync.datahub.dataset.env")
      .defaultValue(DEFAULT_DATAHUB_ENV.name())
      .markAdvanced()
      .withDocumentation("Environment to use when pushing entities to Datahub");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_DOMAIN_IDENTIFIER = ConfigProperty
      .key("hoodie.meta.sync.datahub.domain.identifier")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Domain identifier for the dataset. When provided all datasets will be attached to the provided domain. Must be in urn form (e.g., urn:li:domain:_domain_id).");

  public static final ConfigProperty<String> HIVE_TABLE_SERDE_PROPERTIES = ConfigProperty
      .key("hoodie.datasource.hive_sync.serde_properties")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Serde properties to hive table.");


  public static final ConfigProperty<Integer> HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD = ConfigProperty
      .key("hoodie.datasource.hive_sync.schema_string_length_thresh")
      .defaultValue(4000)
      .markAdvanced()
      .withDocumentation("");

  public static final ConfigProperty<Boolean> META_SYNC_DATAHUB_SYNC_SUPPRESS_EXCEPTIONS = ConfigProperty
      .key("hoodie.meta.sync.datahub.sync.suppress.exceptions")
      .defaultValue(true)
      .markAdvanced()
      .withDocumentation("Suppress exceptions during DataHub sync. This is true by default to ensure that when running inline with other jobs, the sync does not fail the job.");
  public static final ConfigProperty<String> META_SYNC_DATAHUB_DATABASE_NAME = ConfigProperty
      .key("hoodie.meta.sync.datahub.database.name")
      .noDefaultValue()
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(META_SYNC_DATABASE_NAME.key()))
          .or(() -> Option.of(cfg.getStringOrDefault(DATABASE_NAME, META_SYNC_DATABASE_NAME.defaultValue()))))
      .markAdvanced()
      .withDocumentation("The name of the destination database that we should sync the hudi table to.");

  public static final ConfigProperty<String> META_SYNC_DATAHUB_TABLE_NAME = ConfigProperty
      .key("hoodie.meta.sync.datahub.table.name")
      .noDefaultValue()
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(META_SYNC_TABLE_NAME.key()))
          .or(() -> Option.ofNullable(cfg.getString(HOODIE_TABLE_NAME_KEY)))
          .or(() -> Option.ofNullable(cfg.getString(HOODIE_WRITE_TABLE_NAME_KEY))))
      .markAdvanced()
      .withDocumentation("The name of the destination table that we should sync the hudi table to.");

  // TLS Configuration Properties
  public static final ConfigProperty<String> META_SYNC_DATAHUB_TLS_CA_CERT_PATH = ConfigProperty
      .key("hoodie.meta.sync.datahub.tls.ca.cert.path")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Path to the CA certificate file for TLS verification. "
          + "Used when connecting to DataHub over HTTPS with custom CA certificates.");

  public DataHubSyncConfig(Properties props) {
    super(props);
    // Log warning if the domain identifier is provided but is not in urn form
    if (contains(META_SYNC_DATAHUB_DOMAIN_IDENTIFIER) && !getString(META_SYNC_DATAHUB_DOMAIN_IDENTIFIER).startsWith("urn:li:domain:")) {
      LOG.warn(
          "Domain identifier must be in urn form (e.g., urn:li:domain:_domain_id). Provided {}. Will remove this from configuration.",
          getString(META_SYNC_DATAHUB_DOMAIN_IDENTIFIER));
      this.props.remove(META_SYNC_DATAHUB_DOMAIN_IDENTIFIER.key());
    }
  }

  public HoodieDataHubDatasetIdentifier getDatasetIdentifier() {
    String identifierClass = getStringOrDefault(META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS);
    // Use reflection to instantiate the class
    HoodieDataHubDatasetIdentifier datasetIdentifier = (HoodieDataHubDatasetIdentifier) ReflectionUtils.loadClass(identifierClass, new Class<?>[] {Properties.class}, props);
    return datasetIdentifier;
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

  public Boolean suppressExceptions() {
    return getBoolean(META_SYNC_DATAHUB_SYNC_SUPPRESS_EXCEPTIONS);
  }

  public String getDataHubServerEndpoint() {
    return getString(META_SYNC_DATAHUB_EMITTER_SERVER);
  }

  public boolean attachDomain() {
    return contains(META_SYNC_DATAHUB_DOMAIN_IDENTIFIER);
  }

  public String getDomainIdentifier() {
    return getString(META_SYNC_DATAHUB_DOMAIN_IDENTIFIER);
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

    @Parameter(names = {"--data-platform-name"}, description = "String used to represent Hudi when creating its "
        + "corresponding DataPlatform entity within Datahub")
    public String dataPlatformName;

    @Parameter(names = {"--data-platform-instance-name"}, description = "String used to represent Hudi instance when emitting Container and Dataset entities "
        + "with the corresponding DataPlatformInstance, only if given.")
    public String dataPlatformInstanceName;

    @Parameter(names = {"--dataset-env"}, description = "Which Datahub Environment to use when pushing entities")
    public String datasetEnv;

    @Parameter(names = {"--database-name"}, description = "Database name to use for datahub sync")
    public String databaseName;

    @Parameter(names = {"--table-name"}, description = "Table name to use for datahub sync")
    public String tableName;

    @Parameter(names = {
        "--domain"}, description = "Domain identifier for the dataset. When provided all datasets will be attached to the provided domain. Must be in urn form (e.g., urn:li:domain:_domain_id).")
    public String domainIdentifier;

    @Parameter(names = {
        "--suppress-exceptions"}, description = "Suppress exceptions during DataHub sync.")
    public String suppressExceptions;

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
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_DATAPLATFORM_INSTANCE_NAME.key(), dataPlatformInstanceName);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_DATASET_ENV.key(), datasetEnv);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_DOMAIN_IDENTIFIER.key(), domainIdentifier);
      // We want the default behavior of DataHubSync Tool when run as command line to NOT suppress exceptions
      if (suppressExceptions == null) {
        props.setProperty(META_SYNC_DATAHUB_SYNC_SUPPRESS_EXCEPTIONS.key(), "false");
      } else {
        props.setProperty(META_SYNC_DATAHUB_SYNC_SUPPRESS_EXCEPTIONS.key(), String.valueOf(suppressExceptions));
      }
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_DATABASE_NAME.key(), databaseName);
      props.setPropertyIfNonNull(META_SYNC_DATAHUB_TABLE_NAME.key(), tableName);
      return props;
    }
  }
}