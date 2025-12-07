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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HadoopConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;

import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.apache.hudi.common.config.HoodieCommonConfig.META_SYNC_BASE_PATH_KEY;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.common.table.HoodieTableConfig.BASE_FILE_FORMAT;
import static org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_WRITE_TABLE_NAME_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.URL_ENCODE_PARTITIONING;

/**
 * Configs needed to sync data into external meta stores, catalogs, etc.
 */
@Getter
@Immutable
@ConfigClassProperty(name = "Common Metadata Sync Configs",
    groupName = ConfigGroups.Names.META_SYNC,
    areCommonConfigs = true,
    description = "")
public class HoodieSyncConfig extends HoodieConfig {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSyncConfig.class);

  public static final ConfigProperty<String> META_SYNC_BASE_PATH = ConfigProperty
      .key(META_SYNC_BASE_PATH_KEY)
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("Base path of the hoodie table to sync");

  public static final ConfigProperty<String> META_SYNC_ENABLED = ConfigProperty
      .key("hoodie.datasource.meta.sync.enable")
      .defaultValue("false")
      .withDocumentation("Enable Syncing the Hudi Table with an external meta store or data catalog.");

  // ToDo change the prefix of the following configs from hive_sync to meta_sync
  public static final ConfigProperty<String> META_SYNC_DATABASE_NAME = ConfigProperty
      .key("hoodie.datasource.hive_sync.database")
      .defaultValue("default")
      .withInferFunction(cfg -> {
        String databaseName = cfg.getString(DATABASE_NAME);
        // Need to check if database name is empty as Option won't check it
        return StringUtils.isNullOrEmpty(databaseName)
            ? Option.empty()
            : Option.of(databaseName);
      })
      .markAdvanced()
      .withDocumentation("The name of the destination database that we should sync the hudi table to.");

  public static final ConfigProperty<String> META_SYNC_TABLE_NAME = ConfigProperty
      .key("hoodie.datasource.hive_sync.table")
      .defaultValue("unknown")
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(HOODIE_TABLE_NAME_KEY))
          .or(() -> Option.ofNullable(cfg.getString(HOODIE_WRITE_TABLE_NAME_KEY))))
      .markAdvanced()
      .withDocumentation("The name of the destination table that we should sync the hudi table to.");

  public static final ConfigProperty<String> META_SYNC_BASE_FILE_FORMAT = ConfigProperty
      .key("hoodie.datasource.hive_sync.base_file_format")
      .defaultValue("PARQUET")
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(BASE_FILE_FORMAT)))
      .markAdvanced()
      .withDocumentation("Base file format for the sync.");

  public static final ConfigProperty<String> META_SYNC_PARTITION_FIELDS = ConfigProperty
      .key("hoodie.datasource.hive_sync.partition_fields")
      .defaultValue("")
      .withInferFunction(cfg -> HoodieTableConfig.getPartitionFieldProp(cfg)
          .or(() -> Option.ofNullable(cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME))))
      .markAdvanced()
      .withDocumentation("Field in the table to use for determining hive partition columns.");

  public static final ConfigProperty<String> META_SYNC_PARTITION_EXTRACTOR_CLASS = ConfigProperty
      .key("hoodie.datasource.hive_sync.partition_extractor_class")
      .defaultValue("org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .withInferFunction(cfg -> {
        Option<String> partitionFieldsOpt;
        if (StringUtils.nonEmpty(cfg.getString(META_SYNC_PARTITION_FIELDS))) {
          partitionFieldsOpt = Option.ofNullable(cfg.getString(META_SYNC_PARTITION_FIELDS));
        } else {
          partitionFieldsOpt = HoodieTableConfig.getPartitionFieldProp(cfg)
              .or(() -> Option.ofNullable(cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME)));
        }
        if (!partitionFieldsOpt.isPresent()) {
          return Option.empty();
        }
        String partitionFields = partitionFieldsOpt.get();
        if (StringUtils.nonEmpty(partitionFields)) {
          int numOfPartFields = partitionFields.split(",").length;
          if (numOfPartFields == 1) {
            if (cfg.contains(HIVE_STYLE_PARTITIONING_ENABLE)
                && cfg.getString(HIVE_STYLE_PARTITIONING_ENABLE).equals("true")) {
              return Option.of("org.apache.hudi.hive.HiveStylePartitionValueExtractor");
            } else {
              return Option.of("org.apache.hudi.hive.SinglePartPartitionValueExtractor");
            }
          } else {
            return Option.of("org.apache.hudi.hive.MultiPartKeysValueExtractor");
          }
        } else {
          return Option.of("org.apache.hudi.hive.NonPartitionedExtractor");
        }
      })
      .markAdvanced()
      .withDocumentation("Class which implements PartitionValueExtractor to extract the partition values, "
          + "default 'org.apache.hudi.hive.MultiPartKeysValueExtractor'.");

  public static final ConfigProperty<Boolean> META_SYNC_DECODE_PARTITION = ConfigProperty
      .key("hoodie.meta.sync.decode_partition")
      .defaultValue(false)
      .withInferFunction(cfg -> Option.ofNullable(cfg.getBoolean(URL_ENCODE_PARTITIONING)))
      .markAdvanced()
      .withDocumentation("If true, meta sync will url-decode the partition path, as it is deemed as url-encoded. Default to false.");

  public static final ConfigProperty<Boolean> META_SYNC_USE_FILE_LISTING_FROM_METADATA = ConfigProperty
      .key("hoodie.meta.sync.metadata_file_listing")
      .defaultValue(DEFAULT_METADATA_ENABLE_FOR_READERS)
      .withInferFunction(cfg -> Option.of(cfg.getBooleanOrDefault(HoodieMetadataConfig.ENABLE, DEFAULT_METADATA_ENABLE_FOR_READERS)))
      .markAdvanced()
      .withDocumentation("Enable the internal metadata table for file listing for syncing with metastores");

  public static final ConfigProperty<String> META_SYNC_CONDITIONAL_SYNC = ConfigProperty
      .key("hoodie.datasource.meta_sync.condition.sync")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("If true, only sync on conditions like schema change or partition change.");

  public static final ConfigProperty<String> META_SYNC_SPARK_VERSION = ConfigProperty
      .key("hoodie.meta_sync.spark.version")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("The spark version used when syncing with a metastore.");
  public static final ConfigProperty<String> META_SYNC_SNAPSHOT_WITH_TABLE_NAME = ConfigProperty
          .key("hoodie.meta.sync.sync_snapshot_with_table_name")
          .defaultValue("true")
          .markAdvanced()
          .sinceVersion("0.14.0")
          .withDocumentation("sync meta info to origin table if enable");

  public static final ConfigProperty<Boolean> META_SYNC_INCREMENTAL = ConfigProperty
      .key("hoodie.meta.sync.incremental")
      .defaultValue(true)
      .sinceVersion("0.14.0")
      .markAdvanced()
      .withDocumentation("Whether to incrementally sync the partitions to the metastore, i.e., "
          + "only added, changed, and deleted partitions based on the commit metadata. If set to "
          + "`false`, the meta sync executes a full partition sync operation when partitions are "
          + "lost.");

  public static final ConfigProperty<Boolean> META_SYNC_NO_PARTITION_METADATA = ConfigProperty
      .key("hoodie.meta.sync.no_partition_metadata")
      .defaultValue(false)
      .sinceVersion("1.0.0")
      .markAdvanced()
      .withDocumentation("If true, the partition metadata will not be synced to the metastore. "
          + "This is useful when the partition metadata is large, and the partition info can be "
          + "obtained from Hudi's internal metadata table. Note, " + HoodieMetadataConfig.ENABLE + " must be set to true.");

  @Setter
  private Configuration hadoopConf;
  private final HoodieMetricsConfig metricsConfig;

  public HoodieSyncConfig(Properties props) {
    this(props, HadoopConfigUtils.createHadoopConf(props));
  }

  public HoodieSyncConfig(Properties props, Configuration hadoopConf) {
    super(props);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Passed in properties:\n" + props.entrySet()
          .stream()
          .sorted(Comparator.comparing(e -> e.getKey().toString()))
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining("\n")));
    }
    setDefaults(HoodieSyncConfig.class.getName());
    this.hadoopConf = hadoopConf;
    this.metricsConfig = HoodieMetricsConfig.newBuilder().fromProperties(props).build();
  }

  public String getBasePath() {
    return getString(BASE_PATH);
  }

  public FileSystem getHadoopFileSystem() {
    return HadoopFSUtils.getFs(getString(META_SYNC_BASE_PATH), getHadoopConf());
  }

  public String getAbsoluteBasePath() {
    return getString(META_SYNC_BASE_PATH);
  }

  public Boolean shouldNotSyncPartitionMetadata() {
    return getBooleanOrDefault(META_SYNC_NO_PARTITION_METADATA);
  }

  @Override
  public String toString() {
    return props.toString();
  }

  public static class HoodieSyncConfigParams {
    @Parameter(names = {"--database"}, description = "name of the target database in meta store")
    public String databaseName;
    @Parameter(names = {"--table"}, description = "name of the target table in meta store")
    public String tableName;
    @Parameter(names = {"--base-path"}, description = "Base path of the hoodie table to sync", required = true)
    public String basePath;
    @Parameter(names = {"--base-file-format"}, description = "Format of the base files (PARQUET (or) HFILE)")
    public String baseFileFormat;
    @Parameter(names = "--partitioned-by", description = "Fields in the schema partitioned by")
    public List<String> partitionFields;
    @Parameter(names = "--partition-value-extractor", description = "Class which implements PartitionValueExtractor "
        + "to extract the partition values from HDFS path")
    public String partitionValueExtractorClass;
    @Parameter(names = {"--decode-partition"}, description = "Decode the partition value if the partition has encoded during writing")
    public Boolean decodePartition;
    @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
    public Boolean useFileListingFromMetadata;
    @Parameter(names = {"--conditional-sync"}, description = "If true, only sync on conditions like schema change or partition change.")
    public Boolean isConditionalSync;
    @Parameter(names = {"--spark-version"}, description = "The spark version")
    public String sparkVersion;
    @Parameter(names = {"--sync-incremental"}, description =
        "Whether to incrementally sync the partitions to the metastore, i.e., "
            + "only added, changed, and deleted partitions based on the commit metadata. If set to "
            + "`false`, the meta sync executes a full partition sync operation when partitions are "
            + "lost.")
    public Boolean syncIncremental;

    @Parameter(names = {"--sync-no-partition-metadata"}, description = "do not sync partition metadata info to the catalog")
    public Boolean shouldNotSyncPartitionMetadata;

    @Getter
    @Parameter(names = {"--help", "-h"}, help = true)
    public boolean help = false;

    public TypedProperties toProps() {
      final TypedProperties props = new TypedProperties();
      props.setPropertyIfNonNull(META_SYNC_BASE_PATH.key(), basePath);
      props.setPropertyIfNonNull(META_SYNC_DATABASE_NAME.key(), databaseName);
      props.setPropertyIfNonNull(META_SYNC_TABLE_NAME.key(), tableName);
      props.setPropertyIfNonNull(META_SYNC_BASE_FILE_FORMAT.key(), baseFileFormat);
      props.setPropertyIfNonNull(META_SYNC_PARTITION_FIELDS.key(), StringUtils.join(",", partitionFields));
      props.setPropertyIfNonNull(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), partitionValueExtractorClass);
      props.setPropertyIfNonNull(META_SYNC_DECODE_PARTITION.key(), decodePartition);
      props.setPropertyIfNonNull(META_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), useFileListingFromMetadata);
      props.setPropertyIfNonNull(META_SYNC_CONDITIONAL_SYNC.key(), isConditionalSync);
      props.setPropertyIfNonNull(META_SYNC_SPARK_VERSION.key(), sparkVersion);
      props.setPropertyIfNonNull(META_SYNC_INCREMENTAL.key(), syncIncremental);
      props.setPropertyIfNonNull(META_SYNC_NO_PARTITION_METADATA.key(), shouldNotSyncPartitionMetadata);
      return props;
    }
  }
}
