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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sync.common.util.ConfigUtils;

import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_WRITE_TABLE_NAME_KEY;

/**
 * Configs needed to sync data into external meta stores, catalogs, etc.
 */
public class HoodieSyncConfig extends HoodieConfig {

  public static final ConfigProperty<String> META_SYNC_BASE_PATH = ConfigProperty
      .key("hoodie.datasource.meta.sync.base.path")
      .defaultValue("")
      .withDocumentation("Base path of the hoodie table to sync");

  public static final ConfigProperty<String> META_SYNC_ENABLED = ConfigProperty
      .key("hoodie.datasource.meta.sync.enable")
      .defaultValue("false")
      .withDocumentation("Enable Syncing the Hudi Table with an external meta store or data catalog.");

  // ToDo change the prefix of the following configs from hive_sync to meta_sync
  public static final ConfigProperty<String> META_SYNC_DATABASE_NAME = ConfigProperty
      .key("hoodie.datasource.hive_sync.database")
      .defaultValue("default")
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(DATABASE_NAME)))
      .withDocumentation("The name of the destination database that we should sync the hudi table to.");

  public static final ConfigProperty<String> META_SYNC_TABLE_NAME = ConfigProperty
      .key("hoodie.datasource.hive_sync.table")
      .defaultValue("unknown")
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(HOODIE_WRITE_TABLE_NAME_KEY))
          .or(() -> Option.ofNullable(cfg.getString(HOODIE_TABLE_NAME_KEY))))
      .withDocumentation("The name of the destination table that we should sync the hudi table to.");

  public static final ConfigProperty<String> META_SYNC_BASE_FILE_FORMAT = ConfigProperty
      .key("hoodie.datasource.hive_sync.base_file_format")
      .defaultValue("PARQUET")
      .withDocumentation("Base file format for the sync.");

  // If partition fields are not explicitly provided, obtain from the KeyGeneration Configs
  public static final Function<HoodieConfig, Option<String>> PARTITION_FIELDS_INFERENCE_FUNCTION = cfg -> {
    if (cfg.contains(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME)) {
      return Option.of(cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME));
    } else {
      return Option.empty();
    }
  };
  public static final ConfigProperty<String> META_SYNC_PARTITION_FIELDS = ConfigProperty
      .key("hoodie.datasource.hive_sync.partition_fields")
      .defaultValue("")
      .withInferFunction(PARTITION_FIELDS_INFERENCE_FUNCTION)
      .withDocumentation("Field in the table to use for determining hive partition columns.");

  // If partition value extraction class is not explicitly provided, configure based on the partition fields.
  public static final Function<HoodieConfig, Option<String>> PARTITION_EXTRACTOR_CLASS_FUNCTION = cfg -> {
    if (!cfg.contains(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME)) {
      return Option.of("org.apache.hudi.hive.NonPartitionedExtractor");
    } else {
      int numOfPartFields = cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME).split(",").length;
      if (numOfPartFields == 1
          && cfg.contains(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE)
          && cfg.getString(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE).equals("true")) {
        return Option.of("org.apache.hudi.hive.HiveStylePartitionValueExtractor");
      } else {
        return Option.of("org.apache.hudi.hive.MultiPartKeysValueExtractor");
      }
    }
  };
  public static final ConfigProperty<String> META_SYNC_PARTITION_EXTRACTOR_CLASS = ConfigProperty
      .key("hoodie.datasource.hive_sync.partition_extractor_class")
      .defaultValue("org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor")
      .withInferFunction(PARTITION_EXTRACTOR_CLASS_FUNCTION)
      .withDocumentation("Class which implements PartitionValueExtractor to extract the partition values, "
          + "default 'SlashEncodedDayPartitionValueExtractor'.");

  public static final ConfigProperty<String> META_SYNC_ASSUME_DATE_PARTITION = ConfigProperty
      .key("hoodie.datasource.hive_sync.assume_date_partitioning")
      .defaultValue("false")
      .withDocumentation("Assume partitioning is yyyy/mm/dd");

  public static final ConfigProperty<Boolean> META_SYNC_DECODE_PARTITION = ConfigProperty
      .key("hoodie.meta.sync.decode_partition")
      .defaultValue(false) // TODO infer from url encode option
      .withDocumentation("");

  public static final ConfigProperty<Boolean> META_SYNC_USE_FILE_LISTING_FROM_METADATA = ConfigProperty
      .key("hoodie.meta.sync.metadata_file_listing")
      .defaultValue(HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS)
      .withDocumentation("Enable the internal metadata table for file listing for syncing with metastores");

  public static final ConfigProperty<String> META_SYNC_CONDITIONAL_SYNC = ConfigProperty
      .key("hoodie.datasource.meta_sync.condition.sync")
      .defaultValue("false")
      .withDocumentation("If true, only sync on conditions like schema change or partition change.");

  public static final ConfigProperty<String> META_SYNC_SPARK_VERSION = ConfigProperty
      .key("hoodie.meta_sync.spark.version")
      .defaultValue("")
      .withDocumentation("The spark version used when syncing with a metastore.");

  private Configuration hadoopConf;

  public HoodieSyncConfig(Properties props) {
    this(props, ConfigUtils.createHadoopConf(props));
  }

  public HoodieSyncConfig(Properties props, Configuration hadoopConf) {
    super(props);
    setDefaults(getClass().getName());
    this.hadoopConf = hadoopConf;
  }

  public void setHadoopConf(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public FileSystem getHadoopFileSystem() {
    return FSUtils.getFs(getString(META_SYNC_BASE_PATH), getHadoopConf());
  }

  public String getAbsoluteBasePath() {
    return getString(META_SYNC_BASE_PATH);
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
    @Parameter(names = {"--assume-date-partitioning"}, description = "Assume standard yyyy/mm/dd partitioning, this"
        + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter")
    public Boolean assumeDatePartitioning;
    @Parameter(names = {"--decode-partition"}, description = "Decode the partition value if the partition has encoded during writing")
    public Boolean decodePartition;
    @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
    public Boolean useFileListingFromMetadata;
    @Parameter(names = {"--conditional-sync"}, description = "If true, only sync on conditions like schema change or partition change.")
    public Boolean isConditionalSync;
    @Parameter(names = {"--spark-version"}, description = "The spark version")
    public String sparkVersion;

    @Parameter(names = {"--help", "-h"}, help = true)
    public boolean help = false;

    public boolean isHelp() {
      return help;
    }

    public TypedProperties toProps() {
      final TypedProperties props = new TypedProperties();
      props.setPropertyIfNonNull(META_SYNC_BASE_PATH.key(), basePath);
      props.setPropertyIfNonNull(META_SYNC_DATABASE_NAME.key(), databaseName);
      props.setPropertyIfNonNull(META_SYNC_TABLE_NAME.key(), tableName);
      props.setPropertyIfNonNull(META_SYNC_BASE_FILE_FORMAT.key(), baseFileFormat);
      props.setPropertyIfNonNull(META_SYNC_PARTITION_FIELDS.key(), StringUtils.join(",", partitionFields));
      props.setPropertyIfNonNull(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), partitionValueExtractorClass);
      props.setPropertyIfNonNull(META_SYNC_ASSUME_DATE_PARTITION.key(), assumeDatePartitioning);
      props.setPropertyIfNonNull(META_SYNC_DECODE_PARTITION.key(), decodePartition);
      props.setPropertyIfNonNull(META_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), useFileListingFromMetadata);
      props.setPropertyIfNonNull(META_SYNC_CONDITIONAL_SYNC.key(), isConditionalSync);
      props.setPropertyIfNonNull(META_SYNC_SPARK_VERSION.key(), sparkVersion);
      return props;
    }
  }
}
