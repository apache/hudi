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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import javax.annotation.concurrent.Immutable;

import java.util.Arrays;

import static org.apache.hudi.common.util.ConfigUtils.DELTA_STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * Hudi Incremental Pulling Source Configs
 */
@Immutable
@ConfigClassProperty(name = "Hudi Incremental Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of incremental pulling from a Hudi "
        + "table as a source in Hudi Streamer.")
public class HoodieIncrSourceConfig extends HoodieConfig {

  public static final ConfigProperty<String> HOODIE_SRC_BASE_PATH = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.path")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.path")
      .withDocumentation("Base-path for the source Hudi table");

  public static final ConfigProperty<Integer> NUM_INSTANTS_PER_FETCH = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.num_instants")
      .defaultValue(5)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.num_instants")
      .markAdvanced()
      .withDocumentation("Max number of instants whose changes can be incrementally fetched");

  @Deprecated
  public static final ConfigProperty<Boolean> READ_LATEST_INSTANT_ON_MISSING_CKPT = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.read_latest_on_missing_ckpt")
      .defaultValue(false)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.read_latest_on_missing_ckpt")
      .markAdvanced()
      .withDocumentation("If true, allows Hudi Streamer to incrementally fetch from latest committed instant when checkpoint is not provided. "
          + "This config is deprecated. Please refer to hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy");

  public static final ConfigProperty<String> MISSING_CHECKPOINT_STRATEGY = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.missing.checkpoint.strategy")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.missing.checkpoint.strategy")
      .markAdvanced()
      .withDocumentation("Allows Hudi Streamer to decide the instant to consume from when checkpoint is not set.\n"
          + " Possible values: " + Arrays.toString(IncrSourceHelper.MissingCheckpointStrategy.values()));

  public static final ConfigProperty<String> SOURCE_FILE_FORMAT = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.file.format")
      .defaultValue("parquet")
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.file.format")
      .markAdvanced()
      .withDocumentation("This config is passed to the reader while loading dataset. Default value is parquet.");

  public static final ConfigProperty<Boolean> HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.drop.all.meta.fields.from.source")
      .defaultValue(false)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.drop.all.meta.fields.from.source")
      .markAdvanced()
      .withDocumentation("Drops all meta fields from the source hudi table while ingesting into sink hudi table.");

  public static final ConfigProperty<String> HOODIE_SRC_PARTITION_FIELDS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.partition.fields")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.partition.fields")
      .markAdvanced()
      .withDocumentation("Specifies partition fields that needs to be added to source table after parsing _hoodie_partition_path.");

  public static final ConfigProperty<String> HOODIE_SRC_PARTITION_EXTRACTORCLASS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.partition.extractor.class")
      .noDefaultValue(SlashEncodedDayPartitionValueExtractor.class.getCanonicalName())
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.hoodieincr.partition.extractor.class")
      .markAdvanced()
      .withDocumentation("PartitionValueExtractor class to extract partition fields from _hoodie_partition_path");

  public static final ConfigProperty<String> HOODIE_INCREMENTAL_SPARK_DATASOURCE_OPTIONS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.hoodieincr.data.datasource.options")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("A comma-separated list of Hudi options that can be passed to the spark dataframe reader of a hudi table, "
          + "eg: `hoodie.metadata.enable=true,hoodie.enable.data.skipping=true`. Used only for incremental source.");
}
