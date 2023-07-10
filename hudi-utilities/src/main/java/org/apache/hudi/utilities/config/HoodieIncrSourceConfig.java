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

/**
 * Hudi Incremental Pulling Source Configs
 */
@Immutable
@ConfigClassProperty(name = "Hudi Incremental Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of incremental pulling from a Hudi "
        + "table as a source in Deltastreamer.")
public class HoodieIncrSourceConfig extends HoodieConfig {

  public static final ConfigProperty<String> HOODIE_SRC_BASE_PATH = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.path")
      .noDefaultValue()
      .withDocumentation("Base-path for the source Hudi table");

  public static final ConfigProperty<Integer> NUM_INSTANTS_PER_FETCH = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.num_instants")
      .defaultValue(5)
      .markAdvanced()
      .withDocumentation("Max number of instants whose changes can be incrementally fetched");

  @Deprecated
  public static final ConfigProperty<Boolean> READ_LATEST_INSTANT_ON_MISSING_CKPT = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.read_latest_on_missing_ckpt")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("If true, allows Hudi Streamer to incrementally fetch from latest committed instant when checkpoint is not provided. "
          + "This config is deprecated. Please refer to hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy");

  public static final ConfigProperty<String> MISSING_CHECKPOINT_STRATEGY = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Allows Hudi Streamer to decide the instant to consume from when checkpoint is not set.\n"
          + " Possible values: " + Arrays.toString(IncrSourceHelper.MissingCheckpointStrategy.values()));

  public static final ConfigProperty<String> SOURCE_FILE_FORMAT = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.file.format")
      .defaultValue("parquet")
      .markAdvanced()
      .withDocumentation("This config is passed to the reader while loading dataset. Default value is parquet.");

  public static final ConfigProperty<Boolean> HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.drop.all.meta.fields.from.source")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("Drops all meta fields from the source hudi table while ingesting into sink hudi table.");

  public static final ConfigProperty<String> HOODIE_SRC_PARTITION_FIELDS = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.partition.fields")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Specifies partition fields that needs to be added to source table after parsing _hoodie_partition_path.");

  public static final ConfigProperty<String> HOODIE_SRC_PARTITION_EXTRACTORCLASS = ConfigProperty
      .key("hoodie.deltastreamer.source.hoodieincr.partition.extractor.class")
      .noDefaultValue(SlashEncodedDayPartitionValueExtractor.class.getCanonicalName())
      .markAdvanced()
      .withDocumentation("PartitionValueExtractor class to extract partition fields from _hoodie_partition_path");
}
