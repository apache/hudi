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

import javax.annotation.concurrent.Immutable;

/**
 * S3 Event-based Hudi Incremental Source Configs
 */
@Immutable
@ConfigClassProperty(name = "S3 Event-based Hudi Incremental Source Configs",
    groupName = ConfigGroups.Names.DELTA_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of incremental pulling from S3 events "
        + "meta information from Hudi table as a source in Deltastreamer.")
public class S3EventsHoodieIncrSourceConfig extends HoodieConfig {

  public static final ConfigProperty<Boolean> S3_INCR_ENABLE_EXISTS_CHECK = ConfigProperty
      .key("hoodie.deltastreamer.source.s3incr.check.file.exists")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("Control whether we do existence check for files before consuming them");

  public static final ConfigProperty<String> S3_KEY_PREFIX = ConfigProperty
      .key("hoodie.deltastreamer.source.s3incr.key.prefix")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Control whether to filter the s3 objects starting with this prefix");

  public static final ConfigProperty<String> S3_FS_PREFIX = ConfigProperty
      .key("hoodie.deltastreamer.source.s3incr.fs.prefix")
      .defaultValue("s3")
      .markAdvanced()
      .withDocumentation("The file system prefix.");

  public static final ConfigProperty<String> S3_IGNORE_KEY_PREFIX = ConfigProperty
      .key("hoodie.deltastreamer.source.s3incr.ignore.key.prefix")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Control whether to ignore the s3 objects starting with this prefix");

  public static final ConfigProperty<String> S3_IGNORE_KEY_SUBSTRING = ConfigProperty
      .key("hoodie.deltastreamer.source.s3incr.ignore.key.substring")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Control whether to ignore the s3 objects with this substring");

  public static final ConfigProperty<String> SPARK_DATASOURCE_OPTIONS = ConfigProperty
      .key("hoodie.deltastreamer.source.s3incr.spark.datasource.options")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Json string, passed to the reader while loading dataset. Example delta streamer conf \n"
          + " --hoodie-conf hoodie.deltastreamer.source.s3incr.spark.datasource.options={\"header\":\"true\",\"encoding\":\"UTF-8\"}");
}
