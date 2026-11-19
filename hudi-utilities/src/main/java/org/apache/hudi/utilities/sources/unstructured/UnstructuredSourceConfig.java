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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

/**
 * The two keys that tell {@code UnstructuredDataSource} which job it serves and which cloud's events table it reads. Both are
 * required and have no default.
 */
@Immutable
@ConfigClassProperty(name = "Unstructured Data Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations selecting the job and the cloud of the unstructured data source.")
public class UnstructuredSourceConfig extends HoodieConfig {

  public static final String PREFIX = "hoodie.unstructured.source.";

  /** The job: {@code blob} writes the blob table ({@link UnstructuredMode}). */
  public static final ConfigProperty<String> MODE = ConfigProperty
      .key(PREFIX + "mode")
      .noDefaultValue("<required>")
      .sinceVersion("1.3.0")
      .withDocumentation("Which table the unstructured data source's job writes: blob (the blob table, through the quarantine "
          + "transformer). Required, no default.");

  /** The cloud whose events table the source reads: {@code s3} or {@code gcs} ({@link UnstructuredCloud}). */
  public static final ConfigProperty<String> CLOUD = ConfigProperty
      .key(PREFIX + "cloud")
      .noDefaultValue("<required>")
      .sinceVersion("1.3.0")
      .withDocumentation("Which cloud's events table the unstructured data source reads: s3 (the table the S3 events source "
          + "builds) or gcs (the table the GCS events source builds). Required, no default. A key that belongs only to the other "
          + "cloud is refused at start, such as hoodie.streamer.source.s3incr.fs.prefix under gcs.");
}
