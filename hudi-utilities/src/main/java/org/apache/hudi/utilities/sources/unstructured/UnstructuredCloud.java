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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;

/**
 * The cloud whose events table an unstructured data source reads ({@link UnstructuredSourceConfig#CLOUD}): the object key and size
 * columns of that table, and the selector type that sets the path prefix and the bucket column.
 */
public enum UnstructuredCloud {

  /** The events table of the S3 events source; the path prefix is {@code hoodie.streamer.source.s3incr.fs.prefix}. */
  S3("s3", CloudObjectsSelectorCommon.Type.S3, CloudObjectsSelectorCommon.S3_OBJECT_KEY, CloudObjectsSelectorCommon.S3_OBJECT_SIZE),
  /** The events table of the GCS events source; the path prefix is {@code gs://}. */
  GCS("gcs", CloudObjectsSelectorCommon.Type.GCS, CloudObjectsSelectorCommon.GCS_OBJECT_KEY, CloudObjectsSelectorCommon.GCS_OBJECT_SIZE);

  private final String configValue;
  private final CloudObjectsSelectorCommon.Type selectorType;
  private final String keyColumn;
  private final String sizeColumn;

  UnstructuredCloud(String configValue, CloudObjectsSelectorCommon.Type selectorType, String keyColumn, String sizeColumn) {
    this.configValue = configValue;
    this.selectorType = selectorType;
    this.keyColumn = keyColumn;
    this.sizeColumn = sizeColumn;
  }

  /**
   * The cloud {@link UnstructuredSourceConfig#CLOUD} names.
   *
   * @throws IllegalArgumentException naming the key and its values when it is unset or another value
   */
  public static UnstructuredCloud from(TypedProperties props) {
    return UnstructuredSourceKeys.read(props, UnstructuredSourceConfig.CLOUD, values(), UnstructuredCloud::configValue);
  }

  /** The value of {@link UnstructuredSourceConfig#CLOUD} that names this cloud. */
  public String configValue() {
    return configValue;
  }

  /** The selector type, which sets the path prefix and the bucket column of the selected objects. */
  public CloudObjectsSelectorCommon.Type selectorType() {
    return selectorType;
  }

  /** The events table's column of the object key. */
  public String keyColumn() {
    return keyColumn;
  }

  /** The events table's column of the object size. */
  public String sizeColumn() {
    return sizeColumn;
  }

  /**
   * Refuses a key that belongs only to another cloud's events table, and changes nothing. Refusals name {@code sourceName}.
   *
   * @throws IllegalArgumentException when such a key is set
   */
  public void validate(TypedProperties props, String sourceName) {
    if (this == GCS && ConfigUtils.containsConfigProperty(props, S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX)) {
      throw new IllegalArgumentException(sourceName + " refuses " + S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key() + " under "
          + UnstructuredSourceConfig.CLOUD.key() + "=" + configValue + ": the GCS events table's objects are read under gs://");
    }
  }
}
