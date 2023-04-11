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

package org.apache.hudi.utilities.sources.helpers.gcs;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.config.CloudSourceConfig.CLOUD_DATAFILE_EXTENSION;
import static org.apache.hudi.utilities.config.CloudSourceConfig.IGNORE_RELATIVE_PATH_PREFIX;
import static org.apache.hudi.utilities.config.CloudSourceConfig.IGNORE_RELATIVE_PATH_SUBSTR;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SELECT_RELATIVE_PATH_PREFIX;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.getCloudObjectMetadataPerPartition;

/**
 * Extracts a list of GCS {@link CloudObjectMetadata} containing metadata of GCS objects from a given Spark Dataset as input.
 * Optionally:
 * i) Match the filename and path against provided input filter strings
 * ii) Check if each file exists on GCS, in which case it assumes SparkContext is already
 * configured with GCS options through GcsEventsHoodieIncrSource.addGcsAccessConfs().
 */
public class GcsObjectMetadataFetcher implements Serializable {

  /**
   * The default file format to assume if {@link GcsIngestionConfig#GCS_INCR_DATAFILE_EXTENSION} is not given.
   */
  private final String fileFormat;
  private final TypedProperties props;

  private static final String GCS_PREFIX = "gs://";
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(GcsObjectMetadataFetcher.class);

  /**
   * @param fileFormat The default file format to assume if {@link GcsIngestionConfig#GCS_INCR_DATAFILE_EXTENSION}
   *                   is not given.
   */
  public GcsObjectMetadataFetcher(TypedProperties props, String fileFormat) {
    this.props = props;
    this.fileFormat = fileFormat;
  }

  /**
   * @param cloudObjectMetadataDF a Dataset that contains metadata of GCS objects. Assumed to be a persisted form
   *                              of a Cloud Storage Pubsub Notification event.
   * @param checkIfExists         Check if each file exists, before returning its full path
   * @return A {@link List} of {@link CloudObjectMetadata} containing GCS info.
   */
  public List<CloudObjectMetadata> getGcsObjectMetadata(JavaSparkContext jsc, Dataset<Row> cloudObjectMetadataDF, boolean checkIfExists) {
    String filter = createFilter();
    LOG.info("Adding filter string to Dataset: " + filter);

    SerializableConfiguration serializableHadoopConf = new SerializableConfiguration(jsc.hadoopConfiguration());

    return cloudObjectMetadataDF
        .filter(filter)
        .select("bucket", "name", "size")
        .distinct()
        .mapPartitions(getCloudObjectMetadataPerPartition(GCS_PREFIX, serializableHadoopConf, checkIfExists), Encoders.kryo(CloudObjectMetadata.class))
        .collectAsList();
  }

  /**
   * Add optional filters that narrow down the list of GCS objects to fetch.
   */
  private String createFilter() {
    StringBuilder filter = new StringBuilder("size > 0");

    getPropVal(SELECT_RELATIVE_PATH_PREFIX.key()).ifPresent(val -> filter.append(" and name like '" + val + "%'"));
    getPropVal(IGNORE_RELATIVE_PATH_PREFIX.key()).ifPresent(val -> filter.append(" and name not like '" + val + "%'"));
    getPropVal(IGNORE_RELATIVE_PATH_SUBSTR.key()).ifPresent(val -> filter.append(" and name not like '%" + val + "%'"));

    // Match files with a given extension, or use the fileFormat as the default.
    getPropVal(CLOUD_DATAFILE_EXTENSION.key()).or(() -> Option.of(fileFormat))
        .map(val -> filter.append(" and name like '%" + val + "'"));

    return filter.toString();
  }

  private Option<String> getPropVal(String propName) {
    if (!isNullOrEmpty(props.getString(propName, null))) {
      return Option.of(props.getString(propName));
    }

    return Option.empty();
  }
}
