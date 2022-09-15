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
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.sources.helpers.CloudStoreIngestionConfig.CLOUD_DATAFILE_EXTENSION;
import static org.apache.hudi.utilities.sources.helpers.CloudStoreIngestionConfig.IGNORE_RELATIVE_PATH_PREFIX;
import static org.apache.hudi.utilities.sources.helpers.CloudStoreIngestionConfig.IGNORE_RELATIVE_PATH_SUBSTR;
import static org.apache.hudi.utilities.sources.helpers.CloudStoreIngestionConfig.SELECT_RELATIVE_PATH_PREFIX;

/**
 * Extracts a list of fully qualified GCS filepaths from a given Spark Dataset as input.
 * Optionally:
 * i) Match the filename and path against provided input filter strings
 * ii) Check if each file exists on GCS, in which case it assumes SparkContext is already
 * configured with GCS options through GcsEventsHoodieIncrSource.addGcsAccessConfs().
 */
public class FilePathsFetcher implements Serializable {

  /**
   * The default file format to assume if {@link GcsIngestionConfig#GCS_INCR_DATAFILE_EXTENSION} is not given.
   */
  private final String fileFormat;
  private final TypedProperties props;

  private static final String GCS_PREFIX = "gs://";
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LogManager.getLogger(FilePathsFetcher.class);

  /**
   * @param fileFormat The default file format to assume if {@link GcsIngestionConfig#GCS_INCR_DATAFILE_EXTENSION}
   *                   is not given.
   */
  public FilePathsFetcher(TypedProperties props, String fileFormat) {
    this.props = props;
    this.fileFormat = fileFormat;
  }

  /**
   * @param sourceForFilenames a Dataset that contains metadata about files on GCS. Assumed to be a persisted form
   *                           of a Cloud Storage Pubsub Notification event.
   * @param checkIfExists      Check if each file exists, before returning its full path
   * @return A list of fully qualified GCS file paths.
   */
  public List<String> getGcsFilePaths(JavaSparkContext jsc, Dataset<Row> sourceForFilenames, boolean checkIfExists) {
    String filter = createFilter();
    LOG.info("Adding filter string to Dataset: " + filter);

    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(
            jsc.hadoopConfiguration());

    return sourceForFilenames
            .filter(filter)
            .select("bucket", "name")
            .distinct()
            .rdd().toJavaRDD().mapPartitions(
                    CloudObjectsSelectorCommon.getCloudFilesPerPartition(GCS_PREFIX, serializableConfiguration, checkIfExists)
            ).collect();
  }

  /**
   * Add optional filters that narrow down the list of filenames to fetch.
   */
  private String createFilter() {
    StringBuilder filter = new StringBuilder("size > 0");

    getPropVal(SELECT_RELATIVE_PATH_PREFIX).ifPresent(val -> filter.append(" and name like '" + val + "%'"));
    getPropVal(IGNORE_RELATIVE_PATH_PREFIX).ifPresent(val -> filter.append(" and name not like '" + val + "%'"));
    getPropVal(IGNORE_RELATIVE_PATH_SUBSTR).ifPresent(val -> filter.append(" and name not like '%" + val + "%'"));

    // Match files with a given extension, or use the fileFormat as the default.
    getPropVal(CLOUD_DATAFILE_EXTENSION).or(() -> Option.of(fileFormat))
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
