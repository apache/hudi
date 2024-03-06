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
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.getCloudObjectMetadataPerPartition;

/**
 * Extracts a list of GCS {@link CloudObjectMetadata} containing metadata of GCS objects from a given Spark Dataset as input.
 * Optionally:
 * i) Match the filename and path against provided input filter strings
 * ii) Check if each file exists on GCS, in which case it assumes SparkContext is already
 * configured with GCS options through GcsEventsHoodieIncrSource.addGcsAccessConfs().
 */
public class GcsObjectMetadataFetcher implements Serializable {

  private final TypedProperties props;

  private static final String GCS_PREFIX = "gs://";
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(GcsObjectMetadataFetcher.class);

  public GcsObjectMetadataFetcher(TypedProperties props) {
    this.props = props;
  }

  /**
   * @param cloudObjectMetadataDF a Dataset that contains metadata of GCS objects. Assumed to be a persisted form
   *                              of a Cloud Storage Pubsub Notification event.
   * @param checkIfExists         Check if each file exists, before returning its full path
   * @return A {@link List} of {@link CloudObjectMetadata} containing GCS info.
   */
  public List<CloudObjectMetadata> getGcsObjectMetadata(JavaSparkContext jsc, Dataset<Row> cloudObjectMetadataDF, boolean checkIfExists) {
    SerializableConfiguration serializableHadoopConf = new SerializableConfiguration(jsc.hadoopConfiguration());
    return cloudObjectMetadataDF
        .select("bucket", "name", "size")
        .distinct()
        .mapPartitions(getCloudObjectMetadataPerPartition(GCS_PREFIX, serializableHadoopConf, checkIfExists), Encoders.kryo(CloudObjectMetadata.class))
        .collectAsList();
  }

  /**
   * @param cloudObjectMetadataDF a Dataset that contains metadata of GCS objects. Assumed to be a persisted form
   *                              of a Cloud Storage Pubsub Notification event.
   * @return Dataset<Row> after apply the filtering.
   */
  public Dataset<Row> applyFilter(Dataset<Row> cloudObjectMetadataDF) {
    String filter = CloudObjectsSelectorCommon.generateFilter(CloudObjectsSelectorCommon.Type.GCS, props);
    LOG.info("Adding filter string to Dataset: " + filter);

    return cloudObjectMetadataDF.filter(filter);
  }
}
