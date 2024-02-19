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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.CloudSourceConfig.DATAFILE_FORMAT;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.SOURCE_FILE_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.loadAsDataset;

/**
 * Connects to S3/GCS from Spark and downloads data from a given list of files.
 * Assumes SparkContext is already configured.
 */
public class CloudDataFetcher implements Serializable {

  private static final String EMPTY_STRING = "";

  private final TypedProperties props;

  private static final Logger LOG = LoggerFactory.getLogger(CloudDataFetcher.class);

  private static final long serialVersionUID = 1L;

  public CloudDataFetcher(TypedProperties props) {
    this.props = props;
  }

  public static String getFileFormat(TypedProperties props) {
    // This is to ensure backward compatibility where we were using the
    // config SOURCE_FILE_FORMAT for file format in previous versions.
    return StringUtils.isNullOrEmpty(getStringWithAltKeys(props, DATAFILE_FORMAT, EMPTY_STRING))
        ? getStringWithAltKeys(props, SOURCE_FILE_FORMAT, true)
        : getStringWithAltKeys(props, DATAFILE_FORMAT, EMPTY_STRING);
  }

  public Option<Dataset<Row>> getCloudObjectDataDF(SparkSession spark, List<CloudObjectMetadata> cloudObjectMetadata,
                                                   TypedProperties props, Option<SchemaProvider> schemaProviderOption) {
    return loadAsDataset(spark, cloudObjectMetadata, props, getFileFormat(props), schemaProviderOption);
  }
}
