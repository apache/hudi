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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.helpers.IncrSourceCloudStorageHelper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import java.util.List;

/**
 * Connects to GCS from Spark and downloads data from a given list of files.
 * Assumes SparkContext is already configured with GCS options through GcsEventsHoodieIncrSource.addGcsAccessConfs().
 */
public class FileDataFetcher implements Serializable {

  private final String fileFormat;
  private TypedProperties props;

  private static final Logger LOG = LogManager.getLogger(FileDataFetcher.class);

  private static final long serialVersionUID = 1L;

  public FileDataFetcher(TypedProperties props, String fileFormat) {
    this.fileFormat = fileFormat;
    this.props = props;
  }

  public Option<Dataset<Row>> fetchFileData(SparkSession spark, List<String> filepaths, TypedProperties props) {
    return IncrSourceCloudStorageHelper.fetchFileData(spark, filepaths, props, fileFormat);
  }

}
