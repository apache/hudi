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
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SPARK_DATASOURCE_OPTIONS;

/**
 * Helper methods for when the incremental source is fetching from Cloud Storage, like AWS S3 buckets or GCS.
 */
public class IncrSourceCloudStorageHelper {

  private static final Logger LOG = LoggerFactory.getLogger(IncrSourceCloudStorageHelper.class);

  /**
   * @param filepaths Files from which to fetch data
   * @return Data in the given list of files, as a Spark DataSet
   */
  public static Option<Dataset<Row>> fetchFileData(SparkSession spark, List<String> filepaths,
                                                   TypedProperties props, String fileFormat) {
    if (filepaths.isEmpty()) {
      return Option.empty();
    }

    DataFrameReader dfReader = getDataFrameReader(spark, props, fileFormat);
    Dataset<Row> fileDataDs = dfReader.load(filepaths.toArray(new String[0]));
    return Option.of(fileDataDs);
  }

  private static DataFrameReader getDataFrameReader(SparkSession spark, TypedProperties props, String fileFormat) {
    DataFrameReader dataFrameReader = spark.read().format(fileFormat);

    if (isNullOrEmpty(getStringWithAltKeys(props, SPARK_DATASOURCE_OPTIONS, true))) {
      return dataFrameReader;
    }

    final ObjectMapper mapper = new ObjectMapper();
    Map<String, String> sparkOptionsMap = null;

    try {
      sparkOptionsMap = mapper.readValue(getStringWithAltKeys(props, SPARK_DATASOURCE_OPTIONS), Map.class);
    } catch (IOException e) {
      throw new HoodieException(String.format("Failed to parse sparkOptions: %s",
          getStringWithAltKeys(props, SPARK_DATASOURCE_OPTIONS)), e);
    }

    LOG.info("SparkOptions loaded: {}", sparkOptionsMap);

    return dataFrameReader.options(sparkOptionsMap);
  }

}
