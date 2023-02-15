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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generic helper methods to fetch from Cloud Storage during incremental fetch from cloud storage buckets.
 * NOTE: DO NOT use any implementation specific classes here. This class is supposed to across S3EventsSource,
 * GcsEventsSource etc...so you can't assume the classes for your specific implementation will be available here.
 */
public class CloudObjectsSelectorCommon {

  private static final Logger LOG = LogManager.getLogger(CloudObjectsSelectorCommon.class);

  /**
   * Return a function that extracts filepaths from a list of Rows.
   * Here Row is assumed to have the schema [bucket_name, filepath_relative_to_bucket]
   * @param storageUrlSchemePrefix Eg: s3:// or gs://. The storage-provider-specific prefix to use within the URL.
   * @param serializableConfiguration
   * @param checkIfExists check if each file exists, before adding it to the returned list
   * @return
   */
  public static FlatMapFunction<Iterator<Row>, String> getCloudFilesPerPartition(
          String storageUrlSchemePrefix, SerializableConfiguration serializableConfiguration, boolean checkIfExists) {
    return rows -> {
      List<String> cloudFilesPerPartition = new ArrayList<>();
      rows.forEachRemaining(row -> {
        Option<String> filePathUrl = getUrlForFile(row, storageUrlSchemePrefix, serializableConfiguration,
                checkIfExists);
        filePathUrl.ifPresent(url -> {
          LOG.info("Adding file: " + url);
          cloudFilesPerPartition.add(url);
        });
      });

      return cloudFilesPerPartition.iterator();
    };
  }

  /**
   * Construct a full qualified URL string to a cloud file from a given Row. Optionally check if the file exists.
   * Here Row is assumed to have the schema [bucket_name, filepath_relative_to_bucket].
   * The checkIfExists logic assumes that the relevant impl classes for the storageUrlSchemePrefix are already present
   * on the classpath!
   * @param storageUrlSchemePrefix Eg: s3:// or gs://. The storage-provider-specific prefix to use within the URL.
   */
  private static Option<String> getUrlForFile(Row row, String storageUrlSchemePrefix,
                                              SerializableConfiguration serializableConfiguration,
                                              boolean checkIfExists) {
    final Configuration configuration = serializableConfiguration.newCopy();

    String bucket = row.getString(0);
    String filePath = storageUrlSchemePrefix + bucket + "/" + row.getString(1);

    try {
      String filePathUrl = URLDecoder.decode(filePath, StandardCharsets.UTF_8.name());
      if (!checkIfExists) {
        return Option.of(filePathUrl);
      }
      boolean exists = checkIfFileExists(storageUrlSchemePrefix, bucket, filePathUrl, configuration);
      return exists ? Option.of(filePathUrl) : Option.empty();
    } catch (Exception exception) {
      LOG.warn(String.format("Failed to generate path to cloud file %s", filePath), exception);
      throw new HoodieException(String.format("Failed to generate path to cloud file %s", filePath), exception);
    }
  }

  /**
   * Check if file with given path URL exists
   * @param storageUrlSchemePrefix Eg: s3:// or gs://. The storage-provider-specific prefix to use within the URL.
   */
  private static boolean checkIfFileExists(String storageUrlSchemePrefix, String bucket, String filePathUrl,
                                          Configuration configuration) {
    try {
      FileSystem fs = FSUtils.getFs(storageUrlSchemePrefix + bucket, configuration);
      return fs.exists(new Path(filePathUrl));
    } catch (IOException ioe) {
      String errMsg = String.format("Error while checking path exists for %s ", filePathUrl);
      LOG.error(errMsg, ioe);
      throw new HoodieIOException(errMsg, ioe);
    }
  }
}
