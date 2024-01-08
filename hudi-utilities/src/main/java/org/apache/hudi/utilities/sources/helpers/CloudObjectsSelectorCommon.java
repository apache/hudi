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

import org.apache.avro.Schema;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_MAX_FILE_SIZE;
import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.CloudSourceConfig.PATH_BASED_PARTITION_FIELDS;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SOURCE_MAX_BYTES_PER_PARTITION;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SPARK_DATASOURCE_READER_COMMA_SEPARATED_PATH_FORMAT;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.split;

/**
 * Generic helper methods to fetch from Cloud Storage during incremental fetch from cloud storage buckets.
 * NOTE: DO NOT use any implementation specific classes here. This class is supposed to across S3EventsSource,
 * GcsEventsSource etc...so you can't assume the classes for your specific implementation will be available here.
 */
public class CloudObjectsSelectorCommon {

  private static final Logger LOG = LoggerFactory.getLogger(CloudObjectsSelectorCommon.class);

  /**
   * Return a function that extracts filepaths from a list of Rows.
   * Here Row is assumed to have the schema [bucket_name, filepath_relative_to_bucket, object_size]
   * @param storageUrlSchemePrefix    Eg: s3:// or gs://. The storage-provider-specific prefix to use within the URL.
   * @param serializableHadoopConf
   * @param checkIfExists             check if each file exists, before adding it to the returned list
   * @return
   */
  public static MapPartitionsFunction<Row, CloudObjectMetadata> getCloudObjectMetadataPerPartition(
      String storageUrlSchemePrefix, SerializableConfiguration serializableHadoopConf, boolean checkIfExists) {
    return rows -> {
      List<CloudObjectMetadata> cloudObjectMetadataPerPartition = new ArrayList<>();
      rows.forEachRemaining(row -> {
        Option<String> filePathUrl = getUrlForFile(row, storageUrlSchemePrefix, serializableHadoopConf, checkIfExists);
        filePathUrl.ifPresent(url -> {
          LOG.info("Adding file: " + url);
          long size;
          Object obj = row.get(2);
          if (obj instanceof String) {
            size = Long.parseLong((String) obj);
          } else if (obj instanceof Integer) {
            size = ((Integer) obj).longValue();
          } else if (obj instanceof Long) {
            size = (long) obj;
          } else {
            throw new HoodieIOException("unexpected object size's type in Cloud storage events: " + obj.getClass());
          }
          cloudObjectMetadataPerPartition.add(new CloudObjectMetadata(url, size));
        });
      });

      return cloudObjectMetadataPerPartition.iterator();
    };
  }

  /**
   * Construct a full qualified URL string to a cloud file from a given Row. Optionally check if the file exists.
   * Here Row is assumed to have the schema [bucket_name, filepath_relative_to_bucket].
   * The checkIfExists logic assumes that the relevant impl classes for the storageUrlSchemePrefix are already present
   * on the classpath!
   *
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

  public static Option<Dataset<Row>> loadAsDataset(SparkSession spark, List<CloudObjectMetadata> cloudObjectMetadata,
                                                   TypedProperties props, String fileFormat, Option<SchemaProvider> schemaProviderOption) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Extracted distinct files " + cloudObjectMetadata.size()
          + " and some samples " + cloudObjectMetadata.stream().map(CloudObjectMetadata::getPath).limit(10).collect(Collectors.toList()));
    }

    if (isNullOrEmpty(cloudObjectMetadata)) {
      return Option.empty();
    }
    DataFrameReader reader = spark.read().format(fileFormat);
    String datasourceOpts = getStringWithAltKeys(props, CloudSourceConfig.SPARK_DATASOURCE_OPTIONS, true);
    if (schemaProviderOption.isPresent()) {
      Schema sourceSchema = schemaProviderOption.get().getSourceSchema();
      if (sourceSchema != null && !sourceSchema.equals(InputBatch.NULL_SCHEMA)) {
        reader = reader.schema(AvroConversionUtils.convertAvroSchemaToStructType(sourceSchema));
      }
    }
    if (StringUtils.isNullOrEmpty(datasourceOpts)) {
      // fall back to legacy config for BWC. TODO consolidate in HUDI-6020
      datasourceOpts = getStringWithAltKeys(props, S3EventsHoodieIncrSourceConfig.SPARK_DATASOURCE_OPTIONS, true);
    }
    if (StringUtils.nonEmpty(datasourceOpts)) {
      final ObjectMapper mapper = new ObjectMapper();
      Map<String, String> sparkOptionsMap = null;
      try {
        sparkOptionsMap = mapper.readValue(datasourceOpts, Map.class);
      } catch (IOException e) {
        throw new HoodieException(String.format("Failed to parse sparkOptions: %s", datasourceOpts), e);
      }
      LOG.info(String.format("sparkOptions loaded: %s", sparkOptionsMap));
      reader = reader.options(sparkOptionsMap);
    }
    List<String> paths = new ArrayList<>();
    long totalSize = 0;
    for (CloudObjectMetadata o : cloudObjectMetadata) {
      paths.add(o.getPath());
      totalSize += o.getSize();
    }
    // inflate 10% for potential hoodie meta fields
    totalSize *= 1.1;
    // if source bytes are provided, then give preference to that.
    long bytesPerPartition = props.containsKey(SOURCE_MAX_BYTES_PER_PARTITION.key()) ? props.getLong(SOURCE_MAX_BYTES_PER_PARTITION.key()) :
        props.getLong(PARQUET_MAX_FILE_SIZE.key(), Long.parseLong(PARQUET_MAX_FILE_SIZE.defaultValue()));
    int numPartitions = (int) Math.max(Math.ceil(totalSize / bytesPerPartition), 1);
    boolean isCommaSeparatedPathFormat = props.getBoolean(SPARK_DATASOURCE_READER_COMMA_SEPARATED_PATH_FORMAT.key(), false);

    Dataset<Row> dataset;
    if (isCommaSeparatedPathFormat) {
      dataset = reader.load(String.join(",", paths));
    } else {
      dataset = reader.load(paths.toArray(new String[cloudObjectMetadata.size()]));
    }
    dataset = coalesceOrRepartition(dataset, numPartitions);
    // add partition column from source path if configured
    if (containsConfigProperty(props, PATH_BASED_PARTITION_FIELDS)) {
      String[] partitionKeysToAdd = getStringWithAltKeys(props, PATH_BASED_PARTITION_FIELDS).split(",");
      // Add partition column for all path-based partition keys. If key is not present in path, the value will be null.
      for (String partitionKey : partitionKeysToAdd) {
        String partitionPathPattern = String.format("%s=", partitionKey);
        LOG.info(String.format("Adding column %s to dataset", partitionKey));
        dataset = dataset.withColumn(partitionKey, split(split(input_file_name(), partitionPathPattern).getItem(1), "/").getItem(0));
      }
    }
    return Option.of(dataset);
  }

  private static Dataset<Row> coalesceOrRepartition(Dataset dataset, int numPartitions) {
    int existingNumPartitions = dataset.rdd().getNumPartitions();
    LOG.info(String.format("existing number of partitions=%d, required number of partitions=%d", existingNumPartitions, numPartitions));
    if (existingNumPartitions < numPartitions) {
      dataset = dataset.repartition(numPartitions);
    } else {
      dataset = dataset.coalesce(numPartitions);
    }
    return dataset;
  }

  public static Option<Dataset<Row>> loadAsDataset(SparkSession spark, List<CloudObjectMetadata> cloudObjectMetadata, TypedProperties props, String fileFormat) {
    return loadAsDataset(spark, cloudObjectMetadata, props, fileFormat, Option.empty());
  }
}
