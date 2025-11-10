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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.StringUtils;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkDataSourceTableUtils {
  /**
   * Get Spark Sql related table properties with Avro schema for nested comments.
   * @param partitionNames List of partition field names
   * @param sparkVersion Spark version
   * @param schemaLengthThreshold Schema length threshold
   * @param avroSchema Avro schema with nested comments
   * @return Map of Spark table properties
   */
  public static Map<String, String> getSparkTableProperties(List<String> partitionNames, String sparkVersion,
                                                            int schemaLengthThreshold, Schema avroSchema) {
    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.sql.sources.provider", "hudi");
    if (!StringUtils.isNullOrEmpty(sparkVersion)) {
      sparkProperties.put("spark.sql.create.version", sparkVersion);
    }

    // Convert schema using AvroToSparkJson for better type handling and comment support
    String schemaString = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);
    int numSchemaPart = (schemaString.length() + schemaLengthThreshold - 1) / schemaLengthThreshold;
    sparkProperties.put("spark.sql.sources.schema.numParts", String.valueOf(numSchemaPart));
    // Add each part of schema string to sparkProperties
    for (int i = 0; i < numSchemaPart; i++) {
      int start = i * schemaLengthThreshold;
      int end = Math.min(start + schemaLengthThreshold, schemaString.length());
      sparkProperties.put("spark.sql.sources.schema.part." + i, schemaString.substring(start, end));
    }
    // Add partition columns
    if (!partitionNames.isEmpty()) {
      sparkProperties.put("spark.sql.sources.schema.numPartCols", String.valueOf(partitionNames.size()));
      for (int i = 0; i < partitionNames.size(); i++) {
        sparkProperties.put("spark.sql.sources.schema.partCol." + i, partitionNames.get(i));
      }
    }
    return sparkProperties;
  }

  public static Map<String, String> getSparkSerdeProperties(boolean readAsOptimized, String basePath) {
    Map<String, String> sparkSerdeProperties = new HashMap<>();
    sparkSerdeProperties.put(ConfigUtils.TABLE_SERDE_PATH, basePath);
    sparkSerdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(readAsOptimized));
    return sparkSerdeProperties;
  }

}
