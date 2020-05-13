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

package org.apache.hudi.client.bootstrap;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * Bootstrap Schema Provider. Schema provided in config is used. If not available, use schema from Parquet
 */
public class BootstrapSourceSchemaProvider {

  protected final HoodieWriteConfig bootstrapConfig;

  public BootstrapSourceSchemaProvider(HoodieWriteConfig bootstrapConfig) {
    this.bootstrapConfig = bootstrapConfig;
  }

  /**
   * Main API to select avro schema for bootstrapping.
   * @param jsc Java Spark Context
   * @param partitions  List of partitions with files within them
   * @return Avro Schema
   */
  public final Schema getBootstrapSchema(JavaSparkContext jsc, List<Pair<String, List<HoodieFileStatus>>> partitions) {
    if (bootstrapConfig.getSchema() != null) {
      // Use schema specified by user if set
      return Schema.parse(bootstrapConfig.getSchema());
    }
    return getBootstrapSourceSchema(jsc, partitions);
  }

  /**
   * Select a random file to be used to generate avro schema.
   * Override this method to get custom schema selection.
   * @param jsc Java Spark Context
   * @param partitions  List of partitions with files within them
   * @return Avro Schema
   */
  protected Schema getBootstrapSourceSchema(JavaSparkContext jsc,
      List<Pair<String, List<HoodieFileStatus>>> partitions) {
    return partitions.stream().flatMap(p -> p.getValue().stream())
        .map(fs -> {
          try {
            Path filePath = FileStatusUtils.toPath(fs.getPath());
            return ParquetUtils.readAvroSchema(jsc.hadoopConfiguration(), filePath);
          } catch (Exception ex) {
            return null;
          }
        }).filter(x -> x != null).findAny().get();
  }
}
