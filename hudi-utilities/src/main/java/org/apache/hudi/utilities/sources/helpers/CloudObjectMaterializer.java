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
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;

/**
 * Turns the cloud objects an incremental source has selected into rows. Selecting which objects
 * to read is common to every payload, so it stays in {@link CloudDataFetcher}; how those objects
 * become rows is not, which is what implementations supply.
 *
 * <p>Three concerns move together and so belong on one type: the predicate that decides which
 * object keys are eligible, how many Spark partitions the batch is spread over, and the read
 * itself. A columnar reader selects by data-file format, sizes partitions by bytes scanned and
 * defers to a Spark datasource; a reader that parses documents selects by document extension,
 * sizes partitions by the bytes it will actually parse, and builds rows itself.
 */
public interface CloudObjectMaterializer extends Serializable {

  /**
   * Predicate restricting which object keys are eligible, appended to the filter that
   * {@link CloudObjectsSelectorCommon#generateFilter} builds from the size and path configs.
   * Returns an empty string to add no restriction.
   *
   * @param objectKey column holding the object key, which differs per cloud store
   * @param props     streamer configs
   */
  String objectKeyPredicate(String objectKey, TypedProperties props);

  /**
   * Number of Spark partitions to spread this batch over. The default sizes by the bytes
   * referenced, which suits a columnar scan; override where the cost of a batch is not
   * proportional to the bytes it references.
   *
   * @param objects          objects selected for this batch
   * @param bytesPerPartition configured target bytes per partition
   * @param minPartitions    lower bound from the source profile, ignored when smaller
   */
  int partitionCount(List<CloudObjectMetadata> objects, long bytesPerPartition, int minPartitions);

  /**
   * Reads the selected objects into rows. Returns empty when there is nothing to read.
   */
  Option<Dataset<Row>> materialize(SparkSession spark,
                                   List<CloudObjectMetadata> objects,
                                   Option<SchemaProvider> schemaProvider,
                                   int numPartitions);
}
