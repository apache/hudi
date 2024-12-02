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

package org.apache.hudi.utilities.testutils.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.SourceTestConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Test DataSource which scales test-data generation by using spark parallelism.
 */
public class DistributedTestDataSource extends AbstractBaseTestSource {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedTestDataSource.class);

  private final int numTestSourcePartitions;

  public DistributedTestDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.numTestSourcePartitions = ConfigUtils.getIntWithAltKeys(props, SourceTestConfig.NUM_SOURCE_PARTITIONS_PROP);
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    int nextCommitNum = lastCheckpoint.map(s -> Integer.parseInt(s.getCheckpointKey()) + 1).orElse(0);
    String instantTime = String.format("%05d", nextCommitNum);
    LOG.info("Source Limit is set to {}", sourceLimit);

    // No new data.
    if (sourceLimit <= 0) {
      return new InputBatch<>(Option.empty(), instantTime);
    }

    TypedProperties newProps = new TypedProperties();
    newProps.putAll(props);

    // Set the maxUniqueRecords per partition for TestDataSource
    int maxUniqueRecords = ConfigUtils.getIntWithAltKeys(props, SourceTestConfig.MAX_UNIQUE_RECORDS_PROP);
    String maxUniqueRecordsPerPartition = String.valueOf(Math.max(1, maxUniqueRecords / numTestSourcePartitions));
    newProps.setProperty(SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), maxUniqueRecordsPerPartition);
    int perPartitionSourceLimit = Math.max(1, (int) (sourceLimit / numTestSourcePartitions));
    JavaRDD<GenericRecord> avroRDD =
        sparkContext.parallelize(IntStream.range(0, numTestSourcePartitions).boxed().collect(Collectors.toList()),
            numTestSourcePartitions).mapPartitionsWithIndex((p, idx) -> {
              LOG.info("Initializing source with newProps={}", newProps);
              if (!dataGeneratorMap.containsKey(p)) {
                initDataGen(newProps, p);
              }
              return fetchNextBatch(newProps, perPartitionSourceLimit, instantTime, p).iterator();
            }, true);
    return new InputBatch<>(Option.of(avroRDD), instantTime);
  }
}
