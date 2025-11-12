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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.testutils.sources.AbstractBaseTestSource;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * An implementation of {@link Source}, that emits test upserts.
 */
public class TestDataSource extends AbstractBaseTestSource {

  private static final Logger LOG = LoggerFactory.getLogger(TestDataSource.class);
  public static boolean returnEmptyBatch = false;
  public static Option<String> recordInstantTime = Option.empty();
  private static int counter = 0;

  public TestDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    initDataGen();
    returnEmptyBatch = false;
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {

    int nextCommitNum = lastCheckpoint.map(s -> Integer.parseInt(s.getCheckpointKey()) + 1).orElse(0);
    String instantTime = String.format("%05d", nextCommitNum);
    LOG.info("Source Limit is set to " + sourceLimit);

    // No new data.
    if (sourceLimit <= 0 || returnEmptyBatch) {
      LOG.warn("Return no new data from Test Data source " + counter + ", source limit " + sourceLimit);
      return new InputBatch<>(Option.empty(), lastCheckpoint.orElse(null));
    } else {
      LOG.warn("Returning valid data from Test Data source " + counter + ", source limit " + sourceLimit);
    }
    counter++;

    List<GenericRecord> records =
        fetchNextBatch(props, (int) sourceLimit, recordInstantTime.orElse(instantTime), DEFAULT_PARTITION_NUM).collect(Collectors.toList());
    JavaRDD<GenericRecord> avroRDD = sparkContext.<GenericRecord>parallelize(records, 4);
    return new InputBatch<>(Option.of(avroRDD), instantTime);
  }
}
