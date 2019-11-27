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

import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.common.util.collection.RocksDBBasedMap;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.config.TestSourceConfig;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public abstract class AbstractBaseTestSource extends AvroSource {

  static final int DEFAULT_PARTITION_NUM = 0;

  // Static instance, helps with reuse across a test.
  protected static transient Map<Integer, HoodieTestDataGenerator> dataGeneratorMap = new HashMap<>();

  public static void initDataGen() {
    dataGeneratorMap.putIfAbsent(DEFAULT_PARTITION_NUM,
        new HoodieTestDataGenerator(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS));
  }

  public static void initDataGen(TypedProperties props, int partition) {
    try {
      boolean useRocksForTestDataGenKeys = props.getBoolean(TestSourceConfig.USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS,
          TestSourceConfig.DEFAULT_USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS);
      String baseStoreDir = props.getString(TestSourceConfig.ROCKSDB_BASE_DIR_FOR_TEST_DATAGEN_KEYS,
          File.createTempFile("test_data_gen", ".keys").getParent()) + "/" + partition;
      log.info("useRocksForTestDataGenKeys=" + useRocksForTestDataGenKeys + ", BaseStoreDir=" + baseStoreDir);
      dataGeneratorMap.put(partition, new HoodieTestDataGenerator(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS,
          useRocksForTestDataGenKeys ? new RocksDBBasedMap<>(baseStoreDir) : new HashMap<>()));
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public static void resetDataGen() {
    for (HoodieTestDataGenerator dataGenerator : dataGeneratorMap.values()) {
      dataGenerator.close();
    }
    dataGeneratorMap.clear();
  }

  protected AbstractBaseTestSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  protected static Stream<GenericRecord> fetchNextBatch(TypedProperties props, int sourceLimit, String commitTime,
      int partition) {
    int maxUniqueKeys =
        props.getInteger(TestSourceConfig.MAX_UNIQUE_RECORDS_PROP, TestSourceConfig.DEFAULT_MAX_UNIQUE_RECORDS);

    HoodieTestDataGenerator dataGenerator = dataGeneratorMap.get(partition);

    // generate `sourceLimit` number of upserts each time.
    int numExistingKeys = dataGenerator.getNumExistingKeys();
    log.info("NumExistingKeys=" + numExistingKeys);

    int numUpdates = Math.min(numExistingKeys, sourceLimit / 2);
    int numInserts = sourceLimit - numUpdates;
    log.info("Before adjustments => numInserts=" + numInserts + ", numUpdates=" + numUpdates);

    if (numInserts + numExistingKeys > maxUniqueKeys) {
      // Limit inserts so that maxUniqueRecords is maintained
      numInserts = Math.max(0, maxUniqueKeys - numExistingKeys);
    }

    if ((numInserts + numUpdates) < sourceLimit) {
      // try to expand updates to safe limit
      numUpdates = Math.min(numExistingKeys, sourceLimit - numInserts);
    }

    log.info("NumInserts=" + numInserts + ", NumUpdates=" + numUpdates + ", maxUniqueRecords=" + maxUniqueKeys);
    long memoryUsage1 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    log.info("Before DataGen. Memory Usage=" + memoryUsage1 + ", Total Memory=" + Runtime.getRuntime().totalMemory()
        + ", Free Memory=" + Runtime.getRuntime().freeMemory());

    Stream<GenericRecord> updateStream = dataGenerator.generateUniqueUpdatesStream(commitTime, numUpdates)
        .map(hr -> AbstractBaseTestSource.toGenericRecord(hr, dataGenerator));
    Stream<GenericRecord> insertStream = dataGenerator.generateInsertsStream(commitTime, numInserts)
        .map(hr -> AbstractBaseTestSource.toGenericRecord(hr, dataGenerator));
    return Stream.concat(updateStream, insertStream);
  }

  private static GenericRecord toGenericRecord(HoodieRecord hoodieRecord, HoodieTestDataGenerator dataGenerator) {
    try {
      Option<IndexedRecord> recordOpt = hoodieRecord.getData().getInsertValue(dataGenerator.avroSchema);
      return (GenericRecord) recordOpt.get();
    } catch (IOException e) {
      return null;
    }
  }
}
