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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.collection.RocksDBBasedMap;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.AvroSource;
import org.apache.hudi.utilities.testutils.sources.config.SourceConfigs;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class AbstractBaseTestSource extends AvroSource {

  private static final Logger LOG = LogManager.getLogger(AbstractBaseTestSource.class);

  public static final int DEFAULT_PARTITION_NUM = 0;

  // Static instance, helps with reuse across a test.
  public static transient Map<Integer, HoodieTestDataGenerator> dataGeneratorMap = new HashMap<>();

  public static void initDataGen() {
    dataGeneratorMap.putIfAbsent(DEFAULT_PARTITION_NUM,
        new HoodieTestDataGenerator(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS));
  }

  public static void initDataGen(TypedProperties props, int partition) {
    try {
      boolean useRocksForTestDataGenKeys = props.getBoolean(SourceConfigs.USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS,
          SourceConfigs.DEFAULT_USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS);
      String baseStoreDir = props.getString(SourceConfigs.ROCKSDB_BASE_DIR_FOR_TEST_DATAGEN_KEYS,
          File.createTempFile("test_data_gen", ".keys").getParent()) + "/" + partition;
      LOG.info("useRocksForTestDataGenKeys=" + useRocksForTestDataGenKeys + ", BaseStoreDir=" + baseStoreDir);
      dataGeneratorMap.put(partition, new HoodieTestDataGenerator(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS,
          useRocksForTestDataGenKeys ? new RocksDBBasedMap<>(baseStoreDir) : new HashMap<>()));
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public static void initDataGen(SQLContext sqlContext, String globParquetPath, int partition) {
    List<Row> rows = sqlContext.read().format("hudi").load(globParquetPath)
        .select("_hoodie_record_key", "_hoodie_partition_path")
        .collectAsList();
    Map<Integer, HoodieTestDataGenerator.KeyPartition> keyPartitionMap = IntStream
        .range(0, rows.size()).boxed()
        .collect(Collectors.toMap(Function.identity(), i -> {
          Row r = rows.get(i);
          HoodieTestDataGenerator.KeyPartition kp = new HoodieTestDataGenerator.KeyPartition();
          kp.key = new HoodieKey(r.getString(0), r.getString(1));
          kp.partitionPath = r.getString(1);
          return kp;
        }));
    dataGeneratorMap.put(partition,
        new HoodieTestDataGenerator(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, keyPartitionMap));
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

  protected static Stream<GenericRecord> fetchNextBatch(TypedProperties props, int sourceLimit, String instantTime,
      int partition) {
    int maxUniqueKeys =
        props.getInteger(SourceConfigs.MAX_UNIQUE_RECORDS_PROP, SourceConfigs.DEFAULT_MAX_UNIQUE_RECORDS);

    HoodieTestDataGenerator dataGenerator = dataGeneratorMap.get(partition);

    // generate `sourceLimit` number of upserts each time.
    int numExistingKeys = dataGenerator.getNumExistingKeys(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    LOG.info("NumExistingKeys=" + numExistingKeys);

    int numUpdates = Math.min(numExistingKeys, sourceLimit / 2);
    int numInserts = sourceLimit - numUpdates;
    LOG.info("Before adjustments => numInserts=" + numInserts + ", numUpdates=" + numUpdates);
    boolean reachedMax = false;

    if (numInserts + numExistingKeys > maxUniqueKeys) {
      // Limit inserts so that maxUniqueRecords is maintained
      numInserts = Math.max(0, maxUniqueKeys - numExistingKeys);
      reachedMax = true;
    }

    if ((numInserts + numUpdates) < sourceLimit) {
      // try to expand updates to safe limit
      numUpdates = Math.min(numExistingKeys, sourceLimit - numInserts);
    }

    Stream<GenericRecord> deleteStream = Stream.empty();
    Stream<GenericRecord> updateStream;
    long memoryUsage1 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    LOG.info("Before DataGen. Memory Usage=" + memoryUsage1 + ", Total Memory=" + Runtime.getRuntime().totalMemory()
        + ", Free Memory=" + Runtime.getRuntime().freeMemory());
    if (!reachedMax && numUpdates >= 50) {
      LOG.info("After adjustments => NumInserts=" + numInserts + ", NumUpdates=" + (numUpdates - 50) + ", NumDeletes=50, maxUniqueRecords="
          + maxUniqueKeys);
      // if we generate update followed by deletes -> some keys in update batch might be picked up for deletes. Hence generating delete batch followed by updates
      deleteStream = dataGenerator.generateUniqueDeleteRecordStream(instantTime, 50).map(AbstractBaseTestSource::toGenericRecord);
      updateStream = dataGenerator.generateUniqueUpdatesStream(instantTime, numUpdates - 50, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
          .map(AbstractBaseTestSource::toGenericRecord);
    } else {
      LOG.info("After adjustments => NumInserts=" + numInserts + ", NumUpdates=" + numUpdates + ", maxUniqueRecords=" + maxUniqueKeys);
      updateStream = dataGenerator.generateUniqueUpdatesStream(instantTime, numUpdates, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
          .map(AbstractBaseTestSource::toGenericRecord);
    }
    Stream<GenericRecord> insertStream = dataGenerator.generateInsertsStream(instantTime, numInserts, false, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .map(AbstractBaseTestSource::toGenericRecord);
    if (Boolean.valueOf(props.getOrDefault("hoodie.test.source.generate.inserts", "false").toString())) {
      return insertStream;
    }
    return Stream.concat(deleteStream, Stream.concat(updateStream, insertStream));
  }

  private static GenericRecord toGenericRecord(HoodieRecord hoodieRecord) {
    try {
      RawTripTestPayload payload = (RawTripTestPayload) hoodieRecord.getData();
      return (GenericRecord) payload.getRecordToInsert(HoodieTestDataGenerator.AVRO_SCHEMA);
    } catch (IOException e) {
      return null;
    }
  }
}
