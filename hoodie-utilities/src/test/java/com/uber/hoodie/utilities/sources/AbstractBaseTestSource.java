package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.util.Option;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.RocksDBBasedMap;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.config.TestSourceConfig;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

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

  protected AbstractBaseTestSource(TypedProperties props,
      JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  protected static Stream<GenericRecord> fetchNextBatch(TypedProperties props, int sourceLimit, String commitTime,
      int partition) {
    int maxUniqueKeys = props.getInteger(TestSourceConfig.MAX_UNIQUE_RECORDS_PROP,
        TestSourceConfig.DEFAULT_MAX_UNIQUE_RECORDS);

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
