package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.RocksDBBasedMap;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.config.TestSourceConfig;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractBaseTestSource extends AvroSource {

  // Static instance, helps with reuse across a test.
  protected static transient HoodieTestDataGenerator dataGenerator;

  public static void initDataGen() {
    dataGenerator = new HoodieTestDataGenerator(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS);
  }

  public static void initDataGen(TypedProperties props) {
    try {
      boolean useRocksForTestDataGenKeys = props.getBoolean(TestSourceConfig.USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS,
          TestSourceConfig.DEFAULT_USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS);
      String baseStoreDir = props.getString(TestSourceConfig.ROCKSDB_BASE_DIR_FOR_TEST_DATAGEN_KEYS, null);
      if (null == baseStoreDir) {
        baseStoreDir = File.createTempFile("test_data_gen", ".keys").getParent();
      }
      log.info("useRocksForTestDataGenKeys=" + useRocksForTestDataGenKeys + ", BaseStoreDir=" + baseStoreDir);
      dataGenerator = new HoodieTestDataGenerator(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS,
          useRocksForTestDataGenKeys ? new RocksDBBasedMap<>(baseStoreDir) : new HashMap<>());
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public static void resetDataGen() {
    if (null != dataGenerator) {
      dataGenerator.close();
    }
    dataGenerator = null;
  }

  protected AbstractBaseTestSource(TypedProperties props,
      JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  protected static Stream<GenericRecord> fetchNextBatch(TypedProperties props, int sourceLimit, String commitTime) {
    int maxUniqueKeys = props.getInteger(TestSourceConfig.MAX_UNIQUE_RECORDS_PROP,
        TestSourceConfig.DEFAULT_MAX_UNIQUE_RECORDS);

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

    List<GenericRecord> records = new ArrayList<>();
    Stream<GenericRecord> updateStream = dataGenerator.generateUniqueUpdatesStream(commitTime, numUpdates)
        .map(AbstractBaseTestSource::toGenericRecord);
    Stream<GenericRecord> insertStream = dataGenerator.generateInsertsStream(commitTime, numInserts)
        .map(AbstractBaseTestSource::toGenericRecord);
    return Stream.concat(updateStream, insertStream);
  }

  private static GenericRecord toGenericRecord(HoodieRecord hoodieRecord) {
    try {
      Optional<IndexedRecord> recordOpt = hoodieRecord.getData().getInsertValue(dataGenerator.avroSchema);
      return (GenericRecord) recordOpt.get();
    } catch (IOException e) {
      return null;
    }
  }
}
