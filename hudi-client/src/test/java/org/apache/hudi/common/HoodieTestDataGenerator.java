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

package org.apache.hudi.common;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Class to be used in tests to keep generating test inserts and updates against a corpus.
 * <p>
 * Test data uses a toy Uber trips, data model.
 */
public class HoodieTestDataGenerator {

  // based on examination of sample file, the schema produces the following per record size
  public static final int SIZE_PER_RECORD = 50 * 1024;
  public static final String DEFAULT_FIRST_PARTITION_PATH = "2016/03/15";
  public static final String DEFAULT_SECOND_PARTITION_PATH = "2015/03/16";
  public static final String DEFAULT_THIRD_PARTITION_PATH = "2015/03/17";

  public static final String[] DEFAULT_PARTITION_PATHS =
      {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};
  public static final int DEFAULT_PARTITION_DEPTH = 3;
  public static final String TRIP_SCHEMA_PREFIX = "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"}," + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"rider\", \"type\": \"string\"}," + "{\"name\": \"driver\", \"type\": \"string\"},"
      + "{\"name\": \"begin_lat\", \"type\": \"double\"}," + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
      + "{\"name\": \"end_lat\", \"type\": \"double\"}," + "{\"name\": \"end_lon\", \"type\": \"double\"},";
  public static final String TRIP_SCHEMA_SUFFIX = "{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false} ]}";
  public static final String FARE_NESTED_SCHEMA = "{\"name\": \"fare\",\"type\": {\"type\":\"record\", \"name\":\"fare\",\"fields\": ["
      + "{\"name\": \"amount\",\"type\": \"double\"},{\"name\": \"currency\", \"type\": \"string\"}]}},";
  public static final String FARE_FLATTENED_SCHEMA = "{\"name\": \"fare\", \"type\": \"double\"},"
      + "{\"name\": \"currency\", \"type\": \"string\"},";

  public static final String TRIP_EXAMPLE_SCHEMA =
      TRIP_SCHEMA_PREFIX + FARE_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_FLATTENED_SCHEMA =
      TRIP_SCHEMA_PREFIX + FARE_FLATTENED_SCHEMA + TRIP_SCHEMA_SUFFIX;

  public static final String NULL_SCHEMA = Schema.create(Schema.Type.NULL).toString();
  public static final String TRIP_HIVE_COLUMN_TYPES = "double,string,string,string,double,double,double,double,"
                                                  + "struct<amount:double,currency:string>,boolean";

  public static final Schema AVRO_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
  public static final Schema AVRO_SCHEMA_WITH_METADATA_FIELDS =
      HoodieAvroUtils.addMetadataFields(AVRO_SCHEMA);
  public static final Schema FLATTENED_AVRO_SCHEMA = new Schema.Parser().parse(TRIP_FLATTENED_SCHEMA);

  private static final Random RAND = new Random(46474747);

  private final Map<Integer, KeyPartition> existingKeys;
  private final String[] partitionPaths;
  private int numExistingKeys;

  public HoodieTestDataGenerator(String[] partitionPaths) {
    this(partitionPaths, new HashMap<>());
  }

  public HoodieTestDataGenerator() {
    this(DEFAULT_PARTITION_PATHS);
  }

  public HoodieTestDataGenerator(String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
    this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
    this.existingKeys = keyPartitionMap;
  }

  public static void writePartitionMetadata(FileSystem fs, String[] partitionPaths, String basePath) {
    for (String partitionPath : partitionPaths) {
      new HoodiePartitionMetadata(fs, "000", new Path(basePath), new Path(basePath, partitionPath)).trySave(0);
    }
  }

  /**
   * Generates a new avro record of the above nested schema format,
   * retaining the key if optionally provided.
   *
   * @param key  Hoodie key.
   * @param instantTime  Instant time to use.
   * @return  Raw paylaod of a test record.
   * @throws IOException
   */
  public static TestRawTripPayload generateRandomValue(HoodieKey key, String instantTime) throws IOException {
    return generateRandomValue(key, instantTime, false);
  }

  /**
   * Generates a new avro record with the specified schema (nested or flattened),
   * retaining the key if optionally provided.
   *
   * @param key  Hoodie key.
   * @param instantTime  Commit time to use.
   * @param isFlattened  whether the schema of the record should be flattened.
   * @return  Raw paylaod of a test record.
   * @throws IOException
   */
  public static TestRawTripPayload generateRandomValue(
      HoodieKey key, String instantTime, boolean isFlattened) throws IOException {
    GenericRecord rec = generateGenericRecord(
        key.getRecordKey(), "rider-" + instantTime, "driver-" + instantTime, 0.0,
        false, isFlattened);
    return new TestRawTripPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Generates a new avro record of the above schema format for a delete.
   */
  public static TestRawTripPayload generateRandomDeleteValue(HoodieKey key, String instantTime) throws IOException {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + instantTime, "driver-" + instantTime, 0.0,
        true, false);
    return new TestRawTripPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Generates a new avro record of the above schema format, retaining the key if optionally provided.
   */
  public static HoodieAvroPayload generateAvroPayload(HoodieKey key, String instantTime) {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + instantTime, "driver-" + instantTime, 0.0);
    return new HoodieAvroPayload(Option.of(rec));
  }

  public static GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
                                                    double timestamp) {
    return generateGenericRecord(rowKey, riderName, driverName, timestamp, false, false);
  }

  public static GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
                                                    double timestamp, boolean isDeleteRecord,
                                                    boolean isFlattened) {
    GenericRecord rec = new GenericData.Record(isFlattened ? FLATTENED_AVRO_SCHEMA : AVRO_SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("begin_lat", RAND.nextDouble());
    rec.put("begin_lon", RAND.nextDouble());
    rec.put("end_lat", RAND.nextDouble());
    rec.put("end_lon", RAND.nextDouble());

    if (isFlattened) {
      rec.put("fare", RAND.nextDouble() * 100);
      rec.put("currency", "USD");
    } else {
      GenericRecord fareRecord = new GenericData.Record(AVRO_SCHEMA.getField("fare").schema());
      fareRecord.put("amount", RAND.nextDouble() * 100);
      fareRecord.put("currency", "USD");
      rec.put("fare", fareRecord);
    }

    if (isDeleteRecord) {
      rec.put("_hoodie_is_deleted", true);
    } else {
      rec.put("_hoodie_is_deleted", false);
    }
    return rec;
  }

  public static void createCommitFile(String basePath, String instantTime, Configuration configuration) {
    Arrays.asList(HoodieTimeline.makeCommitFileName(instantTime), HoodieTimeline.makeInflightCommitFileName(instantTime),
        HoodieTimeline.makeRequestedCommitFileName(instantTime))
        .forEach(f -> {
          Path commitFile = new Path(
              basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + f);
          FSDataOutputStream os = null;
          try {
            FileSystem fs = FSUtils.getFs(basePath, configuration);
            os = fs.create(commitFile, true);
            HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
            // Write empty commit metadata
            os.writeBytes(new String(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
          } catch (IOException ioe) {
            throw new HoodieIOException(ioe.getMessage(), ioe);
          } finally {
            if (null != os) {
              try {
                os.close();
              } catch (IOException e) {
                throw new HoodieIOException(e.getMessage(), e);
              }
            }
          }
        });
  }

  public static void createCompactionRequestedFile(String basePath, String instantTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeRequestedCompactionFileName(instantTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    FSDataOutputStream os = fs.create(commitFile, true);
    os.close();
  }

  public static void createCompactionAuxiliaryMetadata(String basePath, HoodieInstant instant,
                                                       Configuration configuration) throws IOException {
    Path commitFile =
        new Path(basePath + "/" + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + instant.getFileName());
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    try (FSDataOutputStream os = fs.create(commitFile, true)) {
      HoodieCompactionPlan workload = new HoodieCompactionPlan();
      // Write empty commit metadata
      os.writeBytes(new String(TimelineMetadataUtils.serializeCompactionPlan(workload).get(), StandardCharsets.UTF_8));
    }
  }

  public static void createSavepointFile(String basePath, String instantTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeSavePointFileName(instantTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    try (FSDataOutputStream os = fs.create(commitFile, true)) {
      HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
      // Write empty commit metadata
      os.writeBytes(new String(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    }
  }

  /**
   * Generates new inserts with nested schema, uniformly across the partition paths above.
   * It also updates the list of existing keys.
   */
  public List<HoodieRecord> generateInserts(String instantTime, Integer n) {
    return generateInserts(instantTime, n, false);
  }

  /**
   * Generates new inserts, uniformly across the partition paths above.
   * It also updates the list of existing keys.
   *
   * @param instantTime  Commit time to use.
   * @param n  Number of records.
   * @param isFlattened  whether the schema of the generated record is flattened
   * @return  List of {@link HoodieRecord}s
   */
  public List<HoodieRecord> generateInserts(String instantTime, Integer n, boolean isFlattened) {
    return generateInsertsStream(instantTime, n, isFlattened).collect(Collectors.toList());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(
      String instantTime, Integer n, boolean isFlattened) {
    int currSize = getNumExistingKeys();

    return IntStream.range(0, n).boxed().map(i -> {
      String partitionPath = partitionPaths[RAND.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      existingKeys.put(currSize + i, kp);
      numExistingKeys++;
      try {
        return new HoodieRecord(key, generateRandomValue(key, instantTime, isFlattened));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  public List<HoodieRecord> generateSameKeyInserts(String instantTime, List<HoodieRecord> origin) throws IOException {
    List<HoodieRecord> copy = new ArrayList<>();
    for (HoodieRecord r : origin) {
      HoodieKey key = r.getKey();
      HoodieRecord record = new HoodieRecord(key, generateRandomValue(key, instantTime));
      copy.add(record);
    }
    return copy;
  }

  public List<HoodieRecord> generateInsertsWithHoodieAvroPayload(String instantTime, int limit) {
    List<HoodieRecord> inserts = new ArrayList<>();
    int currSize = getNumExistingKeys();
    for (int i = 0; i < limit; i++) {
      String partitionPath = partitionPaths[RAND.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
      HoodieRecord record = new HoodieRecord(key, generateAvroPayload(key, instantTime));
      inserts.add(record);

      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      existingKeys.put(currSize + i, kp);
      numExistingKeys++;
    }
    return inserts;
  }

  public List<HoodieRecord> generateUpdatesWithHoodieAvroPayload(String instantTime, List<HoodieRecord> baseRecords) {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = new HoodieRecord(baseRecord.getKey(), generateAvroPayload(baseRecord.getKey(), instantTime));
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateDeletes(String instantTime, Integer n) throws IOException {
    List<HoodieRecord> inserts = generateInserts(instantTime, n);
    return generateDeletesFromExistingRecords(inserts);
  }

  public List<HoodieRecord> generateDeletesFromExistingRecords(List<HoodieRecord> existingRecords) throws IOException {
    List<HoodieRecord> deletes = new ArrayList<>();
    for (HoodieRecord existingRecord : existingRecords) {
      HoodieRecord record = generateDeleteRecord(existingRecord);
      deletes.add(record);
    }
    return deletes;
  }

  public HoodieRecord generateDeleteRecord(HoodieRecord existingRecord) throws IOException {
    HoodieKey key = existingRecord.getKey();
    return generateDeleteRecord(key);
  }

  public HoodieRecord generateDeleteRecord(HoodieKey key) throws IOException {
    TestRawTripPayload payload =
        new TestRawTripPayload(Option.empty(), key.getRecordKey(), key.getPartitionPath(), null, true);
    return new HoodieRecord(key, payload);
  }

  public HoodieRecord generateUpdateRecord(HoodieKey key, String instantTime) throws IOException {
    return new HoodieRecord(key, generateRandomValue(key, instantTime));
  }

  public List<HoodieRecord> generateUpdates(String instantTime, List<HoodieRecord> baseRecords) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = generateUpdateRecord(baseRecord.getKey(), instantTime);
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateUpdatesWithDiffPartition(String instantTime, List<HoodieRecord> baseRecords)
      throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      String partition = baseRecord.getPartitionPath();
      String newPartition = "";
      if (partitionPaths[0].equalsIgnoreCase(partition)) {
        newPartition = partitionPaths[1];
      } else {
        newPartition = partitionPaths[0];
      }
      HoodieKey key = new HoodieKey(baseRecord.getRecordKey(), newPartition);
      HoodieRecord record = generateUpdateRecord(key, instantTime);
      updates.add(record);
    }
    return updates;
  }

  /**
   * Generates new updates, randomly distributed across the keys above. There can be duplicates within the returned
   * list
   *
   * @param instantTime Instant Timestamp
   * @param n          Number of updates (including dups)
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUpdates(String instantTime, Integer n) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      KeyPartition kp = existingKeys.get(RAND.nextInt(numExistingKeys - 1));
      HoodieRecord record = generateUpdateRecord(kp.key, instantTime);
      updates.add(record);
    }
    return updates;
  }

  /**
   * Generates deduped updates of keys previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Instant Timestamp
   * @param n          Number of unique records
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUniqueUpdates(String instantTime, Integer n) {
    return generateUniqueUpdatesStream(instantTime, n).collect(Collectors.toList());
  }

  /**
   * Generates deduped delete of keys previously inserted, randomly distributed across the keys above.
   *
   * @param n Number of unique records
   * @return list of hoodie record updates
   */
  public List<HoodieKey> generateUniqueDeletes(Integer n) {
    return generateUniqueDeleteStream(n).collect(Collectors.toList());
  }

  /**
   * Generates deduped updates of keys previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Commit Timestamp
   * @param n          Number of unique records
   * @return stream of hoodie record updates
   */
  public Stream<HoodieRecord> generateUniqueUpdatesStream(String instantTime, Integer n) {
    final Set<KeyPartition> used = new HashSet<>();
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique updates is greater than number of available keys");
    }

    return IntStream.range(0, n).boxed().map(i -> {
      int index = numExistingKeys == 1 ? 0 : RAND.nextInt(numExistingKeys - 1);
      KeyPartition kp = existingKeys.get(index);
      // Find the available keyPartition starting from randomly chosen one.
      while (used.contains(kp)) {
        index = (index + 1) % numExistingKeys;
        kp = existingKeys.get(index);
      }
      used.add(kp);
      try {
        return new HoodieRecord(kp.key, generateRandomValue(kp.key, instantTime));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  /**
   * Generates deduped delete of keys previously inserted, randomly distributed across the keys above.
   *
   * @param n Number of unique records
   * @return stream of hoodie record updates
   */
  public Stream<HoodieKey> generateUniqueDeleteStream(Integer n) {
    final Set<KeyPartition> used = new HashSet<>();
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique deletes is greater than number of available keys");
    }

    List<HoodieKey> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int index = RAND.nextInt(numExistingKeys);
      while (!existingKeys.containsKey(index)) {
        index = (index + 1) % numExistingKeys;
      }
      KeyPartition kp = existingKeys.remove(index);
      existingKeys.put(index, existingKeys.get(numExistingKeys - 1));
      existingKeys.remove(numExistingKeys - 1);
      numExistingKeys--;
      used.add(kp);
      result.add(kp.key);
    }
    return result.stream();
  }

  /**
   * Generates deduped delete records previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Commit Timestamp
   * @param n          Number of unique records
   * @return stream of hoodie records for delete
   */
  public Stream<HoodieRecord> generateUniqueDeleteRecordStream(String instantTime, Integer n) {
    final Set<KeyPartition> used = new HashSet<>();
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique deletes is greater than number of available keys");
    }

    List<HoodieRecord> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int index = RAND.nextInt(numExistingKeys);
      while (!existingKeys.containsKey(index)) {
        index = (index + 1) % numExistingKeys;
      }
      // swap chosen index with last index and remove last entry. 
      KeyPartition kp = existingKeys.remove(index);
      existingKeys.put(index, existingKeys.get(numExistingKeys - 1));
      existingKeys.remove(numExistingKeys - 1);
      numExistingKeys--;
      used.add(kp);
      try {
        result.add(new HoodieRecord(kp.key, generateRandomDeleteValue(kp.key, instantTime)));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }
    return result.stream();
  }

  public String[] getPartitionPaths() {
    return partitionPaths;
  }

  public int getNumExistingKeys() {
    return numExistingKeys;
  }

  public static class KeyPartition implements Serializable {

    HoodieKey key;
    String partitionPath;
  }

  public void close() {
    existingKeys.clear();
  }
}
