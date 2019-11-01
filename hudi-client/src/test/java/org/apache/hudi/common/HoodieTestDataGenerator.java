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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

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
  public static String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"}," + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"rider\", \"type\": \"string\"}," + "{\"name\": \"driver\", \"type\": \"string\"},"
      + "{\"name\": \"begin_lat\", \"type\": \"double\"}," + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
      + "{\"name\": \"end_lat\", \"type\": \"double\"}," + "{\"name\": \"end_lon\", \"type\": \"double\"},"
      + "{\"name\":\"fare\",\"type\": \"double\"}]}";
  public static String TRIP_HIVE_COLUMN_TYPES = "double,string,string,string,double,double,double,double,double";
  public static Schema avroSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
  public static Schema avroSchemaWithMetadataFields = HoodieAvroUtils.addMetadataFields(avroSchema);

  private static Random rand = new Random(46474747);

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
   * Generates a new avro record of the above schema format, retaining the key if optionally provided.
   */
  public static TestRawTripPayload generateRandomValue(HoodieKey key, String commitTime) throws IOException {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0.0);
    return new TestRawTripPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Generates a new avro record of the above schema format, retaining the key if optionally provided.
   */
  public static HoodieAvroPayload generateAvroPayload(HoodieKey key, String commitTime) throws IOException {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0.0);
    return new HoodieAvroPayload(Option.of(rec));
  }

  public static GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
      double timestamp) {
    GenericRecord rec = new GenericData.Record(avroSchema);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("begin_lat", rand.nextDouble());
    rec.put("begin_lon", rand.nextDouble());
    rec.put("end_lat", rand.nextDouble());
    rec.put("end_lon", rand.nextDouble());
    rec.put("fare", rand.nextDouble() * 100);
    return rec;
  }

  public static void createCommitFile(String basePath, String commitTime) throws IOException {
    createCommitFile(basePath, commitTime, HoodieTestUtils.getDefaultHadoopConf());
  }

  public static void createCommitFile(String basePath, String commitTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeCommitFileName(commitTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    FSDataOutputStream os = fs.create(commitFile, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    try {
      // Write empty commit metadata
      os.writeBytes(new String(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } finally {
      os.close();
    }
  }

  public static void createCompactionRequestedFile(String basePath, String commitTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeRequestedCompactionFileName(commitTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    FSDataOutputStream os = fs.create(commitFile, true);
    os.close();
  }

  public static void createCompactionAuxiliaryMetadata(String basePath, HoodieInstant instant,
      Configuration configuration) throws IOException {
    Path commitFile =
        new Path(basePath + "/" + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + instant.getFileName());
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    FSDataOutputStream os = fs.create(commitFile, true);
    HoodieCompactionPlan workload = new HoodieCompactionPlan();
    try {
      // Write empty commit metadata
      os.writeBytes(new String(AvroUtils.serializeCompactionPlan(workload).get(), StandardCharsets.UTF_8));
    } finally {
      os.close();
    }
  }

  public static void createSavepointFile(String basePath, String commitTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeSavePointFileName(commitTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    FSDataOutputStream os = fs.create(commitFile, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    try {
      // Write empty commit metadata
      os.writeBytes(new String(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } finally {
      os.close();
    }
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public List<HoodieRecord> generateInserts(String commitTime, Integer n) throws IOException {
    return generateInsertsStream(commitTime, n).collect(Collectors.toList());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(String commitTime, Integer n) {
    int currSize = getNumExistingKeys();

    return IntStream.range(0, n).boxed().map(i -> {
      String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      existingKeys.put(currSize + i, kp);
      numExistingKeys++;
      try {
        return new HoodieRecord(key, generateRandomValue(key, commitTime));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  public List<HoodieRecord> generateSameKeyInserts(String commitTime, List<HoodieRecord> origin) throws IOException {
    List<HoodieRecord> copy = new ArrayList<>();
    for (HoodieRecord r : origin) {
      HoodieKey key = r.getKey();
      HoodieRecord record = new HoodieRecord(key, generateRandomValue(key, commitTime));
      copy.add(record);
    }
    return copy;
  }

  public List<HoodieRecord> generateInsertsWithHoodieAvroPayload(String commitTime, int limit) throws IOException {
    List<HoodieRecord> inserts = new ArrayList<>();
    int currSize = getNumExistingKeys();
    for (int i = 0; i < limit; i++) {
      String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
      HoodieRecord record = new HoodieRecord(key, generateAvroPayload(key, commitTime));
      inserts.add(record);

      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      existingKeys.put(currSize + i, kp);
      numExistingKeys++;
    }
    return inserts;
  }

  public List<HoodieRecord> generateUpdatesWithHoodieAvroPayload(String commitTime, List<HoodieRecord> baseRecords)
      throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = new HoodieRecord(baseRecord.getKey(), generateAvroPayload(baseRecord.getKey(), commitTime));
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateDeletes(String commitTime, Integer n) throws IOException {
    List<HoodieRecord> inserts = generateInserts(commitTime, n);
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

  public HoodieRecord generateUpdateRecord(HoodieKey key, String commitTime) throws IOException {
    return new HoodieRecord(key, generateRandomValue(key, commitTime));
  }

  public List<HoodieRecord> generateUpdates(String commitTime, List<HoodieRecord> baseRecords) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = generateUpdateRecord(baseRecord.getKey(), commitTime);
      updates.add(record);
    }
    return updates;
  }

  /**
   * Generates new updates, randomly distributed across the keys above. There can be duplicates within the returned list
   *
   * @param commitTime Commit Timestamp
   * @param n Number of updates (including dups)
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUpdates(String commitTime, Integer n) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      KeyPartition kp = existingKeys.get(rand.nextInt(numExistingKeys - 1));
      HoodieRecord record = generateUpdateRecord(kp.key, commitTime);
      updates.add(record);
    }
    return updates;
  }

  /**
   * Generates deduped updates of keys previously inserted, randomly distributed across the keys above.
   *
   * @param commitTime Commit Timestamp
   * @param n Number of unique records
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUniqueUpdates(String commitTime, Integer n) {
    return generateUniqueUpdatesStream(commitTime, n).collect(Collectors.toList());
  }

  /**
   * Generates deduped updates of keys previously inserted, randomly distributed across the keys above.
   *
   * @param commitTime Commit Timestamp
   * @param n Number of unique records
   * @return stream of hoodie record updates
   */
  public Stream<HoodieRecord> generateUniqueUpdatesStream(String commitTime, Integer n) {
    final Set<KeyPartition> used = new HashSet<>();

    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique updates is greater than number of available keys");
    }

    return IntStream.range(0, n).boxed().map(i -> {
      int index = numExistingKeys == 1 ? 0 : rand.nextInt(numExistingKeys - 1);
      KeyPartition kp = existingKeys.get(index);
      // Find the available keyPartition starting from randomly chosen one.
      while (used.contains(kp)) {
        index = (index + 1) % numExistingKeys;
        kp = existingKeys.get(index);
      }
      used.add(kp);
      try {
        return new HoodieRecord(kp.key, generateRandomValue(kp.key, commitTime));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  public List<GenericRecord> generateGenericRecords(int numRecords) {
    List<GenericRecord> list = new ArrayList<>();
    IntStream.range(0, numRecords).forEach(i -> {
      list.add(generateGenericRecord(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID()
          .toString(), rand.nextDouble()));
    });
    return list;
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
