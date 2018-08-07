/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common;

import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

  public static final String[] DEFAULT_PARTITION_PATHS = {
      DEFAULT_FIRST_PARTITION_PATH,
      DEFAULT_SECOND_PARTITION_PATH,
      DEFAULT_THIRD_PARTITION_PATH
  };
  public static final int DEFAULT_PARTITION_DEPTH = 3;

  public static String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},"
      + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"rider\", \"type\": \"string\"},"
      + "{\"name\": \"driver\", \"type\": \"string\"},"
      + "{\"name\": \"begin_lat\", \"type\": \"double\"},"
      + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
      + "{\"name\": \"end_lat\", \"type\": \"double\"},"
      + "{\"name\": \"end_lon\", \"type\": \"double\"},"
      + "{\"name\":\"fare\",\"type\": \"double\"}]}";
  public static Schema avroSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA));
  private static Random rand = new Random(46474747);
  private List<KeyPartition> existingKeysList = new ArrayList<>();
  private String[] partitionPaths;

  public HoodieTestDataGenerator(String[] partitionPaths) {
    this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
  }

  public HoodieTestDataGenerator() {
    this(DEFAULT_PARTITION_PATHS);
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
    HoodieAvroUtils.addCommitMetadataToRecord(rec, commitTime, "-1");
    return new TestRawTripPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA);
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

  public static void createCompactionAuxiliaryMetadata(String basePath, HoodieInstant instant,
      Configuration configuration) throws IOException {
    Path commitFile = new Path(
        basePath + "/" + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + instant.getFileName());
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

  public static void createSavepointFile(String basePath, String commitTime) throws IOException {
    createSavepointFile(basePath, commitTime, HoodieTestUtils.getDefaultHadoopConf());
  }

  public static void createSavepointFile(String basePath, String commitTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTimeline.makeSavePointFileName(commitTime));
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
    List<HoodieRecord> inserts = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
      HoodieRecord record = new HoodieRecord(key, generateRandomValue(key, commitTime));
      inserts.add(record);

      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      existingKeysList.add(kp);
    }
    return inserts;
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
    TestRawTripPayload payload = new TestRawTripPayload(Optional.empty(), key.getRecordKey(), key.getPartitionPath(),
        null, true);
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
      KeyPartition kp = existingKeysList.get(rand.nextInt(existingKeysList.size() - 1));
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
  public List<HoodieRecord> generateUniqueUpdates(String commitTime, Integer n) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    Set<KeyPartition> used = new HashSet<>();

    if (n > existingKeysList.size()) {
      throw new IllegalArgumentException("Requested unique updates is greater than number of available keys");
    }

    for (int i = 0; i < n; i++) {
      int index = rand.nextInt(existingKeysList.size() - 1);
      KeyPartition kp = existingKeysList.get(index);
      // Find the available keyPartition starting from randomly chosen one.
      while (used.contains(kp)) {
        index = (index + 1) % existingKeysList.size();
        kp = existingKeysList.get(index);
      }
      HoodieRecord record = new HoodieRecord(kp.key, generateRandomValue(kp.key, commitTime));
      updates.add(record);
      used.add(kp);
    }
    return updates;
  }

  public String[] getPartitionPaths() {
    return partitionPaths;
  }

  static class KeyPartition {

    HoodieKey key;
    String partitionPath;
  }
}
