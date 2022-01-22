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

package org.apache.hudi.testutils;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class HoodieDataFrameGenerator<T extends HoodieRecordPayload<T>> implements Closeable {

  public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("'year='yyyy/'month='MM/'day='dd")
      .withZone(ZoneOffset.UTC);
  public static final String DEFAULT_FIRST_PARTITION_PATH = "2020/01/01";
  public static final String DEFAULT_SECOND_PARTITION_PATH = "2020/01/02";
  public static final String DEFAULT_THIRD_PARTITION_PATH = "2020/01/03";
  public static final String[] DEFAULT_PARTITION_PATHS = {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH,
      DEFAULT_THIRD_PARTITION_PATH};
  public static String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"triprec\",\"fields\": [ {\"name\": \"ts\",\"type\":"
      + " \"long\"},{\"name\": \"uuid\", \"type\": \"string\"},{\"name\": \"rider\","
      + " \"type\": \"string\"},{\"name\": \"driver\", \"type\": \"string\"},{\"name\":"
      + " \"begin_lat\", \"type\": \"double\"},{\"name\": \"begin_lon\", \"type\":"
      + " \"double\"},{\"name\": \"end_lat\", \"type\": \"double\"},{\"name\":"
      + " \"end_lon\", \"type\": \"double\"},{\"name\":\"fare\",\"type\": \"double\"}]}";
  public static final Schema AVRO_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
  public static final StructType ROW_SCHEMA = AvroConversionUtils.convertAvroSchemaToStructType(AVRO_SCHEMA);
  private static final Random RAND = new Random(46474747);
  private final Map<Integer, KeyPartition> existingKeys;
  private final String[] partitionPaths;
  private int numExistingKeys;
  private String lastCommitTime;

  public HoodieDataFrameGenerator(String[] partitionPaths) {
    this(partitionPaths, new HashMap<>());
  }

  public HoodieDataFrameGenerator() {
    this(DEFAULT_PARTITION_PATHS);
  }

  public HoodieDataFrameGenerator(String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
    this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
    this.existingKeys = keyPartitionMap;
    this.numExistingKeys = keyPartitionMap.size();
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also
   * updates the list of existing keys.
   */
  public List<HoodieRecord<T>> generateInserts(String commitTime, Integer n) {
    return generateInsertsStream(commitTime, n).collect(Collectors.toList());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also
   * updates the list of existing keys.
   */
  public Stream<HoodieRecord<T>> generateInsertsStream(String commitTime, Integer n) {
    lastCommitTime = commitTime;
    final int currSize = numExistingKeys;
    return IntStream.range(0, n).boxed().map(i -> {
      String partitionPath = partitionPaths[RAND.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      long ts = LocalDate.parse(partitionPath, FORMATTER).atStartOfDay(ZoneOffset.UTC).toEpochSecond();
      kp.ts = ts;
      existingKeys.put(currSize + i, kp);
      numExistingKeys++;
      return new HoodieRecord<>(key, generateRandomValue(key, partitionPath, ts, commitTime));
    });
  }

  /**
   * Generates new updates, randomly distributed across the keys above. There can
   * be duplicates within the returned list
   *
   * @param commitTime
   *            Commit Timestamp
   * @param n
   *            Number of updates (including dups)
   * @return list of hoodie record updates
   */
  public List<HoodieRecord<T>> generateUpdates(String commitTime, Integer n) {
    lastCommitTime = commitTime;
    List<HoodieRecord<T>> updates = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      KeyPartition kp = existingKeys.get(RAND.nextInt(numExistingKeys - 1));
      HoodieRecord<T> record = generateUpdateRecord(kp.key, kp.partitionPath, kp.ts, commitTime);
      updates.add(record);
    }
    return updates;
  }

  private HoodieRecord<T> generateUpdateRecord(HoodieKey key, String partition, long previousTs, String commitTime) {
    return new HoodieRecord<>(key, generateRandomValue(key, partition, previousTs + 1, commitTime));
  }

  public Dataset<Row> generateInserts(String commitTime, Integer n, JavaSparkContext jsc, SparkSession spark,
      int parallelism) {
    List<HoodieRecord<T>> records = generateInserts(commitTime, n);
    return toDataset(records, jsc, spark, parallelism);
  }

  public Dataset<Row> generateUpdates(String commitTime, Integer n, JavaSparkContext jsc, SparkSession spark,
      int parallelism) {
    List<HoodieRecord<T>> records = generateUpdates(commitTime, n);
    return toDataset(records, jsc, spark, parallelism);
  }

  private Dataset<Row> toDataset(List<HoodieRecord<T>> records, JavaSparkContext jsc, SparkSession spark,
      int parallelism) {
    JavaRDD<HoodieRecord<T>> writeRecords = jsc.parallelize(records, parallelism);
    JavaRDD<GenericRecord> avroRecords = writeRecords
        .map(r -> (GenericRecord) r.getData().getInsertValue(AVRO_SCHEMA, new Properties()).get());
    return AvroConversionUtils.createDataFrame(avroRecords.rdd(), TRIP_EXAMPLE_SCHEMA, spark);
  }

  /**
   * Generates a new avro record of the above schema format, retaining the key if
   * optionally provided.
   */
  @SuppressWarnings("unchecked")
  private T generateRandomValue(HoodieKey key, String partition, long timestamp, String commitTime) {
    assert LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneOffset.UTC).format(FORMATTER)
        .equals(partition);
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime,
        timestamp);
    return (T) new HoodieAvroPayload(Option.of(rec));
  }

  private GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName, long timestamp) {
    GenericRecord rec = new GenericData.Record(AVRO_SCHEMA);
    rec.put("uuid", rowKey);
    rec.put("ts", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("begin_lat", RAND.nextDouble());
    rec.put("begin_lon", RAND.nextDouble());
    rec.put("end_lat", RAND.nextDouble());
    rec.put("end_lon", RAND.nextDouble());
    rec.put("fare", RAND.nextDouble() * 100);
    return rec;
  }

  private Option<String> convertToString(HoodieRecord<T> record) {
    try {
      String str = HoodieAvroUtils
          .bytesToAvro(((HoodieAvroPayload) record.getData()).getRecordBytes(), AVRO_SCHEMA).toString();
      str = "{" + str.substring(str.indexOf("\"ts\":"));
      return Option.of(str.replaceAll("}", ", \"partitionpath\": \"" + record.getPartitionPath() + "\"}"));
    } catch (IOException e) {
      return Option.empty();
    }
  }

  public List<String> convertToStringList(List<HoodieRecord<T>> records) {
    return records.stream().map(this::convertToString).filter(Option::isPresent).map(Option::get)
        .collect(Collectors.toList());
  }

  public int getNumExistingKeys() {
    return numExistingKeys;
  }

  public String[] getPartitionPaths() {
    return partitionPaths;
  }

  public String getLastCommitTime() {
    return lastCommitTime;
  }

  public static class KeyPartition implements Serializable {
    public HoodieKey key;
    public String partitionPath;
    public long ts;
  }

  public void close() {
    existingKeys.clear();
  }

}
