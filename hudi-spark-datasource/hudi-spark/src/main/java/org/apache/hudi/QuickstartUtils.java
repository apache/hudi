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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Class to be used in quickstart guide for generating inserts and updates against a corpus. Test data uses a toy Uber
 * trips, data model.
 */
public class QuickstartUtils {

  public static class DataGenerator {
    private static final String DEFAULT_FIRST_PARTITION_PATH = "americas/united_states/san_francisco";
    private static final String DEFAULT_SECOND_PARTITION_PATH = "americas/brazil/sao_paulo";
    private static final String DEFAULT_THIRD_PARTITION_PATH = "asia/india/chennai";

    private static final String[] DEFAULT_PARTITION_PATHS =
        {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};
    static String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"triprec\",\"fields\": [ "
        + "{\"name\": \"ts\",\"type\": \"long\"},{\"name\": \"uuid\", \"type\": \"string\"},"
        + "{\"name\": \"rider\", \"type\": \"string\"},{\"name\": \"driver\", \"type\": \"string\"},"
        + "{\"name\": \"begin_lat\", \"type\": \"double\"},{\"name\": \"begin_lon\", \"type\": \"double\"},"
        + "{\"name\": \"end_lat\", \"type\": \"double\"},{\"name\": \"end_lon\", \"type\": \"double\"},"
        + "{\"name\":\"fare\",\"type\": \"double\"}]}";
    static HoodieSchema schema = HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA);

    private static final Random RAND = new Random(46474747);

    private final Map<Integer, HoodieKey> existingKeys;
    private final String[] partitionPaths;
    private int numExistingKeys;

    public DataGenerator() {
      this(DEFAULT_PARTITION_PATHS, new HashMap<>());
    }

    public DataGenerator(String[] partitionPaths) {
      this(partitionPaths, new HashMap<>());
    }

    private DataGenerator(String[] partitionPaths, Map<Integer, HoodieKey> keyPartitionMap) {
      this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
      this.existingKeys = keyPartitionMap;
    }

    private static String generateRandomString() {
      int leftLimit = 48; // ascii for 0
      int rightLimit = 57; // ascii for 9
      int stringLength = 3;
      StringBuilder buffer = new StringBuilder(stringLength);
      for (int i = 0; i < stringLength; i++) {
        int randomLimitedInt = leftLimit + (int) (RAND.nextFloat() * (rightLimit - leftLimit + 1));
        buffer.append((char) randomLimitedInt);
      }
      return buffer.toString();
    }

    public int getNumExistingKeys() {
      return numExistingKeys;
    }

    public static GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
                                                      long timestamp) {
      GenericRecord rec = new GenericData.Record(schema.getAvroSchema());
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

    /**
     * Generates a new avro record of the above schema format, retaining the key if optionally provided. The
     * riderDriverSuffix string is a random String to simulate updates by changing the rider driver fields for records
     * belonging to the same commit. It is purely used for demo purposes. In real world, the actual updates are assumed
     * to be provided based on the application requirements.
     */
    public static OverwriteWithLatestAvroPayload generateRandomValue(HoodieKey key, String riderDriverSuffix)
        throws IOException {
      // The timestamp generated is limited to range from 7 days before to now, to avoid generating too many
      // partitionPaths when user use timestamp as partitionPath filed.
      GenericRecord rec =
          generateGenericRecord(key.getRecordKey(), "rider-" + riderDriverSuffix, "driver-"
              + riderDriverSuffix, generateRangeRandomTimestamp(7));
      return new OverwriteWithLatestAvroPayload(Option.of(rec));
    }

    /**
     * Generate timestamp range from {@param daysTillNow} before to now.
     */
    private static long generateRangeRandomTimestamp(int daysTillNow) {
      long maxIntervalMillis = daysTillNow * 24 * 60 * 60 * 1000L;
      return System.currentTimeMillis() - (long) (Math.random() * maxIntervalMillis);
    }

    /**
     * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
     */
    public Stream<HoodieRecord> generateInsertsStream(String randomString, Integer n) {
      int currSize = getNumExistingKeys();

      return IntStream.range(0, n).boxed().map(i -> {
        String partitionPath = partitionPaths[RAND.nextInt(partitionPaths.length)];
        HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
        existingKeys.put(currSize + i, key);
        numExistingKeys++;
        try {
          return new HoodieAvroRecord(key, generateRandomValue(key, randomString));
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      });
    }

    /**
     * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
     */
    public List<HoodieRecord> generateInserts(Integer n) throws IOException {
      String randomString = generateRandomString();
      return generateInsertsStream(randomString, n).collect(Collectors.toList());
    }

    public HoodieRecord generateUpdateRecord(HoodieKey key, String randomString) throws IOException {
      return new HoodieAvroRecord(key, generateRandomValue(key, randomString));
    }

    /**
     * Generates new updates, randomly distributed across the keys above. There can be duplicates within the returned
     * list
     *
     * @param n Number of updates (including dups)
     * @return list of hoodie record updates
     */
    public List<HoodieRecord> generateUpdates(Integer n) {
      if (numExistingKeys == 0) {
        throw new HoodieException("Data must have been written before performing the update operation");
      }
      String randomString = generateRandomString();
      return IntStream.range(0, n).boxed().map(x -> {
        try {
          return generateUpdateRecord(existingKeys.get(RAND.nextInt(numExistingKeys)), randomString);
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      }).collect(Collectors.toList());
    }

    /**
     * Generates new updates, one for each of the keys above
     * list
     *
     * @param n Number of updates (must be no more than number of existing keys)
     * @return list of hoodie record updates
     */
    public List<HoodieRecord> generateUniqueUpdates(Integer n) {
      if (numExistingKeys < n) {
        throw new HoodieException("Data must have been written before performing the update operation");
      }
      List<Integer> keys = IntStream.range(0, numExistingKeys).boxed()
          .collect(Collectors.toCollection(ArrayList::new));
      Collections.shuffle(keys);
      String randomString = generateRandomString();
      return IntStream.range(0, n).boxed().map(x -> {
        try {
          return generateUpdateRecord(existingKeys.get(keys.get(x)), randomString);
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      }).collect(Collectors.toList());
    }

    /**
     * Generates delete records for the passed in rows.
     *
     * @param rows List of {@link Row}s for which delete record need to be generated
     * @return list of hoodie records to delete
     */
    public List<String> generateDeletes(List<Row> rows) {
      // if row.length() == 2, then the record contains "uuid" and "partitionpath" fields, otherwise,
      // another field "ts" is available
      return rows.stream().map(row -> row.length() == 2
              ? convertToString(row.getAs("uuid"), row.getAs("partitionpath"), null) :
              convertToString(row.getAs("uuid"), row.getAs("partitionpath"), row.getAs("ts"))
          ).filter(os -> os.isPresent()).map(os -> os.get())
          .collect(Collectors.toList());
    }

    public void close() {
      existingKeys.clear();
    }
  }

  private static Option<String> convertToString(HoodieRecord record) {
    try {
      String str = ((OverwriteWithLatestAvroPayload) record.getData())
          .getInsertValue(DataGenerator.schema.getAvroSchema())
          .toString();
      str = "{" + str.substring(str.indexOf("\"ts\":"));
      return Option.of(str.replaceAll("}", ", \"partitionpath\": \"" + record.getPartitionPath() + "\"}"));
    } catch (IOException e) {
      return Option.empty();
    }
  }

  private static Option<String> convertToString(String uuid, String partitionPath, Long ts) {
    String stringBuffer = "{"
        + "\"ts\": \"" + (ts == null ? "0.0" : ts) + "\","
        + "\"uuid\": \"" + uuid + "\","
        + "\"partitionpath\": \"" + partitionPath + "\""
        + "}";
    return Option.of(stringBuffer);
  }

  public static List<String> convertToStringList(List<HoodieRecord> records) {
    return records.stream().map(hr -> convertToString(hr)).filter(os -> os.isPresent()).map(os -> os.get())
        .collect(Collectors.toList());
  }

  public static Map<String, String> getQuickstartWriteConfigs() {
    Map<String, String> demoConfigs = new HashMap<>();
    demoConfigs.put("hoodie.insert.shuffle.parallelism", "2");
    demoConfigs.put("hoodie.upsert.shuffle.parallelism", "2");
    demoConfigs.put("hoodie.bulkinsert.shuffle.parallelism", "2");
    demoConfigs.put("hoodie.delete.shuffle.parallelism", "2");
    return demoConfigs;
  }
}
