/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Class to be used in tests to keep generating test inserts and updates against a corpus.
 * <p>
 * Test data uses a toy Uber trips, data model.
 */
public class HoodieTestDataGenerator implements AutoCloseable {

  /**
   * You may get a different result due to the upgrading of Spark 3.0: reading dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z from Parquet INT96 files can be ambiguous,
   * as the files may be written by Spark 2.x or legacy versions of Hive, which uses a legacy hybrid calendar that is different from Spark 3.0+s Proleptic Gregorian calendar.
   * See more details in SPARK-31404.
   */
  private boolean makeDatesAmbiguous = false;

  // based on examination of sample file, the schema produces the following per record size
  public static final int BYTES_PER_RECORD = (int) (1.2 * 1024);
  // with default bloom filter with 60,000 entries and 0.000000001 FPRate
  public static final int BLOOM_FILTER_BYTES = 323495;
  private static Logger logger = LoggerFactory.getLogger(HoodieTestDataGenerator.class);
  public static final String NO_PARTITION_PATH = "";
  public static final String DEFAULT_FIRST_PARTITION_PATH = "2016/03/15";
  public static final String DEFAULT_SECOND_PARTITION_PATH = "2015/03/16";
  public static final String DEFAULT_THIRD_PARTITION_PATH = "2015/03/17";

  public static final String[] DEFAULT_PARTITION_PATHS =
      {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};
  public static final int DEFAULT_PARTITION_DEPTH = 3;

  public static final String TRIP_TYPE_ENUM_TYPE =
      "{\"type\": \"enum\", \"name\": \"TripType\", \"symbols\": [\"UNKNOWN\", \"UBERX\", \"BLACK\"], \"default\": \"UNKNOWN\"}";
  public static final Schema TRIP_TYPE_ENUM_SCHEMA = new Schema.Parser().parse(TRIP_TYPE_ENUM_TYPE);

  public static final String TRIP_SCHEMA_PREFIX = "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"}," + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"partition_path\", \"type\": [\"null\", \"string\"], \"default\": null },"
      + "{\"name\": \"trip_type\", \"type\": " + TRIP_TYPE_ENUM_TYPE + "},"
      + "{\"name\": \"rider\", \"type\": \"string\"}," + "{\"name\": \"driver\", \"type\": \"string\"},"
      + "{\"name\": \"begin_lat\", \"type\": \"double\"}," + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
      + "{\"name\": \"end_lat\", \"type\": \"double\"}," + "{\"name\": \"end_lon\", \"type\": \"double\"},";
  public static final String HOODIE_IS_DELETED_SCHEMA = "{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false}";
  public static final String TRIP_SCHEMA_SUFFIX = HOODIE_IS_DELETED_SCHEMA + " ]}";
  public static final String FARE_NESTED_SCHEMA = "{\"name\": \"fare\",\"type\": {\"type\":\"record\", \"name\":\"fare\",\"fields\": ["
      + "{\"name\": \"amount\",\"type\": \"double\"},{\"name\": \"currency\", \"type\": \"string\"}]}},";
  public static final String FARE_FLATTENED_SCHEMA = "{\"name\": \"fare\", \"type\": \"double\"},"
      + "{\"name\": \"currency\", \"type\": \"string\"},";
  public static final String TIP_NESTED_SCHEMA = "{\"name\": \"tip_history\", \"default\": [], \"type\": {\"type\": "
      + "\"array\", \"default\": [], \"items\": {\"type\": \"record\", \"default\": null, \"name\": \"tip_history\", \"fields\": ["
      + "{\"name\": \"amount\", \"type\": \"double\"}, {\"name\": \"currency\", \"type\": \"string\"}]}}},";
  public static final String MAP_TYPE_SCHEMA = "{\"name\": \"city_to_state\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},";
  public static final String EXTRA_TYPE_SCHEMA = "{\"name\": \"distance_in_meters\", \"type\": \"int\"},"
      + "{\"name\": \"seconds_since_epoch\", \"type\": \"long\"},"
      + "{\"name\": \"weight\", \"type\": \"float\"},"
      + "{\"name\": \"nation\", \"type\": \"bytes\"},"
      + "{\"name\":\"current_date\",\"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},"
      + "{\"name\":\"current_ts\",\"type\": {\"type\": \"long\"}},"
      + "{\"name\":\"height\",\"type\":{\"type\":\"fixed\",\"name\":\"abc\",\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":6}},";

  public static final String EXTRA_COL_SCHEMA1 = "{\"name\": \"extra_column1\", \"type\": [\"null\", \"string\"], \"default\": null },";
  public static final String EXTRA_COL_SCHEMA2 = "{\"name\": \"extra_column2\", \"type\": [\"null\", \"string\"], \"default\": null},";
  public static final String TRIP_EXAMPLE_SCHEMA =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_EXAMPLE_SCHEMA_EVOLVED_1 =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_COL_SCHEMA1 + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_EXAMPLE_SCHEMA_EVOLVED_2 =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_COL_SCHEMA2 + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_FLATTENED_SCHEMA =
      TRIP_SCHEMA_PREFIX + FARE_FLATTENED_SCHEMA + TRIP_SCHEMA_SUFFIX;

  public static final String TRIP_NESTED_EXAMPLE_SCHEMA =
      TRIP_SCHEMA_PREFIX + FARE_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_ENCODED_DECIMAL_SCHEMA = "{\"type\":\"record\",\"name\":\"tripUberRec\",\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\",\"type\":\"string\"},"
      + "{\"name\":\"decfield\",\"type\":{\"type\":\"bytes\",\"name\":\"abc\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":6}},"
      + "{\"name\":\"lowprecision\",\"type\":{\"type\":\"bytes\",\"name\":\"def\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}},"
      + "{\"name\":\"highprecision\",\"type\":{\"type\":\"bytes\",\"name\":\"ghi\",\"logicalType\":\"decimal\",\"precision\":32,\"scale\":12}},"
      + "{\"name\":\"driver\",\"type\":\"string\"},{\"name\":\"fare\",\"type\":\"double\"},{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false}]}";
  public static final String TRIP_SCHEMA = "{\"type\":\"record\",\"name\":\"tripUberRec\",\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\",\"type\":\"string\"},"
      + "{\"name\":\"driver\",\"type\":\"string\"},{\"name\":\"fare\",\"type\":\"double\"},{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false}]}";
  public static final String SHORT_TRIP_SCHEMA = "{\"type\":\"record\",\"name\":\"shortTripRec\",\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\",\"type\":\"string\"},"
      + "{\"name\":\"driver\",\"type\":\"string\"},{\"name\":\"fare\",\"type\":\"double\"},{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false}]}";

  public static final String NULL_SCHEMA = Schema.create(Schema.Type.NULL).toString();
  public static final String TRIP_HIVE_COLUMN_TYPES = "bigint,string,string,string,string,string,double,double,double,double,int,bigint,float,binary,int,bigint,decimal(10,6),"
      + "map<string,string>,struct<amount:double,currency:string>,array<struct<amount:double,currency:string>>,boolean";


  public static final Schema AVRO_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
  public static final Schema NESTED_AVRO_SCHEMA = new Schema.Parser().parse(TRIP_NESTED_EXAMPLE_SCHEMA);
  public static final Schema AVRO_SCHEMA_WITH_METADATA_FIELDS =
      HoodieAvroUtils.addMetadataFields(AVRO_SCHEMA);
  public static final Schema AVRO_SHORT_TRIP_SCHEMA = new Schema.Parser().parse(SHORT_TRIP_SCHEMA);
  public static final Schema AVRO_TRIP_ENCODED_DECIMAL_SCHEMA = new Schema.Parser().parse(TRIP_ENCODED_DECIMAL_SCHEMA);
  public static final Schema AVRO_TRIP_SCHEMA = new Schema.Parser().parse(TRIP_SCHEMA);
  public static final Schema FLATTENED_AVRO_SCHEMA = new Schema.Parser().parse(TRIP_FLATTENED_SCHEMA);
  private final Random rand;

  //Maintains all the existing keys schema wise
  private final Map<String, Map<Integer, KeyPartition>> existingKeysBySchema;
  private final String[] partitionPaths;
  //maintains the count of existing keys schema wise
  private Map<String, Integer> numKeysBySchema;

  public HoodieTestDataGenerator(long seed) {
    this(seed, DEFAULT_PARTITION_PATHS, new HashMap<>());
  }

  public HoodieTestDataGenerator(String schema, long seed) {
    this(schema, seed, DEFAULT_PARTITION_PATHS, new HashMap<>());
  }

  public HoodieTestDataGenerator(long seed, String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
    this(TRIP_EXAMPLE_SCHEMA, seed, partitionPaths, keyPartitionMap);
  }

  public HoodieTestDataGenerator(String schema, long seed, String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
    this.rand = new Random(seed);
    this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
    this.existingKeysBySchema = new HashMap<>();
    this.existingKeysBySchema.put(schema, keyPartitionMap);
    this.numKeysBySchema = new HashMap<>();
    this.numKeysBySchema.put(schema, keyPartitionMap.size());

    logger.info(String.format("Test DataGenerator's seed (%s)", seed));
  }

  //////////////////////////////////////////////////////////////////////////////////
  // DEPRECATED API
  //////////////////////////////////////////////////////////////////////////////////

  @Deprecated
  public HoodieTestDataGenerator(String[] partitionPaths) {
    this(partitionPaths, new HashMap<>());
  }

  @Deprecated
  public HoodieTestDataGenerator() {
    this(DEFAULT_PARTITION_PATHS);
  }

  public static HoodieTestDataGenerator createTestGeneratorFirstPartition() {
    return new HoodieTestDataGenerator(new String[]{DEFAULT_FIRST_PARTITION_PATH});
  }

  public static HoodieTestDataGenerator createTestGeneratorSecondPartition() {
    return new HoodieTestDataGenerator(new String[]{DEFAULT_SECOND_PARTITION_PATH});
  }

  public static HoodieTestDataGenerator createTestGeneratorThirdPartition() {
    return new HoodieTestDataGenerator(new String[]{DEFAULT_THIRD_PARTITION_PATH});
  }

  public HoodieTestDataGenerator(boolean makeDatesAmbiguous) {
    this();
    this.makeDatesAmbiguous = makeDatesAmbiguous;
  }

  @Deprecated
  public HoodieTestDataGenerator(String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
    // NOTE: This used as a workaround to make sure that new instantiations of the generator
    //       always return "new" random values.
    //       Caveat is that if 2 successive invocations are made w/in the timespan that is smaller
    //       than the resolution of {@code nanoTime}, then this will produce identical results
    this(System.nanoTime(), partitionPaths, keyPartitionMap);
  }

  /**
   * Fetches next commit time in seconds from current one.
   *
   * @param curCommitTime current commit time.
   * @return the next valid commit time.
   */
  public static Long getNextCommitTime(long curCommitTime) {
    if ((curCommitTime + 1) % 1000000000000L >= 60) { // max seconds is 60 and hence
      return Long.parseLong(InProcessTimeGenerator.createNewInstantTime());
    } else {
      return curCommitTime + 1;
    }
  }

  public static String getCommitTimeAtUTC(long epochSecond) {
    return HoodieInstantTimeGenerator.getInstantFromTemporalAccessor(Instant.ofEpochSecond(epochSecond).atZone(ZoneOffset.UTC));
  }

  /**
   * @deprecated please use non-static version
   */
  public static void writePartitionMetadataDeprecated(HoodieStorage storage,
                                                      String[] partitionPaths,
                                                      String basePath) {
    new HoodieTestDataGenerator().writePartitionMetadata(storage, partitionPaths, basePath);
  }

  //////////////////////////////////////////////////////////////////////////////////

  /**
   * @implNote {@link HoodieTestDataGenerator} is supposed to just generate records with schemas. Leave HoodieTable files (metafile, basefile, logfile, etc) to {@link HoodieTestTable}.
   * @deprecated Use {@link HoodieTestTable#withPartitionMetaFiles(java.lang.String...)} instead.
   */
  public void writePartitionMetadata(HoodieStorage storage,
                                     String[] partitionPaths,
                                     String basePath) {
    for (String partitionPath : partitionPaths) {
      new HoodiePartitionMetadata(storage, "000", new StoragePath(basePath),
          new StoragePath(basePath, partitionPath), Option.empty()).trySave();
    }
  }

  public int getEstimatedFileSizeInBytes(int numOfRecords) {
    return numOfRecords * BYTES_PER_RECORD + BLOOM_FILTER_BYTES;
  }

  public RawTripTestPayload generateRandomValueAsPerSchema(String schemaStr, HoodieKey key, String commitTime, boolean isFlattened) throws IOException {
    if (TRIP_FLATTENED_SCHEMA.equals(schemaStr)) {
      return generateRandomValue(key, commitTime, true);
    } else if (TRIP_EXAMPLE_SCHEMA.equals(schemaStr)) {
      return generateRandomValue(key, commitTime, isFlattened);
    } else if (TRIP_ENCODED_DECIMAL_SCHEMA.equals(schemaStr)) {
      return generatePayloadForTripEncodedDecimalSchema(key, commitTime);
    } else if (TRIP_SCHEMA.equals(schemaStr)) {
      return generatePayloadForTripSchema(key, commitTime);
    } else if (SHORT_TRIP_SCHEMA.equals(schemaStr)) {
      return generatePayloadForShortTripSchema(key, commitTime);
    } else if (TRIP_NESTED_EXAMPLE_SCHEMA.equals(schemaStr)) {
      return generateNestedExampleRandomValue(key, commitTime);
    }

    return null;
  }

  /**
   * Generates a new avro record of the above nested schema format,
   * retaining the key if optionally provided.
   *
   * @param key Hoodie key.
   * @param instantTime Instant time to use.
   * @return Raw paylaod of a test record.
   */
  public RawTripTestPayload generateRandomValue(HoodieKey key, String instantTime) throws IOException {
    return generateRandomValue(key, instantTime, false);
  }

  /**
   * Generates a new avro record with the specified schema (nested or flattened),
   * retaining the key if optionally provided.
   *
   * @param key  Hoodie key.
   * @param instantTime  Commit time to use.
   * @param isFlattened  whether the schema of the record should be flattened.
   * @return Raw paylaod of a test record.
   * @throws IOException
   */
  private RawTripTestPayload generateRandomValue(
      HoodieKey key, String instantTime, boolean isFlattened) throws IOException {
    return generateRandomValue(key, instantTime, isFlattened, 0);
  }

  private RawTripTestPayload generateNestedExampleRandomValue(
      HoodieKey key, String instantTime) throws IOException {
    return generateNestedExampleRandomValue(key, instantTime, 0);
  }

  private RawTripTestPayload generateRandomValue(
      HoodieKey key, String instantTime, boolean isFlattened, long timestamp) throws IOException {
    GenericRecord rec = generateGenericRecord(
        key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, timestamp,
        false, isFlattened);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA);
  }

  private RawTripTestPayload generateNestedExampleRandomValue(
      HoodieKey key, String instantTime, int ts) throws IOException {
    GenericRecord rec = generateNestedExampleGenericRecord(
        key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, ts,
        false);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Generates a new avro record with TRIP_ENCODED_DECIMAL_SCHEMA, retaining the key if optionally provided.
   */
  public RawTripTestPayload generatePayloadForTripEncodedDecimalSchema(HoodieKey key, String commitTime)
      throws IOException {
    GenericRecord rec =
        generateRecordForTripEncodedDecimalSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(),
        TRIP_ENCODED_DECIMAL_SCHEMA);
  }

  /**
   * Generates a new avro record with TRIP_SCHEMA, retaining the key if optionally provided.
   */
  public RawTripTestPayload generatePayloadForTripSchema(HoodieKey key, String commitTime) throws IOException {
    GenericRecord rec = generateRecordForTripSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), TRIP_SCHEMA);
  }

  public RawTripTestPayload generatePayloadForShortTripSchema(HoodieKey key, String commitTime) throws IOException {
    GenericRecord rec = generateRecordForShortTripSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0);
    return new RawTripTestPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), SHORT_TRIP_SCHEMA);
  }

  /**
   * Generates a new avro record of the above schema format for a delete.
   */
  private RawTripTestPayload generateRandomDeleteValue(HoodieKey key, String instantTime) throws IOException {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, 0,
        true, false);
    return new RawTripTestPayload(Option.of(rec.toString()), key.getRecordKey(), key.getPartitionPath(), TRIP_EXAMPLE_SCHEMA, true, 0L);
  }

  /**
   * Generates a new avro record of the above schema format, retaining the key if optionally provided.
   */
  private HoodieAvroPayload generateAvroPayload(HoodieKey key, String instantTime) {
    GenericRecord rec = generateGenericRecord(key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, 0);
    return new HoodieAvroPayload(Option.of(rec));
  }

  public GenericRecord generateGenericRecord(String rowKey, String partitionPath, String riderName, String driverName,
                                             long timestamp) {
    return generateGenericRecord(rowKey, partitionPath, riderName, driverName, timestamp, false, false);
  }

  /**
   * Populate rec with values for TRIP_SCHEMA_PREFIX
   */
  private void generateTripPrefixValues(GenericRecord rec, String rowKey, String partitionPath, String riderName, String driverName, long timestamp) {
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("partition_path", partitionPath);
    rec.put("trip_type", new GenericData.EnumSymbol(
        TRIP_TYPE_ENUM_SCHEMA, rand.nextInt(2) == 0 ? "UBERX" : "BLACK"));
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("begin_lat", rand.nextDouble());
    rec.put("begin_lon", rand.nextDouble());
    rec.put("end_lat", rand.nextDouble());
    rec.put("end_lon", rand.nextDouble());
  }

  /**
   * Populate rec with values for FARE_FLATTENED_SCHEMA
   */
  private void generateFareFlattenedValues(GenericRecord rec) {
    rec.put("fare", rand.nextDouble() * 100);
    rec.put("currency", "USD");
  }

  /**
   * Populate rec with values for EXTRA_TYPE_SCHEMA
   */
  private void generateExtraSchemaValues(GenericRecord rec) {
    rec.put("distance_in_meters", rand.nextInt());
    rec.put("seconds_since_epoch", rand.nextLong());
    rec.put("weight", rand.nextFloat());
    byte[] bytes = getUTF8Bytes("Canada");
    rec.put("nation", ByteBuffer.wrap(bytes));
    long randomMillis = genRandomTimeMillis(rand);
    Instant instant = Instant.ofEpochMilli(randomMillis);
    rec.put("current_date", makeDatesAmbiguous ? -1000000 :
        (int) LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate().toEpochDay());
    rec.put("current_ts", randomMillis);

    BigDecimal bigDecimal = new BigDecimal(String.format(Locale.ENGLISH, "%5f", rand.nextFloat()));
    Schema decimalSchema = AVRO_SCHEMA.getField("height").schema();
    Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
    GenericFixed genericFixed = decimalConversions.toFixed(bigDecimal, decimalSchema, LogicalTypes.decimal(10, 6));
    rec.put("height", genericFixed);
  }

  /**
   * Populate rec with values for MAP_TYPE_SCHEMA
   */
  private void generateMapTypeValues(GenericRecord rec) {
    rec.put("city_to_state", Collections.singletonMap("LA", "CA"));
  }

  /**
   * Populate rec with values for FARE_NESTED_SCHEMA
   */
  private void generateFareNestedValues(GenericRecord rec) {
    GenericRecord fareRecord = new GenericData.Record(AVRO_SCHEMA.getField("fare").schema());
    fareRecord.put("amount", rand.nextDouble() * 100);
    fareRecord.put("currency", "USD");
    rec.put("fare", fareRecord);
  }

  /**
   * Populate rec with values for TIP_NESTED_SCHEMA
   */
  private void generateTipNestedValues(GenericRecord rec) {
    GenericArray<GenericRecord> tipHistoryArray = new GenericData.Array<>(1, AVRO_SCHEMA.getField("tip_history").schema());
    Schema tipSchema = new Schema.Parser().parse(AVRO_SCHEMA.getField("tip_history").schema().toString()).getElementType();
    GenericRecord tipRecord = new GenericData.Record(tipSchema);
    tipRecord.put("amount", rand.nextDouble() * 100);
    tipRecord.put("currency", "USD");
    tipHistoryArray.add(tipRecord);
    rec.put("tip_history", tipHistoryArray);
  }

  /**
   * Populate rec with values for TRIP_SCHEMA_SUFFIX
   */
  private void generateTripSuffixValues(GenericRecord rec, boolean isDeleteRecord) {
    if (isDeleteRecord) {
      rec.put("_hoodie_is_deleted", true);
    } else {
      rec.put("_hoodie_is_deleted", false);
    }
  }
  
  /**
   * Generate record conforming to TRIP_EXAMPLE_SCHEMA or TRIP_FLATTENED_SCHEMA if isFlattened is true
   */
  public GenericRecord generateGenericRecord(String rowKey, String partitionPath, String riderName, String driverName,
                                                    long timestamp, boolean isDeleteRecord,
                                                    boolean isFlattened) {
    GenericRecord rec = new GenericData.Record(isFlattened ? FLATTENED_AVRO_SCHEMA : AVRO_SCHEMA);
    generateTripPrefixValues(rec, rowKey, partitionPath, riderName, driverName, timestamp);
    if (isFlattened) {
      generateFareFlattenedValues(rec);
    } else {
      generateExtraSchemaValues(rec);
      generateMapTypeValues(rec);
      generateFareNestedValues(rec);
      generateTipNestedValues(rec);
    }
    generateTripSuffixValues(rec, isDeleteRecord);
    return rec;
  }

  /**
   * Generate record conforming to TRIP_NESTED_EXAMPLE_SCHEMA
   */
  public  GenericRecord generateNestedExampleGenericRecord(String rowKey, String partitionPath, String riderName, String driverName,
                                                        long timestamp, boolean isDeleteRecord) {
    GenericRecord rec = new GenericData.Record(NESTED_AVRO_SCHEMA);
    generateTripPrefixValues(rec, rowKey, partitionPath, riderName, driverName, timestamp);
    generateFareNestedValues(rec);
    generateTripSuffixValues(rec, isDeleteRecord);
    return rec;
  }

  /*
Generate random record using TRIP_ENCODED_DECIMAL_SCHEMA
 */
  public GenericRecord generateRecordForTripEncodedDecimalSchema(String rowKey, String riderName, String driverName,
                                                                 long timestamp) {
    GenericRecord rec = new GenericData.Record(AVRO_TRIP_ENCODED_DECIMAL_SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("decfield", getNonzeroEncodedBigDecimal(rand, 6, 10));
    rec.put("lowprecision", getNonzeroEncodedBigDecimal(rand, 2, 4));
    rec.put("highprecision", getNonzeroEncodedBigDecimal(rand, 12, 32));
    rec.put("driver", driverName);
    rec.put("fare", rand.nextDouble() * 100);
    rec.put("_hoodie_is_deleted", false);
    return rec;
  }

  private static String getNonzeroEncodedBigDecimal(Random rand, int scale, int precision) {
    //scale the value because rand.nextDouble() only returns a val that is between 0 and 1

    //make it between 0.1 and 1 so that we keep all values in the same order of magnitude
    double nextDouble = rand.nextDouble();
    while (nextDouble <= 0.1) {
      nextDouble = rand.nextDouble();
    }
    double valuescale = Math.pow(10, precision - scale);
    BigDecimal dec = BigDecimal.valueOf(nextDouble * valuescale)
        .setScale(scale, RoundingMode.HALF_UP).round(new MathContext(precision, RoundingMode.HALF_UP));
    return Base64.getEncoder().encodeToString(dec.unscaledValue().toByteArray());
  }

  /*
  Generate random record using TRIP_SCHEMA
   */
  public GenericRecord generateRecordForTripSchema(String rowKey, String riderName, String driverName, long timestamp) {
    GenericRecord rec = new GenericData.Record(AVRO_TRIP_SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("fare", rand.nextDouble() * 100);
    rec.put("_hoodie_is_deleted", false);
    return rec;
  }

  public GenericRecord generateRecordForShortTripSchema(String rowKey, String riderName, String driverName, long timestamp) {
    GenericRecord rec = new GenericData.Record(AVRO_SHORT_TRIP_SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("timestamp", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    rec.put("fare", rand.nextDouble() * 100);
    rec.put("_hoodie_is_deleted", false);
    return rec;
  }

  public static void createRequestedCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration) throws IOException {
    Path pendingRequestedFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeRequestedCommitFileName(instantTime));
    createEmptyFile(basePath, pendingRequestedFile, configuration);
  }

  public static void createPendingCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration) throws IOException {
    Path pendingCommitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeInflightCommitFileName(instantTime));
    createEmptyFile(basePath, pendingCommitFile, configuration);
  }

  public static void createCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    createCommitFile(basePath, instantTime, configuration, commitMetadata);
  }

  private static void createCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration, HoodieCommitMetadata commitMetadata) {
    Arrays.asList(INSTANT_FILE_NAME_GENERATOR.makeCommitFileName(instantTime + "_" + InProcessTimeGenerator.createNewInstantTime()),
            INSTANT_FILE_NAME_GENERATOR.makeInflightCommitFileName(instantTime),
            INSTANT_FILE_NAME_GENERATOR.makeRequestedCommitFileName(instantTime))
        .forEach(f -> createMetadataFile(f, basePath, configuration, commitMetadata));
  }

  public static void createOnlyCompletedCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    createOnlyCompletedCommitFile(basePath, instantTime, configuration, commitMetadata);
  }

  public static void createOnlyCompletedCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration, HoodieCommitMetadata commitMetadata) {
    createMetadataFile(INSTANT_FILE_NAME_GENERATOR.makeCommitFileName(instantTime), basePath, configuration, commitMetadata);
  }

  public static void createDeltaCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    createDeltaCommitFile(basePath, instantTime, configuration, commitMetadata);
  }

  private static void createDeltaCommitFile(String basePath, String instantTime, StorageConfiguration<?> configuration, HoodieCommitMetadata commitMetadata) {
    Arrays.asList(INSTANT_FILE_NAME_GENERATOR.makeDeltaFileName(instantTime + "_" + InProcessTimeGenerator.createNewInstantTime()),
            INSTANT_FILE_NAME_GENERATOR.makeInflightDeltaFileName(instantTime),
            INSTANT_FILE_NAME_GENERATOR.makeRequestedDeltaFileName(instantTime))
        .forEach(f -> createMetadataFile(f, basePath, configuration, commitMetadata));
  }

  private static void createMetadataFile(String f, String basePath, StorageConfiguration<?> configuration, HoodieCommitMetadata commitMetadata) {
    createMetadataFile(f, basePath, configuration, COMMIT_METADATA_SER_DE.getInstantWriter(commitMetadata).get());
  }

  private static void createMetadataFile(String f, String basePath, StorageConfiguration<?> configuration, HoodieInstantWriter writer) {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
            + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/" + f);
    OutputStream os = null;
    try {
      HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, configuration);
      os = storage.create(new StoragePath(commitFile.toUri()), true);
      // Write empty commit metadata
      writer.writeToStream(os);
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
  }

  public static void createReplaceCommitRequestedFile(String basePath, String instantTime, StorageConfiguration<?> configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeRequestedReplaceFileName(instantTime));
    createEmptyFile(basePath, commitFile, configuration);
  }

  public static void createReplaceCommitInflightFile(String basePath, String instantTime, StorageConfiguration<?> configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeInflightReplaceFileName(instantTime));
    createEmptyFile(basePath, commitFile, configuration);
  }

  private static void createPendingClusterFile(String basePath, String instantTime, StorageConfiguration<?> configuration, HoodieCommitMetadata commitMetadata) {
    Arrays.asList(INSTANT_FILE_NAME_GENERATOR.makeInflightClusteringFileName(instantTime),
            INSTANT_FILE_NAME_GENERATOR.makeRequestedClusteringFileName(instantTime))
        .forEach(f -> createMetadataFile(f, basePath, configuration, commitMetadata));
  }

  public static void createPendingClusterFile(String basePath, String instantTime, StorageConfiguration<?> configuration) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    createPendingClusterFile(basePath, instantTime, configuration, commitMetadata);
  }

  public static void createEmptyCleanRequestedFile(String basePath, String instantTime, StorageConfiguration<?> configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeRequestedCleanerFileName(instantTime));
    createEmptyFile(basePath, commitFile, configuration);
  }

  private static void createEmptyFile(String basePath, Path filePath, StorageConfiguration<?> configuration) throws IOException {
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, configuration);
    OutputStream os = storage.create(new StoragePath(filePath.toUri()), true);
    os.close();
  }

  public static void createCompactionRequestedFile(String basePath, String instantTime, StorageConfiguration<?> configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeRequestedCompactionFileName(instantTime));
    createEmptyFile(basePath, commitFile, configuration);
  }

  public static void createCompactionAuxiliaryMetadata(String basePath, HoodieInstant instant,
                                                       StorageConfiguration<?> configuration) throws IOException {
    Path commitFile =
        new Path(basePath + "/" + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + INSTANT_FILE_NAME_GENERATOR.getFileName(instant));
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, configuration);
    try (OutputStream os = storage.create(new StoragePath(commitFile.toUri()), true)) {
      HoodieCompactionPlan workload = HoodieCompactionPlan.newBuilder().setVersion(1).build();
      // Write empty commit metadata
      COMMIT_METADATA_SER_DE.getInstantWriter(workload).get().writeToStream(os);
    }
  }

  public static void createSavepointFile(String basePath, String instantTime, StorageConfiguration<?> configuration)
      throws IOException {
    Path commitFile = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
        + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeSavePointFileName(instantTime + "_" + InProcessTimeGenerator.createNewInstantTime()));
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, configuration);
    try (OutputStream os = storage.create(new StoragePath(commitFile.toUri()), true)) {
      HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
      // Write empty commit metadata
      COMMIT_METADATA_SER_DE.getInstantWriter(commitMetadata).get().writeToStream(os);
    }
  }

  public List<HoodieRecord> generateInsertsAsPerSchema(String commitTime, Integer n, String schemaStr) {
    return generateInsertsStream(commitTime, n, false, schemaStr).collect(Collectors.toList());
  }

  /**
   * Generates new inserts for TRIP_EXAMPLE_SCHEMA with nested schema, uniformly across the partition paths above.
   * It also updates the list of existing keys.
   */
  public List<HoodieRecord> generateInserts(String instantTime, Integer n) {
    return generateInserts(instantTime, n, false);
  }

  public List<HoodieRecord> generateInsertsNestedExample(String instantTime, Integer n) {
    return generateInsertsStream(instantTime, n, false, TRIP_NESTED_EXAMPLE_SCHEMA).collect(Collectors.toList());
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
    return generateInsertsStream(instantTime, n, isFlattened, isFlattened ? TRIP_FLATTENED_SCHEMA : TRIP_EXAMPLE_SCHEMA).collect(Collectors.toList());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(String commitTime, Integer n, boolean isFlattened, String schemaStr) {
    return generateInsertsStream(commitTime, n, isFlattened, schemaStr, false);
  }

  public List<HoodieRecord> generateInsertsContainsAllPartitions(String instantTime, Integer n) {
    if (n < partitionPaths.length) {
      throw new HoodieIOException("n must greater then partitionPaths length");
    }
    return generateInsertsStream(instantTime,  n, false, TRIP_EXAMPLE_SCHEMA, true).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateInsertsForPartition(String instantTime, Integer n, String partition) {
    return generateInsertsStream(instantTime,  n, false, TRIP_EXAMPLE_SCHEMA, false, () -> partition, () -> genPseudoRandomUUID(rand).toString()).collect(Collectors.toList());
  }

  public Stream<HoodieRecord> generateInsertsStream(String commitTime, Integer n, boolean isFlattened, String schemaStr, boolean containsAllPartitions) {
    AtomicInteger partitionIndex = new AtomicInteger(0);
    return generateInsertsStream(commitTime, n, isFlattened, schemaStr, containsAllPartitions,
        () -> {
          // round robin to ensure we generate inserts for all partition paths
          String partitionToUse = partitionPaths[partitionIndex.get()];
          partitionIndex.set((partitionIndex.get() + 1) % partitionPaths.length);
          return partitionToUse;
        },
        () -> genPseudoRandomUUID(rand).toString());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(String instantTime, Integer n, boolean isFlattened, String schemaStr, boolean containsAllPartitions,
                                                    Supplier<String> partitionPathSupplier, Supplier<String> recordKeySupplier) {
    int currSize = getNumExistingKeys(schemaStr);
    return IntStream.range(0, n).boxed().map(i -> {
      String partitionPath = partitionPathSupplier.get();
      if (containsAllPartitions && i < partitionPaths.length) {
        partitionPath = partitionPaths[i];
      }
      HoodieKey key = new HoodieKey(recordKeySupplier.get(), partitionPath);
      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      populateKeysBySchema(schemaStr, currSize + i, kp);
      incrementNumExistingKeysBySchema(schemaStr);
      try {
        return new HoodieAvroRecord(key, generateRandomValueAsPerSchema(schemaStr, key, instantTime, isFlattened));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  /*
  Takes care of populating keys schema wise
   */
  private void populateKeysBySchema(String schemaStr, int i, KeyPartition kp) {
    if (existingKeysBySchema.containsKey(schemaStr)) {
      existingKeysBySchema.get(schemaStr).put(i, kp);
    } else {
      existingKeysBySchema.put(schemaStr, new HashMap<>());
      existingKeysBySchema.get(schemaStr).put(i, kp);
    }
  }

  private void incrementNumExistingKeysBySchema(String schemaStr) {
    if (numKeysBySchema.containsKey(schemaStr)) {
      numKeysBySchema.put(schemaStr, numKeysBySchema.get(schemaStr) + 1);
    } else {
      numKeysBySchema.put(schemaStr, 1);
    }
  }

  public List<HoodieRecord> generateSameKeyInserts(String instantTime, List<HoodieRecord> origin) throws IOException {
    List<HoodieRecord> copy = new ArrayList<>();
    for (HoodieRecord r : origin) {
      HoodieKey key = r.getKey();
      HoodieRecord record = new HoodieAvroRecord(key, generateRandomValue(key, instantTime));
      copy.add(record);
    }
    return copy;
  }

  public List<HoodieRecord> generateInsertsWithHoodieAvroPayload(String instantTime, int limit) {
    List<HoodieRecord> inserts = new ArrayList<>();
    int currSize = getNumExistingKeys(TRIP_EXAMPLE_SCHEMA);
    for (int i = 0; i < limit; i++) {
      String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
      HoodieKey key = new HoodieKey(genPseudoRandomUUID(rand).toString(), partitionPath);
      HoodieRecord record = new HoodieAvroRecord(key, generateAvroPayload(key, instantTime));
      inserts.add(record);

      KeyPartition kp = new KeyPartition();
      kp.key = key;
      kp.partitionPath = partitionPath;
      populateKeysBySchema(TRIP_EXAMPLE_SCHEMA, currSize + i, kp);
      incrementNumExistingKeysBySchema(TRIP_EXAMPLE_SCHEMA);
    }
    return inserts;
  }

  public List<HoodieRecord> generateUpdatesWithHoodieAvroPayload(String instantTime, List<HoodieRecord> baseRecords) {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = new HoodieAvroRecord(baseRecord.getKey(), generateAvroPayload(baseRecord.getKey(), instantTime));
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
    RawTripTestPayload payload =
        new RawTripTestPayload(Option.empty(), key.getRecordKey(), key.getPartitionPath(), null, true, 0L);
    return new HoodieAvroRecord(key, payload);
  }

  public HoodieRecord generateUpdateRecord(HoodieKey key, String instantTime) throws IOException {
    return new HoodieAvroRecord(key, generateRandomValue(key, instantTime));
  }

  public HoodieRecord generateUpdateRecordWithTimestamp(HoodieKey key, String instantTime, long timestamp) throws IOException {
    return new HoodieAvroRecord(key, generateRandomValue(key, instantTime, false, timestamp));
  }

  public List<HoodieRecord> generateUpdates(String instantTime, List<HoodieRecord> baseRecords) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = generateUpdateRecord(baseRecord.getKey(), instantTime);
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateUpdatesWithTimestamp(String instantTime, List<HoodieRecord> baseRecords, long timestamp) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      updates.add(generateUpdateRecordWithTimestamp(baseRecord.getKey(), instantTime, timestamp));
    }
    return updates;
  }

  public List<HoodieRecord> generateUpdatesForDifferentPartition(String instantTime, List<HoodieRecord> baseRecords, long timestamp, String newPartition)
      throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      String partition = baseRecord.getPartitionPath();
      checkState(!partition.equals(newPartition), "newPartition should be different from any given record's current partition.");
      HoodieKey key = new HoodieKey(baseRecord.getRecordKey(), newPartition);
      HoodieRecord record = generateUpdateRecordWithTimestamp(key, instantTime, timestamp);
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateUpdates(String instantTime, Integer n) throws IOException {
    return generateUpdates(instantTime, n, TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Generates new updates, randomly distributed across the keys above. There can be duplicates within the returned
   * list
   *
   * @param instantTime Instant Timestamp
   * @param n          Number of updates (including dups)
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUpdates(String instantTime, Integer n, String schemaStr) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(schemaStr);
      Integer numExistingKeys = numKeysBySchema.get(schemaStr);
      KeyPartition kp = existingKeys.get(rand.nextInt(numExistingKeys - 1));
      HoodieRecord record = generateUpdateRecord(kp.key, instantTime);
      updates.add(record);
    }
    return updates;
  }

  /**
   * Generate update for each record in the dataset.
   * @param instantTime
   * @return
   * @throws IOException
   */
  public List<HoodieRecord> generateUpdatesForAllRecords(String instantTime) {
    List<HoodieRecord> updates = new ArrayList<>();
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    existingKeys.values().forEach(kp -> {
      try {
        HoodieRecord record = generateUpdateRecord(kp.key, instantTime);
        updates.add(record);
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    });
    return updates;
  }

  public List<HoodieRecord> generateUpdatesAsPerSchema(String commitTime, Integer n, String schemaStr) {
    return generateUniqueUpdatesStream(commitTime, n, schemaStr).collect(Collectors.toList());
  }

  /**
   * Generates deduped updates of keys previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Instant Timestamp
   * @param n          Number of unique records
   * @return list of hoodie record updates
   */
  public List<HoodieRecord> generateUniqueUpdates(String instantTime, Integer n) {
    return generateUniqueUpdatesStream(instantTime, n, TRIP_EXAMPLE_SCHEMA).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueUpdates(String instantTime, Integer n, String schemaStr) {
    return generateUniqueUpdatesStream(instantTime, n, schemaStr).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueUpdatesNestedExample(String instantTime, Integer n) {
    return generateUniqueUpdatesStream(instantTime, n, TRIP_NESTED_EXAMPLE_SCHEMA).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueUpdatesAsPerSchema(String instantTime, Integer n, String schemaStr) {
    return generateUniqueUpdatesStream(instantTime, n, schemaStr).collect(Collectors.toList());
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
  public Stream<HoodieRecord> generateUniqueUpdatesStream(String instantTime, Integer n, String schemaStr) {
    final Set<KeyPartition> used = new HashSet<>();
    int numExistingKeys = numKeysBySchema.getOrDefault(schemaStr, 0);
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(schemaStr);
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
      logger.debug("key getting updated: {}", kp.key.getRecordKey());
      used.add(kp);
      try {
        return new HoodieAvroRecord(kp.key, generateRandomValueAsPerSchema(schemaStr, kp.key, instantTime, false));
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
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    Integer numExistingKeys = numKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique deletes is greater than number of available keys");
    }

    List<HoodieKey> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int index = rand.nextInt(numExistingKeys);
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
    numKeysBySchema.put(TRIP_EXAMPLE_SCHEMA, numExistingKeys);
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
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    Integer numExistingKeys = numKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    if (n > numExistingKeys) {
      throw new IllegalArgumentException("Requested unique deletes is greater than number of available keys");
    }

    List<HoodieRecord> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int index = rand.nextInt(numExistingKeys);
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
        result.add(new HoodieAvroRecord(kp.key, generateRandomDeleteValue(kp.key, instantTime)));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }
    numKeysBySchema.put(TRIP_EXAMPLE_SCHEMA, numExistingKeys);
    return result.stream();
  }

  /**
   * Generates deduped delete records previously inserted, randomly distributed across the keys above.
   *
   * @param instantTime Commit Timestamp
   * @param n          Number of unique records
   * @return List of hoodie records for delete
   */
  public List<HoodieRecord> generateUniqueDeleteRecords(String instantTime, Integer n) {
    return generateUniqueDeleteRecordStream(instantTime, n).collect(Collectors.toList());
  }

  public boolean deleteExistingKeyIfPresent(HoodieKey key) {
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    Integer numExistingKeys = numKeysBySchema.get(TRIP_EXAMPLE_SCHEMA);
    for (Map.Entry<Integer, KeyPartition> entry : existingKeys.entrySet()) {
      if (entry.getValue().key.equals(key)) {
        int index = entry.getKey();
        existingKeys.put(index, existingKeys.get(numExistingKeys - 1));
        existingKeys.remove(numExistingKeys - 1);
        numExistingKeys--;
        numKeysBySchema.put(TRIP_EXAMPLE_SCHEMA, numExistingKeys);
        return true;
      }
    }
    return false;
  }

  public GenericRecord generateGenericRecord() {
    return generateGenericRecord(genPseudoRandomUUID(rand).toString(), "0",
        genPseudoRandomUUID(rand).toString(), genPseudoRandomUUID(rand).toString(), rand.nextLong());
  }

  public List<GenericRecord> generateGenericRecords(int numRecords) {
    List<GenericRecord> list = new ArrayList<>();
    IntStream.range(0, numRecords).forEach(i -> list.add(generateGenericRecord()));
    return list;
  }

  public String[] getPartitionPaths() {
    return partitionPaths;
  }

  public int getNumExistingKeys(String schemaStr) {
    return numKeysBySchema.getOrDefault(schemaStr, 0);
  }

  /**
   * Object containing the key and partition path for testing.
   */
  public static class KeyPartition implements Serializable {

    public HoodieKey key;
    public String partitionPath;
  }

  @Override
  public void close() {
    existingKeysBySchema.clear();
  }

  private static long genRandomTimeMillis(Random r) {
    // Fri Feb 13 15:31:30 PST 2009
    long anchorTs = 1234567890L;
    // NOTE: To provide for certainty and not generate overly random dates, we will limit
    //       dispersion to be w/in +/- 3 days from the anchor date
    return anchorTs + r.nextLong() % 259200000L;
  }

  public static UUID genPseudoRandomUUID(Random r) {
    byte[] bytes = new byte[16];
    r.nextBytes(bytes);

    bytes[6] &= 0x0f;
    bytes[6] |= 0x40;
    bytes[8] &= 0x3f;
    bytes[8] |= 0x80;

    try {
      Constructor<UUID> ctor = UUID.class.getDeclaredConstructor(byte[].class);
      ctor.setAccessible(true);
      return ctor.newInstance((Object) bytes);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
      logger.info("Failed to generate pseudo-random UUID!");
      throw new HoodieException(e);
    }
  }

  /**
   * Used for equality checks between the expected and actual records for generated by the HoodieTestDataGenerator.
   * The fields identify the record with the combination of the recordKey and partitionPath and assert that the proper
   * value is present with the orderingVal and the riderValue, which is updated as part of the update utility methods.
   */
  public static class RecordIdentifier {
    private final String recordKey;
    private final String orderingVal;
    private final String partitionPath;
    private final String riderValue;

    @JsonCreator
    public RecordIdentifier(@JsonProperty("recordKey") String recordKey,
                            @JsonProperty("partitionPath") String partitionPath,
                            @JsonProperty("orderingVal") String orderingVal,
                            @JsonProperty("riderValue") String riderValue) {
      this.recordKey = recordKey;
      this.orderingVal = orderingVal;
      this.partitionPath = partitionPath;
      this.riderValue = riderValue;
    }

    public static RecordIdentifier fromTripTestPayload(RawTripTestPayload payload) {
      try {
        String recordKey = payload.getRowKey();
        String partitionPath = payload.getPartitionPath();
        String orderingVal = payload.getOrderingValue().toString();
        String riderValue = payload.getJsonDataAsMap().getOrDefault("rider", "").toString();
        return new RecordIdentifier(recordKey, partitionPath, orderingVal, riderValue);
      } catch (IOException ex) {
        throw new HoodieIOException("Failed to parse payload", ex);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RecordIdentifier that = (RecordIdentifier) o;
      return Objects.equals(recordKey, that.recordKey)
          && Objects.equals(orderingVal, that.orderingVal)
          && Objects.equals(partitionPath, that.partitionPath)
          && Objects.equals(riderValue, that.riderValue);
    }

    @Override
    public int hashCode() {
      return Objects.hash(recordKey, orderingVal, partitionPath, riderValue);
    }

    public String getRecordKey() {
      return recordKey;
    }

    public String getOrderingVal() {
      return orderingVal;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    public String getRiderValue() {
      return riderValue;
    }
  }
}
