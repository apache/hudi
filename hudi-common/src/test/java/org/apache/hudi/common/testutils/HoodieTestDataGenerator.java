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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.CollectionUtils;
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
import org.apache.avro.generic.IndexedRecord;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
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
  public static final String[] OPERATIONS = {"i", "u", "d"};

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

  public static final String EXTENDED_LOGICAL_TYPES_SCHEMA_V6 = "{\"name\":\"ts_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
      + "{\"name\":\"ts_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
      + "{\"name\":\"local_ts_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}},"
      + "{\"name\":\"local_ts_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}},"
      + "{\"name\":\"event_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
      + "{\"name\":\"dec_fixed_small\",\"type\":{\"type\":\"fixed\",\"name\":\"decFixedSmall\",\"size\":3,\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}},"
      + "{\"name\":\"dec_fixed_large\",\"type\":{\"type\":\"fixed\",\"name\":\"decFixedLarge\",\"size\":8,\"logicalType\":\"decimal\",\"precision\":18,\"scale\":9}},";

  public static final String EXTENDED_LOGICAL_TYPES_SCHEMA = "{\"name\":\"ts_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
          + "{\"name\":\"ts_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
          + "{\"name\":\"local_ts_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}},"
          + "{\"name\":\"local_ts_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}},"
          + "{\"name\":\"event_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
          + "{\"name\":\"dec_plain_large\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":10}},"
          + "{\"name\":\"dec_fixed_small\",\"type\":{\"type\":\"fixed\",\"name\":\"decFixedSmall\",\"size\":3,\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}},"
          + "{\"name\":\"dec_fixed_large\",\"type\":{\"type\":\"fixed\",\"name\":\"decFixedLarge\",\"size\":8,\"logicalType\":\"decimal\",\"precision\":18,\"scale\":9}},";

  public static final String EXTENDED_LOGICAL_TYPES_SCHEMA_NO_LTS = "{\"name\":\"ts_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
      + "{\"name\":\"ts_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
      + "{\"name\":\"event_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
      + "{\"name\":\"dec_plain_large\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":10}},"
      + "{\"name\":\"dec_fixed_small\",\"type\":{\"type\":\"fixed\",\"name\":\"decFixedSmall\",\"size\":3,\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}},"
      + "{\"name\":\"dec_fixed_large\",\"type\":{\"type\":\"fixed\",\"name\":\"decFixedLarge\",\"size\":8,\"logicalType\":\"decimal\",\"precision\":18,\"scale\":9}},";

  public static final String EXTRA_COL_SCHEMA1 = "{\"name\": \"extra_column1\", \"type\": [\"null\", \"string\"], \"default\": null },";
  public static final String EXTRA_COL_SCHEMA2 = "{\"name\": \"extra_column2\", \"type\": [\"null\", \"string\"], \"default\": null},";
  public static final String EXTRA_COL_SCHEMA_FOR_AWS_DMS_PAYLOAD = "{\"name\": \"Op\", \"type\": [\"null\", \"string\"], \"default\": null},";
  public static final String EXTRA_COL_SCHEMA_FOR_POSTGRES_PAYLOAD = "{\"name\": \"_event_lsn\", \"type\": [\"null\", \"long\"], \"default\": null},";
  public static final String TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA
          + TIP_NESTED_SCHEMA + EXTRA_COL_SCHEMA_FOR_AWS_DMS_PAYLOAD + EXTRA_COL_SCHEMA_FOR_POSTGRES_PAYLOAD + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_EXAMPLE_SCHEMA =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_EXAMPLE_SCHEMA_EVOLVED_1 =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_COL_SCHEMA1 + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_EXAMPLE_SCHEMA_EVOLVED_2 =
      TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_COL_SCHEMA2 + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_FLATTENED_SCHEMA =
      TRIP_SCHEMA_PREFIX + FARE_FLATTENED_SCHEMA + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_LOGICAL_TYPES_SCHEMA_V6 =
      TRIP_SCHEMA_PREFIX + EXTENDED_LOGICAL_TYPES_SCHEMA_V6 + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_LOGICAL_TYPES_SCHEMA =
      TRIP_SCHEMA_PREFIX + EXTENDED_LOGICAL_TYPES_SCHEMA + TRIP_SCHEMA_SUFFIX;
  public static final String TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS =
      TRIP_SCHEMA_PREFIX + EXTENDED_LOGICAL_TYPES_SCHEMA_NO_LTS + TRIP_SCHEMA_SUFFIX;


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
  public static final Schema AVRO_SCHEMA_WITH_SPECIFIC_COLUMNS = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS);
  public static final Schema NESTED_AVRO_SCHEMA = new Schema.Parser().parse(TRIP_NESTED_EXAMPLE_SCHEMA);
  public static final Schema AVRO_SCHEMA_WITH_METADATA_FIELDS =
      HoodieAvroUtils.addMetadataFields(AVRO_SCHEMA);
  public static final Schema AVRO_SHORT_TRIP_SCHEMA = new Schema.Parser().parse(SHORT_TRIP_SCHEMA);
  public static final Schema AVRO_TRIP_ENCODED_DECIMAL_SCHEMA = new Schema.Parser().parse(TRIP_ENCODED_DECIMAL_SCHEMA);
  public static final Schema AVRO_TRIP_LOGICAL_TYPES_SCHEMA = new Schema.Parser().parse(TRIP_LOGICAL_TYPES_SCHEMA);
  public static final Schema AVRO_TRIP_LOGICAL_TYPES_SCHEMA_V6 = new Schema.Parser().parse(TRIP_LOGICAL_TYPES_SCHEMA_V6);
  public static final Schema AVRO_TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS = new Schema.Parser().parse(TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS);
  public static final Schema AVRO_TRIP_SCHEMA = new Schema.Parser().parse(TRIP_SCHEMA);
  public static final Schema FLATTENED_AVRO_SCHEMA = new Schema.Parser().parse(TRIP_FLATTENED_SCHEMA);

  private final Random rand;

  //Maintains all the existing keys schema wise
  private final Map<String, Map<Integer, KeyPartition>> existingKeysBySchema;
  private final String[] partitionPaths;
  //maintains the count of existing keys schema wise
  private Map<String, Integer> numKeysBySchema;
  private Option<Schema> extendedSchema = Option.empty();

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

  public IndexedRecord generateRandomValueAsPerSchema(String schemaStr, HoodieKey key, String commitTime, boolean isFlattened, long timestamp) throws IOException {
    return generateRandomValueAsPerSchema(schemaStr, key, commitTime, isFlattened, false, timestamp);
  }

  public IndexedRecord generateRandomValueAsPerSchema(String schemaStr, HoodieKey key, String commitTime, boolean isFlattened, boolean isDelete, long timestamp) throws IOException {
    if (!isDelete) {
      if (TRIP_FLATTENED_SCHEMA.equals(schemaStr)) {
        return generateRandomValue(key, commitTime, true, timestamp);
      } else if (TRIP_EXAMPLE_SCHEMA.equals(schemaStr)) {
        return generateRandomValue(key, commitTime, isFlattened, timestamp);
      } else if (TRIP_ENCODED_DECIMAL_SCHEMA.equals(schemaStr)) {
        return generatePayloadForTripEncodedDecimalSchema(key, commitTime, timestamp);
      } else if (TRIP_SCHEMA.equals(schemaStr)) {
        return generatePayloadForTripSchema(key, commitTime, timestamp);
      } else if (SHORT_TRIP_SCHEMA.equals(schemaStr)) {
        return generatePayloadForShortTripSchema(key, commitTime, timestamp);
      } else if (TRIP_NESTED_EXAMPLE_SCHEMA.equals(schemaStr)) {
        return generateNestedExampleRandomValue(key, commitTime, timestamp);
      } else if (TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS.equals(schemaStr)) {
        return generateRandomValueWithColumnRequired(key, commitTime);
      } else if (TRIP_LOGICAL_TYPES_SCHEMA.equals(schemaStr)) {
        return generatePayloadForLogicalTypesSchema(key, commitTime, false, timestamp);
      } else if (TRIP_LOGICAL_TYPES_SCHEMA_V6.equals(schemaStr)) {
        return generatePayloadForLogicalTypesSchemaV6(key, commitTime, false, timestamp);
      } else if (TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS.equals(schemaStr)) {
        return generatePayloadForLogicalTypesSchemaNoLTS(key, commitTime, false, timestamp);
      }
    } else {
      if (TRIP_EXAMPLE_SCHEMA.equals(schemaStr)) {
        return generateRandomDeleteValue(key, commitTime, timestamp);
      } else if (TRIP_LOGICAL_TYPES_SCHEMA.equals(schemaStr)) {
        return generatePayloadForLogicalTypesSchema(key, commitTime, true, timestamp);
      } else if (TRIP_LOGICAL_TYPES_SCHEMA_V6.equals(schemaStr)) {
        return generatePayloadForLogicalTypesSchemaV6(key, commitTime, true, timestamp);
      } else if (TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS.equals(schemaStr)) {
        return generatePayloadForLogicalTypesSchemaNoLTS(key, commitTime, true, timestamp);
      }
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
  public IndexedRecord generateRandomValue(HoodieKey key, String instantTime) {
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
  private IndexedRecord generateRandomValue(HoodieKey key, String instantTime, boolean isFlattened) {
    return generateRandomValue(key, instantTime, isFlattened, System.currentTimeMillis());
  }

  private IndexedRecord generateNestedExampleRandomValue(HoodieKey key, String instantTime) {
    return generateNestedExampleRandomValue(key, instantTime, System.currentTimeMillis());
  }

  private IndexedRecord generateRandomValue(HoodieKey key, String instantTime, boolean isFlattened, long timestamp) {
    return generateGenericRecord(
        key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, timestamp,
        false, isFlattened);
  }

  private IndexedRecord generateNestedExampleRandomValue(HoodieKey key, String instantTime, long ts) {
    return generateNestedExampleGenericRecord(
        key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, ts,
        false);
  }

  /**
   * Generates a new avro record with TRIP_ENCODED_DECIMAL_SCHEMA, retaining the key if optionally provided.
   */
  public IndexedRecord generatePayloadForTripEncodedDecimalSchema(HoodieKey key, String commitTime, long timestamp) {
    return generateRecordForTripEncodedDecimalSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, timestamp);
  }

  public IndexedRecord generatePayloadForLogicalTypesSchemaNoLTS(HoodieKey key, String commitTime, boolean isDelete, long timestamp) {
    return generateRecordForTripLogicalTypesSchema(key, "rider-" + commitTime, "driver-" + commitTime, timestamp, isDelete, false, false);
  }

  public IndexedRecord generatePayloadForLogicalTypesSchema(HoodieKey key, String commitTime, boolean isDelete, long timestamp) {
    return generateRecordForTripLogicalTypesSchema(key, "rider-" + commitTime, "driver-" + commitTime, timestamp, isDelete, false, true);
  }

  public IndexedRecord generatePayloadForLogicalTypesSchemaV6(HoodieKey key, String commitTime, boolean isDelete, long timestamp) {
    return generateRecordForTripLogicalTypesSchema(key, "rider-" + commitTime, "driver-" + commitTime, timestamp, isDelete, true, true);
  }

  /**
   * Generates a new avro record with TRIP_SCHEMA, retaining the key if optionally provided.
   */
  public IndexedRecord generatePayloadForTripSchema(HoodieKey key, String commitTime, long timestamp) {
    return generateRecordForTripSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, timestamp);
  }

  public IndexedRecord generatePayloadForShortTripSchema(HoodieKey key, String commitTime, long timestamp) {
    return generateRecordForShortTripSchema(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, timestamp);
  }

  /**
   * Generates a new avro record of the above schema format for a delete.
   */
  private IndexedRecord generateRandomDeleteValue(HoodieKey key, String instantTime, long timestamp) throws IOException {
    return generateGenericRecord(key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, timestamp,
        true, false);
  }

  /**
   * Generates a new avro record of the above schema format, retaining the key if optionally provided.
   */
  private IndexedRecord generateAvroPayload(HoodieKey key, String instantTime, long timestamp) {
    return generateGenericRecord(key.getRecordKey(), key.getPartitionPath(), "rider-" + instantTime, "driver-" + instantTime, timestamp);
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
    GenericRecord fareRecord = new GenericData.Record(extendedSchema.orElse(AVRO_SCHEMA).getField("fare").schema());
    fareRecord.put("amount", rand.nextDouble() * 100);
    fareRecord.put("currency", "USD");
    if (extendedSchema.isPresent()) {
      generateCustomValues(fareRecord, "customFare");
    }
    rec.put("fare", fareRecord);
  }

  /**
   * Populate "Op" column.
   */
  private void generateOpColumnValue(GenericRecord rec) {
    // No delete records; otherwise, it is hard to data validation.
    int index = rand.nextInt(2);
    rec.put("Op", OPERATIONS[index]);
  }

  /**
   * Populate "_event_lsn" column.
   */
  private void generateEventLSNValue(GenericRecord rec) {
    rec.put("_event_lsn", rand.nextLong());
  }

  /**
   * Populate rec with values for TIP_NESTED_SCHEMA
   */
  private void generateTipNestedValues(GenericRecord rec) {
    // TODO [HUDI-9603] remove this check
    if (extendedSchema.isPresent()) {
      if (extendedSchema.get().getField("tip_history") == null) {
        return;
      }
    }
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
    GenericRecord rec = new GenericData.Record(extendedSchema.orElseGet(() -> isFlattened ? FLATTENED_AVRO_SCHEMA : AVRO_SCHEMA));
    generateTripPrefixValues(rec, rowKey, partitionPath, riderName, driverName, timestamp);
    if (isFlattened) {
      generateFareFlattenedValues(rec);
    } else {
      generateExtraSchemaValues(rec);
      generateMapTypeValues(rec);
      generateFareNestedValues(rec);
      generateTipNestedValues(rec);
    }
    generateCustomValues(rec, "customField");
    generateTripSuffixValues(rec, isDeleteRecord);
    return rec;
  }

  public IndexedRecord generateRandomValueWithColumnRequired(HoodieKey key,
                                                             String instantTime) throws IOException {
    GenericRecord rec = new GenericData.Record(AVRO_SCHEMA_WITH_SPECIFIC_COLUMNS);
    generateTripPrefixValues(
        rec,
        key.getRecordKey(),
        key.getPartitionPath(),
        "rider_" + instantTime,
        "driver_" + instantTime,
        0);
    generateExtraSchemaValues(rec);
    generateMapTypeValues(rec);
    generateFareNestedValues(rec);
    generateTipNestedValues(rec);
    generateOpColumnValue(rec);
    generateEventLSNValue(rec);
    generateTripSuffixValues(rec, false);
    return rec;
  }

  /**
   * Generate record conforming to TRIP_NESTED_EXAMPLE_SCHEMA
   */
  public GenericRecord generateNestedExampleGenericRecord(String rowKey, String partitionPath, String riderName, String driverName,
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

  public GenericRecord generateRecordForTripLogicalTypesSchema(HoodieKey key, String riderName, String driverName,
                                                               long timestamp, boolean isDeleteRecord, boolean v6, boolean hasLTS) {
    GenericRecord rec;
    if (!hasLTS) {
      rec = new GenericData.Record(AVRO_TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS);
    } else if (v6) {
      rec = new GenericData.Record(AVRO_TRIP_LOGICAL_TYPES_SCHEMA_V6);
    } else {
      rec = new GenericData.Record(AVRO_TRIP_LOGICAL_TYPES_SCHEMA);
    }
    generateTripPrefixValues(rec, key.getRecordKey(), key.getPartitionPath(), riderName, driverName, timestamp);

    int hash = key.getRecordKey().hashCode();
    boolean above = (hash & 1) == 0; // half above, half below threshold

    // -------------------
    // Threshold definitions
    // -------------------
    Instant tsMillisThreshold = Instant.parse("2020-01-01T00:00:00Z");
    Instant tsMicrosThreshold = Instant.parse("2020-06-01T12:00:00Z");

    //LocalTime timeMillisThreshold = LocalTime.of(12, 0, 0);  // noon
    //LocalTime timeMicrosThreshold = LocalTime.of(6, 0, 0);   // 6 AM

    Instant localTsMillisThreshold = ZonedDateTime.of(
        2015, 5, 20, 12, 34, 56, 0, ZoneOffset.UTC).toInstant();
    Instant localTsMicrosThreshold = ZonedDateTime.of(
        2017, 7, 7, 7, 7, 7, 0, ZoneOffset.UTC).toInstant();

    LocalDate dateThreshold = LocalDate.of(2000, 1, 1);

    // -------------------
    // Assign edge values
    // -------------------

    // ts_millis
    long tsMillisBase = tsMillisThreshold.toEpochMilli();
    rec.put("ts_millis", above ? tsMillisBase + 1 : tsMillisBase - 1);

    // ts_micros
    long tsMicrosBase = TimeUnit.SECONDS.toMicros(tsMicrosThreshold.getEpochSecond()) + tsMicrosThreshold.getNano() / 1_000L;
    rec.put("ts_micros", above ? tsMicrosBase + 1 : tsMicrosBase - 1);

    // time_millis
    //int timeMillisBase = (int) TimeUnit.SECONDS.toMillis(timeMillisThreshold.toSecondOfDay());
    //rec.put("time_millis", above ? timeMillisBase + 1 : timeMillisBase - 1);

    // time_micros
    //long timeMicrosBase = TimeUnit.SECONDS.toMicros(timeMicrosThreshold.toSecondOfDay());
    //rec.put("time_micros", above ? timeMicrosBase + 1 : timeMicrosBase - 1);

    if (hasLTS) {
      // local_ts_millis
      long localTsMillisBase = localTsMillisThreshold.toEpochMilli();
      rec.put("local_ts_millis", above ? localTsMillisBase + 1 : localTsMillisBase - 1);

      // local_ts_micros
      long localTsMicrosBase = TimeUnit.SECONDS.toMicros(localTsMicrosThreshold.getEpochSecond()) + localTsMicrosThreshold.getNano() / 1_000L;
      rec.put("local_ts_micros", above ? localTsMicrosBase + 1 : localTsMicrosBase - 1);
    }

    // event_date
    int eventDateBase = (int) dateThreshold.toEpochDay();
    rec.put("event_date", above ? eventDateBase + 1 : eventDateBase - 1);


    // -------------------
    // Decimal thresholds
    // -------------------
    BigDecimal decPlainLargeThreshold = new BigDecimal("1234567890.0987654321"); // precision=20, scale=10

    BigDecimal decFixedSmallThreshold = new BigDecimal("543.21"); // precision=5, scale=2
    BigDecimal decFixedLargeThreshold = new BigDecimal("987654321.123456789"); // precision=18, scale=9

    // Increment for just-above/below threshold = smallest possible unit for that scale
    BigDecimal incSmallScale2 = new BigDecimal("0.01");
    BigDecimal incLargeScale9 = new BigDecimal("0.000000001");
    BigDecimal incLargeScale10 = new BigDecimal("0.0000000001");

    // Assign thresholded decimals
    if (!v6) {
      rec.put("dec_plain_large", ByteBuffer.wrap((above
          ? decPlainLargeThreshold.add(incLargeScale10)
          : decPlainLargeThreshold.subtract(incLargeScale10)).unscaledValue().toByteArray()));
    }

    Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
    Schema decFixedSmallSchema = AVRO_TRIP_LOGICAL_TYPES_SCHEMA.getField("dec_fixed_small").schema();
    rec.put("dec_fixed_small", decimalConversions.toFixed(above
        ? decFixedSmallThreshold.add(incSmallScale2)
        : decFixedSmallThreshold.subtract(incSmallScale2), decFixedSmallSchema, LogicalTypes.decimal(5, 2)));

    Schema decFixedLargeSchema = AVRO_TRIP_LOGICAL_TYPES_SCHEMA.getField("dec_fixed_large").schema();
    rec.put("dec_fixed_large", decimalConversions.toFixed(above
        ? decFixedLargeThreshold.add(incLargeScale9)
        : decFixedLargeThreshold.subtract(incLargeScale9), decFixedLargeSchema, LogicalTypes.decimal(18, 9)));
    generateTripSuffixValues(rec, isDeleteRecord);
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
    return generateInsertsStream(commitTime, n, false, schemaStr, System.currentTimeMillis()).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateInsertsAsPerSchema(String commitTime, Integer n, String schemaStr, long timestamp) {
    return generateInsertsStream(commitTime, n, false, schemaStr, timestamp).collect(Collectors.toList());
  }

  /**
   * Generates new inserts for TRIP_EXAMPLE_SCHEMA with nested schema, uniformly across the partition paths above.
   * It also updates the list of existing keys.
   */
  public List<HoodieRecord> generateInserts(String instantTime, Integer n) {
    return generateInserts(instantTime, n, false);
  }

  public List<HoodieRecord> generateInserts(String instantTime, Integer n, long timestamp) {
    return generateInsertsStream(instantTime, n, false, TRIP_EXAMPLE_SCHEMA, timestamp).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateInsertsNestedExample(String instantTime, Integer n) {
    return generateInsertsStream(instantTime, n, false, TRIP_NESTED_EXAMPLE_SCHEMA, System.currentTimeMillis()).collect(Collectors.toList());
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
    return generateInsertsStream(instantTime, n, isFlattened, isFlattened ? TRIP_FLATTENED_SCHEMA : TRIP_EXAMPLE_SCHEMA, System.currentTimeMillis()).collect(Collectors.toList());
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(String commitTime, Integer n, boolean isFlattened, String schemaStr, long timestamp) {
    return generateInsertsStream(commitTime, n, isFlattened, schemaStr, false, timestamp);
  }

  public List<HoodieRecord> generateInsertsContainsAllPartitions(String instantTime, Integer n) {
    if (n < partitionPaths.length) {
      throw new HoodieIOException("n must greater then partitionPaths length");
    }
    long timestamp = System.currentTimeMillis();
    return generateInsertsStream(instantTime,  n, false, TRIP_EXAMPLE_SCHEMA, true, timestamp).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateInsertsForPartitionPerSchema(String instantTime, Integer n, String partition, String schemaStr) {
    long timestamp = System.currentTimeMillis();
    return generateInsertsStream(instantTime,  n, false, schemaStr, false, () -> partition, () -> genPseudoRandomUUID(rand).toString(), timestamp).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateInsertsForPartition(String instantTime, Integer n, String partition) {
    long timestamp = System.currentTimeMillis();
    return generateInsertsStream(instantTime,  n, false, TRIP_EXAMPLE_SCHEMA, false, () -> partition, () -> genPseudoRandomUUID(rand).toString(), timestamp).collect(Collectors.toList());
  }

  public Stream<HoodieRecord> generateInsertsStream(String commitTime, Integer n, boolean isFlattened, String schemaStr, boolean containsAllPartitions, long timestamp) {
    AtomicInteger partitionIndex = new AtomicInteger(0);
    return generateInsertsStream(commitTime, n, isFlattened, schemaStr, containsAllPartitions,
        () -> {
          // round robin to ensure we generate inserts for all partition paths
          String partitionToUse = partitionPaths[partitionIndex.get()];
          partitionIndex.set((partitionIndex.get() + 1) % partitionPaths.length);
          return partitionToUse;
        },
        () -> genPseudoRandomUUID(rand).toString(),
        timestamp);
  }

  /**
   * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
   */
  public Stream<HoodieRecord> generateInsertsStream(String instantTime, Integer n, boolean isFlattened, String schemaStr, boolean containsAllPartitions,
                                                    Supplier<String> partitionPathSupplier, Supplier<String> recordKeySupplier, long timestamp) {
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
        return new HoodieAvroIndexedRecord(key, generateRandomValueAsPerSchema(schemaStr, key, instantTime, isFlattened, timestamp),
            null, Option.of(Collections.singletonMap("InputRecordCount_1506582000", "2")));
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

  public List<HoodieRecord> generateSameKeyInserts(String instantTime, List<HoodieRecord> origin) {
    List<HoodieRecord> copy = new ArrayList<>();
    for (HoodieRecord r : origin) {
      HoodieKey key = r.getKey();
      HoodieRecord record = new HoodieAvroIndexedRecord(key, generateRandomValue(key, instantTime));
      copy.add(record);
    }
    return copy;
  }

  public List<HoodieRecord> generateUpdatesWithHoodieAvroPayload(String instantTime, List<HoodieRecord> baseRecords) {
    List<HoodieRecord> updates = new ArrayList<>();
    long timestamp = System.currentTimeMillis();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = new HoodieAvroIndexedRecord(baseRecord.getKey(), generateAvroPayload(baseRecord.getKey(), instantTime, timestamp));
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

  public HoodieRecord generateDeleteRecord(HoodieRecord existingRecord) {
    HoodieKey key = existingRecord.getKey();
    return generateDeleteRecord(key);
  }

  public HoodieRecord generateDeleteRecord(HoodieKey key) {
    return new HoodieEmptyRecord(key, HoodieRecord.HoodieRecordType.AVRO);
  }

  public HoodieRecord generateUpdateRecord(HoodieKey key, String instantTime) throws IOException {
    return new HoodieAvroIndexedRecord(key, generateRandomValue(key, instantTime));
  }

  public HoodieRecord generateUpdateRecordWithTimestamp(HoodieKey key, String instantTime, long timestamp) {
    return new HoodieAvroIndexedRecord(key, generateRandomValue(key, instantTime, false, timestamp));
  }

  public List<HoodieRecord> generateUpdates(String instantTime, List<HoodieRecord> baseRecords) throws IOException {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      HoodieRecord record = generateUpdateRecord(baseRecord.getKey(), instantTime);
      updates.add(record);
    }
    return updates;
  }

  public List<HoodieRecord> generateUpdatesWithTimestamp(String instantTime, List<HoodieRecord> baseRecords, long timestamp) {
    List<HoodieRecord> updates = new ArrayList<>();
    for (HoodieRecord baseRecord : baseRecords) {
      updates.add(generateUpdateRecordWithTimestamp(baseRecord.getKey(), instantTime, timestamp));
    }
    return updates;
  }

  public List<HoodieRecord> generateUpdatesForDifferentPartition(String instantTime, List<HoodieRecord> baseRecords, long timestamp, String newPartition) {
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

  public List<HoodieRecord> generateUniqueUpdates(String instantTime, Integer n, long timestamp) {
    return generateUniqueUpdatesStream(instantTime, n, TRIP_EXAMPLE_SCHEMA, timestamp).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueUpdates(String instantTime, Integer n, String schemaStr) {
    return generateUniqueUpdatesStream(instantTime, n, schemaStr).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueUpdates(String instantTime, Integer n, String schemaStr, long timestamp) {
    return generateUniqueUpdatesStream(instantTime, n, schemaStr, timestamp).collect(Collectors.toList());
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
    long timestamp = System.currentTimeMillis();
    return generateUniqueUpdatesStream(instantTime, n, schemaStr, timestamp);
  }

  public Stream<HoodieRecord> generateUniqueUpdatesStream(String instantTime, Integer n, String schemaStr, long timestamp) {
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
        return new HoodieAvroIndexedRecord(kp.key, generateRandomValueAsPerSchema(schemaStr, kp.key, instantTime, false, timestamp));
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
   * @param updatePartition whether to update the partition path while generating delete record
   * @param timestamp timestamp to set in the record for the ordering value
   * @return stream of hoodie records for delete
   */
  private Stream<HoodieRecord> generateUniqueDeleteRecordStream(String instantTime, Integer n, boolean updatePartition, long timestamp) {
    return generateUniqueDeleteRecordStream(instantTime, n, updatePartition, TRIP_EXAMPLE_SCHEMA, timestamp);
  }

  public Stream<HoodieRecord> generateUniqueDeleteRecordStream(String instantTime, Integer n, boolean updatePartition, String schemaStr, long timestamp) {
    final Set<KeyPartition> used = new HashSet<>();
    Map<Integer, KeyPartition> existingKeys = existingKeysBySchema.get(schemaStr);
    Integer numExistingKeys = numKeysBySchema.get(schemaStr);
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
      HoodieKey key = kp.key;
      if (updatePartition) {
        String updatedPartitionPath = Arrays.stream(partitionPaths).filter(p -> !p.equals(kp.partitionPath))
            .findAny().orElseThrow(() -> new HoodieIOException("No other partition path found to update"));
        key = new HoodieKey(key.getRecordKey(), updatedPartitionPath);
      }
      try {
        result.add(new HoodieAvroIndexedRecord(key, generateRandomValueAsPerSchema(schemaStr, kp.key, instantTime, false, true, timestamp)));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }
    numKeysBySchema.put(schemaStr, numExistingKeys);
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
    return generateUniqueDeleteRecordStream(instantTime, n, false, System.currentTimeMillis()).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueDeleteRecordsWithUpdatedPartition(String instantTime, Integer n) {
    return generateUniqueDeleteRecordStream(instantTime, n, true, System.currentTimeMillis()).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueDeleteRecords(String instantTime, Integer n, long timestamp) {
    return generateUniqueDeleteRecordStream(instantTime, n, false, timestamp).collect(Collectors.toList());
  }

  public List<HoodieRecord> generateUniqueDeleteRecordsWithUpdatedPartition(String instantTime, Integer n, long timestamp) {
    return generateUniqueDeleteRecordStream(instantTime, n, true, timestamp).collect(Collectors.toList());
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

  public List<String> getExistingKeys() {
    return getExistingKeys(TRIP_EXAMPLE_SCHEMA);
  }

  public List<String> getExistingKeys(String schemaStr) {
    return existingKeysBySchema.get(schemaStr).values().stream().map(kp -> kp.key.getRecordKey()).collect(Collectors.toList());
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
    long anchorTs = 1234567890000L;
    // NOTE: To provide for certainty and not generate overly random dates, we will limit
    //       dispersion to be w/in +/- 3 days from the anchor date
    return anchorTs + r.nextInt(259200000);
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

    public static RecordIdentifier clone(RecordIdentifier toClone, String orderingVal) {
      return new RecordIdentifier(toClone.recordKey, toClone.partitionPath, orderingVal, toClone.riderValue);
    }

    public static RecordIdentifier fromTripTestPayload(HoodieAvroIndexedRecord record, String[] orderingFields) {
      String recordKey = record.getRecordKey();
      String partitionPath = record.getPartitionPath();
      Comparable orderingValue = record.getOrderingValue(record.getData().getSchema(), CollectionUtils.emptyProps(), orderingFields);
      String orderingValStr = orderingValue.toString();
      String riderValue = ((GenericRecord) record.getData()).hasField("rider") ? ((GenericRecord) record.getData()).get("rider").toString() : "";
      return new RecordIdentifier(recordKey, partitionPath, orderingValStr, riderValue);
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

    @Override
    public String toString() {
      return "RowKey: " + recordKey + ", PartitionPath: " + partitionPath
          + ", OrderingVal: " + orderingVal + ", RiderValue: " + riderValue;
    }
  }

  public static class SchemaEvolutionConfigs {
    public Schema schema = AVRO_SCHEMA;
    public boolean nestedSupport = true;
    public boolean mapSupport = true;
    public boolean arraySupport = true;
    public boolean addNewFieldSupport = true;
    // TODO: [HUDI-9603] Flink 1.18 array values incorrect in fg reader test
    public boolean anyArraySupport = true;

    // Int
    public boolean intToLongSupport = true;
    public boolean intToFloatSupport = true;
    public boolean intToDoubleSupport = true;
    public boolean intToStringSupport = true;

    // Long
    public boolean longToFloatSupport = true;
    public boolean longToDoubleSupport = true;
    public boolean longToStringSupport = true;

    // Float
    public boolean floatToDoubleSupport = true;
    public boolean floatToStringSupport = true;

    // Double
    public boolean doubleToStringSupport = true;

    // String
    public boolean stringToBytesSupport = true;

    // Bytes
    public boolean bytesToStringSupport = true;
  }

  private enum SchemaEvolutionTypePromotionCase {
    INT_TO_INT(Schema.Type.INT, Schema.Type.INT, config -> true),
    INT_TO_LONG(Schema.Type.INT, Schema.Type.LONG, config -> config.intToLongSupport),
    INT_TO_FLOAT(Schema.Type.INT, Schema.Type.FLOAT, config -> config.intToFloatSupport),
    INT_TO_DOUBLE(Schema.Type.INT, Schema.Type.DOUBLE, config -> config.intToDoubleSupport),
    INT_TO_STRING(Schema.Type.INT, Schema.Type.STRING, config -> config.intToStringSupport),
    LONG_TO_LONG(Schema.Type.LONG, Schema.Type.LONG, config -> true),
    LONG_TO_FLOAT(Schema.Type.LONG, Schema.Type.FLOAT, config -> config.longToFloatSupport),
    LONG_TO_DOUBLE(Schema.Type.LONG, Schema.Type.DOUBLE, config -> config.longToDoubleSupport),
    LONG_TO_STRING(Schema.Type.LONG, Schema.Type.STRING, config -> config.longToStringSupport),
    FLOAT_TO_FLOAT(Schema.Type.FLOAT, Schema.Type.FLOAT, config -> true),
    FLOAT_TO_DOUBLE(Schema.Type.FLOAT, Schema.Type.DOUBLE, config -> config.floatToDoubleSupport),
    FLOAT_TO_STRING(Schema.Type.FLOAT, Schema.Type.STRING, config -> config.floatToStringSupport),
    DOUBLE_TO_DOUBLE(Schema.Type.DOUBLE, Schema.Type.DOUBLE, config -> true),
    DOUBLE_TO_STRING(Schema.Type.DOUBLE, Schema.Type.STRING, config -> config.doubleToStringSupport),
    STRING_TO_STRING(Schema.Type.STRING, Schema.Type.STRING, config -> true),
    STRING_TO_BYTES(Schema.Type.STRING, Schema.Type.BYTES, config -> config.stringToBytesSupport),
    BYTES_TO_BYTES(Schema.Type.BYTES, Schema.Type.BYTES, config -> true),
    BYTES_TO_STRING(Schema.Type.BYTES, Schema.Type.STRING, config -> config.bytesToStringSupport);

    public final Schema.Type before;
    public final Schema.Type after;
    public final Predicate<SchemaEvolutionConfigs> isEnabled;

    SchemaEvolutionTypePromotionCase(Schema.Type before, Schema.Type after, Predicate<SchemaEvolutionConfigs> isEnabled) {
      this.before = before;
      this.after = after;
      this.isEnabled = isEnabled;
    }
  }

  public void extendSchema(SchemaEvolutionConfigs configs, boolean isBefore) {
    List<Schema.Type> baseFields = new ArrayList<>();
    for (SchemaEvolutionTypePromotionCase evolution : SchemaEvolutionTypePromotionCase.values()) {
      if (evolution.isEnabled.test(configs)) {
        baseFields.add(isBefore ? evolution.before : evolution.after);
      }
    }

    // Add new field if we are testing adding new fields
    if (!isBefore && configs.addNewFieldSupport) {
      baseFields.add(Schema.Type.BOOLEAN);
    }

    this.extendedSchema = Option.of(generateExtendedSchema(configs, new ArrayList<>(baseFields)));
  }

  public void extendSchemaBeforeEvolution(SchemaEvolutionConfigs configs) {
    extendSchema(configs, true);
  }

  public void extendSchemaAfterEvolution(SchemaEvolutionConfigs configs) {
    extendSchema(configs, false);
  }

  public Schema getExtendedSchema() {
    return extendedSchema.orElseThrow(IllegalArgumentException::new);
  }

  private static Schema generateExtendedSchema(SchemaEvolutionConfigs configs, List<Schema.Type> baseFields) {
    return generateExtendedSchema(configs.schema, configs, baseFields, "customField", true);
  }

  private static Schema generateExtendedSchema(Schema baseSchema, SchemaEvolutionConfigs configs, List<Schema.Type> baseFields, String fieldPrefix, boolean toplevel) {
    List<Schema.Field> fields =  baseSchema.getFields();
    List<Schema.Field> finalFields = new ArrayList<>(fields.size() + baseFields.size());
    boolean addedFields = false;
    for (Schema.Field field : fields) {
      if (configs.nestedSupport && field.name().equals("fare") && field.schema().getType() == Schema.Type.RECORD) {
        finalFields.add(new Schema.Field(field.name(), generateExtendedSchema(field.schema(), configs, baseFields, "customFare", false), field.doc(), field.defaultVal()));
      } else if (configs.anyArraySupport || !field.name().equals("tip_history")) {
        //TODO: [HUDI-9603] remove the if condition when the issue is fixed
        if (field.name().equals("_hoodie_is_deleted")) {
          addedFields = true;
          addFields(configs, finalFields, baseFields, fieldPrefix, baseSchema.getNamespace(), toplevel);
        }
        finalFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
      }
    }
    if (!addedFields) {
      addFields(configs, finalFields, baseFields, fieldPrefix, baseSchema.getNamespace(), toplevel);
    }
    Schema finalSchema = Schema.createRecord(baseSchema.getName(), baseSchema.getDoc(),
        baseSchema.getNamespace(), baseSchema.isError());
    finalSchema.setFields(finalFields);
    return finalSchema;
  }

  private static void addFields(SchemaEvolutionConfigs configs, List<Schema.Field> finalFields, List<Schema.Type> baseFields, String fieldPrefix, String namespace, boolean toplevel) {
    if (toplevel) {
      if (configs.mapSupport) {
        List<Schema.Field> mapFields = new ArrayList<>(baseFields.size());
        addFieldsHelper(mapFields, baseFields, fieldPrefix + "Map");
        finalFields.add(new Schema.Field(fieldPrefix + "Map", Schema.createMap(Schema.createRecord("customMapRecord", "", namespace, false, mapFields)), "", null));
      }

      if (configs.arraySupport) {
        List<Schema.Field> arrayFields = new ArrayList<>(baseFields.size());
        addFieldsHelper(arrayFields, baseFields, fieldPrefix + "Array");
        finalFields.add(new Schema.Field(fieldPrefix + "Array", Schema.createArray(Schema.createRecord("customArrayRecord", "", namespace, false, arrayFields)), "", null));
      }
    }
    addFieldsHelper(finalFields, baseFields, fieldPrefix);
  }

  private static void addFieldsHelper(List<Schema.Field> finalFields, List<Schema.Type> baseFields, String fieldPrefix) {
    for (int i = 0; i < baseFields.size(); i++) {
      if (baseFields.get(i) == Schema.Type.BOOLEAN) {
        // boolean fields are added fields
        finalFields.add(new Schema.Field(fieldPrefix + i, AvroSchemaUtils.createNullableSchema(Schema.Type.BOOLEAN), "", null));
      } else {
        finalFields.add(new Schema.Field(fieldPrefix + i, Schema.create(baseFields.get(i)), "", null));
      }
    }
  }

  private void generateCustomValues(GenericRecord rec, String customPrefix) {
    for (Schema.Field field : rec.getSchema().getFields()) {
      if (field.name().startsWith(customPrefix)) {
        switch (field.schema().getType()) {
          case INT:
            rec.put(field.name(), rand.nextInt());
            break;
          case LONG:
            rec.put(field.name(), rand.nextLong());
            break;
          case FLOAT:
            rec.put(field.name(), rand.nextFloat());
            break;
          case DOUBLE:
            rec.put(field.name(), rand.nextDouble());
            break;
          case STRING:
            rec.put(field.name(), genPseudoRandomUUID(rand).toString());
            break;
          case BYTES:
            rec.put(field.name(), ByteBuffer.wrap(getUTF8Bytes(genPseudoRandomUUID(rand).toString())));
            break;
          case UNION:
            if (!AvroSchemaUtils.resolveNullableSchema(field.schema()).getType().equals(Schema.Type.BOOLEAN)) {
              throw new IllegalStateException("Union should only be boolean");
            }
            rec.put(field.name(), rand.nextBoolean());
            break;
          case BOOLEAN:
            rec.put(field.name(), rand.nextBoolean());
            break;
          case MAP:
            rec.put(field.name(), genMap(field.schema(), field.name()));
            break;
          case ARRAY:
            rec.put(field.name(), genArray(field.schema(), field.name()));
            break;
          default:
            throw new UnsupportedOperationException("Unsupported type: " + field.schema().getType());
        }
      }
    }
  }

  private GenericArray<GenericRecord> genArray(Schema arraySchema, String customPrefix) {
    GenericArray<GenericRecord> customArray = new GenericData.Array<>(1, arraySchema);
    Schema arrayElementSchema = arraySchema.getElementType();
    GenericRecord customRecord = new GenericData.Record(arrayElementSchema);
    generateCustomValues(customRecord, customPrefix);
    customArray.add(customRecord);
    return customArray;
  }

  private Map<String,GenericRecord> genMap(Schema mapSchema, String customPrefix) {
    Schema mapElementSchema = mapSchema.getValueType();
    GenericRecord customRecord = new GenericData.Record(mapElementSchema);
    generateCustomValues(customRecord, customPrefix);
    return Collections.singletonMap("customMapKey", customRecord);
  }

  public static List<String> recordsToStrings(List<HoodieRecord> records) {
    return records.stream().map(HoodieTestDataGenerator::recordToString).filter(Option::isPresent).map(Option::get)
        .collect(Collectors.toList());
  }

  public static Option<String> recordToString(HoodieRecord record) {
    try {
      String str = ((GenericRecord) record.getData()).toString();
      // Remove the last } bracket
      str = str.substring(0, str.length() - 1);
      return Option.of(str + ", \"partition\": \"" + record.getPartitionPath() + "\"}");
    } catch (Exception e) {
      return Option.empty();
    }
  }

  public static List<String> deleteRecordsToStrings(List<HoodieKey> records) {
    return records.stream().map(record -> "{\"_row_key\": \"" + record.getRecordKey() + "\",\"partition\": \"" + record.getPartitionPath() + "\"}")
        .collect(Collectors.toList());
  }
}
