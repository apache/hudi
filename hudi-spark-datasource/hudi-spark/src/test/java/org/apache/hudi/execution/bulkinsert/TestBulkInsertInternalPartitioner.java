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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.FlatLists;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.HoodieUTF8StringFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBulkInsertInternalPartitioner extends HoodieClientTestBase implements Serializable {
  private static final Comparator<HoodieRecord<? extends HoodieRecordPayload>> KEY_COMPARATOR =
      Comparator.comparing(o -> (o.getPartitionPath() + "+" + o.getRecordKey()));
  private static final HoodieUTF8StringFactory UTF8STRING_FACTORY =
      SparkAdapterSupport$.MODULE$.sparkAdapter().getUTF8StringFactory();

  public static JavaRDD<HoodieRecord> generateTestRecordsForBulkInsert(JavaSparkContext jsc) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    // RDD partition 1
    List<HoodieRecord> records1 = dataGenerator.generateInserts("0", 100);
    // RDD partition 2
    List<HoodieRecord> records2 = dataGenerator.generateInserts("0", 150);
    return jsc.parallelize(records1, 1).union(jsc.parallelize(records2, 1));
  }

  public static JavaRDD<HoodieRecord> generateTestRecordsForBulkInsert(JavaSparkContext jsc, int count) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGenerator.generateInserts("0", count);
    return jsc.parallelize(records, 1);
  }

  public static Map<String, Long> generateExpectedPartitionNumRecords(JavaRDD<HoodieRecord> records) {
    return records.map(record -> record.getPartitionPath()).countByValue();
  }

  private static JavaRDD<HoodieRecord> generateTripleTestRecordsForBulkInsert(JavaSparkContext jsc) {
    return generateTestRecordsForBulkInsert(jsc).union(generateTestRecordsForBulkInsert(jsc))
        .union(generateTestRecordsForBulkInsert(jsc));
  }

  private static Stream<Arguments> configParams() {
    // Parameters:
    //   BulkInsertSortMode sortMode,
    //   boolean isTablePartitioned,
    //   boolean enforceNumOutputPartitions,
    //   boolean isGloballySorted,
    //   boolean isLocallySorted,
    //   boolean populateMetaFields
    Object[][] data = new Object[][] {
        {BulkInsertSortMode.GLOBAL_SORT, true, true, true, true, true},
        {BulkInsertSortMode.PARTITION_SORT, true, true, false, true, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION, true, true, false, false, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION, false, true, false, false, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT, true, true, false, false, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT, false, true, false, false, true},
        {BulkInsertSortMode.NONE, true, true, false, false, true},
        {BulkInsertSortMode.NONE, true, false, false, false, true},
        {BulkInsertSortMode.GLOBAL_SORT, true, true, true, true, false},
        {BulkInsertSortMode.PARTITION_SORT, true, true, false, true, false},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION, true, true, false, false, false},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT, true, true, false, false, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private void verifyRecordAscendingOrder(List<HoodieRecord<? extends HoodieRecordPayload>> records,
      Option<Comparator<HoodieRecord<? extends HoodieRecordPayload>>> comparator) {
    List<HoodieRecord<? extends HoodieRecordPayload>> expectedRecords = new ArrayList<>(records);
    Collections.sort(expectedRecords, comparator.orElse(KEY_COMPARATOR));
    assertEquals(expectedRecords, records);
  }

  private void testBulkInsertInternalPartitioner(BulkInsertPartitioner partitioner,
      JavaRDD<HoodieRecord> records,
      boolean enforceNumOutputPartitions,
      boolean isGloballySorted,
      boolean isLocallySorted,
      Map<String, Long> expectedPartitionNumRecords,
      boolean populateMetaFields) {
    testBulkInsertInternalPartitioner(
        partitioner,
        records,
        enforceNumOutputPartitions,
        isGloballySorted,
        isLocallySorted,
        expectedPartitionNumRecords,
        Option.empty(),
        populateMetaFields);
  }

  private void testBulkInsertInternalPartitioner(BulkInsertPartitioner partitioner,
      JavaRDD<HoodieRecord> records,
      boolean enforceNumOutputPartitions,
      boolean isGloballySorted,
      boolean isLocallySorted,
      Map<String, Long> expectedPartitionNumRecords,
      Option<Comparator<HoodieRecord<? extends HoodieRecordPayload>>> comparator,
      boolean populateMetaFields) {
    int numPartitions = 2;
    if (!populateMetaFields) {
      assertThrows(HoodieException.class, () -> partitioner.repartitionRecords(records, numPartitions));
      return;
    }
    JavaRDD<HoodieRecord<? extends HoodieRecordPayload>> actualRecords =
        (JavaRDD<HoodieRecord<? extends HoodieRecordPayload>>) partitioner.repartitionRecords(records, numPartitions);
    assertEquals(
        enforceNumOutputPartitions ? numPartitions : records.getNumPartitions(),
        actualRecords.getNumPartitions());
    List<HoodieRecord<? extends HoodieRecordPayload>> collectedActualRecords = actualRecords.collect();
    if (isGloballySorted) {
      // Verify global order
      verifyRecordAscendingOrder(collectedActualRecords, comparator);
    } else if (isLocallySorted) {
      // Verify local order
      actualRecords.mapPartitions(partition -> {
        List<HoodieRecord<? extends HoodieRecordPayload>> partitionRecords = new ArrayList<>();
        partition.forEachRemaining(partitionRecords::add);
        verifyRecordAscendingOrder(partitionRecords, comparator);
        return Collections.emptyList().iterator();
      }).collect();
    }

    // Verify number of records per partition path
    Map<String, Long> actualPartitionNumRecords = new HashMap<>();
    for (HoodieRecord record : collectedActualRecords) {
      String partitionPath = record.getPartitionPath();
      actualPartitionNumRecords.put(partitionPath,
          actualPartitionNumRecords.getOrDefault(partitionPath, 0L) + 1);
    }
    assertEquals(expectedPartitionNumRecords, actualPartitionNumRecords);
  }

  @ParameterizedTest(name = "[{index}] {0} isTablePartitioned={1} enforceNumOutputPartitions={2}")
  @MethodSource("configParams")
  public void testBulkInsertInternalPartitioner(BulkInsertSortMode sortMode,
      boolean isTablePartitioned,
      boolean enforceNumOutputPartitions,
      boolean isGloballySorted,
      boolean isLocallySorted,
      boolean populateMetaFields) {
    JavaRDD<HoodieRecord> records1 = generateTestRecordsForBulkInsert(jsc);
    JavaRDD<HoodieRecord> records2 = generateTripleTestRecordsForBulkInsert(jsc);

    HoodieWriteConfig config = HoodieWriteConfig
        .newBuilder()
        .withPath("/")
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withBulkInsertSortMode(sortMode.name())
        .withPopulateMetaFields(populateMetaFields)
        .build();

    testBulkInsertInternalPartitioner(
        BulkInsertInternalPartitionerFactory.get(config, isTablePartitioned, enforceNumOutputPartitions),
        records1,
        enforceNumOutputPartitions,
        isGloballySorted,
        isLocallySorted,
        generateExpectedPartitionNumRecords(records1),
        populateMetaFields);
    testBulkInsertInternalPartitioner(
        BulkInsertInternalPartitionerFactory.get(config, isTablePartitioned, enforceNumOutputPartitions),
        records2,
        enforceNumOutputPartitions,
        isGloballySorted,
        isLocallySorted,
        generateExpectedPartitionNumRecords(records2),
        populateMetaFields);
  }

  @Test
  public void testCustomColumnSortPartitioner() {
    String sortColumnString = "begin_lat";
    HoodieWriteConfig config = HoodieWriteConfig
        .newBuilder()
        .withPath("/")
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withUserDefinedBulkInsertPartitionerClass(RDDCustomColumnsSortPartitioner.class.getName())
        .withUserDefinedBulkInsertPartitionerSortColumns(sortColumnString)
        .build();
    String[] sortColumns = sortColumnString.split(",");
    Comparator<HoodieRecord<? extends HoodieRecordPayload>> columnComparator = getCustomColumnComparator(HoodieTestDataGenerator.HOODIE_SCHEMA, true, sortColumns);

    JavaRDD<HoodieRecord> records1 = generateTestRecordsForBulkInsert(jsc);
    JavaRDD<HoodieRecord> records2 = generateTripleTestRecordsForBulkInsert(jsc);
    testBulkInsertInternalPartitioner(new RDDCustomColumnsSortPartitioner(sortColumns, HoodieTestDataGenerator.HOODIE_SCHEMA, config),
        records1, true, true, true, generateExpectedPartitionNumRecords(records1), Option.of(columnComparator), true);
    testBulkInsertInternalPartitioner(new RDDCustomColumnsSortPartitioner(sortColumns, HoodieTestDataGenerator.HOODIE_SCHEMA, config),
        records2, true, true, true, generateExpectedPartitionNumRecords(records2), Option.of(columnComparator), true);


    testBulkInsertInternalPartitioner(new RDDCustomColumnsSortPartitioner(config),
        records1, true, true, true, generateExpectedPartitionNumRecords(records1), Option.of(columnComparator), true);
    testBulkInsertInternalPartitioner(new RDDCustomColumnsSortPartitioner(config),
        records2, true, true, true, generateExpectedPartitionNumRecords(records2), Option.of(columnComparator), true);
  }

  /**
   * On the Spark clustering / bulk-insert record path a binary sort column arrives as a
   * raw byte[], which is not {@link Comparable}. {@code FlatLists.ofComparableArray} casts every
   * element to Comparable, so this threw "ClassCastException: [B cannot be cast to
   * java.lang.Comparable" and wedged every clustering attempt. {@code wrapArrayOfObjects} must wrap
   * byte[] into a Comparable that preserves the Avro path's byte-lexicographic (ByteBuffer) ordering:
   * signed bytes, shorter-prefix-first.
   */
  @Test
  public void testSortColumnsWithBinaryValueAreComparable() {
    // Before the fix each sortKeyOf(...) threw ClassCastException: [B cannot be cast to java.lang.Comparable.
    assertTrue(sortKeyOf(new byte[] {0x01, 0x02}).compareTo(sortKeyOf(new byte[] {0x01, 0x03})) < 0,
        "byte[] sort key must order like the Avro ByteBuffer path");
    assertTrue(sortKeyOf(new byte[] {0x01, 0x03}).compareTo(sortKeyOf(new byte[] {0x01, 0x02})) > 0);
    assertEquals(0, sortKeyOf(new byte[] {0x01, 0x02}).compareTo(sortKeyOf(new byte[] {0x01, 0x02})));
    // ByteBuffer.compareTo is signed, so 0x80 (-128) sorts before 0x7F (127); lock in the sign.
    assertTrue(sortKeyOf(new byte[] {(byte) 0x80}).compareTo(sortKeyOf(new byte[] {0x7F})) < 0,
        "0x80 must sort before 0x7F under the Avro path's signed byte ordering");
    // A shorter value that is a prefix of a longer one sorts first.
    assertTrue(sortKeyOf(new byte[] {0x01, 0x02}).compareTo(sortKeyOf(new byte[] {0x01, 0x02, 0x00})) < 0);
    // Empty binary sorts first.
    assertTrue(sortKeyOf(new byte[] {}).compareTo(sortKeyOf(new byte[] {0x00})) < 0);
  }

  private static FlatLists.ComparableList sortKeyOf(byte[] value) {
    return FlatLists.ofComparableArray(UTF8STRING_FACTORY.wrapArrayOfObjects(new Object[] {value}));
  }

  /**
   * The wrapped binary sort key must also survive the {@code sortBy} shuffle, not merely be
   * {@link Comparable} in-JVM. The shuffle key ({@link FlatLists.ComparableList}) is Kryo-serializable
   * and Hudi always runs the write path with Kryo ({@code HoodieSparkSqlWriter} rejects any other
   * {@code spark.serializer}), so this round-trips the key through Hudi's Kryo ({@link SerializationUtils})
   * and asserts the element stays a {@link ByteBuffer} with its ordering intact. Without the
   * {@code ByteBuffer.wrap} the key holds a raw byte[]: the deserialized element is not a ByteBuffer
   * (and comparison throws the same ClassCastException the shuffle would), so this test fails.
   */
  @Test
  public void testBinarySortKeySurvivesKryoRoundTrip() throws IOException {
    FlatLists.ComparableList lo = kryoRoundTrip(sortKeyOf(new byte[] {0x01, 0x02}));
    FlatLists.ComparableList hi = kryoRoundTrip(sortKeyOf(new byte[] {0x01, 0x03}));
    FlatLists.ComparableList neg = kryoRoundTrip(sortKeyOf(new byte[] {(byte) 0x80}));
    FlatLists.ComparableList pos = kryoRoundTrip(sortKeyOf(new byte[] {0x7F}));
    // Element type must survive Kryo serde — a raw byte[] (no fix) fails this assertion.
    assertTrue(lo.get(0) instanceof ByteBuffer,
        "binary sort key element must remain a ByteBuffer after the Kryo round-trip");
    // Ordering identity and relative order must survive the round-trip (position/limit preserved).
    assertEquals(0, lo.compareTo(kryoRoundTrip(sortKeyOf(new byte[] {0x01, 0x02}))),
        "equal keys must stay equal after the Kryo round-trip");
    assertTrue(lo.compareTo(hi) < 0, "relative ordering must survive the Kryo round-trip");
    assertTrue(neg.compareTo(pos) < 0,
        "signed byte ordering (0x80 < 0x7F) must survive the Kryo round-trip");
  }

  @SuppressWarnings("unchecked")
  private static FlatLists.ComparableList kryoRoundTrip(FlatLists.ComparableList key) throws IOException {
    return (FlatLists.ComparableList) SerializationUtils.deserialize(SerializationUtils.serialize(key));
  }

  private Comparator<HoodieRecord<? extends HoodieRecordPayload>> getCustomColumnComparator(HoodieSchema schema, boolean prependPartitionPath, String[] sortColumns) {
    Comparator<HoodieRecord<? extends HoodieRecordPayload>> comparator = Comparator.comparing(record -> {
      try {
        GenericRecord genericRecord = (GenericRecord) record.toIndexedRecord(schema, CollectionUtils.emptyProps()).get().getData();
        List<Object> keys = new ArrayList<>();
        if (prependPartitionPath) {
          keys.add(record.getPartitionPath());
        }
        for (String col : sortColumns) {
          keys.add(genericRecord.get(col));
        }
        return keys;
      } catch (IOException e) {
        throw new HoodieIOException("unable to read value for " + sortColumns);
      }
    }, (o1, o2) -> {
      FlatLists.ComparableList values1 = FlatLists.ofComparableArray(UTF8STRING_FACTORY.wrapArrayOfObjects(o1.toArray()));
      FlatLists.ComparableList values2 = FlatLists.ofComparableArray(UTF8STRING_FACTORY.wrapArrayOfObjects(o2.toArray()));
        return values1.compareTo(values2);
      }
    );

    return comparator;
  }
}
