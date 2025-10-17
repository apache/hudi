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

package org.apache.hudi.common.table.log.block;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockContentLocation;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieAvroDataBlock {
  // Record key field of the test data
  private static final String RECORD_KEY_FIELD = HoodieRecord.RECORD_KEY_METADATA_FIELD;

  private static final String SCHEMA_STRING = "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"RandomRecord\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"" + RECORD_KEY_FIELD + "\", \"type\": \"string\"},\n"
          + "    {\"name\": \"value\", \"type\": \"double\"}\n"
          + "  ]\n"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  // Just to avoid errors when the HoodieAvroDataBlock constructor wraps HoodieLogBlockContentLocation as an Option and encounters a null value
  private static final HoodieLogBlockContentLocation NULL_BLOCK_CONTENT_LOCATION = new HoodieLogBlockContentLocation(null, null, 0, 0, 0);

  /**
   * Tests the getRecordIterator method of HoodieAvroDataBlock when records are present.
   *
   * @param useKeyFilter          Whether to use a key filter.
   * @param keyFilterFullKeyMatch Whether the key filter should match the full key.
   * @throws IOException If an I/O error occurs during the test.
   */
  @ParameterizedTest
  @CsvSource({
      "true, true",
      "true, false",
      "false, true",
      "false, false"
  })
  public void testGetRecordIteratorWithRecordsPresent(boolean useKeyFilter, boolean keyFilterFullKeyMatch) throws IOException {
    List<HoodieRecord> records = generateRandomHoodieRecords(SCHEMA, 1000);
    HoodieAvroDataBlock block = createHoodieAvroDataBlock(SCHEMA, records);

    // Expect records for using key filter
    List<HoodieRecord> recordsForFilter = selectRandomRecords(records, keyFilterFullKeyMatch);
    List<String> keysForFilter = recordsForFilter.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList());

    // Get record iterator
    try (ClosableIterator<HoodieRecord<Object>> recordIterator = useKeyFilter
        ? block.getRecordIterator(keysForFilter, keyFilterFullKeyMatch, HoodieRecord.HoodieRecordType.AVRO)
        : block.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
      List<HoodieRecord> retrievedRecords = new ArrayList<>();
      recordIterator.forEachRemaining(retrievedRecords::add);

      verifyRecords(useKeyFilter ? recordsForFilter : records, retrievedRecords);
    }
  }

  /**
   * Tests the getRecordIterator method of HoodieAvroDataBlock using content directly.
   *
   * @param useKeyFilter          Whether to use a key filter.
   * @param keyFilterFullKeyMatch Whether the key filter should match the full key.
   * @throws IOException If an I/O error occurs during the test.
   */
  @ParameterizedTest
  @CsvSource({
      "true, true",
      "true, false",
      "false, true",
      "false, false"
  })
  public void testGetRecordIteratorWithContent(boolean useKeyFilter, boolean keyFilterFullKeyMatch) throws IOException {
    List<HoodieRecord> records = generateRandomHoodieRecords(SCHEMA, 1000);
    byte[] content = createHoodieAvroDataBlockContent(SCHEMA, records);
    Option<byte[]> contentOpt = Option.of(content);
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, SCHEMA.toString());

    // Expect records for using key filter
    List<HoodieRecord> recordsForFilter = selectRandomRecords(records, keyFilterFullKeyMatch);
    List<String> keysForFilter = recordsForFilter.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList());

    // With log block content
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(() -> null, contentOpt, false,
        NULL_BLOCK_CONTENT_LOCATION, Option.of(SCHEMA), header, new HashMap<>(), RECORD_KEY_FIELD);

    // Get record iterator
    try (ClosableIterator<HoodieRecord<Object>> recordIterator = useKeyFilter
        ? dataBlock.getRecordIterator(keysForFilter, keyFilterFullKeyMatch, HoodieRecord.HoodieRecordType.AVRO)
        : dataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
      List<HoodieRecord> retrievedRecords = new ArrayList<>();
      recordIterator.forEachRemaining(retrievedRecords::add);

      verifyRecords(useKeyFilter ? recordsForFilter : records, retrievedRecords);
    }
  }

  /**
   * Tests the getRecordIterator method of HoodieAvroDataBlock with input stream (read block lazily).
   *
   * @param useStreamingRead    Whether to use streaming read.
   * @param useKeyFilter        Whether to use a key filter.
   * @param keyFilterFullKeyMatch Whether the key filter should match the full key.
   * @throws IOException If an I/O error occurs during the test.
   */
  @ParameterizedTest
  @CsvSource({
      "true, true, true",
      "true, true, false",
      "true, false, true",
      "true, false, false",
      "false, true, true",
      "false, true, false",
      "false, false, true",
      "false, false, false"
  })
  public void testGetRecordIteratorWithInputStream(boolean useStreamingRead, boolean useKeyFilter, boolean keyFilterFullKeyMatch) throws IOException {
    List<HoodieRecord> records = generateRandomHoodieRecords(SCHEMA, 1000);
    byte[] blockContent = createHoodieAvroDataBlockContent(SCHEMA, records);
    SeekableDataInputStream inputStream = createSeekableDataInputStream(blockContent);
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.SCHEMA, SCHEMA.toString());
    HoodieLogBlockContentLocation logBlockContentLocation = new HoodieLogBlockContentLocation(null, null, 0, blockContent.length, blockContent.length);
    int bufferSize = useStreamingRead ? 100 : 0; // bytes

    // Expect records for using key filter
    List<HoodieRecord> recordsForFilter = selectRandomRecords(records, keyFilterFullKeyMatch);
    List<String> keysForFilter = recordsForFilter.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList());

    // With input stream
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(() -> inputStream, Option.empty(), true,
        logBlockContentLocation, Option.of(SCHEMA), header, new HashMap<>(), RECORD_KEY_FIELD);

    // Get record iterator
    try (ClosableIterator<HoodieRecord<Object>> recordIterator = useKeyFilter
        ? dataBlock.getRecordIterator(keysForFilter, keyFilterFullKeyMatch, HoodieRecord.HoodieRecordType.AVRO, bufferSize)
        : dataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO, bufferSize)) {
      List<HoodieRecord> retrievedRecords = new ArrayList<>();
      recordIterator.forEachRemaining(retrievedRecords::add);

      verifyRecords(useKeyFilter ? recordsForFilter : records, retrievedRecords);
    }
  }

  /**
   * Tests the getRecordIterator method of HoodieAvroDataBlock with streaming read.
   *
   * @param bufferSize The size of the buffer to be used for the streaming read.
   * @throws IOException If an I/O error occurs during the test.
   */
  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -2, -1, 0, 1, 2, 100, 200, 300, 1024, Integer.MAX_VALUE})
  public void testGetRecordIteratorWithStreamingRead(int bufferSize) throws IOException {
    List<HoodieRecord> records = generateRandomHoodieRecords(SCHEMA, 1000);
    byte[] blockContent = createHoodieAvroDataBlockContent(SCHEMA, records);
    SeekableDataInputStream inputStream = createSeekableDataInputStream(blockContent);
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.SCHEMA, SCHEMA.toString());
    HoodieLogBlockContentLocation logBlockContentLocation = new HoodieLogBlockContentLocation(null, null, 0, blockContent.length, blockContent.length);

    // Create an instance of HoodieAvroDataBlock with streaming read
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(() -> inputStream, Option.empty(), true,
        logBlockContentLocation, Option.of(SCHEMA), header, new HashMap<>(), RECORD_KEY_FIELD);

    // Get record iterator
    try (ClosableIterator<HoodieRecord<Object>> recordIterator = dataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO, bufferSize)) {
      List<HoodieRecord> retrievedRecords = new ArrayList<>();
      recordIterator.forEachRemaining(retrievedRecords::add);

      verifyRecords(records, retrievedRecords);
    }
  }

  /**
   * Tests the getRecordIterator method of HoodieAvroDataBlock with empty content.
   *
   */
  @Test
  public void testGetRecordIteratorWithEmptyContent() {
    byte[] content = new byte[0]; // Simulate empty content bytes
    Option<byte[]> contentOption = Option.of(content);
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.SCHEMA, SCHEMA.toString());

    // With content
    HoodieAvroDataBlock avroDataBlock = new HoodieAvroDataBlock(() -> null, contentOption, false,
        NULL_BLOCK_CONTENT_LOCATION, Option.of(SCHEMA), header, new HashMap<>(), RECORD_KEY_FIELD);

    // Mock the behavior of deserializeRecords to throw IOException
    assertThrows(EOFException.class, () -> avroDataBlock.deserializeRecords(content, HoodieRecord.HoodieRecordType.AVRO, false));

    // Call getRecordIterator and verify the behavior
    assertThrows(HoodieIOException.class, () -> avroDataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO));
  }

  /**
   * Tests the getRecordIterator method of HoodieAvroDataBlock with empty content and input stream.
   *
   */
  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -2, -1, 0, 1, 2, 100, 200, 300, 1024, Integer.MAX_VALUE})
  public void testGetRecordIteratorWithEmptyContentAndInputStream(int bufferSize) throws IOException {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.SCHEMA, SCHEMA.toString());

    // empty input stream supplier and empty content
    HoodieAvroDataBlock avroDataBlock = new HoodieAvroDataBlock(() -> null, Option.empty(), true,
        NULL_BLOCK_CONTENT_LOCATION, Option.of(SCHEMA), header, new HashMap<>(), RECORD_KEY_FIELD);

    assertThrows(NullPointerException.class, () -> avroDataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO, bufferSize));
  }

  /**
   * Generates a list of random HoodieRecord objects based on the provided Avro schema.
   *
   * @param schema      The Avro schema to use for generating the records.
   * @param recordCount The number of records to generate.
   * @return A list of HoodieRecord objects.
   */
  private static List<HoodieRecord> generateRandomHoodieRecords(Schema schema, int recordCount) {
    List<HoodieRecord> records = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < recordCount; i++) {
      GenericRecord record = new GenericData.Record(schema);
      String recordKey = "key_" + i;
      record.put("_hoodie_record_key", "key_" + i);
      record.put("value", random.nextDouble());
      records.add(new HoodieAvroIndexedRecord(new HoodieKey(recordKey, ""), record));
    }

    return records;
  }

  /**
   * Creates a HoodieAvroDataBlock object from the provided schema and list of HoodieRecord objects.
   *
   * @param schema  The Avro schema used to define the structure of the records.
   * @param records The list of HoodieRecord objects to be included in the data block.
   * @return A HoodieAvroDataBlock object containing the provided records and schema.
   */
  private static HoodieAvroDataBlock createHoodieAvroDataBlock(Schema schema, List<HoodieRecord> records) {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.SCHEMA, schema.toString());

    return new HoodieAvroDataBlock(records, header, RECORD_KEY_FIELD);
  }

  /**
   * Creates the content bytes of a HoodieAvroDataBlock from the provided schema and list of HoodieRecord objects.
   *
   * @param schema  The Avro schema used to define the structure of the records.
   * @param records The list of HoodieRecord objects to be included in the data block.
   * @return A byte array representing the content of the HoodieAvroDataBlock.
   * @throws IOException If an I/O error occurs while generating the content bytes.
   */
  private static byte[] createHoodieAvroDataBlockContent(Schema schema, List<HoodieRecord> records) throws IOException {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.SCHEMA, schema.toString());

    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getConf()).thenReturn(mock(StorageConfiguration.class));
    return new HoodieAvroDataBlock(records, header, RECORD_KEY_FIELD).getContentBytes(storage).toByteArray();
  }

  /**
   * Creates a SeekableDataInputStream from the provided byte array content.
   *
   * @param content The byte array containing the data to be read.
   * @return A SeekableDataInputStream that allows seeking within the provided byte array content.
   * @throws IOException If an I/O error occurs while creating the SeekableDataInputStream.
   */
  private static SeekableDataInputStream createSeekableDataInputStream(byte[] content) throws IOException {
    return new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(content));
  }

  /**
   * Selects a random subset of HoodieRecord objects from the provided list.
   *
   * @param records The list of HoodieRecord objects to sample from.
   * @param fullKey A boolean flag indicating whether to match records by full key or by prefix.
   *                If true, records are matched by full key; if false, records are matched by key prefix.
   * @return A list of randomly selected HoodieRecord objects.
   */
  private static List<HoodieRecord> selectRandomRecords(List<HoodieRecord> records, boolean fullKey) {
    Set<String> keys = new Random()
        .ints(records.size() / 4, 0, records.size())
        .mapToObj(records::get)
        .map(r -> r.getRecordKey(SCHEMA, RECORD_KEY_FIELD))
        .collect(Collectors.toSet());

    // simulate KeyFilter matching logic
    return records.stream()
        .filter(r -> fullKey
            ? keys.contains(r.getRecordKey(SCHEMA, RECORD_KEY_FIELD))
            : keys.stream().anyMatch(r.getRecordKey(SCHEMA, RECORD_KEY_FIELD)::startsWith))
        .collect(Collectors.toList());
  }

  /**
   * Verifies that the actual records match the expected records.
   * This method converts the HoodieRecord objects to GenericRecord objects, sorts them,
   * and then compares the sorted lists for equality.
   *
   * @param expectedRecords The list of expected HoodieRecord objects.
   * @param actualRecords The list of actual HoodieRecord objects to verify.
   * @throws AssertionError If the record counts do not match or if any record data mismatches.
   */
  private static void verifyRecords(List<HoodieRecord> expectedRecords, List<HoodieRecord> actualRecords) {
    List<GenericRecord> expectedGenericRecords = convertAndSortRecords(expectedRecords);
    List<GenericRecord> actualGenericRecords = convertAndSortRecords(actualRecords);

    assertEquals(expectedGenericRecords.size(), actualGenericRecords.size(), "Record count mismatch");
    for (int i = 0; i < expectedGenericRecords.size(); i++) {
      assertEquals(expectedGenericRecords, actualGenericRecords, "Record data mismatch");
    }
  }

  private static List<GenericRecord> convertAndSortRecords(List<HoodieRecord> hoodieRecords) {
    return hoodieRecords.stream()
            .map(HoodieRecord::getData)
            .map(r -> (GenericRecord) r)
            .sorted(Comparator.comparing(r -> r.get(RECORD_KEY_FIELD).toString()))
            .collect(Collectors.toList());
  }
}
