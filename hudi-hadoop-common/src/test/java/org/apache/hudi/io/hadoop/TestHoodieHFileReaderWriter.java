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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.UTF8StringKey;
import org.apache.hudi.io.storage.HFileReaderFactory;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieNativeAvroHFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.bloom.BloomFilterFactory.createBloomFilter;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getHoodieSchemaFromResource;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.io.hfile.TestHFileReader.BOOTSTRAP_INDEX_HFILE_SUFFIX;
import static org.apache.hudi.io.hfile.TestHFileReader.COMPLEX_SCHEMA_HFILE_SUFFIX;
import static org.apache.hudi.io.hfile.TestHFileReader.SIMPLE_SCHEMA_HFILE_SUFFIX;
import static org.apache.hudi.io.hfile.TestHFileReader.readHFileFromResources;
import static org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_META_BLOCK;
import static org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase.KEY_MAX_RECORD;
import static org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase.KEY_MIN_RECORD;
import static org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase.SCHEMA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieHFileReaderWriter extends TestHoodieReaderWriterBase {
  protected static final int NUM_RECORDS_FIXTURE = 50;
  protected static final TypedProperties DEFAULT_PROPS = new TypedProperties();

  protected static Stream<Arguments> populateMetaFieldsAndTestAvroWithMeta() {
    return Arrays.stream(new Boolean[][] {
        {true, true},
        {false, true},
        {true, false},
        {false, false}
    }).map(Arguments::of);
  }

  @Override
  protected HoodieAvroFileReader createReader(
      HoodieStorage storage) throws Exception {
    return createReader(storage, true);
  }

  protected HoodieAvroHFileReaderImplBase createReader(HoodieStorage storage,
                                                       boolean useBloomFilter) throws Exception {
    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage).withProps(DEFAULT_PROPS)
        .withPath(getFilePath()).build();
    return HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory).path(getFilePath()).useBloomFilter(useBloomFilter).build();
  }

  protected HoodieAvroHFileReaderImplBase createHFileReader(HoodieStorage storage,
                                                            byte[] content,
                                                            boolean useBloomFilter) throws IOException {
    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage).withProps(DEFAULT_PROPS)
        .withContent(content).build();
    return HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory).path(getFilePath()).useBloomFilter(useBloomFilter).build();
  }

  protected void verifyHFileReader(byte[] content,
                                   String hfileName,
                                   boolean mayUseDefaultComparator,
                                   boolean useBloomFilter,
                                   int count) throws IOException {
    try (HoodieAvroHFileReaderImplBase hfileReader =
             createHFileReader(HoodieTestUtils.getStorage(hfileName), content, useBloomFilter)) {
      assertEquals(count, hfileReader.getTotalRecords());
    }
  }

  @Override
  protected HoodieAvroHFileWriter createWriter(
      Schema avroSchema, boolean populateMetaFields) throws Exception {
    String instantTime = "000";
    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), Boolean.toString(populateMetaFields));
    TaskContextSupplier mockTaskContextSupplier = mock(TaskContextSupplier.class);
    Supplier<Integer> partitionSupplier = mock(Supplier.class);
    when(mockTaskContextSupplier.getPartitionIdSupplier()).thenReturn(partitionSupplier);
    when(partitionSupplier.get()).thenReturn(10);

    return (HoodieAvroHFileWriter) HoodieFileWriterFactory.getFileWriter(
        instantTime, getFilePath(), storage, HoodieStorageConfig.newBuilder().fromProperties(props).build(), HoodieSchema.fromAvroSchema(avroSchema),
        mockTaskContextSupplier, HoodieRecord.HoodieRecordType.AVRO);
  }

  @Override
  protected StoragePath getFilePath() {
    return new StoragePath(tempDir.toString() + "/f1_1-0-1_000.hfile");
  }

  @Override
  protected void verifyMetadata(HoodieStorage storage) throws IOException {
    try (HoodieAvroFileReader reader = createReader(storage)) {
      assertEquals(NUM_RECORDS, reader.getTotalRecords());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void verifySchema(HoodieStorage storage, String schemaPath) throws IOException {
    try (HoodieAvroFileReader reader = createReader(storage)) {
      assertEquals(
          getHoodieSchemaFromResource(TestHoodieHFileReaderWriter.class, schemaPath),
          reader.getSchema());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @MethodSource("populateMetaFieldsAndTestAvroWithMeta")
  public void testWriteReadHFileWithMetaFields(boolean populateMetaFields, boolean testAvroWithMeta) throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchemaWithMetaFields.avsc");
    HoodieAvroHFileWriter writer = createWriter(schema.toAvroSchema(), populateMetaFields);
    List<String> keys = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new TreeMap<>();
    for (int i = 0; i < 100; i++) {
      GenericRecord record = new GenericData.Record(schema.toAvroSchema());
      String key = String.format("%s%04d", "key", i);
      record.put("_row_key", key);
      keys.add(key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);
      if (testAvroWithMeta) {
        // payload does not matter. GenericRecord passed in is what matters
        writer.writeAvroWithMetadata(
            new HoodieAvroRecord(new HoodieKey((String) record.get("_row_key"),
                Integer.toString((Integer) record.get("number"))),
                new EmptyHoodieRecordPayload()).getKey(), record);
        // only HoodieKey will be looked up from the 2nd arg(HoodieRecord).
      } else {
        writer.writeAvro(key, record);
      }
      recordMap.put(key, record);
    }
    writer.close();

    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    HoodieAvroHFileReaderImplBase hoodieHFileReader =
        (HoodieAvroHFileReaderImplBase) createReader(storage);
    List<IndexedRecord> records = HoodieAvroHFileReaderImplBase.readAllRecords(hoodieHFileReader);
    assertEquals(new ArrayList<>(recordMap.values()), records);

    hoodieHFileReader.close();

    for (int i = 0; i < 2; i++) {
      int randomRowstoFetch = 5 + RANDOM.nextInt(10);
      Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);

      List<String> rowsList = new ArrayList<>(rowsToFetch);
      Collections.sort(rowsList);

      List<GenericRecord> expectedRecords =
          rowsList.stream().map(recordMap::get).collect(Collectors.toList());

      hoodieHFileReader = (HoodieAvroHFileReaderImplBase) createReader(storage);
      List<GenericRecord> result =
          HoodieAvroHFileReaderImplBase.readRecords(hoodieHFileReader, rowsList).stream()
              .map(r -> (GenericRecord) r).collect(Collectors.toList());

      assertEquals(expectedRecords, result);

      result.forEach(entry -> {
        if (populateMetaFields && testAvroWithMeta) {
          assertNotNull(entry.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        } else {
          assertNull(entry.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        }
      });
      hoodieHFileReader.close();
    }
  }

  @Disabled("Disable the test with evolved schema for HFile since it's not supported")
  @ParameterizedTest
  @Override
  public void testWriteReadWithEvolvedSchema(String evolvedSchemaPath) throws Exception {
    // TODO(HUDI-3683): fix the schema evolution for HFile
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testReadHFileFormatRecords(boolean useBloomFilter) throws Exception {
    writeFileWithSimpleSchema();
    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    byte[] content = FileIOUtils.readAsByteArray(
        storage.open(getFilePath()), (int) storage.getPathInfo(getFilePath()).getLength());
    // Reading byte array in HFile format, without actual file path
    try (HoodieAvroHFileReaderImplBase hfileReader = createHFileReader(storage, content, useBloomFilter)) {
      HoodieSchema schema =
          getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
      assertEquals(NUM_RECORDS, hfileReader.getTotalRecords());
      verifySimpleRecords(hfileReader.getRecordIterator(schema));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBloomFilterInitializationInRecordIterator(boolean useBloomFilter) throws IOException {
    HFileReaderFactory readerFactory = mock(HFileReaderFactory.class);
    HFileReader hfileReader = mock(HFileReader.class);
    when(readerFactory.createHFileReader()).thenReturn(hfileReader);
    when(hfileReader.getMetaInfo(new UTF8StringKey(KEY_MIN_RECORD)))
        .thenReturn(Option.of(getUTF8Bytes("key00")));
    when(hfileReader.getMetaInfo(new UTF8StringKey(KEY_MAX_RECORD)))
        .thenReturn(Option.of(getUTF8Bytes("key99")));
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.STRING);
    when(hfileReader.getMetaInfo(new UTF8StringKey(SCHEMA_KEY)))
        .thenReturn(Option.of(getUTF8Bytes(schema.toString())));

    BloomFilter bloomFilter = createBloomFilter(100, 0.001, 1000, BloomFilterTypeCode.DYNAMIC_V0.name());
    when(hfileReader.getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK))
        .thenReturn(Option.of(ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString()))));
    when(hfileReader.getMetaInfo(new UTF8StringKey(KEY_BLOOM_FILTER_TYPE_CODE)))
        .thenReturn(Option.of(getUTF8Bytes(bloomFilter.getBloomFilterTypeCode().toString())));

    List<String> filterKeys = Collections.singletonList("key");
    try (HoodieNativeAvroHFileReader reader = HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory).path(new StoragePath("dummy")).useBloomFilter(useBloomFilter)
        .build()) {
      // These iterators should not use bloom filters in any case
      reader.getRecordIterator();
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getRecordIterator(schema);
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getRecordIterator(schema, schema);
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getRecordKeyIterator();
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getIndexedRecordIterator(schema, schema);
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getRecordsByKeyPrefixIterator(filterKeys);
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getRecordsByKeyPrefixIterator(filterKeys, schema);
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getIndexedRecordsByKeyPrefixIterator(filterKeys, schema);
      verify(hfileReader, never()).getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      // Only iterators filtering full keys should use bloom filters for filtering
      reader.getRecordsByKeysIterator(filterKeys);
      verify(hfileReader, useBloomFilter ? times(1) : never())
          .getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getRecordsByKeysIterator(filterKeys, schema);
      verify(hfileReader, useBloomFilter ? times(2) : never())
          .getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);

      reader.getIndexedRecordsByKeysIterator(filterKeys, schema);
      verify(hfileReader, useBloomFilter ? times(3) : never())
          .getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testReaderGetRecordIterator(boolean useBloomFilter) throws Exception {
    writeFileWithSimpleSchema();
    try (HoodieAvroHFileReaderImplBase hfileReader =
             createReader(HoodieTestUtils.getStorage(getFilePath()), useBloomFilter)) {
      List<String> keys =
          IntStream.concat(IntStream.range(10, 20), IntStream.range(40, NUM_RECORDS * 2))
              .mapToObj(i -> "key" + String.format("%02d", i)).collect(Collectors.toList());
      HoodieSchema schema =
          getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
      Iterator<HoodieRecord<IndexedRecord>> iterator =
          hfileReader.getRecordsByKeysIterator(keys, schema);

      List<Integer> expectedIds =
          IntStream.concat(IntStream.range(10, 20), IntStream.range(40, NUM_RECORDS))
              .boxed().collect(Collectors.toList());
      int index = 0;
      while (iterator.hasNext()) {
        GenericRecord record = (GenericRecord) iterator.next().getData();
        String key = "key" + String.format("%02d", expectedIds.get(index));
        assertEquals(key, record.get("_row_key").toString());
        assertEquals(Integer.toString(expectedIds.get(index)), record.get("time").toString());
        assertEquals(expectedIds.get(index), record.get("number"));
        index++;
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testReaderGetRecordIteratorByKeys(boolean useBloomFilter) throws Exception {
    writeFileWithSimpleSchema();
    HoodieSchema schema =
        getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    try (HoodieAvroHFileReaderImplBase hfileReader =
             createReader(HoodieTestUtils.getStorage(getFilePath()), useBloomFilter)) {
      List<String> keys = Collections.singletonList("key");
      Iterator<IndexedRecord> iterator =
          hfileReader.getIndexedRecordsByKeysIterator(keys, schema);

      List<GenericRecord> recordsByKeys =
          toStream(iterator).map(r -> (GenericRecord) r).collect(Collectors.toList());

      List<GenericRecord> allRecords = toStream(hfileReader.getRecordIterator())
          .map(r -> (GenericRecord) r.getData()).collect(Collectors.toList());

      // no entries should match since this is exact match.
      assertEquals(Collections.emptyList(), recordsByKeys);

      iterator =
          hfileReader.getEngineRecordsByKeysIterator(keys, schema);

      recordsByKeys =
          toStream(iterator).map(r -> (GenericRecord) r).collect(Collectors.toList());

      // no entries should match since this is exact match.
      assertEquals(Collections.emptyList(), recordsByKeys);

      // filter for "key00001, key05, key12, key24, key2, key31, key49, key61, key50". Valid entries should be matched.
      // key00001 should not match.
      // key2 : we don't have an exact match
      // key61 is greater than max key.
      // again, by the time we reach key50, cursor is at EOF. So no entries will be returned.
      List<GenericRecord> expectedKey1s = allRecords.stream().filter(entry -> (
          (entry.get("_row_key").toString()).contains("key05")
              || (entry.get("_row_key").toString()).contains("key12")
              || (entry.get("_row_key").toString()).contains("key24")
              || (entry.get("_row_key").toString()).contains("key31")
              || (entry.get("_row_key").toString()).contains("key49"))).collect(Collectors.toList());
      iterator =
          hfileReader.getIndexedRecordsByKeysIterator(
              Arrays.asList("key00001", "key05", "key12", "key24", "key31", "key49", "key61", "key50"),
              schema);
      recordsByKeys =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey1s, recordsByKeys);

      iterator =
          hfileReader.getEngineRecordsByKeysIterator(
              Arrays.asList("key00001", "key05", "key12", "key24", "key31", "key49", "key61", "key50"),
              schema);
      recordsByKeys =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey1s, recordsByKeys);
    }
    try (HoodieAvroHFileReaderImplBase hfileReader = getHFileReaderFromFixture(useBloomFilter)) {
      List<GenericRecord> allRecords = toStream(hfileReader.getRecordIterator())
          .map(r -> (GenericRecord) r.getData()).collect(Collectors.toList());
      Iterator<IndexedRecord> iterator = hfileReader.getIndexedRecordsByKeysIterator(
          Arrays.asList("key000000", "key000066-abcdefghij", "key001424-aa", "key001424-aaa",
              "key004958-abcdefghij", "key010769", "key019889-abcdefghij", "key030000"),
          schema);
      List<GenericRecord> actual =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      List<GenericRecord> expected = allRecords.stream().filter(entry -> (
          (entry.get("_row_key").toString()).equals("key000066-abcdefghij")
              || (entry.get("_row_key").toString()).equals("key004958-abcdefghij")
              || (entry.get("_row_key").toString()).equals("key019889-abcdefghij"))).collect(Collectors.toList());
      assertEquals(expected, actual);

      iterator = hfileReader.getEngineRecordsByKeysIterator(
          Arrays.asList("key000000", "key000066-abcdefghij", "key001424-aa", "key001424-aaa",
              "key004958-abcdefghij", "key010769", "key019889-abcdefghij", "key030000"),
          schema);
      actual =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expected, actual);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testReaderGetRecordIteratorByKeyPrefixes(boolean useBloomFilter) throws Exception {
    writeFileWithSimpleSchema();
    HoodieSchema schema =
        getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    try (HoodieAvroHFileReaderImplBase hfileReader =
             createReader(HoodieTestUtils.getStorage(getFilePath()), useBloomFilter)) {
      List<String> keyPrefixes = Collections.singletonList("key");
      Iterator<IndexedRecord> iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(keyPrefixes, schema);

      List<GenericRecord> recordsByPrefix =
          toStream(iterator).map(r -> (GenericRecord) r).collect(Collectors.toList());

      List<GenericRecord> allRecords = toStream(hfileReader.getRecordIterator())
          .map(r -> (GenericRecord) r.getData()).collect(Collectors.toList());

      assertEquals(allRecords, recordsByPrefix);

      // test getEngineRecordsByKeyPrefix
      iterator = hfileReader.getEngineRecordsByKeyPrefixIterator(keyPrefixes, schema);
      recordsByPrefix =
          toStream(iterator).map(r -> (GenericRecord) r).collect(Collectors.toList());
      assertEquals(allRecords, recordsByPrefix);

      // filter for "key1" : entries from key10 to key19 should be matched
      List<GenericRecord> expectedKey1s =
          allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key1"))
              .collect(Collectors.toList());
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Collections.singletonList("key1"),
              schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                  false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey1s, recordsByPrefix);

      // test getEngineRecordsByKeyPrefix
      iterator =
          hfileReader.getEngineRecordsByKeyPrefixIterator(Collections.singletonList("key1"),
              schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                  false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey1s, recordsByPrefix);

      // exact match
      List<GenericRecord> expectedKey25 =
          allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key25"))
              .collect(Collectors.toList());
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Collections.singletonList("key25"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey25, recordsByPrefix);

      iterator =
          hfileReader.getEngineRecordsByKeyPrefixIterator(Collections.singletonList("key25"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey25, recordsByPrefix);

      // no match. key prefix is beyond entries in file.
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Collections.singletonList("key99"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(Collections.emptyList(), recordsByPrefix);

      iterator =
          hfileReader.getEngineRecordsByKeyPrefixIterator(Collections.singletonList("key99"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(Collections.emptyList(), recordsByPrefix);

      // no match. but keyPrefix is in between the entries found in file.
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Collections.singletonList("key1234"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(Collections.emptyList(), recordsByPrefix);
      iterator =
          hfileReader.getEngineRecordsByKeysIterator(Collections.singletonList("key1234"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(Collections.emptyList(), recordsByPrefix);

      // filter for "key1", "key30" and "key60" : entries from 'key10 to key19' and 'key30' should be matched.
      List<GenericRecord> expectedKey50and1s =
          allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key1")
              || (entry.get("_row_key").toString()).contains("key30")).collect(Collectors.toList());
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Arrays.asList("key1", "key30", "key6"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey50and1s, recordsByPrefix);

      iterator =
          hfileReader.getEngineRecordsByKeyPrefixIterator(Arrays.asList("key1", "key30", "key6"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey50and1s, recordsByPrefix);

      // filter for "key50" and "key0" : entries from key50 and 'key00 to key09' should be matched.
      List<GenericRecord> expectedKey50and0s =
          allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key0")
              || (entry.get("_row_key").toString()).contains("key50")).collect(Collectors.toList());
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Arrays.asList("key0", "key50"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey50and0s, recordsByPrefix);
      iterator =
          hfileReader.getEngineRecordsByKeyPrefixIterator(Arrays.asList("key0", "key50"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey50and0s, recordsByPrefix);

      // filter for "key1" and "key0" : entries from 'key10 to key19' and 'key00 to key09' should be matched.
      List<GenericRecord> expectedKey1sand0s = allRecords.stream()
          .filter(entry -> (entry.get("_row_key").toString()).contains("key1")
              || (entry.get("_row_key").toString()).contains("key0"))
          .collect(Collectors.toList());
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Arrays.asList("key0", "key1"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      Collections.sort(recordsByPrefix, new Comparator<GenericRecord>() {
        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
          return o1.get("_row_key").toString().compareTo(o2.get("_row_key").toString());
        }
      });
      assertEquals(expectedKey1sand0s, recordsByPrefix);

      iterator =
          hfileReader.getEngineRecordsByKeyPrefixIterator(Arrays.asList("key0", "key1"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      Collections.sort(recordsByPrefix, new Comparator<GenericRecord>() {
        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
          return o1.get("_row_key").toString().compareTo(o2.get("_row_key").toString());
        }
      });
      assertEquals(expectedKey1sand0s, recordsByPrefix);

      // We expect the keys to be looked up in sorted order. If not, matching entries may not be returned.
      // key1 should have matching entries, but not key0.
      iterator =
          hfileReader.getIndexedRecordsByKeyPrefixIterator(Arrays.asList("key1", "key0"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      Collections.sort(recordsByPrefix, new Comparator<GenericRecord>() {
        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
          return o1.get("_row_key").toString().compareTo(o2.get("_row_key").toString());
        }
      });
      assertEquals(expectedKey1s, recordsByPrefix);

      iterator =
          hfileReader.getEngineRecordsByKeyPrefixIterator(Arrays.asList("key1", "key0"), schema);
      recordsByPrefix =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      Collections.sort(recordsByPrefix, new Comparator<GenericRecord>() {
        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
          return o1.get("_row_key").toString().compareTo(o2.get("_row_key").toString());
        }
      });
    }
    try (HoodieAvroHFileReaderImplBase hfileReader = getHFileReaderFromFixture(useBloomFilter)) {
      List<GenericRecord> allRecords = toStream(hfileReader.getRecordIterator())
          .map(r -> (GenericRecord) r.getData()).collect(Collectors.toList());
      Iterator<IndexedRecord> iterator = hfileReader.getIndexedRecordsByKeyPrefixIterator(
          Arrays.asList("key000000", "key000066-abcdefghij", "key001424-aa", "key001424-aaa",
              "key004958", "key010769", "key01988", "key030000"),
          schema);
      List<GenericRecord> actual =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      List<GenericRecord> expected = allRecords.stream().filter(entry -> (
          (entry.get("_row_key").toString()).contains("key000000")
              || (entry.get("_row_key").toString()).contains("key000066-abcdefghij")
              || (entry.get("_row_key").toString()).contains("key004958")
              || (entry.get("_row_key").toString()).contains("key010769")
              || (entry.get("_row_key").toString()).contains("key01988"))).collect(Collectors.toList());
      assertEquals(expected, actual);

      iterator = hfileReader.getEngineRecordsByKeyPrefixIterator(
          Arrays.asList("key000000", "key000066-abcdefghij", "key001424-aa", "key001424-aaa",
              "key004958", "key010769", "key01988", "key030000"),
          schema);
      actual =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expected, actual);

    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testReaderGetRecordIteratorByKeysWithBackwardSeek(boolean useBloomFilter) throws Exception {
    writeFileWithSimpleSchema();
    try (HoodieAvroHFileReaderImplBase hfileReader =
             createReader(HoodieTestUtils.getStorage(getFilePath()), useBloomFilter)) {
      HoodieSchema schema =
          getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
      // Filter for "key00001, key05, key24, key16, key31, key61".
      // Even though key16 exists, it's a backward seek not in order.
      // Our native HFile reader does not allow backward seek, and throws an exception
      // Note that backward seek is not expected to happen in production code
      try (ClosableIterator<IndexedRecord> iterator =
               hfileReader.getIndexedRecordsByKeysIterator(
                   Arrays.asList("key00001", "key05", "key24", "key16", "key31", "key61"),
                   schema)) {
        assertThrows(
            IllegalStateException.class,
            () -> StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .collect(Collectors.toList()));
      }

      try (ClosableIterator<IndexedRecord> iterator =
               hfileReader.getEngineRecordsByKeysIterator(
                   Arrays.asList("key00001", "key05", "key24", "key16", "key31", "key61"),
                   schema)) {
        assertThrows(
            IllegalStateException.class,
            () -> StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .collect(Collectors.toList()));
      }
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
      "/hfile/hudi_0_9_hbase_1_2_3,true", "/hfile/hudi_0_9_hbase_1_2_3,false",
      "/hfile/hudi_0_10_hbase_1_2_3,true", "/hfile/hudi_0_10_hbase_1_2_3,false",
      "/hfile/hudi_0_11_hbase_2_4_9,true", "/hfile/hudi_0_11_hbase_2_4_9,false"})
  public void testHoodieHFileCompatibility(String hfilePrefix, boolean useBloomFilter) throws IOException {
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadPrimitiveRecord()
    // using different Hudi releases
    String simpleHFile = hfilePrefix + SIMPLE_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadComplexRecord()
    // using different Hudi releases
    String complexHFile = hfilePrefix + COMPLEX_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestBootstrapIndex#testBootstrapIndex()
    // using different Hudi releases.  The file is copied from .hoodie/.aux/.bootstrap/.partitions/
    String bootstrapIndexFile = hfilePrefix + BOOTSTRAP_INDEX_HFILE_SUFFIX;

    FileSystem fs = HadoopFSUtils.getFs(getFilePath().toString(), new Configuration());
    byte[] content = readHFileFromResources(simpleHFile);
    verifyHFileReader(
        content, hfilePrefix, true, useBloomFilter, NUM_RECORDS_FIXTURE);

    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    try (HoodieAvroHFileReaderImplBase hfileReader = createHFileReader(storage, content, useBloomFilter)) {
      HoodieSchema schema =
          getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
      assertEquals(NUM_RECORDS_FIXTURE, hfileReader.getTotalRecords());
      verifySimpleRecords(hfileReader.getRecordIterator(schema));
    }

    content = readHFileFromResources(complexHFile);
    verifyHFileReader(
        content, hfilePrefix, true, useBloomFilter, NUM_RECORDS_FIXTURE);
    try (HoodieAvroHFileReaderImplBase hfileReader = createHFileReader(storage, content, useBloomFilter)) {
      HoodieSchema schema =
          getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchemaWithUDT.avsc");
      assertEquals(NUM_RECORDS_FIXTURE, hfileReader.getTotalRecords());
      verifySimpleRecords(hfileReader.getRecordIterator(schema));
    }

    content = readHFileFromResources(bootstrapIndexFile);
    verifyHFileReader(
        content, hfilePrefix, false, useBloomFilter, 4);
  }

  private Set<String> getRandomKeys(int count, List<String> keys) {
    Set<String> rowKeys = new HashSet<>();
    int totalKeys = keys.size();
    while (rowKeys.size() < count) {
      int index = RANDOM.nextInt(totalKeys);
      if (!rowKeys.contains(index)) {
        rowKeys.add(keys.get(index));
      }
    }
    return rowKeys;
  }

  private HoodieAvroHFileReaderImplBase getHFileReaderFromFixture(boolean useBloomFilter) throws IOException {
    // This HFile has the following key and value
    // key: key000000-abcdefghij to key019999-abcdefghij
    // value: avro record based on exampleSchema.avsc
    // The block index of interests:
    // First: "Key{key000000-abcdefghij}" -> "BlockIndexEntry{firstKey=Key{key000000-abcdefghij}, offset=0, size=22
    // "Key{key001424}" -> "BlockIndexEntry{firstKey=Key{key001424}, offset=13022, size=2157}"
    // "Key{key010769}" -> "BlockIndexEntry{firstKey=Key{key010769}, offset=98878, size=2112}"
    // Last: "Key{key019889}" -> "BlockIndexEntry{firstKey=Key{key019889}, offset=183324, size=1103}"
    return createHFileReader(HoodieTestUtils.getStorage(getFilePath()),
        readHFileFromResources("/hfile/hudi_1_0_hbase_2_4_13_avro_simple_schema_fake_first_key.hfile"),
        useBloomFilter);
  }
}
