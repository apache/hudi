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

package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.io.storage.HoodieHFileConfig.HFILE_COMPARATOR;
import static org.apache.hudi.io.storage.HoodieHFileReader.SCHEMA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

public class TestHoodieHFileReaderWriter extends TestHoodieReaderWriterBase {
  private static final String DUMMY_BASE_PATH = "dummy_base_path";
  // Number of records in HFile fixtures for compatibility tests
  private static final int NUM_RECORDS_FIXTURE = 50;
  private static final String SIMPLE_SCHEMA_HFILE_SUFFIX = "_simple.hfile";
  private static final String COMPLEX_SCHEMA_HFILE_SUFFIX = "_complex.hfile";
  private static final String BOOTSTRAP_INDEX_HFILE_SUFFIX = "_bootstrap_index_partitions.hfile";

  @Override
  protected Path getFilePath() {
    return new Path(tempDir.toString() + "/f1_1-0-1_000.hfile");
  }

  @Override
  protected HoodieFileWriter<GenericRecord> createWriter(
      Schema avroSchema, boolean populateMetaFields) throws Exception {
    String instantTime = "000";
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(DUMMY_BASE_PATH)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .bloomFilterNumEntries(1000).bloomFilterFPP(0.00001).build())
        .withPopulateMetaFields(populateMetaFields)
        .build();
    Configuration conf = new Configuration();
    TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
    Supplier<Integer> partitionSupplier = Mockito.mock(Supplier.class);
    when(mockTaskContextSupplier.getPartitionIdSupplier()).thenReturn(partitionSupplier);
    when(partitionSupplier.get()).thenReturn(10);

    return HoodieFileWriterFactory.newHFileFileWriter(
        instantTime, getFilePath(), writeConfig, avroSchema, conf, mockTaskContextSupplier);
  }

  @Override
  protected HoodieFileReader<GenericRecord> createReader(
      Configuration conf) throws Exception {
    CacheConfig cacheConfig = new CacheConfig(conf);
    return new HoodieHFileReader<>(conf, getFilePath(), cacheConfig, getFilePath().getFileSystem(conf));
  }

  @Override
  protected void verifyMetadata(Configuration conf) throws IOException {
    FileSystem fs = getFilePath().getFileSystem(conf);
    HFile.Reader hfileReader = HoodieHFileUtils.createHFileReader(fs, getFilePath(), new CacheConfig(conf), conf);
    assertEquals(HFILE_COMPARATOR.getClass(), hfileReader.getComparator().getClass());
    assertEquals(NUM_RECORDS, hfileReader.getEntries());
  }

  @Override
  protected void verifySchema(Configuration conf, String schemaPath) throws IOException {
    FileSystem fs = getFilePath().getFileSystem(conf);
    HFile.Reader hfileReader = HoodieHFileUtils.createHFileReader(fs, getFilePath(), new CacheConfig(conf), conf);
    assertEquals(getSchemaFromResource(TestHoodieHFileReaderWriter.class, schemaPath),
        new Schema.Parser().parse(new String(hfileReader.getHFileInfo().get(SCHEMA_KEY.getBytes()))));
  }

  private static Stream<Arguments> populateMetaFieldsAndTestAvroWithMeta() {
    return Arrays.stream(new Boolean[][] {
        {true, true},
        {false, true},
        {true, false},
        {false, false}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("populateMetaFieldsAndTestAvroWithMeta")
  public void testWriteReadHFileWithMetaFields(boolean populateMetaFields, boolean testAvroWithMeta) throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchemaWithMetaFields.avsc");
    HoodieFileWriter<GenericRecord> writer = createWriter(avroSchema, populateMetaFields);
    List<String> keys = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new TreeMap<>();
    for (int i = 0; i < 100; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = String.format("%s%04d", "key", i);
      record.put("_row_key", key);
      keys.add(key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);
      if (testAvroWithMeta) {
        // payload does not matter. GenericRecord passed in is what matters
        writer.writeAvroWithMetadata(new HoodieAvroRecord(new HoodieKey((String) record.get("_row_key"),
                Integer.toString((Integer) record.get("number"))), new EmptyHoodieRecordPayload()).getKey(), record);
        // only HoodieKey will be looked up from the 2nd arg(HoodieRecord).
      } else {
        writer.writeAvro(key, record);
      }
      recordMap.put(key, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) createReader(conf);
    List<IndexedRecord> records = HoodieHFileReader.readAllRecords(hoodieHFileReader);
    assertEquals(new ArrayList<>(recordMap.values()), records);

    hoodieHFileReader.close();

    for (int i = 0; i < 2; i++) {
      int randomRowstoFetch = 5 + RANDOM.nextInt(10);
      Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);

      List<String> rowsList = new ArrayList<>(rowsToFetch);
      Collections.sort(rowsList);

      List<GenericRecord> expectedRecords = rowsList.stream().map(recordMap::get).collect(Collectors.toList());

      hoodieHFileReader = (HoodieHFileReader<GenericRecord>) createReader(conf);
      List<GenericRecord> result = HoodieHFileReader.readRecords(hoodieHFileReader, rowsList);

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

  @Override
  @Test
  public void testWriteReadWithEvolvedSchema() throws Exception {
    // Disable the test with evolved schema for HFile since it's not supported
    // TODO(HUDI-3683): fix the schema evolution for HFile
  }

  @Test
  public void testReadHFileFormatRecords() throws Exception {
    writeFileWithSimpleSchema();
    FileSystem fs = FSUtils.getFs(getFilePath().toString(), new Configuration());
    byte[] content = FileIOUtils.readAsByteArray(
        fs.open(getFilePath()), (int) fs.getFileStatus(getFilePath()).getLen());
    // Reading byte array in HFile format, without actual file path
    HoodieHFileReader<GenericRecord> hfileReader =
        new HoodieHFileReader<>(fs, new Path(DUMMY_BASE_PATH), content, Option.empty());
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    assertEquals(NUM_RECORDS, hfileReader.getTotalRecords());
    verifySimpleRecords(hfileReader.getRecordIterator(avroSchema));
  }

  @Test
  public void testReaderGetRecordIterator() throws Exception {
    writeFileWithSimpleSchema();
    HoodieHFileReader<GenericRecord> hfileReader =
        (HoodieHFileReader<GenericRecord>) createReader(new Configuration());
    List<String> keys =
        IntStream.concat(IntStream.range(40, NUM_RECORDS * 2), IntStream.range(10, 20))
            .mapToObj(i -> "key" + String.format("%02d", i)).collect(Collectors.toList());
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    Iterator<GenericRecord> iterator = hfileReader.getRecordsByKeysIterator(keys, avroSchema);

    List<Integer> expectedIds =
        IntStream.concat(IntStream.range(40, NUM_RECORDS), IntStream.range(10, 20))
            .boxed().collect(Collectors.toList());
    int index = 0;
    while (iterator.hasNext()) {
      GenericRecord record = iterator.next();
      String key = "key" + String.format("%02d", expectedIds.get(index));
      assertEquals(key, record.get("_row_key").toString());
      assertEquals(Integer.toString(expectedIds.get(index)), record.get("time").toString());
      assertEquals(expectedIds.get(index), record.get("number"));
      index++;
    }
  }

  @Test
  public void testReaderGetRecordIteratorByKeyPrefixes() throws Exception {
    writeFileWithSimpleSchema();
    HoodieHFileReader<GenericRecord> hfileReader =
        (HoodieHFileReader<GenericRecord>) createReader(new Configuration());

    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");

    List<String> keyPrefixes = Collections.singletonList("key");
    Iterator<GenericRecord> iterator =
        hfileReader.getRecordsByKeyPrefixIterator(keyPrefixes, avroSchema);

    List<GenericRecord> recordsByPrefix = toStream(iterator).collect(Collectors.toList());

    List<GenericRecord> allRecords = toStream(hfileReader.getRecordIterator()).collect(Collectors.toList());

    assertEquals(allRecords, recordsByPrefix);

    // filter for "key1" : entries from key10 to key19 should be matched
    List<GenericRecord> expectedKey1s = allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key1")).collect(Collectors.toList());
    iterator =
        hfileReader.getRecordsByKeyPrefixIterator(Collections.singletonList("key1"), avroSchema);
    recordsByPrefix =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    assertEquals(expectedKey1s, recordsByPrefix);

    // exact match
    List<GenericRecord> expectedKey25 = allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key25")).collect(Collectors.toList());
    iterator =
        hfileReader.getRecordsByKeyPrefixIterator(Collections.singletonList("key25"), avroSchema);
    recordsByPrefix =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    assertEquals(expectedKey25, recordsByPrefix);

    // no match. key prefix is beyond entries in file.
    iterator =
        hfileReader.getRecordsByKeyPrefixIterator(Collections.singletonList("key99"), avroSchema);
    recordsByPrefix =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    assertEquals(Collections.emptyList(), recordsByPrefix);

    // no match. but keyPrefix is in between the entries found in file.
    iterator =
        hfileReader.getRecordsByKeyPrefixIterator(Collections.singletonList("key1234"), avroSchema);
    recordsByPrefix =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    assertEquals(Collections.emptyList(), recordsByPrefix);

    // filter for "key50" and "key1" : entries from key50 and 'key10 to key19' should be matched.
    List<GenericRecord> expectedKey50and1s = allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key1")
        || (entry.get("_row_key").toString()).contains("key50")).collect(Collectors.toList());
    iterator =
        hfileReader.getRecordsByKeyPrefixIterator(Arrays.asList("key1", "key50"), avroSchema);
    recordsByPrefix =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    assertEquals(expectedKey50and1s, recordsByPrefix);

    // filter for "key50" and "key0" : entries from key50 and 'key00 to key09' should be matched.
    List<GenericRecord> expectedKey50and0s = allRecords.stream().filter(entry -> (entry.get("_row_key").toString()).contains("key0")
        || (entry.get("_row_key").toString()).contains("key50")).collect(Collectors.toList());
    iterator =
        hfileReader.getRecordsByKeyPrefixIterator(Arrays.asList("key0", "key50"), avroSchema);
    recordsByPrefix =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    assertEquals(expectedKey50and0s, recordsByPrefix);

    // filter for "key1" and "key0" : entries from 'key10 to key19' and 'key00 to key09' should be matched.
    List<GenericRecord> expectedKey1sand0s = allRecords.stream()
        .filter(entry -> (entry.get("_row_key").toString()).contains("key0") || (entry.get("_row_key").toString()).contains("key1"))
        .collect(Collectors.toList());
    iterator =
        hfileReader.getRecordsByKeyPrefixIterator(Arrays.asList("key0", "key1"), avroSchema);
    recordsByPrefix =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
    assertEquals(expectedKey1sand0s, recordsByPrefix);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "/hudi_0_9_hbase_1_2_3", "/hudi_0_10_hbase_1_2_3", "/hudi_0_11_hbase_2_4_9"})
  public void testHoodieHFileCompatibility(String hfilePrefix) throws IOException {
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadPrimitiveRecord()
    // using different Hudi releases
    String simpleHFile = hfilePrefix + SIMPLE_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadComplexRecord()
    // using different Hudi releases
    String complexHFile = hfilePrefix + COMPLEX_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestBootstrapIndex#testBootstrapIndex()
    // using different Hudi releases.  The file is copied from .hoodie/.aux/.bootstrap/.partitions/
    String bootstrapIndexFile = hfilePrefix + BOOTSTRAP_INDEX_HFILE_SUFFIX;

    FileSystem fs = FSUtils.getFs(getFilePath().toString(), new Configuration());
    byte[] content = readHFileFromResources(simpleHFile);
    verifyHFileReader(
        HoodieHFileUtils.createHFileReader(fs, new Path(DUMMY_BASE_PATH), content),
        hfilePrefix, true, HFILE_COMPARATOR.getClass(), NUM_RECORDS_FIXTURE);
    HoodieHFileReader<GenericRecord> hfileReader =
        new HoodieHFileReader<>(fs, new Path(DUMMY_BASE_PATH), content, Option.empty());
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    assertEquals(NUM_RECORDS_FIXTURE, hfileReader.getTotalRecords());
    verifySimpleRecords(hfileReader.getRecordIterator(avroSchema));

    content = readHFileFromResources(complexHFile);
    verifyHFileReader(HoodieHFileUtils.createHFileReader(fs, new Path(DUMMY_BASE_PATH), content),
        hfilePrefix, true, HFILE_COMPARATOR.getClass(), NUM_RECORDS_FIXTURE);
    hfileReader = new HoodieHFileReader<>(fs, new Path(DUMMY_BASE_PATH), content, Option.empty());
    avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchemaWithUDT.avsc");
    assertEquals(NUM_RECORDS_FIXTURE, hfileReader.getTotalRecords());
    verifySimpleRecords(hfileReader.getRecordIterator(avroSchema));

    content = readHFileFromResources(bootstrapIndexFile);
    verifyHFileReader(HoodieHFileUtils.createHFileReader(fs, new Path(DUMMY_BASE_PATH), content),
        hfilePrefix, false, HFileBootstrapIndex.HoodieKVComparator.class, 4);
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

  private byte[] readHFileFromResources(String filename) throws IOException {
    long size = TestHoodieHFileReaderWriter.class
        .getResource(filename).openConnection().getContentLength();
    return FileIOUtils.readAsByteArray(
        TestHoodieHFileReaderWriter.class.getResourceAsStream(filename), (int) size);
  }

  private void verifyHFileReader(
      HFile.Reader reader, String hfileName, boolean mayUseDefaultComparator,
      Class<?> clazz, int count) {
    // HFile version is 3
    assertEquals(3, reader.getTrailer().getMajorVersion());
    if (mayUseDefaultComparator && hfileName.contains("hudi_0_9")) {
      // Pre Hudi 0.10, the default comparator is used for metadata table HFiles
      // For bootstrap index HFiles, the custom comparator is always used
      assertEquals(CellComparatorImpl.class, reader.getComparator().getClass());
    } else {
      assertEquals(clazz, reader.getComparator().getClass());
    }
    assertEquals(count, reader.getEntries());
  }
}
