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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.io.storage.HoodieHFileConfig.HFILE_COMPARATOR;
import static org.apache.hudi.io.storage.HoodieHFileReader.KEY_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

public class TestHoodieHFileReaderWriter extends TestHoodieReaderWriterBase {

  @Override
  protected Path getFilePath() {
    return new Path(tempDir.toString() + "/f1_1-0-1_000.hfile");
  }

  @Override
  protected HoodieFileWriter<GenericRecord> createWriter(
      Schema avroSchema, boolean populateMetaFields) throws Exception {
    String instantTime = "000";
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("dummy_base_path")
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
        new Schema.Parser().parse(new String(hfileReader.getHFileInfo().get(KEY_SCHEMA.getBytes()))));
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
    Map<String, GenericRecord> recordMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = String.format("%s%04d", "key", i);
      record.put("_row_key", key);
      keys.add(key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);
      if (testAvroWithMeta) {
        writer.writeAvroWithMetadata(record, new HoodieAvroRecord(new HoodieKey((String) record.get("_row_key"),
            Integer.toString((Integer) record.get("number"))), new EmptyHoodieRecordPayload())); // payload does not matter. GenericRecord passed in is what matters
        // only HoodieKey will be looked up from the 2nd arg(HoodieRecord).
      } else {
        writer.writeAvro(key, record);
      }
      recordMap.put(key, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    HoodieHFileReader hoodieHFileReader = (HoodieHFileReader) createReader(conf);
    List<Pair<String, IndexedRecord>> records = hoodieHFileReader.readAllRecords();
    records.forEach(entry -> assertEquals(entry.getSecond(), recordMap.get(entry.getFirst())));
    hoodieHFileReader.close();

    for (int i = 0; i < 2; i++) {
      int randomRowstoFetch = 5 + RANDOM.nextInt(10);
      Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);
      List<String> rowsList = new ArrayList<>(rowsToFetch);
      Collections.sort(rowsList);
      hoodieHFileReader = (HoodieHFileReader) createReader(conf);
      List<Pair<String, GenericRecord>> result = hoodieHFileReader.readRecords(rowsList);
      assertEquals(result.size(), randomRowstoFetch);
      result.forEach(entry -> {
        assertEquals(entry.getSecond(), recordMap.get(entry.getFirst()));
        if (populateMetaFields && testAvroWithMeta) {
          assertNotNull(entry.getSecond().get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        } else {
          assertNull(entry.getSecond().get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        }
      });
      hoodieHFileReader.close();
    }
  }

  @Override
  @Test
  public void testWriteReadWithEvolvedSchema() throws Exception {
    // Disable the test with evolved schema for HFile since it's not supported
  }

  @Test
  public void testReadHFileFormatRecords() throws Exception {
    writeFileWithSimpleSchema();
    FileSystem fs = FSUtils.getFs(getFilePath().toString(), new Configuration());
    byte[] content = FileIOUtils.readAsByteArray(
        fs.open(getFilePath()), (int) fs.getFileStatus(getFilePath()).getLen());
    // Reading byte array in HFile format, without actual file path
    HoodieHFileReader<GenericRecord> hfileReader =
        new HoodieHFileReader<>(fs, new Path("dummy_base_path"), content);
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    Iterator<GenericRecord> iterator = hfileReader.getRecordIterator(avroSchema);

    int index = 0;
    while (iterator.hasNext()) {
      GenericRecord record = iterator.next();
      String key = "key" + String.format("%02d", index);
      assertEquals(key, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      index++;
    }
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
    Iterator<GenericRecord> iterator = hfileReader.getRecordIterator(keys, avroSchema);

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
}
