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

package org.apache.hudi.common.table.log;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for HoodieLogFileReader.
 */
public class TestHoodieLogFileReader {

  private static StoragePath LOG_FILE_PATH = new StoragePath("file:///test.log");

  // Record key field of the test data
  private static final String RECORD_KEY_FIELD = HoodieRecord.RECORD_KEY_METADATA_FIELD;

  private static final String SCHEMA_STRING = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"RandomRecord\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"" + RECORD_KEY_FIELD + "\", \"type\": \"string\"},\n"
      + "    {\"name\": \"value\", \"type\": \"double\"}\n"
      + "  ]\n"
      + "}";;

  @ParameterizedTest
  @CsvSource({"10, 0", "10, 1", "10, 5", "10, 10"})
  public void testReadBlock(int numLogBlock, int numCorruptedBlocks) throws IOException {
    assertTrue(numLogBlock >= numCorruptedBlocks);

    Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
    List<List<HoodieRecord>> recordBlocks = Stream.generate(() -> generateRandomHoodieRecords(schema, 1000)).limit(numLogBlock).collect(Collectors.toList());
    HoodieLogFile logFile = new HoodieLogFile(LOG_FILE_PATH);
    HoodieStorage storage = mockStorage(LOG_FILE_PATH, schema, recordBlocks, numCorruptedBlocks);

    HoodieLogFileReader logFileReader = new HoodieLogFileReader(storage, logFile, schema, 1024);
    List<HoodieLogBlock> logBlocks = new ArrayList<>();
    while (logFileReader.hasNext()) {
      logBlocks.add(logFileReader.next());
    }
    logFileReader.close();

    // Verify that the number of log blocks and corrupted blocks matches the expected counts
    assertEquals(numLogBlock, logBlocks.size());
    assertEquals(numCorruptedBlocks, logBlocks.stream().filter(logBlock -> logBlock.getBlockType() == HoodieLogBlockType.CORRUPT_BLOCK).count());

    for (int i = 0; i < numLogBlock; i++) {
      HoodieLogBlock logBlock = logBlocks.get(i);
      if (logBlock.getBlockType() != HoodieLogBlockType.CORRUPT_BLOCK) {
        HoodieDataBlock dataBlock = (HoodieDataBlock) logBlock;
        try (ClosableIterator<HoodieRecord<Object>> recordIterator = dataBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
          List<HoodieRecord> retrievedRecords = new ArrayList<>();
          recordIterator.forEachRemaining(retrievedRecords::add);

          // Verify that the records retrieved from the block match the expected records
          verifyRecords(recordBlocks.get(i), retrievedRecords);
        }
      }
    }
  }

  private HoodieStorage mockStorage(StoragePath logPath, Schema schema, List<List<HoodieRecord>> recordBlocks, int numCorruptedBlocks) throws IOException {
    HoodieStorage storage = mock(HoodieStorage.class);

    // create input stream
    byte[] fileBytes = generateAvroLogFileBytes(schema, recordBlocks, numCorruptedBlocks);
    SeekableDataInputStream inputStream = new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(fileBytes));

    // mock storage
    when(storage.getPathInfo(logPath)).thenReturn(new StoragePathInfo(logPath, fileBytes.length, false, (short) 1, 256L, 0L));
    when(storage.openSeekable(eq(logPath), anyInt(), anyBoolean())).thenReturn(inputStream);
    when(storage.getUri()).thenReturn(URI.create(logPath.getName()));
    when(storage.getScheme()).thenReturn("file");

    return storage;
  }

  private byte[] generateAvroLogFileBytes(Schema schema, List<List<HoodieRecord>> recordBlocks, int numCorruptedBlocks) throws IOException {
    int numLogBlock = recordBlocks.size();
    ByteBuffer fileBytes = ByteBuffer.allocate(10 * 1024 * 1024);

    Random random = new Random();
    Set<Integer> corruptedBlockIndices = new HashSet<>();

    // If there are multiple corrupted blocks, choose random positions
    if (numCorruptedBlocks > 1) {
      while (corruptedBlockIndices.size() < numCorruptedBlocks) {
        corruptedBlockIndices.add(random.nextInt(numLogBlock));
      }
    } else if (numCorruptedBlocks == 1) {
      // Single corrupted block is placed at the end
      corruptedBlockIndices.add(numLogBlock - 1);
    }

    for (int i = 0; i < numLogBlock; i++) {
      List<HoodieRecord> records = recordBlocks.get(i);
      byte[] blockBytes = generateAvroDataBlockBytes(schema, records);

      // If the current block is corrupted, truncate it
      if (corruptedBlockIndices.contains(i)) {
        // Truncate the block to half
        int truncatedLength = blockBytes.length / 2;
        byte[] corruptedBlockContent = Arrays.copyOf(blockBytes, truncatedLength);
        fileBytes.put(corruptedBlockContent);
      } else {
        fileBytes.put(blockBytes);
      }
    }

    // Return the byte array of the file
    return Arrays.copyOf(fileBytes.array(), fileBytes.position());
  }

  private byte[] generateAvroDataBlockBytes(Schema schema, List<HoodieRecord> records) throws IOException {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.SCHEMA, schema.toString());

    // block content bytes
    byte[] content = new HoodieAvroDataBlock(records, false, header, RECORD_KEY_FIELD).getContentBytes(null);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024 + content.length);
    long blockLen = getLogBlockLength(content.length, HoodieLogBlock.getHeaderMetadataBytes(header).length, HoodieLogBlock.getFooterMetadataBytes(new HashMap<>()).length);
    long startSize = byteBuffer.position();
    byteBuffer.put(HoodieLogFormat.MAGIC);
    byteBuffer.putLong(blockLen);
    byteBuffer.putInt(HoodieLogFormat.CURRENT_VERSION);                 // log block version
    byteBuffer.putInt(HoodieLogBlockType.AVRO_DATA_BLOCK.ordinal());    // block type
    byteBuffer.put(HoodieLogBlock.getHeaderMetadataBytes(header));      // header
    byteBuffer.putLong(content.length);                                 // content length
    byteBuffer.put(content);                                            // content
    byteBuffer.put(HoodieLogBlock.getHeaderMetadataBytes(new HashMap<>())); // footer
    byteBuffer.putLong(byteBuffer.position() - startSize);

    return Arrays.copyOf(byteBuffer.array(), byteBuffer.position());
  }

  // TODO This function, along with HoodieLogFormatWriter#getLogBlockLength from hudi-hadoop-common, should be consolidated into a public function
  private int getLogBlockLength(int contentLength, int headerLength, int footerLength) {
    return Integer.BYTES  // Number of bytes to write version
        + Integer.BYTES   // Number of bytes to write ordinal
        + headerLength    // Length of the headers
        + Long.BYTES      // Number of bytes used to write content length
        + contentLength   // Length of the content
        + footerLength    // Length of the footers
        + Long.BYTES;     // bytes to write totalLogBlockLength at end of block (for reverse ptr)
  }

  // TODO: In the future, remove the following and replace it with a call to the public function from PR (https://github.com/apache/hudi/pull/11924)
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
