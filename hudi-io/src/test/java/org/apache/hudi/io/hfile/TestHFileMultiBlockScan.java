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

package org.apache.hudi.io.hfile;

import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.compress.CompressionCodec;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.util.Locale;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reproduces a full-scan record drop on native-writer HFiles that span many data blocks. A large
 * block size packs many records per block; the trailer entry count is correct, but a
 * {@code seekTo()} then {@code next()} forward scan stops short of a block's content end because
 * the content-end bound subtracts the trailing checksum size. The drop only surfaces once the
 * checksum region exceeds the last record's size, which is why small-block tests never caught it.
 */
public class TestHFileMultiBlockScan {

  @ParameterizedTest
  @CsvSource({
      "NONE, 64, 5000", "GZIP, 64, 5000",
      "NONE, 1048576, 200000", "GZIP, 1048576, 200000",
      "NONE, 65536, 200000", "GZIP, 65536, 200000"
  })
  public void fullScanReturnsAllRecords(String codec, int blockSize, int numRecords) throws Exception {
    HFileContext context = new HFileContext.Builder()
        .blockSize(blockSize)
        .compressionCodec(CompressionCodec.valueOf(codec))
        .build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (HFileWriter writer = new HFileWriterImpl(context, baos)) {
      for (int i = 0; i < numRecords; i++) {
        writer.append(key(i), getUTF8Bytes(value(i)));
      }
    }
    byte[] fileBytes = baos.toByteArray();

    try (HFileReader reader = new HFileReaderImpl(
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(fileBytes)),
        fileBytes.length)) {
      reader.initializeMetadata();
      assertEquals(numRecords, reader.getNumKeyValueEntries(),
          "trailer entry count for codec=" + codec + " blockSize=" + blockSize);

      String scenario = "codec=" + codec + " blockSize=" + blockSize + " numRecords=" + numRecords;
      int scanned = 0;
      int firstSkipAt = -1;
      boolean hasNext = reader.seekTo();
      while (hasNext) {
        KeyValue kv = reader.getKeyValue().get();
        if (firstSkipAt < 0 && !key(scanned).equals(kv.getKey().getContentInString())) {
          firstSkipAt = scanned;
        }
        scanned++;
        hasNext = reader.next();
      }
      assertEquals(numRecords, scanned,
          "forward scan returned fewer records than written; " + scenario
              + " firstSkipAtPosition=" + firstSkipAt);
    }
  }

  private static String key(int i) {
    return String.format(Locale.ROOT, "%010d", i);
  }

  private static String value(int i) {
    return String.format(Locale.ROOT, "value-%010d", i);
  }
}
