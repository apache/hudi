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

package org.apache.hudi.common.fs.inline;

import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.io.hadoop.HoodieHFileUtils;
import org.apache.hudi.io.util.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests {@link InLineFileSystem} with HBase HFile reader.
 */
public class TestInLineFileSystemWithHBaseHFileReader extends TestInLineFileSystemHFileInLiningBase {
  @Override
  protected void validateHFileReading(InLineFileSystem inlineFileSystem,
                                      Configuration conf,
                                      Configuration inlineConf,
                                      Path inlinePath,
                                      int maxRows) throws IOException {
    try (HFile.Reader reader =
             HoodieHFileUtils.createHFileReader(inlineFileSystem, inlinePath, new CacheConfig(conf), inlineConf)) {
      // Get a scanner that caches and that does not use pread.
      HFileScanner scanner = reader.getScanner(true, false);
      // Align scanner at start of the file.
      scanner.seekTo();
      readAllRecords(scanner, maxRows);

      Set<Integer> rowIdsToSearch = getRandomValidRowIds(10);
      for (int rowId : rowIdsToSearch) {
        KeyValue keyValue = new KeyValue.KeyOnlyKeyValue(getSomeKey(rowId));
        assertEquals(0, scanner.seekTo(keyValue),
            "location lookup failed");
        // read the key and see if it matches
        Cell cell = scanner.getCell();
        byte[] key = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowOffset() + cell.getRowLength());
        byte[] expectedKey = Arrays.copyOfRange(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowOffset() + keyValue.getRowLength());
        assertArrayEquals(expectedKey, key, "seeked key does not match");
        scanner.seekTo(keyValue);
        ByteBuffer val1 = scanner.getValue();
        scanner.seekTo(keyValue);
        ByteBuffer val2 = scanner.getValue();
        assertArrayEquals(IOUtils.toBytes(val1), IOUtils.toBytes(val2));
      }

      int[] invalidRowIds = {-4, maxRows, maxRows + 1, maxRows + 120, maxRows + 160, maxRows + 1000};
      for (int rowId : invalidRowIds) {
        assertNotEquals(0, scanner.seekTo(new KeyValue.KeyOnlyKeyValue(getSomeKey(rowId))),
            "location lookup should have failed");
      }
    }
  }

  private byte[] getSomeKey(int rowId) {
    KeyValue kv = new KeyValue(getUTF8Bytes(String.format(LOCAL_FORMATTER, rowId)),
        getUTF8Bytes("family"), getUTF8Bytes("qual"), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put);
    return kv.getKey();
  }

  private void readAllRecords(HFileScanner scanner, int maxRows) throws IOException {
    readAndCheckbytes(scanner, 0, maxRows);
  }

  // read the records and check
  private void readAndCheckbytes(HFileScanner scanner, int start, int n)
      throws IOException {
    int i = start;
    for (; i < (start + n); i++) {
      Cell cell = scanner.getCell();
      byte[] key = Arrays.copyOfRange(
          cell.getRowArray(), cell.getRowOffset(), cell.getRowOffset() + cell.getRowLength());
      byte[] val = Arrays.copyOfRange(
          cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength());
      String keyStr = String.format(LOCAL_FORMATTER, i);
      String valStr = VALUE_PREFIX + keyStr;
      KeyValue kv = new KeyValue(getUTF8Bytes(keyStr), getUTF8Bytes("family"),
          getUTF8Bytes("qual"), getUTF8Bytes(valStr));
      byte[] keyBytes = new KeyValue.KeyOnlyKeyValue(key, 0, key.length).getKey();
      byte[] expectedKeyBytes = Arrays.copyOfRange(
          kv.getRowArray(), kv.getRowOffset(), kv.getRowOffset() + kv.getRowLength());
      assertArrayEquals(expectedKeyBytes, keyBytes,
          "bytes for keys do not match " + keyStr + " " + fromUTF8Bytes(key));
      assertArrayEquals(getUTF8Bytes(valStr), val,
          "bytes for vals do not match " + valStr + " " + fromUTF8Bytes(val));
      if (!scanner.next()) {
        break;
      }
    }
    assertEquals(i, start + n - 1);
  }
}
