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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopSeekableDataInputStream;
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.io.hfile.Key;
import org.apache.hudi.io.hfile.KeyValue;
import org.apache.hudi.io.hfile.UTF8StringKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.io.hfile.HFileUtils.getValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link InLineFileSystem} with native HFile reader.
 */
public class TestInLineFileSystemWithHFileReader extends TestInLineFileSystemHFileInLiningBase {
  @Override
  protected void validateHFileReading(InLineFileSystem inlineFileSystem,
                                      Configuration conf,
                                      Configuration inlineConf,
                                      Path inlinePath,
                                      int maxRows) throws IOException {
    long fileSize = inlineFileSystem.getFileStatus(inlinePath).getLen();
    try (SeekableDataInputStream stream =
             new HadoopSeekableDataInputStream(inlineFileSystem.open(inlinePath))) {
      try (HFileReader reader = new HFileReaderImpl(stream, fileSize)) {
        // Align scanner at start of the file.
        reader.seekTo();
        readAllRecords(reader, maxRows);

        reader.seekTo();
        List<Integer> rowIdsToSearch = getRandomValidRowIds(10)
            .stream().sorted().collect(Collectors.toList());
        for (int rowId : rowIdsToSearch) {
          Key lookupKey = getKey(rowId);
          assertEquals(0, reader.seekTo(lookupKey), "location lookup failed");
          // read the key and see if it matches
          Option<KeyValue> keyValue = reader.getKeyValue();
          assertTrue(keyValue.isPresent());
          assertEquals(lookupKey, keyValue.get().getKey(), "seeked key does not match");
          reader.seekTo(lookupKey);
          String val1 = getValue(reader.getKeyValue().get());
          reader.seekTo(lookupKey);
          String val2 = getValue(reader.getKeyValue().get());
          assertEquals(val1, val2);
        }

        reader.seekTo();
        int[] invalidRowIds = {-4, maxRows, maxRows + 1, maxRows + 120, maxRows + 160, maxRows + 1000};
        for (int rowId : invalidRowIds) {
          assertNotEquals(0, reader.seekTo(getKey(rowId)),
              "location lookup should have failed");
        }
      }
    }
  }

  private Key getKey(int rowId) {
    return new UTF8StringKey(String.format(LOCAL_FORMATTER, rowId));
  }

  private void readAllRecords(HFileReader reader, int maxRows) throws IOException {
    for (int i = 0; i < maxRows; i++) {
      Option<KeyValue> keyValue = reader.getKeyValue();
      assertTrue(keyValue.isPresent());
      String key = keyValue.get().getKey().getContentInString();
      String value = getValue(keyValue.get());
      String expectedKeyStr = String.format(LOCAL_FORMATTER, i);
      String expectedValStr = VALUE_PREFIX + expectedKeyStr;

      assertEquals(expectedKeyStr, key, "keys do not match " + expectedKeyStr + " " + key);
      assertEquals(expectedValStr, value, "values do not match " + expectedValStr + " " + value);
      assertEquals(i != maxRows - 1, reader.next());
    }
  }
}
