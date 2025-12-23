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

import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.hadoop.fs.inline.InMemoryFileSystem;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hadoop.HadoopSeekableDataInputStream;
import org.apache.hudi.io.hfile.HFileContext;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.io.hfile.HFileWriter;
import org.apache.hudi.io.hfile.HFileWriterImpl;
import org.apache.hudi.io.hfile.Key;
import org.apache.hudi.io.hfile.KeyValue;
import org.apache.hudi.io.hfile.UTF8StringKey;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.FILE_SCHEME;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getPhantomFile;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getRandomOuterInMemPath;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.io.hfile.HFileUtils.getValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link InLineFileSystem} with native HFile reader.
 */
class TestInLineFileSystemWithHFileReader {
  protected static final String LOCAL_FORMATTER = "%010d";
  protected static final String VALUE_PREFIX = "value";
  private final Configuration inMemoryConf;
  private final Configuration inlineConf;
  private final int maxRows = 100 + RANDOM.nextInt(1000);
  private Path generatedPath;

  TestInLineFileSystemWithHFileReader() {
    inMemoryConf = new Configuration();
    inMemoryConf.set(
        "fs." + InMemoryFileSystem.SCHEME + ".impl",
        InMemoryFileSystem.class.getName());
    inlineConf = new Configuration();
    inlineConf.set(
        "fs." + InLineFileSystem.SCHEME + ".impl",
        InLineFileSystem.class.getName());
  }

  @AfterEach
  public void teardown() throws IOException {
    if (generatedPath != null) {
      File filePath = new File(generatedPath.toString().substring(generatedPath.toString().indexOf(':') + 1));
      if (filePath.exists()) {
        FileSystemTestUtils.deleteFile(filePath);
      }
    }
  }

  @Test
  void testSimpleInlineFileSystem() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    Path outerPath = new Path(FILE_SCHEME + outerInMemFSPath.toString().substring(outerInMemFSPath.toString().indexOf(':')));
    generatedPath = outerPath;
    DataOutputStream out = createFSOutput(outerInMemFSPath, inMemoryConf);
    HFileContext context = new HFileContext.Builder().build();
    HFileWriter writer = new HFileWriterImpl(context, out);

    writeRecords(writer);
    out.close();

    byte[] inlineBytes = getBytesToInline(outerInMemFSPath);
    long startOffset = generateOuterFile(outerPath, inlineBytes);

    long inlineLength = inlineBytes.length;

    // Generate phantom inline file
    Path inlinePath = new Path(getPhantomFile(new StoragePath(outerPath.toUri()), startOffset, inlineLength).toUri());

    InLineFileSystem inlineFileSystem = (InLineFileSystem) inlinePath.getFileSystem(inlineConf);
    FSDataInputStream fin = inlineFileSystem.open(inlinePath);

    validateHFileReading(inlineFileSystem, inMemoryConf, inlineConf, inlinePath, maxRows);

    fin.close();
    outerPath.getFileSystem(inMemoryConf).delete(outerPath, true);
  }

  protected Set<Integer> getRandomValidRowIds(int count) {
    Set<Integer> rowIds = new HashSet<>();
    while (rowIds.size() < count) {
      int index = RANDOM.nextInt(maxRows);
      if (!rowIds.contains(index)) {
        rowIds.add(index);
      }
    }
    return rowIds;
  }

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

  private FSDataOutputStream createFSOutput(Path name, Configuration conf) throws IOException {
    return name.getFileSystem(conf).create(name);
  }

  private void writeRecords(HFileWriter writer) throws IOException {
    writeSomeRecords(writer);
    writer.close();
  }

  private void writeSomeRecords(HFileWriter writer)
      throws IOException {
    for (int i = 0; i < (maxRows); i++) {
      String key = String.format(LOCAL_FORMATTER, i);
      writer.append(key, getUTF8Bytes(VALUE_PREFIX + key));
    }
  }

  private long generateOuterFile(Path outerPath, byte[] inlineBytes) throws IOException {
    FSDataOutputStream wrappedOut = outerPath.getFileSystem(inMemoryConf).create(outerPath, true);
    // write random bytes
    writeRandomBytes(wrappedOut, 10);

    // save position for start offset
    long startOffset = wrappedOut.getPos();
    // embed inline file
    wrappedOut.write(inlineBytes);

    // write random bytes
    writeRandomBytes(wrappedOut, 5);
    wrappedOut.hsync();
    wrappedOut.close();
    return startOffset;
  }

  private byte[] getBytesToInline(Path outerInMemFSPath) throws IOException {
    InMemoryFileSystem inMemoryFileSystem = (InMemoryFileSystem) outerInMemFSPath.getFileSystem(inMemoryConf);
    return inMemoryFileSystem.getFileAsBytes();
  }

  private void writeRandomBytes(FSDataOutputStream writer, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      writer.writeUTF(UUID.randomUUID().toString());
    }
  }
}
