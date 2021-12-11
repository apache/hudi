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

package org.apache.hudi.common.fs.inline;

import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.io.storage.HoodieHBaseKVComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.FILE_SCHEME;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getPhantomFile;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getRandomOuterInMemPath;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests {@link InLineFileSystem} to inline HFile.
 */
public class TestInLineFileSystemHFileInLining {

  private final Configuration inMemoryConf;
  private final Configuration inlineConf;
  private final int minBlockSize = 1024;
  private static final String LOCAL_FORMATTER = "%010d";
  private int maxRows = 100 + RANDOM.nextInt(1000);
  private Path generatedPath;

  public TestInLineFileSystemHFileInLining() {
    inMemoryConf = new Configuration();
    inMemoryConf.set("fs." + InMemoryFileSystem.SCHEME + ".impl", InMemoryFileSystem.class.getName());
    inlineConf = new Configuration();
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
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
  public void testSimpleInlineFileSystem() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    Path outerPath = new Path(FILE_SCHEME + outerInMemFSPath.toString().substring(outerInMemFSPath.toString().indexOf(':')));
    generatedPath = outerPath;
    CacheConfig cacheConf = new CacheConfig(inMemoryConf);
    FSDataOutputStream fout = createFSOutput(outerInMemFSPath, inMemoryConf);
    HFileContext meta = new HFileContextBuilder()
        .withBlockSize(minBlockSize)
        .build();
    HFile.Writer writer = HFile.getWriterFactory(inMemoryConf, cacheConf)
        .withOutputStream(fout)
        .withFileContext(meta)
        .withComparator(new HoodieHBaseKVComparator())
        .create();

    writeRecords(writer);
    fout.close();

    byte[] inlineBytes = getBytesToInline(outerInMemFSPath);
    long startOffset = generateOuterFile(outerPath, inlineBytes);

    long inlineLength = inlineBytes.length;

    // Generate phantom inline file
    Path inlinePath = getPhantomFile(outerPath, startOffset, inlineLength);

    InLineFileSystem inlineFileSystem = (InLineFileSystem) inlinePath.getFileSystem(inlineConf);
    FSDataInputStream fin = inlineFileSystem.open(inlinePath);

    HFile.Reader reader = HFile.createReader(inlineFileSystem, inlinePath, cacheConf, inlineConf);
    // Load up the index.
    reader.loadFileInfo();
    // Get a scanner that caches and that does not use pread.
    HFileScanner scanner = reader.getScanner(true, false);
    // Align scanner at start of the file.
    scanner.seekTo();
    readAllRecords(scanner);

    Set<Integer> rowIdsToSearch = getRandomValidRowIds(10);
    for (int rowId : rowIdsToSearch) {
      assertEquals(0, scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(rowId))),
          "location lookup failed");
      // read the key and see if it matches
      ByteBuffer readKey = scanner.getKey();
      assertArrayEquals(getSomeKey(rowId), Bytes.toBytes(readKey), "seeked key does not match");
      scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(rowId)));
      ByteBuffer val1 = scanner.getValue();
      scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(rowId)));
      ByteBuffer val2 = scanner.getValue();
      assertArrayEquals(Bytes.toBytes(val1), Bytes.toBytes(val2));
    }

    int[] invalidRowIds = {-4, maxRows, maxRows + 1, maxRows + 120, maxRows + 160, maxRows + 1000};
    for (int rowId : invalidRowIds) {
      assertNotEquals(0, scanner.seekTo(KeyValue.createKeyValueFromKey(getSomeKey(rowId))),
          "location lookup should have failed");
    }
    reader.close();
    fin.close();
    outerPath.getFileSystem(inMemoryConf).delete(outerPath, true);
  }

  private Set<Integer> getRandomValidRowIds(int count) {
    Set<Integer> rowIds = new HashSet<>();
    while (rowIds.size() < count) {
      int index = RANDOM.nextInt(maxRows);
      if (!rowIds.contains(index)) {
        rowIds.add(index);
      }
    }
    return rowIds;
  }

  private byte[] getSomeKey(int rowId) {
    KeyValue kv = new KeyValue(String.format(LOCAL_FORMATTER, Integer.valueOf(rowId)).getBytes(),
        Bytes.toBytes("family"), Bytes.toBytes("qual"), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put);
    return kv.getKey();
  }

  private FSDataOutputStream createFSOutput(Path name, Configuration conf) throws IOException {
    return name.getFileSystem(conf).create(name);
  }

  private void writeRecords(HFile.Writer writer) throws IOException {
    writeSomeRecords(writer);
    writer.close();
  }

  private int writeSomeRecords(HFile.Writer writer)
      throws IOException {
    String value = "value";
    KeyValue kv;
    for (int i = 0; i < (maxRows); i++) {
      String key = String.format(LOCAL_FORMATTER, Integer.valueOf(i));
      kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
          Bytes.toBytes(value + key));
      writer.append(kv);
    }
    return (maxRows);
  }

  private void readAllRecords(HFileScanner scanner) throws IOException {
    readAndCheckbytes(scanner, 0, maxRows);
  }

  // read the records and check
  private int readAndCheckbytes(HFileScanner scanner, int start, int n)
      throws IOException {
    String value = "value";
    int i = start;
    for (; i < (start + n); i++) {
      ByteBuffer key = scanner.getKey();
      ByteBuffer val = scanner.getValue();
      String keyStr = String.format(LOCAL_FORMATTER, Integer.valueOf(i));
      String valStr = value + keyStr;
      KeyValue kv = new KeyValue(Bytes.toBytes(keyStr), Bytes.toBytes("family"),
          Bytes.toBytes("qual"), Bytes.toBytes(valStr));
      byte[] keyBytes = new KeyValue.KeyOnlyKeyValue(Bytes.toBytes(key), 0,
          Bytes.toBytes(key).length).getKey();
      assertArrayEquals(kv.getKey(), keyBytes,
          "bytes for keys do not match " + keyStr + " " + Bytes.toString(Bytes.toBytes(key)));
      byte[] valBytes = Bytes.toBytes(val);
      assertArrayEquals(Bytes.toBytes(valStr), valBytes,
          "bytes for vals do not match " + valStr + " " + Bytes.toString(valBytes));
      if (!scanner.next()) {
        break;
      }
    }
    assertEquals(i, start + n - 1);
    return (start + n);
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

