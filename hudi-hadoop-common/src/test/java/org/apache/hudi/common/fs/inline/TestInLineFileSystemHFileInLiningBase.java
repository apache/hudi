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
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.hadoop.fs.inline.InMemoryFileSystem;
import org.apache.hudi.io.hfile.HFileContext;
import org.apache.hudi.io.hfile.HFileWriter;
import org.apache.hudi.io.hfile.HFileWriterImpl;
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
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.FILE_SCHEME;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getPhantomFile;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getRandomOuterInMemPath;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Tests {@link InLineFileSystem} to inline HFile.
 */
public abstract class TestInLineFileSystemHFileInLiningBase {

  protected static final String LOCAL_FORMATTER = "%010d";
  protected static final String VALUE_PREFIX = "value";
  private static final int MIN_BLOCK_BYTES = 1024;
  private final Configuration inMemoryConf;
  private final Configuration inlineConf;
  private final int maxRows = 100 + RANDOM.nextInt(1000);
  private Path generatedPath;

  public TestInLineFileSystemHFileInLiningBase() {
    inMemoryConf = new Configuration();
    inMemoryConf.set("fs." + InMemoryFileSystem.SCHEME + ".impl", InMemoryFileSystem.class.getName());
    inlineConf = new Configuration();
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
  }

  protected abstract void validateHFileReading(InLineFileSystem inlineFileSystem,
                                               Configuration conf,
                                               Configuration inlineConf,
                                               Path inlinePath,
                                               int maxRows) throws IOException;

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

