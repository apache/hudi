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

import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.fs.inline.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.fs.inline.FileSystemTestUtils.getRandomOuterFSPath;

/**
 * Tests {@link InLineFileSystem}.
 */
public class TestInLineFileSystem {
  private Configuration conf;
  private List<Path> listOfGeneratedPaths;

  public TestInLineFileSystem() {
    conf = new Configuration();
    conf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
    this.listOfGeneratedPaths = new ArrayList<>();
  }

  @After
  public void teardown() throws IOException {
    for (Path pathToDelete : listOfGeneratedPaths) {
      File filePath = new File(pathToDelete.toString().substring(pathToDelete.toString().indexOf(':') + 1));
      if (filePath.exists()) {
        FileSystemTestUtils.deleteFile(filePath);
      }
    }
  }

  @Test
  public void testReadInlineFile() throws IOException {
    Path outerPath = getRandomOuterFSPath();
    listOfGeneratedPaths.add(outerPath);

    int totalSlices = 5; // embed n slices so that we can test N inline seqPaths
    List<Pair<Long, Integer>> startOffsetLengthPairs = new ArrayList<>();
    List<byte[]> expectedByteArrays = new ArrayList<>();

    FSDataOutputStream wrappedOut = outerPath.getFileSystem(conf).create(outerPath, true);
    for (int i = 0; i < totalSlices; i++) {
      // append random bytes
      byte[] randomBytes = new byte[RANDOM.nextInt(1000)];
      RANDOM.nextBytes(randomBytes);
      wrappedOut.write(randomBytes);
      long startOffset = wrappedOut.getPos();
      // add inline content
      byte[] embeddedInlineBytes = new byte[RANDOM.nextInt(1000)];
      RANDOM.nextBytes(embeddedInlineBytes);
      wrappedOut.write(embeddedInlineBytes);
      expectedByteArrays.add(embeddedInlineBytes);
      startOffsetLengthPairs.add(Pair.of(startOffset, embeddedInlineBytes.length));
    }
    // suffix random bytes
    byte[] randomBytes = new byte[RANDOM.nextInt(1000)];
    RANDOM.nextBytes(randomBytes);
    wrappedOut.write(randomBytes);
    wrappedOut.flush();
    wrappedOut.close();
    FileStatus expectedFileStatus = outerPath.getFileSystem(conf).getFileStatus(outerPath);

    for (int i = 0; i < totalSlices; i++) {
      Pair<Long, Integer> startOffsetLengthPair = startOffsetLengthPairs.get(i);
      byte[] expectedBytes = expectedByteArrays.get(i);
      Path inlinePath = FileSystemTestUtils.getPhantomFile(outerPath, startOffsetLengthPair.getLeft(), startOffsetLengthPair.getRight());
      InLineFileSystem inlineFileSystem = (InLineFileSystem) inlinePath.getFileSystem(conf);
      FSDataInputStream fsDataInputStream = inlineFileSystem.open(inlinePath);
      Assert.assertTrue(inlineFileSystem.exists(inlinePath));
      verifyFileStatus(expectedFileStatus, inlinePath, startOffsetLengthPair.getRight(), inlineFileSystem.getFileStatus(inlinePath));
      FileStatus[] actualFileStatuses = inlineFileSystem.listStatus(inlinePath);
      Assert.assertEquals(1, actualFileStatuses.length);
      verifyFileStatus(expectedFileStatus, inlinePath, startOffsetLengthPair.getRight(), actualFileStatuses[0]);
      byte[] actualBytes = new byte[expectedBytes.length];
      fsDataInputStream.readFully(0, actualBytes);
      Assert.assertArrayEquals(expectedBytes, actualBytes);
      fsDataInputStream.close();
      Assert.assertEquals(InLineFileSystem.SCHEME, inlineFileSystem.getScheme());
      Assert.assertEquals(URI.create(InLineFileSystem.SCHEME), inlineFileSystem.getUri());
    }
  }

  @Test
  @Ignore // Disabling flaky test for now https://issues.apache.org/jira/browse/HUDI-786
  public void testFileSystemApis() throws IOException {
    OuterPathInfo outerPathInfo = generateOuterFileAndGetInfo(1000);
    Path inlinePath = FileSystemTestUtils.getPhantomFile(outerPathInfo.outerPath, outerPathInfo.startOffset, outerPathInfo.length);
    InLineFileSystem inlineFileSystem = (InLineFileSystem) inlinePath.getFileSystem(conf);
    FSDataInputStream fsDataInputStream = inlineFileSystem.open(inlinePath);
    byte[] actualBytes = new byte[outerPathInfo.expectedBytes.length];
    // verify pos
    Assert.assertEquals(0 - outerPathInfo.startOffset, fsDataInputStream.getPos());
    fsDataInputStream.readFully(0, actualBytes);
    Assert.assertArrayEquals(outerPathInfo.expectedBytes, actualBytes);

    // read partial data
    // test read(long position, byte[] buffer, int offset, int length)
    actualBytes = new byte[100];
    fsDataInputStream.read(0, actualBytes, 10, 10);
    verifyArrayEquality(outerPathInfo.expectedBytes, 0, 10, actualBytes, 10, 10);
    actualBytes = new byte[310];
    fsDataInputStream.read(25, actualBytes, 100, 210);
    verifyArrayEquality(outerPathInfo.expectedBytes, 25, 210, actualBytes, 100, 210);
    // give length to read > than actual inline content
    actualBytes = new byte[1100];
    try {
      fsDataInputStream.read(0, actualBytes, 0, 1101);
      Assert.fail("Should have thrown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // no op
    }

    // test readFully(long position, byte[] buffer, int offset, int length)
    actualBytes = new byte[100];
    fsDataInputStream.readFully(0, actualBytes, 10, 20);
    verifyArrayEquality(outerPathInfo.expectedBytes, 0, 10, actualBytes, 10, 10);
    actualBytes = new byte[310];
    fsDataInputStream.readFully(25, actualBytes, 100, 210);
    verifyArrayEquality(outerPathInfo.expectedBytes, 25, 210, actualBytes, 100, 210);
    // give length to read > than actual inline content
    actualBytes = new byte[1100];
    try {
      fsDataInputStream.readFully(0, actualBytes, 0, 1101);
      Assert.fail("Should have thrown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // no op
    }

    // test readFully(long position, byte[] buffer)
    actualBytes = new byte[100];
    fsDataInputStream.readFully(0, actualBytes);
    verifyArrayEquality(outerPathInfo.expectedBytes, 0, 100, actualBytes, 0, 100);
    actualBytes = new byte[310];
    fsDataInputStream.readFully(25, actualBytes);
    verifyArrayEquality(outerPathInfo.expectedBytes, 25, 210, actualBytes, 0, 210);
    // give length to read > than actual inline content
    actualBytes = new byte[1100];
    fsDataInputStream.readFully(0, actualBytes);
    verifyArrayEquality(outerPathInfo.expectedBytes, 0, 1000, actualBytes, 0, 1000);

    // TODO. seek does not move the position. need to investigate.
    // test seekToNewSource(long targetPos)
    /* fsDataInputStream.seekToNewSource(75);
    Assert.assertEquals(outerPathInfo.startOffset + 75, fsDataInputStream.getPos());
    fsDataInputStream.seekToNewSource(180);
    Assert.assertEquals(outerPathInfo.startOffset + 180, fsDataInputStream.getPos());
    fsDataInputStream.seekToNewSource(910);
    Assert.assertEquals(outerPathInfo.startOffset + 910, fsDataInputStream.getPos());
    */
    // test read(ByteBuffer buf)
    ByteBuffer actualByteBuffer = ByteBuffer.allocate(100);
    try {
      fsDataInputStream.read(actualByteBuffer);
      Assert.fail("Should have thrown");
    } catch (UnsupportedOperationException e) {
      // ignore
    }

    Assert.assertEquals(outerPathInfo.outerPath.getFileSystem(conf).open(outerPathInfo.outerPath).getFileDescriptor(), fsDataInputStream.getFileDescriptor());

    try {
      fsDataInputStream.setReadahead(10L);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }

    try {
      fsDataInputStream.setDropBehind(true);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }

    // yet to test
    // read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts)
    // releaseBuffer(ByteBuffer buffer)
    // unbuffer()

    fsDataInputStream.close();
  }

  private void verifyArrayEquality(byte[] expected, int expectedOffset, int expectedLength,
                                   byte[] actual, int actualOffset, int actualLength) {
    Assert.assertArrayEquals(Arrays.copyOfRange(expected, expectedOffset, expectedOffset + expectedLength), Arrays.copyOfRange(actual, actualOffset, actualOffset + actualLength));
  }

  private OuterPathInfo generateOuterFileAndGetInfo(int inlineContentSize) throws IOException {
    OuterPathInfo toReturn = new OuterPathInfo();
    Path outerPath = getRandomOuterFSPath();
    listOfGeneratedPaths.add(outerPath);
    toReturn.outerPath = outerPath;
    FSDataOutputStream wrappedOut = outerPath.getFileSystem(conf).create(outerPath, true);
    // append random bytes
    byte[] randomBytes = new byte[RANDOM.nextInt(1000)];
    RANDOM.nextBytes(randomBytes);
    wrappedOut.write(randomBytes);
    toReturn.startOffset = wrappedOut.getPos();
    // add inline content
    byte[] embeddedInlineBytes = new byte[inlineContentSize];
    RANDOM.nextBytes(embeddedInlineBytes);
    wrappedOut.write(embeddedInlineBytes);
    toReturn.expectedBytes = embeddedInlineBytes;
    toReturn.length = embeddedInlineBytes.length;
    // suffix random bytes
    randomBytes = new byte[RANDOM.nextInt(1000)];
    RANDOM.nextBytes(randomBytes);
    wrappedOut.write(randomBytes);
    wrappedOut.flush();
    wrappedOut.close();
    return toReturn;
  }

  @Test
  public void testOpen() throws IOException {
    Path inlinePath = getRandomInlinePath();
    // open non existant path
    try {
      inlinePath.getFileSystem(conf).open(inlinePath);
      Assert.fail("Should have thrown exception");
    } catch (FileNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testCreate() throws IOException {
    Path inlinePath = getRandomInlinePath();
    try {
      inlinePath.getFileSystem(conf).create(inlinePath, true);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testAppend() throws IOException {
    Path inlinePath = getRandomInlinePath();
    try {
      inlinePath.getFileSystem(conf).append(inlinePath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testRename() throws IOException {
    Path inlinePath = getRandomInlinePath();
    try {
      inlinePath.getFileSystem(conf).rename(inlinePath, inlinePath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testDelete() throws IOException {
    Path inlinePath = getRandomInlinePath();
    try {
      inlinePath.getFileSystem(conf).delete(inlinePath, true);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testgetWorkingDir() throws IOException {
    Path inlinePath = getRandomInlinePath();
    try {
      inlinePath.getFileSystem(conf).getWorkingDirectory();
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testsetWorkingDirectory() throws IOException {
    Path inlinePath = getRandomInlinePath();
    try {
      inlinePath.getFileSystem(conf).setWorkingDirectory(inlinePath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testExists() throws IOException {
    Path inlinePath = getRandomInlinePath();
    Assert.assertFalse(inlinePath.getFileSystem(conf).exists(inlinePath));
  }

  private Path getRandomInlinePath() {
    Path outerPath = getRandomOuterFSPath();
    listOfGeneratedPaths.add(outerPath);
    return FileSystemTestUtils.getPhantomFile(outerPath, 100, 100);
  }

  private void verifyFileStatus(FileStatus expected, Path inlinePath, long expectedLength, FileStatus actual) {
    Assert.assertEquals(inlinePath, actual.getPath());
    Assert.assertEquals(expectedLength, actual.getLen());
    Assert.assertEquals(expected.getAccessTime(), actual.getAccessTime());
    Assert.assertEquals(expected.getBlockSize(), actual.getBlockSize());
    Assert.assertEquals(expected.getGroup(), actual.getGroup());
    Assert.assertEquals(expected.getModificationTime(), actual.getModificationTime());
    Assert.assertEquals(expected.getOwner(), actual.getOwner());
    Assert.assertEquals(expected.getPermission(), actual.getPermission());
    Assert.assertEquals(expected.getReplication(), actual.getReplication());
  }

  class OuterPathInfo {
    Path outerPath;
    long startOffset;
    int length;
    byte[] expectedBytes;
  }
}
