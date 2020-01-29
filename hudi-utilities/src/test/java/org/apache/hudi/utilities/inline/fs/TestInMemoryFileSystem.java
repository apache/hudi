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

package org.apache.hudi.utilities.inline.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.apache.hudi.utilities.inline.fs.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.utilities.inline.fs.FileSystemTestUtils.getRandomOuterInMemPath;

/**
 * Unit tests {@link InMemoryFileSystem}.
 */
public class TestInMemoryFileSystem {

  private Configuration conf;

  public TestInMemoryFileSystem() {
    conf = new Configuration();
    conf.set("fs." + InMemoryFileSystem.SCHEME + ".impl", InMemoryFileSystem.class.getName());
  }

  @Test
  public void testCreateWriteGetFileAsBytes() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    FSDataOutputStream out = outerInMemFSPath.getFileSystem(conf).create(outerInMemFSPath, true);
    // write random bytes
    byte[] randomBytes = new byte[RANDOM.nextInt(1000)];
    RANDOM.nextBytes(randomBytes);
    out.write(randomBytes);
    out.close();
    InMemoryFileSystem inMemoryFileSystem = (InMemoryFileSystem) outerInMemFSPath.getFileSystem(conf);
    byte[] bytesRead = inMemoryFileSystem.getFileAsBytes();
    Assert.assertArrayEquals(randomBytes, bytesRead);
    Assert.assertEquals(InMemoryFileSystem.SCHEME, inMemoryFileSystem.getScheme());
    Assert.assertEquals(URI.create(outerInMemFSPath.toString()), inMemoryFileSystem.getUri());
  }

  @Test
  public void testOpen() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    Assert.assertNull(outerInMemFSPath.getFileSystem(conf).open(outerInMemFSPath));
  }

  @Test
  public void testAppend() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    Assert.assertNull(outerInMemFSPath.getFileSystem(conf).append(outerInMemFSPath));
  }

  @Test
  public void testRename() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    try {
      outerInMemFSPath.getFileSystem(conf).rename(outerInMemFSPath, outerInMemFSPath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testDelete() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    try {
      outerInMemFSPath.getFileSystem(conf).delete(outerInMemFSPath, true);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testgetWorkingDir() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    Assert.assertNull(outerInMemFSPath.getFileSystem(conf).getWorkingDirectory());
  }

  @Test
  public void testsetWorkingDirectory() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    try {
      outerInMemFSPath.getFileSystem(conf).setWorkingDirectory(outerInMemFSPath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testExists() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    try {
      outerInMemFSPath.getFileSystem(conf).exists(outerInMemFSPath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testFileStatus() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    try {
      outerInMemFSPath.getFileSystem(conf).getFileStatus(outerInMemFSPath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

  @Test
  public void testListStatus() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    try {
      outerInMemFSPath.getFileSystem(conf).listStatus(outerInMemFSPath);
      Assert.fail("Should have thrown exception");
    } catch (UnsupportedOperationException e) {
      // ignore
    }
  }

}
