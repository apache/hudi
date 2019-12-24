package org.apache.hudi.utilities.inline.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import static org.apache.hudi.utilities.inline.fs.FileSystemTestUtils.getRandomOuterInMemPath;

public class TestInMemoryFileSystem {

  private Configuration conf;
  private final Random RANDOM = new Random();

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
    InMemoryFileSystem inMemoryFileSystem = (InMemoryFileSystem) outerInMemFSPath.getFileSystem(conf);
    byte[] bytesRead = inMemoryFileSystem.getFileAsBytes();
    Assert.assertArrayEquals(randomBytes, bytesRead);
    Assert.assertEquals(InMemoryFileSystem.SCHEME, inMemoryFileSystem.getScheme());
    Assert.assertEquals(URI.create(InMemoryFileSystem.SCHEME), inMemoryFileSystem.getUri());
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
