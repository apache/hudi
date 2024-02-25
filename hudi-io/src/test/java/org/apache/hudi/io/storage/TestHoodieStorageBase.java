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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.util.IOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for testing different implementation of {@link HoodieStorage}.
 */
public abstract class TestHoodieStorageBase {
  @TempDir
  protected Path tempDir;

  protected static final String[] RELATIVE_FILE_PATHS = new String[] {
      "w/1.file", "w/2.file", "x/1.file", "x/2.file",
      "x/y/1.file", "x/y/2.file", "x/z/1.file", "x/z/2.file"
  };
  private static final byte[] EMPTY_BYTES = new byte[] {};

  /**
   * @param fs   file system instance.
   * @param conf configuration instance.
   * @return {@link HoodieStorage} instance based on the implementation for testing.
   */
  protected abstract HoodieStorage getHoodieStorage(Object fs, Object conf);

  /**
   * @param conf configuration instance.
   * @return the underlying file system instance used if required.
   */
  protected abstract Object getFileSystem(Object conf);

  /**
   * @return configurations for the storage.
   */
  protected abstract Object getConf();

  @AfterEach
  public void cleanUpTempDir() {
    HoodieStorage storage = getHoodieStorage();
    try {
      for (StoragePathInfo pathInfo : storage.listDirectEntries(new StoragePath(getTempDir()))) {
        StoragePath path = pathInfo.getPath();
        if (pathInfo.isDirectory()) {
          storage.deleteDirectory(path);
        } else {
          storage.deleteFile(path);
        }
      }
    } catch (IOException e) {
      // Silently fail
    }
  }

  @Test
  public void testGetScheme() {
    assertEquals("file", getHoodieStorage().getScheme());
  }

  @Test
  public void testGetUri() throws URISyntaxException {
    assertEquals(new URI("file:///"), getHoodieStorage().getUri());
  }

  @Test
  public void testCreateWriteAndRead() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    StoragePath path = new StoragePath(getTempDir(), "testCreateAppendAndRead/1.file");
    assertFalse(storage.exists(path));
    storage.create(path).close();
    validatePathInfo(storage, path, EMPTY_BYTES, false);

    byte[] data = new byte[] {2, 42, 49, (byte) 158, (byte) 233, 66, 9};

    // By default, create overwrites the file
    try (OutputStream stream = storage.create(path)) {
      stream.write(data);
      stream.flush();
    }
    validatePathInfo(storage, path, data, false);

    assertThrows(IOException.class, () -> storage.create(path, false));
    validatePathInfo(storage, path, data, false);

    assertThrows(IOException.class, () -> storage.create(path, false));
    validatePathInfo(storage, path, data, false);

    StoragePath path2 = new StoragePath(getTempDir(), "testCreateAppendAndRead/2.file");
    assertFalse(storage.exists(path2));
    assertTrue(storage.createNewFile(path2));
    validatePathInfo(storage, path2, EMPTY_BYTES, false);
    assertFalse(storage.createNewFile(path2));

    StoragePath path3 = new StoragePath(getTempDir(), "testCreateAppendAndRead/3.file");
    assertFalse(storage.exists(path3));
    storage.createImmutableFileInPath(path3, Option.of(data));
    validatePathInfo(storage, path3, data, false);

    StoragePath path4 = new StoragePath(getTempDir(), "testCreateAppendAndRead/4");
    assertFalse(storage.exists(path4));
    assertTrue(storage.createDirectory(path4));
    validatePathInfo(storage, path4, EMPTY_BYTES, true);
    assertTrue(storage.createDirectory(path4));
  }

  @Test
  public void testListing() throws IOException {
    HoodieStorage storage = getHoodieStorage();
    // Full list:
    // w/1.file
    // w/2.file
    // x/1.file
    // x/2.file
    // x/y/1.file
    // x/y/2.file
    // x/z/1.file
    // x/z/2.file
    prepareFilesOnStorage(storage);

    validatePathInfoList(
        Arrays.stream(new StoragePathInfo[] {
            new StoragePathInfo(new StoragePath(getTempDir(), "x/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/2.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/y"), 0, true, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/z"), 0, true, 0),
        }).collect(Collectors.toList()),
        storage.listDirectEntries(new StoragePath(getTempDir(), "x")));

    validatePathInfoList(
        Arrays.stream(new StoragePathInfo[] {
            new StoragePathInfo(new StoragePath(getTempDir(), "x/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/2.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/y/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/y/2.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/z/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/z/2.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.listFiles(new StoragePath(getTempDir(), "x")));

    validatePathInfoList(
        Arrays.stream(new StoragePathInfo[] {
            new StoragePathInfo(new StoragePath(getTempDir(), "x/2.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.listDirectEntries(
            new StoragePath(getTempDir(), "x"), e -> e.getName().contains("2")));

    validatePathInfoList(
        Arrays.stream(new StoragePathInfo[] {
            new StoragePathInfo(new StoragePath(getTempDir(), "w/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "w/2.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/z/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/z/2.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.listDirectEntries(Arrays.stream(new StoragePath[] {
            new StoragePath(getTempDir(), "w"),
            new StoragePath(getTempDir(), "x/z")
        }).collect(Collectors.toList())));

    assertThrows(FileNotFoundException.class,
        () -> storage.listDirectEntries(new StoragePath(getTempDir(), "*")));

    validatePathInfoList(
        Arrays.stream(new StoragePathInfo[] {
            new StoragePathInfo(new StoragePath(getTempDir(), "x/y/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/z/1.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.globEntries(new StoragePath(getTempDir(), "x/*/1.file")));

    validatePathInfoList(
        Arrays.stream(new StoragePathInfo[] {
            new StoragePathInfo(new StoragePath(getTempDir(), "x/1.file"), 0, false, 0),
            new StoragePathInfo(new StoragePath(getTempDir(), "x/2.file"), 0, false, 0),
        }).collect(Collectors.toList()),
        storage.globEntries(new StoragePath(getTempDir(), "x/*.file")));

    validatePathInfoList(
        Arrays.stream(new StoragePathInfo[] {
            new StoragePathInfo(new StoragePath(getTempDir(), "x/y/1.file"), 0, false, 0),
        }).collect(Collectors.toList()),
        storage.globEntries(
            new StoragePath(getTempDir(), "x/*/*.file"),
            e -> e.getParent().getName().equals("y") && e.getName().contains("1")));
  }

  @Test
  public void testFileNotFound() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    StoragePath filePath = new StoragePath(getTempDir(), "testFileNotFound/1.file");
    StoragePath dirPath = new StoragePath(getTempDir(), "testFileNotFound/2");
    assertFalse(storage.exists(filePath));
    assertThrows(FileNotFoundException.class, () -> storage.open(filePath));
    assertThrows(FileNotFoundException.class, () -> storage.getPathInfo(filePath));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(filePath));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(dirPath));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(dirPath, e -> true));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(
        Arrays.stream(new StoragePath[] {dirPath}).collect(Collectors.toList())));
  }

  @Test
  public void testRename() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    StoragePath path = new StoragePath(getTempDir(), "testRename/1.file");
    assertFalse(storage.exists(path));
    storage.create(path).close();
    validatePathInfo(storage, path, EMPTY_BYTES, false);

    StoragePath newPath = new StoragePath(getTempDir(), "testRename/1_renamed.file");
    assertTrue(storage.rename(path, newPath));
    assertFalse(storage.exists(path));
    validatePathInfo(storage, newPath, EMPTY_BYTES, false);
  }

  @Test
  public void testDelete() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    StoragePath path = new StoragePath(getTempDir(), "testDelete/1.file");
    assertFalse(storage.exists(path));
    storage.create(path).close();
    assertTrue(storage.exists(path));

    assertTrue(storage.deleteFile(path));
    assertFalse(storage.exists(path));
    assertFalse(storage.deleteFile(path));

    StoragePath path2 = new StoragePath(getTempDir(), "testDelete/2");
    assertFalse(storage.exists(path2));
    assertTrue(storage.createDirectory(path2));
    assertTrue(storage.exists(path2));

    assertTrue(storage.deleteDirectory(path2));
    assertFalse(storage.exists(path2));
    assertFalse(storage.deleteDirectory(path2));
  }

  @Test
  public void testMakeQualified() {
    HoodieStorage storage = getHoodieStorage();
    StoragePath path = new StoragePath("/tmp/testMakeQualified/1.file");
    assertEquals(
        new StoragePath("file:/tmp/testMakeQualified/1.file"),
        storage.makeQualified(path));
  }

  @Test
  public void testGetFileSystem() {
    Object conf = getConf();
    Object fs = getFileSystem(conf);
    HoodieStorage storage = getHoodieStorage(fs, conf);
    assertSame(fs, storage.getFileSystem());
  }

  protected String getTempDir() {
    return "file:" + tempDir.toUri().getPath();
  }

  /**
   * Prepares files on storage for testing.
   *
   * @storage {@link HoodieStorage} to use.
   */
  private void prepareFilesOnStorage(HoodieStorage storage) throws IOException {
    String dir = getTempDir();
    for (String relativePath : RELATIVE_FILE_PATHS) {
      storage.create(new StoragePath(dir, relativePath)).close();
    }
  }

  private HoodieStorage getHoodieStorage() {
    Object conf = getConf();
    return getHoodieStorage(getFileSystem(conf), conf);
  }

  private void validatePathInfo(HoodieStorage storage,
                                StoragePath path,
                                byte[] data,
                                boolean isDirectory) throws IOException {
    assertTrue(storage.exists(path));
    StoragePathInfo pathInfo = storage.getPathInfo(path);
    assertEquals(path, pathInfo.getPath());
    assertEquals(isDirectory, pathInfo.isDirectory());
    assertEquals(!isDirectory, pathInfo.isFile());
    if (!isDirectory) {
      assertEquals(data.length, pathInfo.getLength());
      try (InputStream stream = storage.open(path)) {
        assertArrayEquals(data, IOUtils.readAsByteArray(stream, data.length));
      }
    }
    assertTrue(pathInfo.getModificationTime() > 0);
  }

  private void validatePathInfoList(List<StoragePathInfo> expected,
                                    List<StoragePathInfo> actual) {
    assertEquals(expected.size(), actual.size());
    List<StoragePathInfo> sortedExpected = expected.stream()
        .sorted(Comparator.comparing(StoragePathInfo::getPath))
        .collect(Collectors.toList());
    List<StoragePathInfo> sortedActual = actual.stream()
        .sorted(Comparator.comparing(StoragePathInfo::getPath))
        .collect(Collectors.toList());
    for (int i = 0; i < expected.size(); i++) {
      // We cannot use StoragePathInfo#equals as that only compares the path
      assertEquals(sortedExpected.get(i).getPath(), sortedActual.get(i).getPath());
      assertEquals(sortedExpected.get(i).isDirectory(), sortedActual.get(i).isDirectory());
      assertEquals(sortedExpected.get(i).isFile(), sortedActual.get(i).isFile());
      if (sortedExpected.get(i).isFile()) {
        assertEquals(sortedExpected.get(i).getLength(), sortedActual.get(i).getLength());
      }
      assertTrue(sortedActual.get(i).getModificationTime() > 0);
    }
  }
}
