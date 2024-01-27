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
import org.apache.hudi.storage.HoodieFileStatus;
import org.apache.hudi.storage.HoodieLocation;
import org.apache.hudi.storage.HoodieStorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
      for (HoodieFileStatus status : storage.listDirectEntries(new HoodieLocation(getTempDir()))) {
        HoodieLocation location = status.getLocation();
        if (status.isDirectory()) {
          storage.deleteDirectory(location);
        } else {
          storage.deleteFile(location);
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
  public void testCreateWriteAndRead() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    HoodieLocation location = new HoodieLocation(getTempDir(), "testCreateAppendAndRead/1.file");
    assertFalse(storage.exists(location));
    storage.create(location).close();
    validateFileStatus(storage, location, EMPTY_BYTES, false);

    byte[] data = new byte[] {2, 42, 49, (byte) 158, (byte) 233, 66, 9};

    // By default, create overwrites the file
    try (OutputStream stream = storage.create(location)) {
      stream.write(data);
      stream.flush();
    }
    validateFileStatus(storage, location, data, false);

    assertThrows(IOException.class, () -> storage.create(location, false));
    validateFileStatus(storage, location, data, false);

    assertThrows(IOException.class, () -> storage.create(location, false));
    validateFileStatus(storage, location, data, false);

    HoodieLocation location2 = new HoodieLocation(getTempDir(), "testCreateAppendAndRead/2.file");
    assertFalse(storage.exists(location2));
    assertTrue(storage.createNewFile(location2));
    validateFileStatus(storage, location2, EMPTY_BYTES, false);
    assertFalse(storage.createNewFile(location2));

    HoodieLocation location3 = new HoodieLocation(getTempDir(), "testCreateAppendAndRead/3.file");
    assertFalse(storage.exists(location3));
    storage.createImmutableFileInPath(location3, Option.of(data));
    validateFileStatus(storage, location3, data, false);

    HoodieLocation location4 = new HoodieLocation(getTempDir(), "testCreateAppendAndRead/4");
    assertFalse(storage.exists(location4));
    assertTrue(storage.createDirectory(location4));
    validateFileStatus(storage, location4, EMPTY_BYTES, true);
    assertTrue(storage.createDirectory(location4));
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

    validateHoodieFileStatusList(
        Arrays.stream(new HoodieFileStatus[] {
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/2.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/y"), 0, true, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/z"), 0, true, 0),
        }).collect(Collectors.toList()),
        storage.listDirectEntries(new HoodieLocation(getTempDir(), "x")));

    validateHoodieFileStatusList(
        Arrays.stream(new HoodieFileStatus[] {
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/2.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/y/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/y/2.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/z/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/z/2.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.listFiles(new HoodieLocation(getTempDir(), "x")));

    validateHoodieFileStatusList(
        Arrays.stream(new HoodieFileStatus[] {
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/2.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.listDirectEntries(
            new HoodieLocation(getTempDir(), "x"), e -> e.getName().contains("2")));

    validateHoodieFileStatusList(
        Arrays.stream(new HoodieFileStatus[] {
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "w/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "w/2.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/z/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/z/2.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.listDirectEntries(Arrays.stream(new HoodieLocation[] {
            new HoodieLocation(getTempDir(), "w"),
            new HoodieLocation(getTempDir(), "x/z")
        }).collect(Collectors.toList())));

    assertThrows(FileNotFoundException.class,
        () -> storage.listDirectEntries(new HoodieLocation(getTempDir(), "*")));

    validateHoodieFileStatusList(
        Arrays.stream(new HoodieFileStatus[] {
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/y/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/z/1.file"), 0, false, 0)
        }).collect(Collectors.toList()),
        storage.globEntries(new HoodieLocation(getTempDir(), "x/*/1.file")));

    validateHoodieFileStatusList(
        Arrays.stream(new HoodieFileStatus[] {
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/1.file"), 0, false, 0),
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/2.file"), 0, false, 0),
        }).collect(Collectors.toList()),
        storage.globEntries(new HoodieLocation(getTempDir(), "x/*.file")));

    validateHoodieFileStatusList(
        Arrays.stream(new HoodieFileStatus[] {
            new HoodieFileStatus(new HoodieLocation(getTempDir(), "x/y/1.file"), 0, false, 0),
        }).collect(Collectors.toList()),
        storage.globEntries(
            new HoodieLocation(getTempDir(), "x/*/*.file"),
            e -> e.getParent().getName().equals("y") && e.getName().contains("1")));
  }

  @Test
  public void testFileNotFound() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    HoodieLocation fileLocation = new HoodieLocation(getTempDir(), "testFileNotFound/1.file");
    HoodieLocation dirLocation = new HoodieLocation(getTempDir(), "testFileNotFound/2");
    assertFalse(storage.exists(fileLocation));
    assertThrows(FileNotFoundException.class, () -> storage.open(fileLocation));
    assertThrows(FileNotFoundException.class, () -> storage.getFileStatus(fileLocation));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(fileLocation));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(dirLocation));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(dirLocation, e -> true));
    assertThrows(FileNotFoundException.class, () -> storage.listDirectEntries(
        Arrays.stream(new HoodieLocation[] {dirLocation}).collect(Collectors.toList())));
  }

  @Test
  public void testRename() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    HoodieLocation location = new HoodieLocation(getTempDir(), "testRename/1.file");
    assertFalse(storage.exists(location));
    storage.create(location).close();
    validateFileStatus(storage, location, EMPTY_BYTES, false);

    HoodieLocation newLocation = new HoodieLocation(getTempDir(), "testRename/1_renamed.file");
    assertTrue(storage.rename(location, newLocation));
    assertFalse(storage.exists(location));
    validateFileStatus(storage, newLocation, EMPTY_BYTES, false);
  }

  @Test
  public void testDelete() throws IOException {
    HoodieStorage storage = getHoodieStorage();

    HoodieLocation location = new HoodieLocation(getTempDir(), "testDelete/1.file");
    assertFalse(storage.exists(location));
    storage.create(location).close();
    assertTrue(storage.exists(location));

    assertTrue(storage.deleteFile(location));
    assertFalse(storage.exists(location));
    assertFalse(storage.deleteFile(location));

    HoodieLocation location2 = new HoodieLocation(getTempDir(), "testDelete/2");
    assertFalse(storage.exists(location2));
    assertTrue(storage.createDirectory(location2));
    assertTrue(storage.exists(location2));

    assertTrue(storage.deleteDirectory(location2));
    assertFalse(storage.exists(location2));
    assertFalse(storage.deleteDirectory(location2));
  }

  @Test
  public void testMakeQualified() {
    HoodieStorage storage = getHoodieStorage();
    HoodieLocation location = new HoodieLocation("/tmp/testMakeQualified/1.file");
    assertEquals(
        new HoodieLocation("file:/tmp/testMakeQualified/1.file"),
        storage.makeQualified(location));
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
      storage.create(new HoodieLocation(dir, relativePath)).close();
    }
  }

  private HoodieStorage getHoodieStorage() {
    Object conf = getConf();
    return getHoodieStorage(getFileSystem(conf), conf);
  }

  private void validateFileStatus(HoodieStorage storage,
                                  HoodieLocation location,
                                  byte[] data,
                                  boolean isDirectory) throws IOException {
    assertTrue(storage.exists(location));
    HoodieFileStatus fileStatus = storage.getFileStatus(location);
    assertEquals(location, fileStatus.getLocation());
    assertEquals(isDirectory, fileStatus.isDirectory());
    assertEquals(!isDirectory, fileStatus.isFile());
    if (!isDirectory) {
      assertEquals(data.length, fileStatus.getLength());
      try (InputStream stream = storage.open(location)) {
        assertArrayEquals(data, IOUtils.readAsByteArray(stream, data.length));
      }
    }
    assertTrue(fileStatus.getModificationTime() > 0);
  }

  private void validateHoodieFileStatusList(List<HoodieFileStatus> expected,
                                            List<HoodieFileStatus> actual) {
    assertEquals(expected.size(), actual.size());
    List<HoodieFileStatus> sortedExpected = expected.stream()
        .sorted(Comparator.comparing(HoodieFileStatus::getLocation))
        .collect(Collectors.toList());
    List<HoodieFileStatus> sortedActual = actual.stream()
        .sorted(Comparator.comparing(HoodieFileStatus::getLocation))
        .collect(Collectors.toList());
    for (int i = 0; i < expected.size(); i++) {
      // We cannot use HoodieFileStatus#equals as that only compares the location
      assertEquals(sortedExpected.get(i).getLocation(), sortedActual.get(i).getLocation());
      assertEquals(sortedExpected.get(i).isDirectory(), sortedActual.get(i).isDirectory());
      assertEquals(sortedExpected.get(i).isFile(), sortedActual.get(i).isFile());
      if (sortedExpected.get(i).isFile()) {
        assertEquals(sortedExpected.get(i).getLength(), sortedActual.get(i).getLength());
      }
      assertTrue(sortedActual.get(i).getModificationTime() > 0);
    }
  }
}
