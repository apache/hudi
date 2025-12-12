/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.io.util;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Bunch of utility methods for working with files and byte streams.
 */
public class FileIOUtils {
  public static final Logger LOG = LoggerFactory.getLogger(FileIOUtils.class);
  public static final long KB = 1024;

  public static void deleteDirectory(File directory) throws IOException {
    if (directory.exists()) {
      Files.walk(directory.toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      directory.delete();
      if (directory.exists()) {
        throw new IOException("Unable to delete directory " + directory);
      }
    }
  }

  public static void mkdir(File directory) throws IOException {
    if (!directory.exists()) {
      directory.mkdirs();
    }

    if (!directory.isDirectory()) {
      throw new IOException("Unable to create :" + directory);
    }
  }

  public static String readAsUTFString(InputStream input) throws IOException {
    return readAsUTFString(input, 128);
  }

  public static String readAsUTFString(InputStream input, int length) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
    copy(input, bos);
    return new String(bos.toByteArray(), StandardCharsets.UTF_8);
  }

  /**
   * Reads the input stream into String lines.
   *
   * @param input {@code InputStream} instance.
   * @return String lines in a list.
   */
  public static List<String> readAsUTFStringLines(InputStream input) {
    List<String> lines = new ArrayList<>();
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
    lines = bufferedReader.lines().collect(Collectors.toList());
    closeQuietly(bufferedReader);
    return lines;
  }

  public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
    byte[] buffer = new byte[1024];
    int len;
    while ((len = inputStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, len);
    }
  }

  /**
   * Copies the file content from source path to destination path.
   *
   * @param storage     {@link HoodieStorage} instance.
   * @param sourceFilePath Source file path.
   * @param destFilePath   Destination file path.
   */
  public static void copy(HoodieStorage storage,
                          StoragePath sourceFilePath,
                          StoragePath destFilePath) {
    InputStream inputStream = null;
    OutputStream outputStream = null;
    try {
      inputStream = storage.open(sourceFilePath);
      outputStream = storage.create(destFilePath, false);
      copy(inputStream, outputStream);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Cannot copy from %s to %s",
          sourceFilePath.toString(), destFilePath.toString()), e);
    } finally {
      closeQuietly(inputStream);
      closeQuietly(outputStream);
    }
  }

  public static byte[] readAsByteArray(InputStream input) throws IOException {
    return readAsByteArray(input, 128);
  }

  public static byte[] readAsByteArray(InputStream input, int outputSize) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(outputSize);
    copy(input, bos);
    return bos.toByteArray();
  }

  public static void writeStringToFile(String str, String filePath) throws IOException {
    try (PrintStream out = new PrintStream(new FileOutputStream(filePath))) {
      out.println(str);
      out.flush();
    }
  }

  /**
   * Closes {@code Closeable} quietly.
   *
   * @param closeable {@code Closeable} to close
   */
  public static void closeQuietly(Closeable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException e) {
      LOG.warn("Failed to close the closeable", e);
    }
  }

  public static void createFileInPath(HoodieStorage storage,
                                      StoragePath fullPath,
                                      Option<HoodieInstantWriter> contentWriter, boolean ignoreIOE) {
    try {
      if (contentWriter.isPresent()) {
        try (OutputStream out = storage.create(fullPath, true)) {
          contentWriter.get().writeToStream(out);
        }
      } else {
        storage.createNewFile(fullPath);
      }
    } catch (IOException e) {
      if (!ignoreIOE) {
        throw new HoodieIOException("Failed to create file " + fullPath, e);
      } else {
        LOG.warn("Failed to create file {}", fullPath, e);
      }
    }
  }

  public static void createFileInPath(HoodieStorage storage, StoragePath fullPath, Option<HoodieInstantWriter> contentWriter) {
    createFileInPath(storage, fullPath, contentWriter, false);
  }

  public static boolean copy(HoodieStorage srcStorage, StoragePath src,
                             HoodieStorage dstStorage, StoragePath dst,
                             boolean deleteSource,
                             boolean overwrite) throws IOException {
    StoragePathInfo pathInfo = srcStorage.getPathInfo(src);
    return copy(srcStorage, pathInfo, dstStorage, dst, deleteSource, overwrite);
  }

  /**
   * Copy files between FileSystems.
   */
  public static boolean copy(HoodieStorage srcStorage, StoragePathInfo srcPathInfo,
                             HoodieStorage dstStorage, StoragePath dst,
                             boolean deleteSource,
                             boolean overwrite) throws IOException {
    StoragePath src = srcPathInfo.getPath();
    if (srcPathInfo.isDirectory()) {
      if (!dstStorage.createDirectory(dst)) {
        return false;
      }
      List<StoragePathInfo> contents = srcStorage.listDirectEntries(src);
      for (StoragePathInfo subPathInfo : contents) {
        copy(srcStorage, subPathInfo, dstStorage,
            new StoragePath(dst, subPathInfo.getPath().getName()),
            deleteSource, overwrite);
      }
    } else {
      try (InputStream in = srcStorage.open(src);
           OutputStream out = dstStorage.create(dst, overwrite)) {
        copy(in, out);
      } catch (IOException e) {
        throw new IOException(
            "Error copying source file " + src + " to the destination file " + dst, e);
      }
    }
    if (deleteSource) {
      if (srcPathInfo.isDirectory()) {
        return srcStorage.deleteDirectory(src);
      }
      return srcStorage.deleteFile(src);
    } else {
      return true;
    }
  }

  public static Option<byte[]> readDataFromPath(HoodieStorage storage, StoragePath detailPath, boolean ignoreIOE) {
    try (InputStream is = storage.open(detailPath)) {
      return Option.of(FileIOUtils.readAsByteArray(is));
    } catch (FileNotFoundException fnfe) {
      LOG.debug("No file found at path {}", detailPath);
      return Option.empty();
    } catch (IOException e) {
      if (!ignoreIOE) {
        throw new HoodieIOException("Could not read commit details from " + detailPath, e);
      } else {
        LOG.warn("Could not read commit details from {}", detailPath, e);
      }
      return Option.empty();
    }
  }

  public static Option<byte[]> readDataFromPath(HoodieStorage storage, StoragePath detailPath) {
    return readDataFromPath(storage, detailPath, false);
  }

  /**
   * Return the configured local directories where hudi can write files. This
   * method does not create any directories on its own, it only encapsulates the
   * logic of locating the local directories according to deployment mode.
   */
  public static String[] getConfiguredLocalDirs() {
    if (isRunningInYarnContainer()) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available. Note this assumes that Yarn has
      // created the directories already, and that they are secured so that only the
      // user has access to them.
      return getYarnLocalDirs().split(",");
    } else if (System.getProperty("java.io.tmpdir") != null) {
      return System.getProperty("java.io.tmpdir").split(",");
    } else {
      return null;
    }
  }

  private static boolean isRunningInYarnContainer() {
    // These environment variables are set by YARN.
    return System.getenv("CONTAINER_ID") != null
        && System.getenv("LOCAL_DIRS") != null;
  }

  /**
   * Get the Yarn approved local directories.
   */
  private static String getYarnLocalDirs() {
    String localDirs = System.getenv("LOCAL_DIRS");

    if (localDirs == null) {
      throw new HoodieIOException("Yarn Local dirs can't be empty");
    }
    return localDirs;
  }

  public static String getDefaultSpillableMapBasePath() {
    String[] localDirs = getConfiguredLocalDirs();
    if (localDirs == null) {
      return "/tmp/";
    }
    List<String> localDirLists = Arrays.asList(localDirs);
    Collections.shuffle(localDirLists);
    return !localDirLists.isEmpty() ? localDirLists.get(0) : "/tmp/";
  }
}
