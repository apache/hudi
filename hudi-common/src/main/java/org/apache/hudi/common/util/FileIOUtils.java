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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Bunch of utility methods for working with files and byte streams.
 */
public class FileIOUtils {
  public static final Logger LOG = LogManager.getLogger(FileIOUtils.class);
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
   * @param fileSystem     {@link FileSystem} instance.
   * @param sourceFilePath Source file path.
   * @param destFilePath   Destination file path.
   */
  public static void copy(
      FileSystem fileSystem, org.apache.hadoop.fs.Path sourceFilePath,
      org.apache.hadoop.fs.Path destFilePath) {
    FSDataInputStream fsDataInputStream = null;
    FSDataOutputStream fsDataOutputStream = null;
    try {
      fsDataInputStream = fileSystem.open(sourceFilePath);
      fsDataOutputStream = fileSystem.create(destFilePath, false);
      copy(fsDataInputStream, fsDataOutputStream);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Cannot copy from %s to %s",
          sourceFilePath.toString(), destFilePath.toString()), e);
    } finally {
      closeQuietly(fsDataInputStream);
      closeQuietly(fsDataOutputStream);
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
    PrintStream out = new PrintStream(new FileOutputStream(filePath));
    out.println(str);
    out.flush();
    out.close();
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
      LOG.warn("IOException during close", e);
    }
  }

  public static void createFileInPath(FileSystem fileSystem, org.apache.hadoop.fs.Path fullPath, Option<byte[]> content, boolean ignoreIOE) {
    try {
      // If the path does not exist, create it first
      if (!fileSystem.exists(fullPath)) {
        if (fileSystem.createNewFile(fullPath)) {
          LOG.info("Created a new file in meta path: " + fullPath);
        } else {
          throw new HoodieIOException("Failed to create file " + fullPath);
        }
      }

      if (content.isPresent()) {
        FSDataOutputStream fsout = fileSystem.create(fullPath, true);
        fsout.write(content.get());
        fsout.close();
      }
    } catch (IOException e) {
      LOG.warn("Failed to create file " + fullPath, e);
      if (!ignoreIOE) {
        throw new HoodieIOException("Failed to create file " + fullPath, e);
      }
    }
  }

  public static void createFileInPath(FileSystem fileSystem, org.apache.hadoop.fs.Path fullPath, Option<byte[]> content) {
    createFileInPath(fileSystem, fullPath, content, false);
  }

  public static Option<byte[]> readDataFromPath(FileSystem fileSystem, org.apache.hadoop.fs.Path detailPath, boolean ignoreIOE) {
    try (FSDataInputStream is = fileSystem.open(detailPath)) {
      return Option.of(FileIOUtils.readAsByteArray(is));
    } catch (IOException e) {
      LOG.warn("Could not read commit details from " + detailPath, e);
      if (!ignoreIOE) {
        throw new HoodieIOException("Could not read commit details from " + detailPath, e);
      }
      return Option.empty();
    }
  }

  public static Option<byte[]> readDataFromPath(FileSystem fileSystem, org.apache.hadoop.fs.Path detailPath) {
    return readDataFromPath(fileSystem, detailPath, false);
  }
}
