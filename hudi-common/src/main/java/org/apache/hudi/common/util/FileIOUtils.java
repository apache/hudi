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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Bunch of utility methods for working with files and byte streams.
 */
public class FileIOUtils {

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

  public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
    byte[] buffer = new byte[1024];
    int len;
    while ((len = inputStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, len);
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
}
