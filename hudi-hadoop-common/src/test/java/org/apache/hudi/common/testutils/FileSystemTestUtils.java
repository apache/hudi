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

package org.apache.hudi.common.testutils;

import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.hadoop.fs.inline.InMemoryFileSystem;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

/**
 * Utils class to assist in testing {@link InMemoryFileSystem} and {@link InLineFileSystem}.
 */
public class FileSystemTestUtils {

  public static final String TEMP = "tmp";
  public static final String FORWARD_SLASH = "/";
  public static final String FILE_SCHEME = "file";
  public static final String COLON = ":";
  public static final Random RANDOM = new Random();

  public static Path getRandomOuterInMemPath() {
    String randomFileName = UUID.randomUUID().toString();
    String fileSuffix = COLON + FORWARD_SLASH + TEMP + FORWARD_SLASH + randomFileName;
    return new Path(InMemoryFileSystem.SCHEME + fileSuffix);
  }

  public static StoragePath getRandomOuterFSPath() {
    String randomFileName = UUID.randomUUID().toString();
    String fileSuffix = COLON + FORWARD_SLASH + TEMP + FORWARD_SLASH + randomFileName;
    return new StoragePath(FILE_SCHEME + fileSuffix);
  }

  public static StoragePath getPhantomFile(StoragePath outerPath, long startOffset, long inlineLength) {
    // Generate phantom inline file
    return InLineFSUtils.getInlineFilePath(outerPath, FILE_SCHEME, startOffset, inlineLength);
  }

  public static void deleteFile(File fileToDelete) throws IOException {
    if (!fileToDelete.exists()) {
      return;
    }
    if (!fileToDelete.delete()) {
      String message =
          "Unable to delete file " + fileToDelete + ".";
      throw new IOException(message);
    }
  }
}
