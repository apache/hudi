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

package org.apache.hudi.common.fs;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Ensures file create/delete operation is visible.
 */
public interface ConsistencyGuard {

  /**
   * File Visibility.
   */
  enum FileVisibility {
    APPEAR, DISAPPEAR,
  }

  /**
   * Wait for file to be listable based on configurable timeout.
   * 
   * @param filePath
   * @throws IOException when having trouble listing the path
   * @throws TimeoutException when retries exhausted
   */
  void waitTillFileAppears(Path filePath) throws IOException, TimeoutException;

  /**
   * Wait for file to be listable based on configurable timeout.
   * 
   * @param filePath
   * @throws IOException when having trouble listing the path
   * @throws TimeoutException when retries exhausted
   */
  void waitTillFileDisappears(Path filePath) throws IOException, TimeoutException;

  /**
   * Wait till all passed files belonging to a directory shows up in the listing.
   */
  void waitTillAllFilesAppear(String dirPath, List<String> files) throws IOException, TimeoutException;

  /**
   * Wait till all passed files belonging to a directory disappears from listing.
   */
  void waitTillAllFilesDisappear(String dirPath, List<String> files) throws IOException, TimeoutException;

  /**
   * Wait Till target visibility is reached.
   * 
   * @param dirPath Directory Path
   * @param files Files
   * @param targetVisibility Target Visibitlity
   * @throws IOException
   * @throws TimeoutException
   */
  default void waitTill(String dirPath, List<String> files, FileVisibility targetVisibility)
      throws IOException, TimeoutException {
    switch (targetVisibility) {
      case APPEAR: {
        waitTillAllFilesAppear(dirPath, files);
        break;
      }
      case DISAPPEAR: {
        waitTillAllFilesDisappear(dirPath, files);
        break;
      }
      default:
        throw new IllegalStateException("Unknown File Visibility");
    }
  }
}
