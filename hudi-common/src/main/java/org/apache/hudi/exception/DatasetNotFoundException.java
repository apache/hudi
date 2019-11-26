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

package org.apache.hudi.exception;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * <p>
 * Exception thrown to indicate that a hoodie dataset was not found on the path provided.
 * <p>
 */
public class DatasetNotFoundException extends HoodieException {

  public DatasetNotFoundException(String basePath) {
    super(getErrorMessage(basePath));
  }

  private static String getErrorMessage(String basePath) {
    return "Hoodie dataset not found in path " + basePath;
  }

  public static void checkValidDataset(FileSystem fs, Path basePathDir, Path metaPathDir)
      throws DatasetNotFoundException {
    // Check if the base path is found
    try {
      if (!fs.exists(basePathDir) || !fs.isDirectory(basePathDir)) {
        throw new DatasetNotFoundException(basePathDir.toString());
      }
      // Check if the meta path is found
      if (!fs.exists(metaPathDir) || !fs.isDirectory(metaPathDir)) {
        throw new DatasetNotFoundException(metaPathDir.toString());
      }
    } catch (IllegalArgumentException e) {
      // if the base path is file:///, then we have a IllegalArgumentException
      throw new DatasetNotFoundException(metaPathDir.toString());
    } catch (IOException e) {
      throw new HoodieIOException("Could not check if dataset " + basePathDir + " is valid dataset", e);
    }
  }
}
