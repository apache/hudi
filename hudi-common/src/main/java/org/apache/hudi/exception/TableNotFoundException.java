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

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * <p>
 * Exception thrown to indicate that a hoodie table was not found on the path provided.
 * <p>
 */
public class TableNotFoundException extends HoodieException {

  public TableNotFoundException(String basePath) {
    super(getErrorMessage(basePath));
  }

  public TableNotFoundException(String basePath, Throwable t) {
    super(getErrorMessage(basePath), t);
  }

  private static String getErrorMessage(String basePath) {
    return "Hoodie table not found in path " + basePath;
  }

  public static void checkTableValidity(HoodieStorage storage, StoragePath basePathDir, StoragePath metaPathDir) {
    // Check if the base and meta paths are found
    try {
      // Since metaPath is within the basePath, it is enough to check the metaPath exists
      StoragePathInfo pathInfo = storage.getPathInfo(metaPathDir);
      if (!pathInfo.isDirectory()) {
        throw new TableNotFoundException(metaPathDir.toString());
      }
    } catch (FileNotFoundException | IllegalArgumentException e) {
      // if the base path is file:///, then we have a IllegalArgumentException
      throw new TableNotFoundException(metaPathDir.toString(), e);
    } catch (IOException e) {
      throw new HoodieIOException("Could not check if " + basePathDir + " is a valid table", e);
    }
  }
}
