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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.FileIOUtils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * This interface provides the map interface for storing records in disk after they
 * spill over from memory. Used by {@link ExternalSpillableMap}.
 *
 * @param <T> The generic type of the keys
 * @param <R> The generic type of the values
 */
public abstract class DiskMap<T extends Serializable, R extends Serializable> implements Map<T, R>, Iterable<R> {

  private static final Logger LOG = LogManager.getLogger(DiskMap.class);
  private static final String SUBFOLDER_PREFIX = "hudi";
  // Base path for the write file
  protected final String diskMapPath;

  public DiskMap(String basePath, String prefix) throws IOException {
    this.diskMapPath =
        String.format("%s/%s-%s-%s", basePath, SUBFOLDER_PREFIX, prefix, UUID.randomUUID().toString());
    FileIOUtils.deleteDirectory(new File(diskMapPath));
    FileIOUtils.mkdir(new File(diskMapPath));
  }

  /**
   * @returns a stream of the values stored in the disk.
   */
  abstract Stream<R> valueStream();

  /**
   * Number of bytes spilled to disk.
   */
  abstract long sizeOfFileOnDiskInBytes();

  /**
   * Cleanup.
   */
  void close() {
    try {
      FileIOUtils.deleteDirectory(new File(diskMapPath));
    } catch (IOException exception) {
      LOG.warn("Error while deleting the disk map directory=" + diskMapPath, exception);
    }
  }
}
