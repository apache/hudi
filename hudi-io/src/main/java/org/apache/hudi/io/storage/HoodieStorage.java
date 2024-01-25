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
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public interface HoodieStorage extends Closeable {
  static final Logger LOG = LoggerFactory.getLogger(HoodieStorage.class);
  static final String TMP_PATH_POSTFIX = ".tmp";

  String getScheme();

  OutputStream create(HoodieLocation location, boolean overwrite) throws IOException;

  InputStream open(HoodieLocation location) throws IOException;

  SeekableDataInputStream openLogFile(HoodieLocation location, int bufferSize) throws IOException;

  OutputStream append(HoodieLocation location) throws IOException;

  boolean exists(HoodieLocation location) throws IOException;

  // should throw FileNotFoundException if not found
  HoodieFileStatus getFileStatus(HoodieLocation location) throws IOException;

  boolean createDirectory(HoodieLocation location) throws IOException;

  List<HoodieFileStatus> listDirectEntries(HoodieLocation location) throws IOException;

  List<HoodieFileStatus> listFiles(HoodieLocation location) throws IOException;

  List<HoodieFileStatus> listDirectEntries(HoodieLocation location,
                                           HoodieLocationFilter filter) throws IOException;

  List<HoodieFileStatus> globEntries(HoodieLocation location, HoodieLocationFilter filter)
      throws IOException;

  boolean rename(HoodieLocation oldLocation, HoodieLocation newLocation) throws IOException;

  boolean deleteDirectory(HoodieLocation location) throws IOException;

  boolean deleteFile(HoodieLocation location) throws IOException;

  HoodieLocation makeQualified(HoodieLocation location);

  Object getFileSystem();

  Object getConf();

  default OutputStream create(HoodieLocation location) throws IOException {
    return create(location, true);
  }

  /**
   * Creates a new file with overwrite set to false. This ensures files are created
   * only once and never rewritten, also, here we take care if the content is not
   * empty, will first write the content to a temp file if {needCreateTempFile} is
   * true, and then rename it back after the content is written.
   *
   * @param location file Path
   * @param content  content to be stored
   */
  default void createImmutableFileInPath(HoodieLocation location,
                                         Option<byte[]> content) throws IOException {
    OutputStream fsout = null;
    HoodieLocation tmpLocation = null;

    boolean needTempFile = needCreateTempFile();

    try {
      if (!content.isPresent()) {
        fsout = create(location, false);
      }

      if (content.isPresent() && needTempFile) {
        HoodieLocation parent = location.getParent();
        tmpLocation = new HoodieLocation(parent, location.getName() + TMP_PATH_POSTFIX);
        fsout = create(tmpLocation, false);
        fsout.write(content.get());
      }

      if (content.isPresent() && !needTempFile) {
        fsout = create(location, false);
        fsout.write(content.get());
      }
    } catch (IOException e) {
      String errorMsg = "Failed to create file " + (tmpLocation != null ? tmpLocation : location);
      throw new HoodieIOException(errorMsg, e);
    } finally {
      try {
        if (null != fsout) {
          fsout.close();
        }
      } catch (IOException e) {
        String errorMsg = "Failed to close file " + (needTempFile ? tmpLocation : location);
        throw new HoodieIOException(errorMsg, e);
      }

      boolean renameSuccess = false;
      try {
        if (null != tmpLocation) {
          renameSuccess = rename(tmpLocation, location);
        }
      } catch (IOException e) {
        throw new HoodieIOException(
            "Failed to rename " + tmpLocation + " to the target " + location,
            e);
      } finally {
        if (!renameSuccess && null != tmpLocation) {
          try {
            deleteFile(tmpLocation);
            LOG.warn("Fail to rename " + tmpLocation + " to " + location
                + ", target file exists: " + exists(location));
          } catch (IOException e) {
            throw new HoodieIOException("Failed to delete tmp file " + tmpLocation, e);
          }
        }
      }
    }
  }

  default boolean createNewFile(HoodieLocation location) throws IOException {
    if (exists(location)) {
      return false;
    } else {
      create(location, false).close();
      return true;
    }
  }

  default List<HoodieFileStatus> listDirectEntries(List<HoodieLocation> locationList) throws IOException {
    List<HoodieFileStatus> result = new ArrayList<>();
    for (HoodieLocation location : locationList) {
      result.addAll(listDirectEntries(location));
    }
    return result;
  }

  default List<HoodieFileStatus> globEntries(HoodieLocation location) throws IOException {
    return globEntries(location, e -> true);
  }

  default boolean needCreateTempFile() {
    return StorageSchemes.HDFS.getScheme().equals(getScheme());
  }
}
