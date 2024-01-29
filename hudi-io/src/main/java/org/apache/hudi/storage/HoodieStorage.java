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

package org.apache.hudi.storage;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides I/O APIs on files and directories on storage.
 * The APIs are mainly based on {@code org.apache.hadoop.fs.FileSystem} class.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieStorage implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(HoodieStorage.class);
  public static final String TMP_PATH_POSTFIX = ".tmp";

  /**
   * @return the scheme of the storage.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract String getScheme();

  /**
   * Creates an OutputStream at the indicated location.
   *
   * @param location  the file to create.
   * @param overwrite if a file with this name already exists, then if {@code true},
   *                  the file will be overwritten, and if {@code false} an exception will be thrown.
   * @return the OutputStream to write to.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract OutputStream create(HoodieLocation location, boolean overwrite) throws IOException;

  /**
   * Opens an InputStream at the indicated location.
   *
   * @param location the file to open.
   * @return the InputStream to read from.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract InputStream open(HoodieLocation location) throws IOException;

  /**
   * Appends to an existing file (optional operation).
   *
   * @param location the file to append.
   * @return the OutputStream to write to.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract OutputStream append(HoodieLocation location) throws IOException;

  /**
   * Checks if a location exists.
   *
   * @param location location to check.
   * @return {@code true} if the location exists.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean exists(HoodieLocation location) throws IOException;

  /**
   * Returns a file status object that represents the location.
   *
   * @param location location to check.
   * @return a {@link HoodieFileStatus} object.
   * @throws FileNotFoundException when the path does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieFileStatus getFileStatus(HoodieLocation location) throws IOException;

  /**
   * Creates the directory and non-existent parent directories.
   *
   * @param location location to create.
   * @return {@code true} if the directory was created.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean createDirectory(HoodieLocation location) throws IOException;

  /**
   * Lists the statuses of the direct files/directories in the given location if the path is a directory.
   *
   * @param location given location.
   * @return the statuses of the files/directories in the given location.
   * @throws FileNotFoundException when the location does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<HoodieFileStatus> listDirectEntries(HoodieLocation location) throws IOException;

  /**
   * Lists the statuses of all files under the give location recursively.
   *
   * @param location given location.
   * @return the statuses of the files under the given location.
   * @throws FileNotFoundException when the location does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<HoodieFileStatus> listFiles(HoodieLocation location) throws IOException;

  /**
   * Lists the statuses of the direct files/directories in the given location
   * and filters the results, if the path is a directory.
   *
   * @param location given location.
   * @param filter   filter to apply.
   * @return the statuses of the files/directories in the given location.
   * @throws FileNotFoundException when the location does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<HoodieFileStatus> listDirectEntries(HoodieLocation location,
                                                           HoodieLocationFilter filter) throws IOException;

  /**
   * Returns all the files that match the locationPattern and are not checksum files,
   * and filters the results.
   *
   * @param locationPattern given pattern.
   * @param filter          filter to apply.
   * @return the statuses of the files.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<HoodieFileStatus> globEntries(HoodieLocation locationPattern,
                                                     HoodieLocationFilter filter) throws IOException;

  /**
   * Renames the location from old to new.
   *
   * @param oldLocation source location.
   * @param newLocation destination location.
   * @return {@true} if rename is successful.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean rename(HoodieLocation oldLocation,
                                 HoodieLocation newLocation) throws IOException;

  /**
   * Deletes a directory at location.
   *
   * @param location directory to delete.
   * @return {@code true} if successful.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean deleteDirectory(HoodieLocation location) throws IOException;

  /**
   * Deletes a file at location.
   *
   * @param location file to delete.
   * @return {@code true} if successful.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean deleteFile(HoodieLocation location) throws IOException;

  /**
   * Qualifies a path to one which uses this storage and, if relative, made absolute.
   *
   * @param location to qualify.
   * @return Qualified location.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieLocation makeQualified(HoodieLocation location);

  /**
   * @return the underlying file system instance if exists.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract Object getFileSystem();

  /**
   * @return the underlying configuration instance if exists.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract Object getConf();

  /**
   * Creates a new file with overwrite set to false. This ensures files are created
   * only once and never rewritten, also, here we take care if the content is not
   * empty, will first write the content to a temp file if {needCreateTempFile} is
   * true, and then rename it back after the content is written.
   *
   * @param location file Path.
   * @param content  content to be stored.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public final void createImmutableFileInPath(HoodieLocation location,
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

  /**
   * @return whether a temporary file needs to be created for immutability.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public final boolean needCreateTempFile() {
    return StorageSchemes.HDFS.getScheme().equals(getScheme());
  }

  /**
   * Create an OutputStream at the indicated location.
   * The file is overwritten by default.
   *
   * @param location the file to create.
   * @return the OutputStream to write to.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public OutputStream create(HoodieLocation location) throws IOException {
    return create(location, true);
  }

  /**
   * Creates an empty new file at the indicated location.
   *
   * @param location the file to create.
   * @return {@code true} if successfully created; {@code false} if already exists.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public boolean createNewFile(HoodieLocation location) throws IOException {
    if (exists(location)) {
      return false;
    } else {
      create(location, false).close();
      return true;
    }
  }

  /**
   * Lists the statuses of the direct files/directories in the given list of locations,
   * if the locations are directory.
   *
   * @param locationList given location list.
   * @return the statuses of the files/directories in the given locations.
   * @throws FileNotFoundException when the location does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public List<HoodieFileStatus> listDirectEntries(List<HoodieLocation> locationList) throws IOException {
    List<HoodieFileStatus> result = new ArrayList<>();
    for (HoodieLocation location : locationList) {
      result.addAll(listDirectEntries(location));
    }
    return result;
  }

  /**
   * Returns all the files that match the locationPattern and are not checksum files.
   *
   * @param locationPattern given pattern.
   * @return the statuses of the files.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public List<HoodieFileStatus> globEntries(HoodieLocation locationPattern) throws IOException {
    return globEntries(locationPattern, e -> true);
  }
}
