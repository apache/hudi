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
import org.apache.hudi.io.SeekableDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Provides I/O APIs on files and directories on storage.
 * The APIs are mainly based on {@code org.apache.hadoop.fs.FileSystem} class.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieStorage implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(HoodieStorage.class);

  protected final StorageConfiguration<?> storageConf;

  public HoodieStorage(StorageConfiguration<?> storageConf) {
    this.storageConf = storageConf;
  }

  /**
   * @param path        path to instantiate the storage.
   * @param storageConf new storage configuration.
   * @return new {@link HoodieStorage} instance with the configuration.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieStorage newInstance(StoragePath path,
                                            StorageConfiguration<?> storageConf);

  /**
   * @return the scheme of the storage.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract String getScheme();

  /**
   * @return the default block size.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract int getDefaultBlockSize(StoragePath path);

  /**
   * @return the default buffer size.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract int getDefaultBufferSize();

  /**
   * @return the default block replication
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract short getDefaultReplication(StoragePath path);

  /**
   * Returns a URI which identifies this HoodieStorage.
   *
   * @return the URI of this storage instance.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract URI getUri();

  /**
   * Creates an OutputStream at the indicated path.
   *
   * @param path      the file to create.
   * @param overwrite if a file with this name already exists, then if {@code true},
   *                  the file will be overwritten, and if {@code false} an exception will be thrown.
   * @return the OutputStream to write to.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract OutputStream create(StoragePath path, boolean overwrite) throws IOException;

  /**
   * Creates an OutputStream at the indicated path.
   *
   * @param path          the file to create
   * @param overwrite     if a file with this name already exists, then if {@code true},
   *                      the file will be overwritten, and if {@code false} an exception will be thrown.
   * @param bufferSize    the size of the buffer to be used
   * @param replication   required block replication for the file
   * @param sizeThreshold block size
   * @return the OutputStream to write to.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract OutputStream create(StoragePath path, boolean overwrite, Integer bufferSize, Short replication, Long sizeThreshold) throws IOException;

  /**
   * Opens an InputStream at the indicated path.
   *
   * @param path the file to open.
   * @return the InputStream to read from.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract InputStream open(StoragePath path) throws IOException;

  /**
   * Opens an SeekableDataInputStream at the indicated path with seeks supported.
   *
   * @param path       the file to open.
   * @param bufferSize buffer size to use.
   * @param wrapStream true if we want to wrap the inputstream based on filesystem specific criteria
   * @return the InputStream to read from.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream) throws IOException;

  /**
   * Appends to an existing file (optional operation).
   *
   * @param path the file to append.
   * @return the OutputStream to write to.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract OutputStream append(StoragePath path) throws IOException;

  /**
   * Checks if a path exists.
   *
   * @param path to check.
   * @return {@code true} if the path exists.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean exists(StoragePath path) throws IOException;

  /**
   * Returns a {@link StoragePathInfo} object that represents the path.
   *
   * @param path to check.
   * @return a {@link StoragePathInfo} object.
   * @throws FileNotFoundException when the path does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract StoragePathInfo getPathInfo(StoragePath path) throws IOException;

  /**
   * Creates the directory and non-existent parent directories.
   *
   * @param path to create.
   * @return {@code true} if the directory was created.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean createDirectory(StoragePath path) throws IOException;

  /**
   * Lists the path info of the direct files/directories in the given path if the path is a directory.
   *
   * @param path given path.
   * @return the list of path info of the files/directories in the given path.
   * @throws FileNotFoundException when the path does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException;

  /**
   * Lists the path info of all files under the give path recursively.
   *
   * @param path given path.
   * @return the list of path info of the files under the given path.
   * @throws FileNotFoundException when the path does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<StoragePathInfo> listFiles(StoragePath path) throws IOException;

  /**
   * Lists the path info of the direct files/directories in the given path
   * and filters the results, if the path is a directory.
   *
   * @param path   given path.
   * @param filter filter to apply.
   * @return the list of path info of the files/directories in the given path.
   * @throws FileNotFoundException when the path does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<StoragePathInfo> listDirectEntries(StoragePath path,
                                                          StoragePathFilter filter) throws IOException;

  /**
   * Sets Modification Time for the storage Path
   * @param path
   * @param modificationTimeInMillisEpoch Millis since Epoch
   * @throws IOException
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract void setModificationTime(StoragePath path, long modificationTimeInMillisEpoch) throws IOException;

  /**
   * Returns all the files that match the pathPattern and are not checksum files,
   * and filters the results.
   *
   * @param pathPattern given pattern.
   * @param filter      filter to apply.
   * @return the list of path info of the files.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract List<StoragePathInfo> globEntries(StoragePath pathPattern,
                                                    StoragePathFilter filter) throws IOException;

  /**
   * Renames the path from old to new.
   *
   * @param oldPath source path.
   * @param newPath destination path.
   * @return {@true} if rename is successful.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean rename(StoragePath oldPath,
                                 StoragePath newPath) throws IOException;

  /**
   * Deletes a directory at path.
   *
   * @param path directory to delete.
   * @return {@code true} if successful.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean deleteDirectory(StoragePath path) throws IOException;

  /**
   * Deletes a file at path.
   *
   * @param path file to delete.
   * @return {@code true} if successful.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean deleteFile(StoragePath path) throws IOException;

  /**
   * @return the underlying file system instance if exists.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract Object getFileSystem();

  /**
   * @return the raw storage.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieStorage getRawStorage();

  /**
   * @return the storage configuration.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public final StorageConfiguration<?> getConf() {
    return storageConf;
  }

  /**
   * Creates a new file with overwrite set to false. This ensures files are created
   * only once and never rewritten, also, here we take care if the content is not
   * empty, will first write the content to a temp file if {needCreateTempFile} is
   * true, and then rename it back after the content is written.
   *
   * <p>CAUTION: if this method is invoked in multi-threads for concurrent write of the same file,
   * an existence check of the file is recommended.
   *
   * @param path    File path.
   * @param contentWriter handles writing the content to the outputstream
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public final void createImmutableFileInPath(StoragePath path,
                                              Option<HoodieInstantWriter> contentWriter) throws HoodieIOException {
    createImmutableFileInPath(path, contentWriter, needCreateTempFile());
  }

  /**
   * Creates a new file with overwrite set to false. This ensures files are created
   * only once and never rewritten, also, here we take care if the content is not
   * empty, will first write the content to a temp file if {needCreateTempFile} is
   * true, and then rename it back after the content is written.
   *
   * <p>CAUTION: if this method is invoked in multi-threads for concurrent write of the same file,
   * an existence check of the file is recommended.
   *
   * @param path    File path.
   * @param contentWriter handles writing the content to the outputstream
   * @param needTempFile Whether to create auxiliary temp file.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public final void createImmutableFileInPath(StoragePath path,
                                              Option<HoodieInstantWriter> contentWriter,
                                              boolean needTempFile) throws HoodieIOException {
    OutputStream fsout = null;
    StoragePath tmpPath = null;

    try {
      if (!contentWriter.isPresent()) {
        fsout = create(path, false);
      }

      if (contentWriter.isPresent() && needTempFile) {
        StoragePath parent = path.getParent();
        tmpPath = new StoragePath(parent, path.getName() + "." + UUID.randomUUID());
        fsout = create(tmpPath, false);
        contentWriter.get().writeToStream(fsout);
      }

      if (contentWriter.isPresent() && !needTempFile) {
        fsout = create(path, false);
        contentWriter.get().writeToStream(fsout);
      }
    } catch (IOException e) {
      String errorMsg = "Failed to create file " + (tmpPath != null ? tmpPath : path);
      throw new HoodieIOException(errorMsg, e);
    } finally {
      try {
        if (null != fsout) {
          fsout.close();
        }
      } catch (IOException e) {
        String errorMsg = "Failed to close file " + (needTempFile ? tmpPath : path);
        throw new HoodieIOException(errorMsg, e);
      }

      boolean renameSuccess = false;
      try {
        if (null != tmpPath) {
          renameSuccess = rename(tmpPath, path);
        }
      } catch (IOException e) {
        throw new HoodieIOException(
            "Failed to rename " + tmpPath + " to the target " + path,
            e);
      } finally {
        if (!renameSuccess && null != tmpPath) {
          try {
            deleteFile(tmpPath);
            LOG.debug("Failed to rename {} to {}, target file exists: {}", tmpPath, path, exists(path));
          } catch (IOException e) {
            throw new HoodieIOException("Failed to delete tmp file " + tmpPath, e);
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
    return StorageSchemes.HDFS.getScheme().equals(getScheme())
        // Local file will be visible immediately after LocalFileSystem#create(..), even before the output
        // stream is closed, so temporary file is also needed for atomic file creating with content written.
        || StorageSchemes.FILE.getScheme().equals(getScheme());
  }

  /**
   * Create an OutputStream at the indicated path.
   * The file is overwritten by default.
   *
   * @param path the file to create.
   * @return the OutputStream to write to.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public OutputStream create(StoragePath path) throws IOException {
    return create(path, true);
  }

  /**
   * Creates an empty new file at the indicated path.
   *
   * @param path the file to create.
   * @return {@code true} if successfully created; {@code false} if already exists.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public boolean createNewFile(StoragePath path) throws IOException {
    if (exists(path)) {
      return false;
    } else {
      create(path, false).close();
      return true;
    }
  }

  /**
   * Opens an SeekableDataInputStream at the indicated path with seeks supported.
   *
   * @param path the file to open.
   * @param wrapStream true if we want to wrap the inputstream based on filesystem specific criteria
   * @return the InputStream to read from.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public SeekableDataInputStream openSeekable(StoragePath path, boolean wrapStream) throws IOException {
    return openSeekable(path, getDefaultBlockSize(path), wrapStream);
  }

  /**
   * Lists the file info of the direct files/directories in the given list of paths,
   * if the paths are directory.
   *
   * @param pathList given path list.
   * @return the list of path info of the files/directories in the given paths.
   * @throws FileNotFoundException when the path does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public List<StoragePathInfo> listDirectEntries(List<StoragePath> pathList) throws IOException {
    List<StoragePathInfo> result = new ArrayList<>();
    for (StoragePath path : pathList) {
      result.addAll(listDirectEntries(path));
    }
    return result;
  }

  /**
   * Lists the file info of the direct files/directories in the given list of paths
   * and filters the results, if the paths are directory.
   *
   * @param pathList given path list.
   * @param filter filter to apply.
   * @return the list of path info of the files/directories in the given paths.
   * @throws FileNotFoundException when the path does not exist.
   * @throws IOException           IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public List<StoragePathInfo> listDirectEntries(List<StoragePath> pathList,
                                                 StoragePathFilter filter) throws IOException {
    List<StoragePathInfo> result = new ArrayList<>();
    for (StoragePath path : pathList) {
      result.addAll(listDirectEntries(path, filter));
    }
    return result;
  }

  /**
   * Returns all the files that match the pathPattern and are not checksum files.
   *
   * @param pathPattern given pattern.
   * @return the list of file info of the files.
   * @throws IOException IO error.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public List<StoragePathInfo> globEntries(StoragePath pathPattern) throws IOException {
    return globEntries(pathPattern, e -> true);
  }
}
