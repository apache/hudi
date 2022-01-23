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

package org.apache.hudi.hbase.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hudi.hbase.util.CommonFSUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FileLink is a sort of hardlink, that allows access to a file given a set of locations.
 *
 * <p><b>The Problem:</b>
 * <ul>
 *  <li>
 *    HDFS doesn't have support for hardlinks, and this make impossible to referencing
 *    the same data blocks using different names.
 *  </li>
 *  <li>
 *    HBase store files in one location (e.g. table/region/family/) and when the file is not
 *    needed anymore (e.g. compaction, region deletion, ...) moves it to an archive directory.
 *  </li>
 * </ul>
 * If we want to create a reference to a file, we need to remember that it can be in its
 * original location or in the archive folder.
 * The FileLink class tries to abstract this concept and given a set of locations
 * it is able to switch between them making this operation transparent for the user.
 * {@link HFileLink} is a more concrete implementation of the {@code FileLink}.
 *
 * <p><b>Back-references:</b>
 * To help the {@link org.apache.hadoop.hbase.master.cleaner.CleanerChore} to keep track of
 * the links to a particular file, during the {@code FileLink} creation, a new file is placed
 * inside a back-reference directory. There's one back-reference directory for each file that
 * has links, and in the directory there's one file per link.
 *
 * <p>HFileLink Example
 * <ul>
 *  <li>
 *      /hbase/table/region-x/cf/file-k
 *      (Original File)
 *  </li>
 *  <li>
 *      /hbase/table-cloned/region-y/cf/file-k.region-x.table
 *     (HFileLink to the original file)
 *  </li>
 *  <li>
 *      /hbase/table-2nd-cloned/region-z/cf/file-k.region-x.table
 *      (HFileLink to the original file)
 *  </li>
 *  <li>
 *      /hbase/.archive/table/region-x/.links-file-k/region-y.table-cloned
 *      (Back-reference to the link in table-cloned)
 *  </li>
 *  <li>
 *      /hbase/.archive/table/region-x/.links-file-k/region-z.table-2nd-cloned
 *      (Back-reference to the link in table-2nd-cloned)
 *  </li>
 * </ul>
 */
@InterfaceAudience.Private
public class FileLink {
  private static final Logger LOG = LoggerFactory.getLogger(FileLink.class);

  /** Define the Back-reference directory name prefix: .links-&lt;hfile&gt;/ */
  public static final String BACK_REFERENCES_DIRECTORY_PREFIX = ".links-";

  /**
   * FileLink InputStream that handles the switch between the original path
   * and the alternative locations, when the file is moved.
   */
  private static class FileLinkInputStream extends InputStream
      implements Seekable, PositionedReadable, CanSetDropBehind, CanSetReadahead, CanUnbuffer {
    private FSDataInputStream in = null;
    private Path currentPath = null;
    private long pos = 0;

    private final FileLink fileLink;
    private final int bufferSize;
    private final FileSystem fs;

    public FileLinkInputStream(final FileSystem fs, final FileLink fileLink)
        throws IOException {
      this(fs, fileLink, CommonFSUtils.getDefaultBufferSize(fs));
    }

    public FileLinkInputStream(final FileSystem fs, final FileLink fileLink, int bufferSize)
        throws IOException {
      this.bufferSize = bufferSize;
      this.fileLink = fileLink;
      this.fs = fs;

      this.in = tryOpen();
    }

    @Override
    public int read() throws IOException {
      int res;
      try {
        res = in.read();
      } catch (FileNotFoundException e) {
        res = tryOpen().read();
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        res = tryOpen().read();
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        res = tryOpen().read();
      }
      if (res > 0) pos += 1;
      return res;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n;
      try {
        n = in.read(b, off, len);
      } catch (FileNotFoundException e) {
        n = tryOpen().read(b, off, len);
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        n = tryOpen().read(b, off, len);
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        n = tryOpen().read(b, off, len);
      }
      if (n > 0) pos += n;
      assert(in.getPos() == pos);
      return n;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      int n;
      try {
        n = in.read(position, buffer, offset, length);
      } catch (FileNotFoundException e) {
        n = tryOpen().read(position, buffer, offset, length);
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        n = tryOpen().read(position, buffer, offset, length);
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        n = tryOpen().read(position, buffer, offset, length);
      }
      return n;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try {
        in.readFully(position, buffer, offset, length);
      } catch (FileNotFoundException e) {
        tryOpen().readFully(position, buffer, offset, length);
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        tryOpen().readFully(position, buffer, offset, length);
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        tryOpen().readFully(position, buffer, offset, length);
      }
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped;

      try {
        skipped = in.skip(n);
      } catch (FileNotFoundException e) {
        skipped = tryOpen().skip(n);
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        skipped = tryOpen().skip(n);
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        skipped = tryOpen().skip(n);
      }

      if (skipped > 0) pos += skipped;
      return skipped;
    }

    @Override
    public int available() throws IOException {
      try {
        return in.available();
      } catch (FileNotFoundException e) {
        return tryOpen().available();
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        return tryOpen().available();
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        return tryOpen().available();
      }
    }

    @Override
    public void seek(long pos) throws IOException {
      try {
        in.seek(pos);
      } catch (FileNotFoundException e) {
        tryOpen().seek(pos);
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        tryOpen().seek(pos);
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        tryOpen().seek(pos);
      }
      this.pos = pos;
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      boolean res;
      try {
        res = in.seekToNewSource(targetPos);
      } catch (FileNotFoundException e) {
        res = tryOpen().seekToNewSource(targetPos);
      } catch (NullPointerException e) { // HDFS 1.x - DFSInputStream.getBlockAt()
        res = tryOpen().seekToNewSource(targetPos);
      } catch (AssertionError e) { // assert in HDFS 1.x - DFSInputStream.getBlockAt()
        res = tryOpen().seekToNewSource(targetPos);
      }
      if (res) pos = targetPos;
      return res;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
    }

    @Override
    public synchronized void reset() throws IOException {
      throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public void unbuffer() {
      if (in == null) {
        return;
      }
      in.unbuffer();
    }

    /**
     * Try to open the file from one of the available locations.
     *
     * @return FSDataInputStream stream of the opened file link
     * @throws IOException on unexpected error, or file not found.
     */
    private FSDataInputStream tryOpen() throws IOException {
      IOException exception = null;
      for (Path path: fileLink.getLocations()) {
        if (path.equals(currentPath)) continue;
        try {
          in = fs.open(path, bufferSize);
          if (pos != 0) in.seek(pos);
          assert(in.getPos() == pos) : "Link unable to seek to the right position=" + pos;
          if (LOG.isTraceEnabled()) {
            if (currentPath == null) {
              LOG.debug("link open path=" + path);
            } else {
              LOG.trace("link switch from path=" + currentPath + " to path=" + path);
            }
          }
          currentPath = path;
          return(in);
        } catch (FileNotFoundException | AccessControlException | RemoteException e) {
          exception = FileLink.handleAccessLocationException(fileLink, e, exception);
        }
      }
      throw exception;
    }

    @Override
    public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
      in.setReadahead(readahead);
    }

    @Override
    public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
      in.setDropBehind(dropCache);
    }
  }

  private Path[] locations = null;

  protected FileLink() {
    this.locations = null;
  }

  /**
   * @param originPath Original location of the file to link
   * @param alternativePaths Alternative locations to look for the linked file
   */
  public FileLink(Path originPath, Path... alternativePaths) {
    setLocations(originPath, alternativePaths);
  }

  /**
   * @param locations locations to look for the linked file
   */
  public FileLink(final Collection<Path> locations) {
    this.locations = locations.toArray(new Path[locations.size()]);
  }

  /**
   * @return the locations to look for the linked file.
   */
  public Path[] getLocations() {
    return locations;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(getClass().getSimpleName());
    str.append(" locations=[");
    for (int i = 0; i < locations.length; ++i) {
      if (i > 0) str.append(", ");
      str.append(locations[i].toString());
    }
    str.append("]");
    return str.toString();
  }

  /**
   * @return true if the file pointed by the link exists
   */
  public boolean exists(final FileSystem fs) throws IOException {
    for (int i = 0; i < locations.length; ++i) {
      if (fs.exists(locations[i])) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return the path of the first available link.
   */
  public Path getAvailablePath(FileSystem fs) throws IOException {
    for (int i = 0; i < locations.length; ++i) {
      if (fs.exists(locations[i])) {
        return locations[i];
      }
    }
    throw new FileNotFoundException(toString());
  }

  /**
   * Get the FileStatus of the referenced file.
   *
   * @param fs {@link FileSystem} on which to get the file status
   * @return InputStream for the hfile link.
   * @throws IOException on unexpected error.
   */
  public FileStatus getFileStatus(FileSystem fs) throws IOException {
    IOException exception = null;
    for (int i = 0; i < locations.length; ++i) {
      try {
        return fs.getFileStatus(locations[i]);
      } catch (FileNotFoundException | AccessControlException e) {
        exception = handleAccessLocationException(this, e, exception);
      }
    }
    throw exception;
  }

  /**
   * Handle exceptions which are thrown when access locations of file link
   * @param fileLink the file link
   * @param newException the exception caught by access the current location
   * @param previousException the previous exception caught by access the other locations
   * @return return AccessControlException if access one of the locations caught, otherwise return
   *         FileNotFoundException. The AccessControlException is threw if user scan snapshot
   *         feature is enabled, see
   *         {@link org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclController}.
   * @throws IOException if the exception is neither AccessControlException nor
   *           FileNotFoundException
   */
  private static IOException handleAccessLocationException(FileLink fileLink,
                                                           IOException newException, IOException previousException) throws IOException {
    if (newException instanceof RemoteException) {
      newException = ((RemoteException) newException)
          .unwrapRemoteException(FileNotFoundException.class, AccessControlException.class);
    }
    if (newException instanceof FileNotFoundException) {
      // Try another file location
      if (previousException == null) {
        previousException = new FileNotFoundException(fileLink.toString());
      }
    } else if (newException instanceof AccessControlException) {
      // Try another file location
      previousException = newException;
    } else {
      throw newException;
    }
    return previousException;
  }

  /**
   * Open the FileLink for read.
   * <p>
   * It uses a wrapper of FSDataInputStream that is agnostic to the location
   * of the file, even if the file switches between locations.
   *
   * @param fs {@link FileSystem} on which to open the FileLink
   * @return InputStream for reading the file link.
   * @throws IOException on unexpected error.
   */
  public FSDataInputStream open(final FileSystem fs) throws IOException {
    return new FSDataInputStream(new FileLinkInputStream(fs, this));
  }

  /**
   * Open the FileLink for read.
   * <p>
   * It uses a wrapper of FSDataInputStream that is agnostic to the location
   * of the file, even if the file switches between locations.
   *
   * @param fs {@link FileSystem} on which to open the FileLink
   * @param bufferSize the size of the buffer to be used.
   * @return InputStream for reading the file link.
   * @throws IOException on unexpected error.
   */
  public FSDataInputStream open(final FileSystem fs, int bufferSize) throws IOException {
    return new FSDataInputStream(new FileLinkInputStream(fs, this, bufferSize));
  }

  /**
   * NOTE: This method must be used only in the constructor!
   * It creates a List with the specified locations for the link.
   */
  protected void setLocations(Path originPath, Path... alternativePaths) {
    assert this.locations == null : "Link locations already set";

    List<Path> paths = new ArrayList<>(alternativePaths.length +1);
    if (originPath != null) {
      paths.add(originPath);
    }

    for (int i = 0; i < alternativePaths.length; i++) {
      if (alternativePaths[i] != null) {
        paths.add(alternativePaths[i]);
      }
    }
    this.locations = paths.toArray(new Path[0]);
  }

  /**
   * Get the directory to store the link back references
   *
   * <p>To simplify the reference count process, during the FileLink creation
   * a back-reference is added to the back-reference directory of the specified file.
   *
   * @param storeDir Root directory for the link reference folder
   * @param fileName File Name with links
   * @return Path for the link back references.
   */
  public static Path getBackReferencesDir(final Path storeDir, final String fileName) {
    return new Path(storeDir, BACK_REFERENCES_DIRECTORY_PREFIX + fileName);
  }

  /**
   * Get the referenced file name from the reference link directory path.
   *
   * @param dirPath Link references directory path
   * @return Name of the file referenced
   */
  public static String getBackReferenceFileName(final Path dirPath) {
    return dirPath.getName().substring(BACK_REFERENCES_DIRECTORY_PREFIX.length());
  }

  /**
   * Checks if the specified directory path is a back reference links folder.
   * @param dirPath Directory path to verify
   * @return True if the specified directory is a link references folder
   */
  public static boolean isBackReferencesDir(final Path dirPath) {
    if (dirPath == null) {
      return false;
    }
    return dirPath.getName().startsWith(BACK_REFERENCES_DIRECTORY_PREFIX);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    // Assumes that the ordering of locations between objects are the same. This is true for the
    // current subclasses already (HFileLink, WALLink). Otherwise, we may have to sort the locations
    // or keep them presorted
    if (this.getClass().equals(obj.getClass())) {
      return Arrays.equals(this.locations, ((FileLink) obj).locations);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(locations);
  }
}
