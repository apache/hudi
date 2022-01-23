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

package org.apache.hudi.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.TableName;
import org.apache.hudi.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hudi.hbase.exceptions.DeserializationException;
import org.apache.hudi.hbase.fs.HFileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSHedgedReadMetrics;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Progressable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

import org.apache.hudi.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hudi.hbase.shaded.protobuf.generated.FSProtos;

import javax.annotation.CheckForNull;

/**
 * Utility methods for interacting with the underlying file system.
 */
@InterfaceAudience.Private
public final class FSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);

  private static final String THREAD_POOLSIZE = "hbase.client.localityCheck.threadPoolSize";
  private static final int DEFAULT_THREAD_POOLSIZE = 2;

  /** Set to true on Windows platforms */
  // currently only used in testing. TODO refactor into a test class
  public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  private FSUtils() {
  }

  /**
   * @return True is <code>fs</code> is instance of DistributedFileSystem
   * @throws IOException
   */
  public static boolean isDistributedFileSystem(final FileSystem fs) throws IOException {
    FileSystem fileSystem = fs;
    // If passed an instance of HFileSystem, it fails instanceof DistributedFileSystem.
    // Check its backing fs for dfs-ness.
    if (fs instanceof HFileSystem) {
      fileSystem = ((HFileSystem)fs).getBackingFs();
    }
    return fileSystem instanceof DistributedFileSystem;
  }

  /**
   * Compare path component of the Path URI; e.g. if hdfs://a/b/c and /a/b/c, it will compare the
   * '/a/b/c' part. If you passed in 'hdfs://a/b/c and b/c, it would return true.  Does not consider
   * schema; i.e. if schemas different but path or subpath matches, the two will equate.
   * @param pathToSearch Path we will be trying to match.
   * @param pathTail
   * @return True if <code>pathTail</code> is tail on the path of <code>pathToSearch</code>
   */
  public static boolean isMatchingTail(final Path pathToSearch, final Path pathTail) {
    Path tailPath = pathTail;
    String tailName;
    Path toSearch = pathToSearch;
    String toSearchName;
    boolean result = false;

    if (pathToSearch.depth() != pathTail.depth()) {
      return false;
    }

    do {
      tailName = tailPath.getName();
      if (tailName == null || tailName.isEmpty()) {
        result = true;
        break;
      }
      toSearchName = toSearch.getName();
      if (toSearchName == null || toSearchName.isEmpty()) {
        break;
      }
      // Move up a parent on each path for next go around.  Path doesn't let us go off the end.
      tailPath = tailPath.getParent();
      toSearch = toSearch.getParent();
    } while(tailName.equals(toSearchName));
    return result;
  }

  /**
   * Create the specified file on the filesystem. By default, this will:
   * <ol>
   * <li>overwrite the file if it exists</li>
   * <li>apply the umask in the configuration (if it is enabled)</li>
   * <li>use the fs configured buffer size (or 4096 if not set)</li>
   * <li>use the configured column family replication or default replication if
   * {@link ColumnFamilyDescriptorBuilder#DEFAULT_DFS_REPLICATION}</li>
   * <li>use the default block size</li>
   * <li>not track progress</li>
   * </ol>
   * @param conf configurations
   * @param fs {@link FileSystem} on which to write the file
   * @param path {@link Path} to the file to write
   * @param perm permissions
   * @param favoredNodes favored data nodes
   * @return output stream to the created file
   * @throws IOException if the file cannot be created
   */
  public static FSDataOutputStream create(Configuration conf, FileSystem fs, Path path,
                                          FsPermission perm, InetSocketAddress[] favoredNodes) throws IOException {
    if (fs instanceof HFileSystem) {
      FileSystem backingFs = ((HFileSystem) fs).getBackingFs();
      if (backingFs instanceof DistributedFileSystem) {
        // Try to use the favoredNodes version via reflection to allow backwards-
        // compatibility.
        short replication = Short.parseShort(conf.get(ColumnFamilyDescriptorBuilder.DFS_REPLICATION,
            String.valueOf(ColumnFamilyDescriptorBuilder.DEFAULT_DFS_REPLICATION)));
        try {
          return (FSDataOutputStream) (DistributedFileSystem.class
              .getDeclaredMethod("create", Path.class, FsPermission.class, boolean.class, int.class,
                  short.class, long.class, Progressable.class, InetSocketAddress[].class)
              .invoke(backingFs, path, perm, true, CommonFSUtils.getDefaultBufferSize(backingFs),
                  replication > 0 ? replication : CommonFSUtils.getDefaultReplication(backingFs, path),
                  CommonFSUtils.getDefaultBlockSize(backingFs, path), null, favoredNodes));
        } catch (InvocationTargetException ite) {
          // Function was properly called, but threw it's own exception.
          throw new IOException(ite.getCause());
        } catch (NoSuchMethodException e) {
          LOG.debug("DFS Client does not support most favored nodes create; using default create");
          LOG.trace("Ignoring; use default create", e);
        } catch (IllegalArgumentException | SecurityException | IllegalAccessException e) {
          LOG.debug("Ignoring (most likely Reflection related exception) " + e);
        }
      }
    }
    return CommonFSUtils.create(fs, path, perm, true);
  }

  /**
   * Checks to see if the specified file system is available
   *
   * @param fs filesystem
   * @throws IOException e
   */
  public static void checkFileSystemAvailable(final FileSystem fs)
      throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      return;
    }
    IOException exception = null;
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    try {
      if (dfs.exists(new Path("/"))) {
        return;
      }
    } catch (IOException e) {
      exception = e instanceof RemoteException ?
          ((RemoteException)e).unwrapRemoteException() : e;
    }
    try {
      fs.close();
    } catch (Exception e) {
      LOG.error("file system close failed: ", e);
    }
    throw new IOException("File system is not available", exception);
  }

  /**
   * We use reflection because {@link DistributedFileSystem#setSafeMode(
   * HdfsConstants.SafeModeAction action, boolean isChecked)} is not in hadoop 1.1
   *
   * @param dfs
   * @return whether we're in safe mode
   * @throws IOException
   */
  private static boolean isInSafeMode(DistributedFileSystem dfs) throws IOException {
    boolean inSafeMode = false;
    try {
      Method m = DistributedFileSystem.class.getMethod("setSafeMode", new Class<?> []{
          org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.class, boolean.class});
      inSafeMode = (Boolean) m.invoke(dfs,
          org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_GET, true);
    } catch (Exception e) {
      if (e instanceof IOException) throw (IOException) e;

      // Check whether dfs is on safemode.
      inSafeMode = dfs.setSafeMode(
          org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_GET);
    }
    return inSafeMode;
  }

  /**
   * Check whether dfs is in safemode.
   * @param conf
   * @throws IOException
   */
  public static void checkDfsSafeMode(final Configuration conf)
      throws IOException {
    boolean isInSafeMode = false;
    FileSystem fs = FileSystem.get(conf);
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      isInSafeMode = isInSafeMode(dfs);
    }
    if (isInSafeMode) {
      throw new IOException("File system is in safemode, it can't be written now");
    }
  }

  /**
   * Verifies current version of file system
   *
   * @param fs filesystem object
   * @param rootdir root hbase directory
   * @return null if no version file exists, version string otherwise
   * @throws IOException if the version file fails to open
   * @throws DeserializationException if the version data cannot be translated into a version
   */
  public static String getVersion(FileSystem fs, Path rootdir)
      throws IOException, DeserializationException {
    final Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    FileStatus[] status = null;
    try {
      // hadoop 2.0 throws FNFE if directory does not exist.
      // hadoop 1.0 returns null if directory does not exist.
      status = fs.listStatus(versionFile);
    } catch (FileNotFoundException fnfe) {
      return null;
    }
    if (ArrayUtils.getLength(status) == 0) {
      return null;
    }
    String version = null;
    byte [] content = new byte [(int)status[0].getLen()];
    FSDataInputStream s = fs.open(versionFile);
    try {
      IOUtils.readFully(s, content, 0, content.length);
      if (ProtobufUtil.isPBMagicPrefix(content)) {
        version = parseVersionFrom(content);
      } else {
        // Presume it pre-pb format.
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(content))) {
          version = dis.readUTF();
        }
      }
    } catch (EOFException eof) {
      LOG.warn("Version file was empty, odd, will try to set it.");
    } finally {
      s.close();
    }
    return version;
  }

  /**
   * Parse the content of the ${HBASE_ROOTDIR}/hbase.version file.
   * @param bytes The byte content of the hbase.version file
   * @return The version found in the file as a String
   * @throws DeserializationException if the version data cannot be translated into a version
   */
  static String parseVersionFrom(final byte [] bytes)
      throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(bytes);
    int pblen = ProtobufUtil.lengthOfPBMagic();
    FSProtos.HBaseVersionFileContent.Builder builder =
        FSProtos.HBaseVersionFileContent.newBuilder();
    try {
      ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
      return builder.getVersion();
    } catch (IOException e) {
      // Convert
      throw new DeserializationException(e);
    }
  }

  /**
   * Create the content to write into the ${HBASE_ROOTDIR}/hbase.version file.
   * @param version Version to persist
   * @return Serialized protobuf with <code>version</code> content and a bit of pb magic for a prefix.
   */
  static byte [] toVersionByteArray(final String version) {
    FSProtos.HBaseVersionFileContent.Builder builder =
        FSProtos.HBaseVersionFileContent.newBuilder();
    return ProtobufUtil.prependPBMagic(builder.setVersion(version).build().toByteArray());
  }

  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir)
      throws IOException {
    setVersion(fs, rootdir, HConstants.FILE_SYSTEM_VERSION, 0,
        HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
  }

  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root
   * @param wait time to wait for retry
   * @param retries number of times to retry before failing
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir, int wait, int retries)
      throws IOException {
    setVersion(fs, rootdir, HConstants.FILE_SYSTEM_VERSION, wait, retries);
  }


  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root directory
   * @param version version to set
   * @param wait time to wait for retry
   * @param retries number of times to retry before throwing an IOException
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir, String version,
                                int wait, int retries) throws IOException {
    Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    Path tempVersionFile = new Path(rootdir, HConstants.HBASE_TEMP_DIRECTORY + Path.SEPARATOR +
        HConstants.VERSION_FILE_NAME);
    while (true) {
      try {
        // Write the version to a temporary file
        FSDataOutputStream s = fs.create(tempVersionFile);
        try {
          s.write(toVersionByteArray(version));
          s.close();
          s = null;
          // Move the temp version file to its normal location. Returns false
          // if the rename failed. Throw an IOE in that case.
          if (!fs.rename(tempVersionFile, versionFile)) {
            throw new IOException("Unable to move temp version file to " + versionFile);
          }
        } finally {
          // Cleaning up the temporary if the rename failed would be trying
          // too hard. We'll unconditionally create it again the next time
          // through anyway, files are overwritten by default by create().

          // Attempt to close the stream on the way out if it is still open.
          try {
            if (s != null) s.close();
          } catch (IOException ignore) { }
        }
        LOG.info("Created version file at " + rootdir.toString() + " with version=" + version);
        return;
      } catch (IOException e) {
        if (retries > 0) {
          LOG.debug("Unable to create version file at " + rootdir.toString() + ", retrying", e);
          fs.delete(versionFile, false);
          try {
            if (wait > 0) {
              Thread.sleep(wait);
            }
          } catch (InterruptedException ie) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(ie);
          }
          retries--;
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Checks that a cluster ID file exists in the HBase root directory
   * @param fs the root directory FileSystem
   * @param rootdir the HBase root directory in HDFS
   * @param wait how long to wait between retries
   * @return <code>true</code> if the file exists, otherwise <code>false</code>
   * @throws IOException if checking the FileSystem fails
   */
  public static boolean checkClusterIdExists(FileSystem fs, Path rootdir,
                                             long wait) throws IOException {
    while (true) {
      try {
        Path filePath = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
        return fs.exists(filePath);
      } catch (IOException ioe) {
        if (wait > 0L) {
          LOG.warn("Unable to check cluster ID file in {}, retrying in {}ms", rootdir, wait, ioe);
          try {
            Thread.sleep(wait);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw (InterruptedIOException) new InterruptedIOException().initCause(e);
          }
        } else {
          throw ioe;
        }
      }
    }
  }

  /**
   * If DFS, check safe mode and if so, wait until we clear it.
   * @param conf configuration
   * @param wait Sleep between retries
   * @throws IOException e
   */
  public static void waitOnSafeMode(final Configuration conf,
                                    final long wait)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) return;
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    // Make sure dfs is not in safe mode
    while (isInSafeMode(dfs)) {
      LOG.info("Waiting for dfs to exit safe mode...");
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      }
    }
  }

  /**
   * Directory filter that doesn't include any of the directories in the specified blacklist
   */
  public static class BlackListDirFilter extends AbstractFileStatusFilter {
    private final FileSystem fs;
    private List<String> blacklist;

    /**
     * Create a filter on the givem filesystem with the specified blacklist
     * @param fs filesystem to filter
     * @param directoryNameBlackList list of the names of the directories to filter. If
     *          <tt>null</tt>, all directories are returned
     */
    @SuppressWarnings("unchecked")
    public BlackListDirFilter(final FileSystem fs, final List<String> directoryNameBlackList) {
      this.fs = fs;
      blacklist =
          (List<String>) (directoryNameBlackList == null ? Collections.emptyList()
              : directoryNameBlackList);
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!isValidName(p.getName())) {
        return false;
      }

      try {
        return isDirectory(fs, isDir, p);
      } catch (IOException e) {
        LOG.warn("An error occurred while verifying if [{}] is a valid directory."
            + " Returning 'not valid' and continuing.", p, e);
        return false;
      }
    }

    protected boolean isValidName(final String name) {
      return !blacklist.contains(name);
    }
  }

  /**
   * A {@link PathFilter} that only allows directories.
   */
  public static class DirFilter extends BlackListDirFilter {

    public DirFilter(FileSystem fs) {
      super(fs, null);
    }
  }

  /**
   * A {@link PathFilter} that returns usertable directories. To get all directories use the
   * {@link BlackListDirFilter} with a <tt>null</tt> blacklist
   */
  public static class UserTableDirFilter extends BlackListDirFilter {
    public UserTableDirFilter(FileSystem fs) {
      super(fs, HConstants.HBASE_NON_TABLE_DIRS);
    }

    @Override
    protected boolean isValidName(final String name) {
      if (!super.isValidName(name))
        return false;

      try {
        TableName.isLegalTableQualifierName(Bytes.toBytes(name));
      } catch (IllegalArgumentException e) {
        LOG.info("Invalid table name: {}", name);
        return false;
      }
      return true;
    }
  }

  public static List<Path> getTableDirs(final FileSystem fs, final Path rootdir)
      throws IOException {
    List<Path> tableDirs = new ArrayList<>();
    Path baseNamespaceDir = new Path(rootdir, HConstants.BASE_NAMESPACE_DIR);
    if (fs.exists(baseNamespaceDir)) {
      for (FileStatus status : fs.globStatus(new Path(baseNamespaceDir, "*"))) {
        tableDirs.addAll(FSUtils.getLocalTableDirs(fs, status.getPath()));
      }
    }
    return tableDirs;
  }

  /**
   * @param fs
   * @param rootdir
   * @return All the table directories under <code>rootdir</code>. Ignore non table hbase folders such as
   * .logs, .oldlogs, .corrupt folders.
   * @throws IOException
   */
  public static List<Path> getLocalTableDirs(final FileSystem fs, final Path rootdir)
      throws IOException {
    // presumes any directory under hbase.rootdir is a table
    FileStatus[] dirs = fs.listStatus(rootdir, new UserTableDirFilter(fs));
    List<Path> tabledirs = new ArrayList<>(dirs.length);
    for (FileStatus dir: dirs) {
      tabledirs.add(dir.getPath());
    }
    return tabledirs;
  }

  /**
   * Filter for all dirs that don't start with '.'
   */
  public static class RegionDirFilter extends AbstractFileStatusFilter {
    // This pattern will accept 0.90+ style hex region dirs and older numeric region dir names.
    final public static Pattern regionDirPattern = Pattern.compile("^[0-9a-f]*$");
    final FileSystem fs;

    public RegionDirFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    protected boolean accept(Path p, @CheckForNull Boolean isDir) {
      if (!regionDirPattern.matcher(p.getName()).matches()) {
        return false;
      }

      try {
        return isDirectory(fs, isDir, p);
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file {} due to IOException", p, ioe);
        return false;
      }
    }
  }

  /**
   * Check if short circuit read buffer size is set and if not, set it to hbase value.
   * @param conf
   */
  public static void checkShortCircuitReadBufferSize(final Configuration conf) {
    final int defaultSize = HConstants.DEFAULT_BLOCKSIZE * 2;
    final int notSet = -1;
    // DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY is only defined in h2
    final String dfsKey = "dfs.client.read.shortcircuit.buffer.size";
    int size = conf.getInt(dfsKey, notSet);
    // If a size is set, return -- we will use it.
    if (size != notSet) return;
    // But short circuit buffer size is normally not set.  Put in place the hbase wanted size.
    int hbaseSize = conf.getInt("hbase." + dfsKey, defaultSize);
    conf.setIfUnset(dfsKey, Integer.toString(hbaseSize));
  }

  /**
   * @param c
   * @return The DFSClient DFSHedgedReadMetrics instance or null if can't be found or not on hdfs.
   * @throws IOException
   */
  public static DFSHedgedReadMetrics getDFSHedgedReadMetrics(final Configuration c)
      throws IOException {
    if (!CommonFSUtils.isHDFS(c)) {
      return null;
    }
    // getHedgedReadMetrics is package private. Get the DFSClient instance that is internal
    // to the DFS FS instance and make the method getHedgedReadMetrics accessible, then invoke it
    // to get the singleton instance of DFSHedgedReadMetrics shared by DFSClients.
    final String name = "getHedgedReadMetrics";
    DFSClient dfsclient = ((DistributedFileSystem)FileSystem.get(c)).getClient();
    Method m;
    try {
      m = dfsclient.getClass().getDeclaredMethod(name);
    } catch (NoSuchMethodException e) {
      LOG.warn("Failed find method " + name + " in dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    } catch (SecurityException e) {
      LOG.warn("Failed find method " + name + " in dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    }
    m.setAccessible(true);
    try {
      return (DFSHedgedReadMetrics)m.invoke(dfsclient);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      LOG.warn("Failed invoking method " + name + " on dfsclient; no hedged read metrics: " +
          e.getMessage());
      return null;
    }
  }

  public static List<Path> copyFilesParallel(FileSystem srcFS, Path src, FileSystem dstFS, Path dst,
                                             Configuration conf, int threads) throws IOException {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<Void>> futures = new ArrayList<>();
    List<Path> traversedPaths;
    try {
      traversedPaths = copyFiles(srcFS, src, dstFS, dst, conf, pool, futures);
      for (Future<Void> future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException | IOException e) {
      throw new IOException("Copy snapshot reference files failed", e);
    } finally {
      pool.shutdownNow();
    }
    return traversedPaths;
  }

  private static List<Path> copyFiles(FileSystem srcFS, Path src, FileSystem dstFS, Path dst,
                                      Configuration conf, ExecutorService pool, List<Future<Void>> futures) throws IOException {
    List<Path> traversedPaths = new ArrayList<>();
    traversedPaths.add(dst);
    FileStatus currentFileStatus = srcFS.getFileStatus(src);
    if (currentFileStatus.isDirectory()) {
      if (!dstFS.mkdirs(dst)) {
        throw new IOException("Create directory failed: " + dst);
      }
      FileStatus[] subPaths = srcFS.listStatus(src);
      for (FileStatus subPath : subPaths) {
        traversedPaths.addAll(copyFiles(srcFS, subPath.getPath(), dstFS,
            new Path(dst, subPath.getPath().getName()), conf, pool, futures));
      }
    } else {
      Future<Void> future = pool.submit(() -> {
        FileUtil.copy(srcFS, src, dstFS, dst, false, false, conf);
        return null;
      });
      futures.add(future);
    }
    return traversedPaths;
  }

  /**
   * @return A set containing all namenode addresses of fs
   */
  private static Set<InetSocketAddress> getNNAddresses(DistributedFileSystem fs,
                                                       Configuration conf) {
    Set<InetSocketAddress> addresses = new HashSet<>();
    String serviceName = fs.getCanonicalServiceName();

    if (serviceName.startsWith("ha-hdfs")) {
      try {
        Map<String, Map<String, InetSocketAddress>> addressMap =
            DFSUtil.getNNServiceRpcAddressesForCluster(conf);
        String nameService = serviceName.substring(serviceName.indexOf(":") + 1);
        if (addressMap.containsKey(nameService)) {
          Map<String, InetSocketAddress> nnMap = addressMap.get(nameService);
          for (Map.Entry<String, InetSocketAddress> e2 : nnMap.entrySet()) {
            InetSocketAddress addr = e2.getValue();
            addresses.add(addr);
          }
        }
      } catch (Exception e) {
        LOG.warn("DFSUtil.getNNServiceRpcAddresses failed. serviceName=" + serviceName, e);
      }
    } else {
      URI uri = fs.getUri();
      int port = uri.getPort();
      if (port < 0) {
        int idx = serviceName.indexOf(':');
        port = Integer.parseInt(serviceName.substring(idx + 1));
      }
      InetSocketAddress addr = new InetSocketAddress(uri.getHost(), port);
      addresses.add(addr);
    }

    return addresses;
  }

  /**
   * @param conf the Configuration of HBase
   * @return Whether srcFs and desFs are on same hdfs or not
   */
  public static boolean isSameHdfs(Configuration conf, FileSystem srcFs, FileSystem desFs) {
    // By getCanonicalServiceName, we could make sure both srcFs and desFs
    // show a unified format which contains scheme, host and port.
    String srcServiceName = srcFs.getCanonicalServiceName();
    String desServiceName = desFs.getCanonicalServiceName();

    if (srcServiceName == null || desServiceName == null) {
      return false;
    }
    if (srcServiceName.equals(desServiceName)) {
      return true;
    }
    if (srcServiceName.startsWith("ha-hdfs") && desServiceName.startsWith("ha-hdfs")) {
      Collection<String> internalNameServices =
          conf.getTrimmedStringCollection("dfs.internal.nameservices");
      if (!internalNameServices.isEmpty()) {
        if (internalNameServices.contains(srcServiceName.split(":")[1])) {
          return true;
        } else {
          return false;
        }
      }
    }
    if (srcFs instanceof DistributedFileSystem && desFs instanceof DistributedFileSystem) {
      // If one serviceName is an HA format while the other is a non-HA format,
      // maybe they refer to the same FileSystem.
      // For example, srcFs is "ha-hdfs://nameservices" and desFs is "hdfs://activeNamenode:port"
      Set<InetSocketAddress> srcAddrs = getNNAddresses((DistributedFileSystem) srcFs, conf);
      Set<InetSocketAddress> desAddrs = getNNAddresses((DistributedFileSystem) desFs, conf);
      if (Sets.intersection(srcAddrs, desAddrs).size() > 0) {
        return true;
      }
    }

    return false;
  }
}
