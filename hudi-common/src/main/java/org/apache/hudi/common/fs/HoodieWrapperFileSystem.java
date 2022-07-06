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

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hudi.hadoop.CachingPath;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import static org.apache.hudi.common.fs.StorageSchemes.HDFS;

/**
 * HoodieWrapperFileSystem wraps the default file system. It holds state about the open streams in the file system to
 * support getting the written size to each of the open streams.
 */
public class HoodieWrapperFileSystem extends FileSystem {

  public static final String HOODIE_SCHEME_PREFIX = "hoodie-";

  private static final String TMP_PATH_POSTFIX = ".tmp";

  protected enum MetricName {
    create, rename, delete, listStatus, mkdirs, getFileStatus, globStatus, listFiles, read, write
  }

  private static Registry METRICS_REGISTRY_DATA;
  private static Registry METRICS_REGISTRY_META;

  public static void setMetricsRegistry(Registry registry, Registry registryMeta) {
    METRICS_REGISTRY_DATA = registry;
    METRICS_REGISTRY_META = registryMeta;
  }


  private ConcurrentMap<String, SizeAwareFSDataOutputStream> openStreams = new ConcurrentHashMap<>();
  private FileSystem fileSystem;
  private URI uri;
  private ConsistencyGuard consistencyGuard = new NoOpConsistencyGuard();

  @FunctionalInterface
  public interface CheckedFunction<R> {
    R get() throws IOException;
  }

  private static Registry getMetricRegistryForPath(Path p) {
    return ((p != null) && (p.toString().contains(HoodieTableMetaClient.METAFOLDER_NAME)))
        ? METRICS_REGISTRY_META : METRICS_REGISTRY_DATA;
  }

  protected static <R> R executeFuncWithTimeMetrics(String metricName, Path p, CheckedFunction<R> func) throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    R res = func.get();

    Registry registry = getMetricRegistryForPath(p);
    if (registry != null) {
      registry.increment(metricName);
      registry.add(metricName + ".totalDuration", timer.endTimer());
    }

    return res;
  }

  protected static <R> R executeFuncWithTimeAndByteMetrics(String metricName, Path p, long byteCount,
                                                           CheckedFunction<R> func) throws IOException {
    Registry registry = getMetricRegistryForPath(p);
    if (registry != null) {
      registry.add(metricName + ".totalBytes", byteCount);
    }

    return executeFuncWithTimeMetrics(metricName, p, func);
  }

  public HoodieWrapperFileSystem() {}

  public HoodieWrapperFileSystem(FileSystem fileSystem, ConsistencyGuard consistencyGuard) {
    this.fileSystem = fileSystem;
    this.uri = fileSystem.getUri();
    this.consistencyGuard = consistencyGuard;
  }

  public static Path convertToHoodiePath(Path file, Configuration conf) {
    try {
      String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
      return convertPathWithScheme(file, getHoodieScheme(scheme));
    } catch (HoodieIOException e) {
      throw e;
    }
  }

  public static Path convertPathWithScheme(Path oldPath, String newScheme) {
    URI oldURI = oldPath.toUri();
    URI newURI;
    try {
      newURI = new URI(newScheme, oldURI.getUserInfo(), oldURI.getHost(), oldURI.getPort(), oldURI.getPath(),
          oldURI.getQuery(), oldURI.getFragment());
      return new CachingPath(newURI);
    } catch (URISyntaxException e) {
      // TODO - Better Exception handling
      throw new RuntimeException(e);
    }
  }

  public static String getHoodieScheme(String scheme) {
    String newScheme;
    if (StorageSchemes.isSchemeSupported(scheme)) {
      newScheme = HOODIE_SCHEME_PREFIX + scheme;
    } else {
      throw new IllegalArgumentException("BlockAlignedAvroParquetWriter does not support scheme " + scheme);
    }
    return newScheme;
  }

  @Override
  public void initialize(URI uri, Configuration conf) {
    // Get the default filesystem to decorate
    Path path = new Path(uri);
    // Remove 'hoodie-' prefix from path
    if (path.toString().startsWith(HOODIE_SCHEME_PREFIX)) {
      path = new Path(path.toString().replace(HOODIE_SCHEME_PREFIX, ""));
      this.uri = path.toUri();
    } else {
      this.uri = uri;
    }
    this.fileSystem = FSUtils.getFs(path.toString(), conf);
    // Do not need to explicitly initialize the default filesystem, its done already in the above
    // FileSystem.get
    // fileSystem.initialize(FileSystem.getDefaultUri(conf), conf);
    // fileSystem.setConf(conf);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return wrapInputStream(f, fileSystem.open(convertToDefaultPath(f), bufferSize));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      final Path translatedPath = convertToDefaultPath(f);
      return wrapOutputStream(f,
          fileSystem.create(translatedPath, permission, overwrite, bufferSize, replication, blockSize, progress));
    });
  }

  private FSDataOutputStream wrapOutputStream(final Path path, FSDataOutputStream fsDataOutputStream)
      throws IOException {
    if (fsDataOutputStream instanceof SizeAwareFSDataOutputStream) {
      return fsDataOutputStream;
    }

    SizeAwareFSDataOutputStream os = new SizeAwareFSDataOutputStream(path, fsDataOutputStream, consistencyGuard,
        () -> openStreams.remove(path.getName()));
    openStreams.put(path.getName(), os);
    return os;
  }

  private FSDataInputStream wrapInputStream(final Path path, FSDataInputStream fsDataInputStream) throws IOException {
    if (fsDataInputStream instanceof TimedFSDataInputStream) {
      return fsDataInputStream;
    }
    return new TimedFSDataInputStream(path, fsDataInputStream);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), overwrite));
    });
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f)));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), replication));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), replication, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f,
          fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize, replication, blockSize, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f,
          fileSystem.create(convertToDefaultPath(f), permission, flags, bufferSize, replication, blockSize, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), permission, flags, bufferSize, replication,
          blockSize, progress, checksumOpt));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f,
          fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize, replication, blockSize));
    });
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return wrapOutputStream(f, fileSystem.append(convertToDefaultPath(f), bufferSize, progress));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.rename.name(), src, () -> {
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(src));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for " + src + " to appear", e);
      }

      boolean success = fileSystem.rename(convertToDefaultPath(src), convertToDefaultPath(dst));

      if (success) {
        try {
          consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for " + dst + " to appear", e);
        }

        try {
          consistencyGuard.waitTillFileDisappears(convertToDefaultPath(src));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for " + src + " to disappear", e);
        }
      }
      return success;
    });
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.delete.name(), f, () -> {
      boolean success = fileSystem.delete(convertToDefaultPath(f), recursive);

      if (success) {
        try {
          consistencyGuard.waitTillFileDisappears(f);
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for " + f + " to disappear", e);
        }
      }
      return success;
    });
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), f, () -> {
      return fileSystem.listStatus(convertToDefaultPath(f));
    });
  }

  @Override
  public Path getWorkingDirectory() {
    return convertToHoodiePath(fileSystem.getWorkingDirectory());
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    fileSystem.setWorkingDirectory(convertToDefaultPath(newDir));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.mkdirs.name(), f, () -> {
      boolean success = fileSystem.mkdirs(convertToDefaultPath(f), permission);
      if (success) {
        try {
          consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for directory " + f + " to appear", e);
        }
      }
      return success;
    });
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.getFileStatus.name(), f, () -> {
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
      } catch (TimeoutException e) {
        // pass
      }
      return fileSystem.getFileStatus(convertToDefaultPath(f));
    });
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public String getCanonicalServiceName() {
    return fileSystem.getCanonicalServiceName();
  }

  @Override
  public String getName() {
    return fileSystem.getName();
  }

  @Override
  public Path makeQualified(Path path) {
    return convertToHoodiePath(fileSystem.makeQualified(convertToDefaultPath(path)));
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return fileSystem.getDelegationToken(renewer);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
    return fileSystem.addDelegationTokens(renewer, credentials);
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    return fileSystem.getChildFileSystems();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    return fileSystem.getFileBlockLocations(file, start, len);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    return fileSystem.getFileBlockLocations(convertToDefaultPath(p), start, len);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return fileSystem.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    return fileSystem.getServerDefaults(convertToDefaultPath(p));
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    return convertToHoodiePath(fileSystem.resolvePath(convertToDefaultPath(p)));
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return wrapInputStream(f, fileSystem.open(convertToDefaultPath(f)));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
                                               long blockSize, Progressable progress) throws IOException {
    Path p = convertToDefaultPath(f);
    return wrapOutputStream(p,
        fileSystem.createNonRecursive(p, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                               short replication, long blockSize, Progressable progress) throws IOException {
    Path p = convertToDefaultPath(f);
    return wrapOutputStream(p,
        fileSystem.createNonRecursive(p, permission, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags,
                                               int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    Path p = convertToDefaultPath(f);
    return wrapOutputStream(p,
        fileSystem.createNonRecursive(p, permission, flags, bufferSize, replication, blockSize, progress));
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    boolean newFile = fileSystem.createNewFile(convertToDefaultPath(f));
    if (newFile) {
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for " + f + " to appear", e);
      }
    }
    return newFile;
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    return wrapOutputStream(f, fileSystem.append(convertToDefaultPath(f)));
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return wrapOutputStream(f, fileSystem.append(convertToDefaultPath(f), bufferSize));
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    Path[] psrcsNew = convertDefaults(psrcs);
    fileSystem.concat(convertToDefaultPath(trg), psrcsNew);
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(trg));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for " + trg + " to appear", e);
    }
  }

  @Override
  public short getReplication(Path src) throws IOException {
    return fileSystem.getReplication(convertToDefaultPath(src));
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    return fileSystem.setReplication(convertToDefaultPath(src), replication);
  }

  @Override
  public boolean delete(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.delete.name(), f, () -> {
      return delete(f, true);
    });
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    return fileSystem.deleteOnExit(convertToDefaultPath(f));
  }

  @Override
  public boolean cancelDeleteOnExit(Path f) {
    return fileSystem.cancelDeleteOnExit(convertToDefaultPath(f));
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return fileSystem.exists(convertToDefaultPath(f));
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    return fileSystem.isDirectory(convertToDefaultPath(f));
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    return fileSystem.isFile(convertToDefaultPath(f));
  }

  @Override
  public long getLength(Path f) throws IOException {
    return fileSystem.getLength(convertToDefaultPath(f));
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return fileSystem.getContentSummary(convertToDefaultPath(f));
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
    return fileSystem.listCorruptFileBlocks(convertToDefaultPath(path));
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), f, () -> {
      return fileSystem.listStatus(convertToDefaultPath(f), filter);
    });
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), files.length > 0 ? files[0] : null, () -> {
      return fileSystem.listStatus(convertDefaults(files));
    });
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), files.length > 0 ? files[0] : null, () -> {
      return fileSystem.listStatus(convertDefaults(files), filter);
    });
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.globStatus.name(), pathPattern, () -> {
      return fileSystem.globStatus(convertToDefaultPath(pathPattern));
    });
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.globStatus.name(), pathPattern, () -> {
      return fileSystem.globStatus(convertToDefaultPath(pathPattern), filter);
    });
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    return fileSystem.listLocatedStatus(convertToDefaultPath(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listFiles.name(), f, () -> {
      return fileSystem.listFiles(convertToDefaultPath(f), recursive);
    });
  }

  @Override
  public Path getHomeDirectory() {
    return convertToHoodiePath(fileSystem.getHomeDirectory());
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.mkdirs.name(), f, () -> {
      boolean success = fileSystem.mkdirs(convertToDefaultPath(f));
      if (success) {
        try {
          consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for directory " + f + " to appear", e);
        }
      }
      return success;
    });
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    fileSystem.copyFromLocalFile(convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    fileSystem.moveFromLocalFile(convertLocalPaths(srcs), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    fileSystem.moveFromLocalFile(convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    fileSystem.copyFromLocalFile(delSrc, convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    fileSystem.copyFromLocalFile(delSrc, overwrite, convertLocalPaths(srcs), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    fileSystem.copyFromLocalFile(delSrc, overwrite, convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    fileSystem.copyToLocalFile(convertToDefaultPath(src), convertToLocalPath(dst));
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    fileSystem.moveToLocalFile(convertToDefaultPath(src), convertToLocalPath(dst));
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    fileSystem.copyToLocalFile(delSrc, convertToDefaultPath(src), convertToLocalPath(dst));
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    fileSystem.copyToLocalFile(delSrc, convertToDefaultPath(src), convertToLocalPath(dst), useRawLocalFileSystem);
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    return convertToHoodiePath(
        fileSystem.startLocalOutput(convertToDefaultPath(fsOutputFile), convertToDefaultPath(tmpLocalFile)));
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    fileSystem.completeLocalOutput(convertToDefaultPath(fsOutputFile), convertToDefaultPath(tmpLocalFile));
  }

  @Override
  public void close() throws IOException {
    // Don't close the wrapped `fileSystem` object. This will end up closing it for every thread since it
    // could be cached across jvm. We don't own that object anyway.
    super.close();
  }

  @Override
  public long getUsed() throws IOException {
    return fileSystem.getUsed();
  }

  @Override
  public long getBlockSize(Path f) throws IOException {
    return fileSystem.getBlockSize(convertToDefaultPath(f));
  }

  @Override
  public long getDefaultBlockSize() {
    return fileSystem.getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return fileSystem.getDefaultBlockSize(convertToDefaultPath(f));
  }

  @Override
  public short getDefaultReplication() {
    return fileSystem.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return fileSystem.getDefaultReplication(convertToDefaultPath(path));
  }

  @Override
  public void access(Path path, FsAction mode) throws IOException {
    fileSystem.access(convertToDefaultPath(path), mode);
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
    fileSystem.createSymlink(convertToDefaultPath(target), convertToDefaultPath(link), createParent);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    return fileSystem.getFileLinkStatus(convertToDefaultPath(f));
  }

  @Override
  public boolean supportsSymlinks() {
    return fileSystem.supportsSymlinks();
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    return convertToHoodiePath(fileSystem.getLinkTarget(convertToDefaultPath(f)));
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return fileSystem.getFileChecksum(convertToDefaultPath(f));
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    return fileSystem.getFileChecksum(convertToDefaultPath(f), length);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    fileSystem.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    fileSystem.setWriteChecksum(writeChecksum);
  }

  @Override
  public FsStatus getStatus() throws IOException {
    return fileSystem.getStatus();
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return fileSystem.getStatus(convertToDefaultPath(p));
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    fileSystem.setPermission(convertToDefaultPath(p), permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    fileSystem.setOwner(convertToDefaultPath(p), username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    fileSystem.setTimes(convertToDefaultPath(p), mtime, atime);
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    return convertToHoodiePath(fileSystem.createSnapshot(convertToDefaultPath(path), snapshotName));
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
    fileSystem.renameSnapshot(convertToDefaultPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    fileSystem.deleteSnapshot(convertToDefaultPath(path), snapshotName);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    fileSystem.modifyAclEntries(convertToDefaultPath(path), aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    fileSystem.removeAclEntries(convertToDefaultPath(path), aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    fileSystem.removeDefaultAcl(convertToDefaultPath(path));
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    fileSystem.removeAcl(convertToDefaultPath(path));
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    fileSystem.setAcl(convertToDefaultPath(path), aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return fileSystem.getAclStatus(convertToDefaultPath(path));
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    fileSystem.setXAttr(convertToDefaultPath(path), name, value);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    fileSystem.setXAttr(convertToDefaultPath(path), name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return fileSystem.getXAttr(convertToDefaultPath(path), name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return fileSystem.getXAttrs(convertToDefaultPath(path));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    return fileSystem.getXAttrs(convertToDefaultPath(path), names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return fileSystem.listXAttrs(convertToDefaultPath(path));
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    fileSystem.removeXAttr(convertToDefaultPath(path), name);
  }

  @Override
  public Configuration getConf() {
    return fileSystem.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    // ignore this. we will set conf on init
  }

  @Override
  public int hashCode() {
    return fileSystem.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return fileSystem.equals(obj);
  }

  @Override
  public String toString() {
    return fileSystem.toString();
  }

  public Path convertToHoodiePath(Path oldPath) {
    return convertPathWithScheme(oldPath, getHoodieScheme(getScheme()));
  }

  private Path convertToDefaultPath(Path oldPath) {
    return convertPathWithScheme(oldPath, getScheme());
  }

  private Path convertToLocalPath(Path oldPath) {
    try {
      return convertPathWithScheme(oldPath, FileSystem.getLocal(getConf()).getScheme());
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private Path[] convertLocalPaths(Path[] psrcs) {
    Path[] psrcsNew = new Path[psrcs.length];
    for (int i = 0; i < psrcs.length; i++) {
      psrcsNew[i] = convertToLocalPath(psrcs[i]);
    }
    return psrcsNew;
  }

  private Path[] convertDefaults(Path[] psrcs) {
    Path[] psrcsNew = new Path[psrcs.length];
    for (int i = 0; i < psrcs.length; i++) {
      psrcsNew[i] = convertToDefaultPath(psrcs[i]);
    }
    return psrcsNew;
  }

  public long getBytesWritten(Path file) {
    if (openStreams.containsKey(file.getName())) {
      return openStreams.get(file.getName()).getBytesWritten();
    }
    // When the file is first written, we do not have a track of it
    throw new IllegalArgumentException(
        file.toString() + " does not have a open stream. Cannot get the bytes written on the stream");
  }

  protected boolean needCreateTempFile() {
    return HDFS.getScheme().equals(fileSystem.getScheme());
  }

  /**
   * Creates a new file with overwrite set to false. This ensures files are created
   * only once and never rewritten, also, here we take care if the content is not
   * empty, will first write the content to a temp file if {needCreateTempFile} is
   * true, and then rename it back after the content is written.
   *
   * @param fullPath File Path
   * @param content Content to be stored
   */
  public void createImmutableFileInPath(Path fullPath, Option<byte[]> content)
      throws HoodieIOException {
    FSDataOutputStream fsout = null;
    Path tmpPath = null;

    boolean needTempFile = needCreateTempFile();

    try {
      if (!content.isPresent()) {
        fsout = fileSystem.create(fullPath, false);
      }

      if (content.isPresent() && needTempFile) {
        Path parent = fullPath.getParent();
        tmpPath = new Path(parent, fullPath.getName() + TMP_PATH_POSTFIX);
        fsout = fileSystem.create(tmpPath, false);
        fsout.write(content.get());
      }

      if (content.isPresent() && !needTempFile) {
        fsout = fileSystem.create(fullPath, false);
        fsout.write(content.get());
      }
    } catch (IOException e) {
      String errorMsg = "Failed to create file" + (tmpPath != null ? tmpPath : fullPath);
      throw new HoodieIOException(errorMsg, e);
    } finally {
      try {
        if (null != fsout) {
          fsout.close();
        }
      } catch (IOException e) {
        String errorMsg = "Failed to close file" + (needTempFile ? tmpPath : fullPath);
        throw new HoodieIOException(errorMsg, e);
      }

      try {
        if (null != tmpPath) {
          fileSystem.rename(tmpPath, fullPath);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to rename " + tmpPath + " to the target " + fullPath, e);
      }
    }
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }
}
