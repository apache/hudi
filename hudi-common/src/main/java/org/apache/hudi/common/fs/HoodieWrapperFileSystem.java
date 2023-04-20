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
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableConfig.StorageStrategy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.CachingPath;

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

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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

  /**
   * Names for metrics.
   */
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
  private FileSystem storageFileSystem;
  private FileSystem tableFileSystem;
  private URI uri;
  private ConsistencyGuard consistencyGuard = new NoOpConsistencyGuard();
  private HoodieStorageStrategy storageStrategy;

  /**
   * Checked function interface.
   *
   * @param <R> Type of return value.
   */
  @FunctionalInterface
  public interface CheckedFunction<R> {
    R get() throws IOException;
  }

  private static Registry getMetricRegistryForPath(Path p) {
    return ((p != null) && (p.toString().contains(HoodieTableMetaClient.METAFOLDER_NAME)))
        ? METRICS_REGISTRY_META : METRICS_REGISTRY_DATA;
  }

  protected static <R> R executeFuncWithTimeMetrics(String metricName, Path p, CheckedFunction<R> func) throws IOException {
    HoodieTimer timer = HoodieTimer.start();
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
    this.tableFileSystem = fileSystem;
    this.storageFileSystem = fileSystem;
    this.uri = fileSystem.getUri();
    this.consistencyGuard = consistencyGuard;
  }

  public HoodieWrapperFileSystem(FileSystem tableFileSystem, FileSystem storageFileSystem, ConsistencyGuard consistencyGuard, HoodieStorageStrategy storageStrategy) {
    this.tableFileSystem = tableFileSystem;
    this.storageFileSystem = storageFileSystem;
    this.uri = tableFileSystem.getUri();
    this.consistencyGuard = consistencyGuard;
    this.storageStrategy = storageStrategy;
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
      newURI = new URI(newScheme,
                oldURI.getAuthority(),
                oldURI.getPath(),
                oldURI.getQuery(),
                oldURI.getFragment());
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
    this.tableFileSystem = FSUtils.getFs(path.toString(), conf);

    // Do not need to explicitly initialize the default filesystem, its done already in the above
    // FileSystem.get
    // fileSystem.initialize(FileSystem.getDefaultUri(conf), conf);
    // fileSystem.setConf(conf);

    // extracting properties from conf to initialize storage strategy
    String storageStrategyClass =
        conf.get(HoodieTableConfig.HOODIE_STORAGE_STRATEGY_CLASS_NAME_KEY, StorageStrategy.DEFAULT.value);
    String basePath = conf.get(HoodieTableConfig.HOODIE_BASE_PATH_KEY);
    Map<String, String> storageStrategyProps = new HashMap<>();
    String storagePath = conf.get(HoodieTableConfig.HOODIE_STORAGE_PATH_KEY);
    storageStrategyProps.put(HoodieTableConfig.HOODIE_STORAGE_PATH_KEY, storagePath);
    storageStrategyProps.put(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, conf.get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY));

    this.storageFileSystem = FSUtils.getFs(storagePath, conf);
    this.storageStrategy = (HoodieStorageStrategy) ReflectionUtils
        .loadClass(storageStrategyClass, new Class[]{String.class, Map.class}, basePath, storageStrategyProps);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return wrapInputStream(f, getFileSystem(f).open(convertToDefaultPath(f), bufferSize));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      final Path translatedPath = convertToDefaultPath(f);
      return wrapOutputStream(f,
          getFileSystem(translatedPath).create(translatedPath, permission, overwrite, bufferSize, replication, blockSize, progress));
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
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f), overwrite));
    });
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f)));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f), progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f), replication));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f), replication, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f), overwrite, bufferSize));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f), overwrite, bufferSize, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f,
          getFileSystem(f).create(convertToDefaultPath(f), overwrite, bufferSize, replication, blockSize, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f,
          getFileSystem(f).create(convertToDefaultPath(f), permission, flags, bufferSize, replication, blockSize, progress));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f, getFileSystem(f).create(convertToDefaultPath(f), permission, flags, bufferSize, replication,
          blockSize, progress, checksumOpt));
    });
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws IOException {
    return executeFuncWithTimeMetrics(MetricName.create.name(), f, () -> {
      return wrapOutputStream(f,
          getFileSystem(f).create(convertToDefaultPath(f), overwrite, bufferSize, replication, blockSize));
    });
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return wrapOutputStream(f, getFileSystem(f).append(convertToDefaultPath(f), bufferSize, progress));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.rename.name(), src, () -> {
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(src));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for " + src + " to appear", e);
      }

      boolean success = getFileSystem(src).rename(convertToDefaultPath(src), convertToDefaultPath(dst));

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
      boolean success = getFileSystem(f).delete(convertToDefaultPath(f), recursive);

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
      return getFileSystem(f).listStatus(convertToDefaultPath(f));
    });
  }

  @Override
  public Path getWorkingDirectory() {
    return convertToHoodiePath(getTableFileSystem().getWorkingDirectory());
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    getFileSystem(newDir).setWorkingDirectory(convertToDefaultPath(newDir));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.mkdirs.name(), f, () -> {
      boolean success = getFileSystem(f).mkdirs(convertToDefaultPath(f), permission);
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
      return getFileSystem(f).getFileStatus(convertToDefaultPath(f));
    });
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public String getCanonicalServiceName() {
    return getTableFileSystem().getCanonicalServiceName();
  }

  @Override
  public String getName() {
    return getTableFileSystem().getName();
  }

  @Override
  public Path makeQualified(Path path) {
    return convertToHoodiePath(getFileSystem(path).makeQualified(convertToDefaultPath(path)));
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return getTableFileSystem().getDelegationToken(renewer);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
    return getTableFileSystem().addDelegationTokens(renewer, credentials);
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    return getTableFileSystem().getChildFileSystems();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    return getFileSystem(file.getPath()).getFileBlockLocations(file, start, len);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    return getFileSystem(p).getFileBlockLocations(convertToDefaultPath(p), start, len);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return getTableFileSystem().getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    return getFileSystem(p).getServerDefaults(convertToDefaultPath(p));
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    return convertToHoodiePath(getFileSystem(p).resolvePath(convertToDefaultPath(p)));
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return wrapInputStream(f, getFileSystem(f).open(convertToDefaultPath(f)));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
                                               long blockSize, Progressable progress) throws IOException {
    Path p = convertToDefaultPath(f);
    return wrapOutputStream(p,
        getFileSystem(f).createNonRecursive(p, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                               short replication, long blockSize, Progressable progress) throws IOException {
    Path p = convertToDefaultPath(f);
    return wrapOutputStream(p,
        getFileSystem(p).createNonRecursive(p, permission, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags,
                                               int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    Path p = convertToDefaultPath(f);
    return wrapOutputStream(p,
        getFileSystem(p).createNonRecursive(p, permission, flags, bufferSize, replication, blockSize, progress));
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    boolean newFile = getFileSystem(f).createNewFile(convertToDefaultPath(f));
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
    return wrapOutputStream(f, getFileSystem(f).append(convertToDefaultPath(f)));
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return wrapOutputStream(f, getFileSystem(f).append(convertToDefaultPath(f), bufferSize));
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    Path[] psrcsNew = convertDefaults(psrcs);
    getFileSystem(trg).concat(convertToDefaultPath(trg), psrcsNew);
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(trg));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for " + trg + " to appear", e);
    }
  }

  @Override
  public short getReplication(Path src) throws IOException {
    return getFileSystem(src).getReplication(convertToDefaultPath(src));
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    return getFileSystem(src).setReplication(convertToDefaultPath(src), replication);
  }

  @Override
  public boolean delete(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.delete.name(), f, () -> {
      return delete(f, true);
    });
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    return getFileSystem(f).deleteOnExit(convertToDefaultPath(f));
  }

  @Override
  public boolean cancelDeleteOnExit(Path f) {
    return getFileSystem(f).cancelDeleteOnExit(convertToDefaultPath(f));
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return getFileSystem(f).exists(convertToDefaultPath(f));
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    return getFileSystem(f).isDirectory(convertToDefaultPath(f));
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    return getFileSystem(f).isFile(convertToDefaultPath(f));
  }

  @Override
  public long getLength(Path f) throws IOException {
    return getFileSystem(f).getLength(convertToDefaultPath(f));
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return getFileSystem(f).getContentSummary(convertToDefaultPath(f));
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
    return getFileSystem(path).listCorruptFileBlocks(convertToDefaultPath(path));
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), f, () -> {
      return getFileSystem(f).listStatus(convertToDefaultPath(f), filter);
    });
  }

  public FileStatus[] listStatus(Path path, String fileId, PathFilter filter) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), path, () -> {
      return getFileSystem(path).listStatus(convertToDefaultPath(path, fileId), filter);
    });
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), files.length > 0 ? files[0] : null, () -> {
      return getFileSystem(files[0]).listStatus(convertDefaults(files));
    });
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listStatus.name(), files.length > 0 ? files[0] : null, () -> {
      return getFileSystem(files[0]).listStatus(convertDefaults(files), filter);
    });
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.globStatus.name(), pathPattern, () -> {
      return getFileSystem(pathPattern).globStatus(convertToDefaultPath(pathPattern));
    });
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.globStatus.name(), pathPattern, () -> {
      return getFileSystem(pathPattern).globStatus(convertToDefaultPath(pathPattern), filter);
    });
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    return getFileSystem(f).listLocatedStatus(convertToDefaultPath(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.listFiles.name(), f, () -> {
      return getFileSystem(f).listFiles(convertToDefaultPath(f), recursive);
    });
  }

  @Override
  public Path getHomeDirectory() {
    return convertToHoodiePath(getTableFileSystem().getHomeDirectory());
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return executeFuncWithTimeMetrics(MetricName.mkdirs.name(), f, () -> {
      boolean success = getFileSystem(f).mkdirs(convertToDefaultPath(f));
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
    getFileSystem(src).copyFromLocalFile(convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    getFileSystem(dst).moveFromLocalFile(convertLocalPaths(srcs), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    getFileSystem(dst).moveFromLocalFile(convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    getFileSystem(dst).copyFromLocalFile(delSrc, convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    getFileSystem(dst).copyFromLocalFile(delSrc, overwrite, convertLocalPaths(srcs), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    getFileSystem(dst).copyFromLocalFile(delSrc, overwrite, convertToLocalPath(src), convertToDefaultPath(dst));
    try {
      consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
    }
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    getFileSystem(src).copyToLocalFile(convertToDefaultPath(src), convertToLocalPath(dst));
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    getFileSystem(src).moveToLocalFile(convertToDefaultPath(src), convertToLocalPath(dst));
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    getFileSystem(src).copyToLocalFile(delSrc, convertToDefaultPath(src), convertToLocalPath(dst));
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    getFileSystem(src).copyToLocalFile(delSrc, convertToDefaultPath(src), convertToLocalPath(dst), useRawLocalFileSystem);
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    return convertToHoodiePath(
        getFileSystem(fsOutputFile).startLocalOutput(convertToDefaultPath(fsOutputFile), convertToDefaultPath(tmpLocalFile)));
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    getFileSystem(fsOutputFile).completeLocalOutput(convertToDefaultPath(fsOutputFile), convertToDefaultPath(tmpLocalFile));
  }

  @Override
  public void close() throws IOException {
    // Don't close the wrapped `fileSystem` object. This will end up closing it for every thread since it
    // could be cached across jvm. We don't own that object anyway.
    super.close();
  }

  @Override
  public long getUsed() throws IOException {
    return getTableFileSystem().getUsed();
  }

  @Override
  public long getBlockSize(Path f) throws IOException {
    return getFileSystem(f).getBlockSize(convertToDefaultPath(f));
  }

  @Override
  public long getDefaultBlockSize() {
    return getTableFileSystem().getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return getFileSystem(f).getDefaultBlockSize(convertToDefaultPath(f));
  }

  @Override
  public short getDefaultReplication() {
    return getTableFileSystem().getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return getFileSystem(path).getDefaultReplication(convertToDefaultPath(path));
  }

  @Override
  public void access(Path path, FsAction mode) throws IOException {
    getFileSystem(path).access(convertToDefaultPath(path), mode);
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
    getFileSystem(target).createSymlink(convertToDefaultPath(target), convertToDefaultPath(link), createParent);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    return getFileSystem(f).getFileLinkStatus(convertToDefaultPath(f));
  }

  @Override
  public boolean supportsSymlinks() {
    return getTableFileSystem().supportsSymlinks();
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    return convertToHoodiePath(getFileSystem(f).getLinkTarget(convertToDefaultPath(f)));
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return getFileSystem(f).getFileChecksum(convertToDefaultPath(f));
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    return getFileSystem(f).getFileChecksum(convertToDefaultPath(f), length);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    tableFileSystem.setVerifyChecksum(verifyChecksum);
    storageFileSystem.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    tableFileSystem.setWriteChecksum(writeChecksum);
    storageFileSystem.setWriteChecksum(writeChecksum);
  }

  @Override
  public FsStatus getStatus() throws IOException {
    return getTableFileSystem().getStatus();
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return getFileSystem(p).getStatus(convertToDefaultPath(p));
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    getFileSystem(p).setPermission(convertToDefaultPath(p), permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    getFileSystem(p).setOwner(convertToDefaultPath(p), username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    getFileSystem(p).setTimes(convertToDefaultPath(p), mtime, atime);
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    return convertToHoodiePath(
        getFileSystem(path).createSnapshot(convertToDefaultPath(path), snapshotName));
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
    getFileSystem(path).renameSnapshot(convertToDefaultPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    getFileSystem(path).deleteSnapshot(convertToDefaultPath(path), snapshotName);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    getFileSystem(path).modifyAclEntries(convertToDefaultPath(path), aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    getFileSystem(path).removeAclEntries(convertToDefaultPath(path), aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    getFileSystem(path).removeDefaultAcl(convertToDefaultPath(path));
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    getFileSystem(path).removeAcl(convertToDefaultPath(path));
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    getFileSystem(path).setAcl(convertToDefaultPath(path), aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return getFileSystem(path).getAclStatus(convertToDefaultPath(path));
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    getFileSystem(path).setXAttr(convertToDefaultPath(path), name, value);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    getFileSystem(path).setXAttr(convertToDefaultPath(path), name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return getFileSystem(path).getXAttr(convertToDefaultPath(path), name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return getFileSystem(path).getXAttrs(convertToDefaultPath(path));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    return getFileSystem(path).getXAttrs(convertToDefaultPath(path), names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return getFileSystem(path).listXAttrs(convertToDefaultPath(path));
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    getFileSystem(path).removeXAttr(convertToDefaultPath(path), name);
  }

  @Override
  public Configuration getConf() {
    return getTableFileSystem().getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    // ignore this. we will set conf on init
  }

  @Override
  public int hashCode() {
    return getTableFileSystem().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return getTableFileSystem().equals(obj);
  }

  @Override
  public String toString() {
    return getTableFileSystem().toString();
  }

  public Path convertToHoodiePath(Path oldPath) {
    return convertPathWithScheme(oldPath, getHoodieScheme(getScheme()));
  }

  /* NOTE: Can only be used when oldPath is a file path */
  private Path convertToDefaultPath(Path oldPath) {
    return convertPathWithScheme(convertToPhysicalFilePath(oldPath), getScheme());
  }

  /* NOTE: Can only be used when oldPath is a directory */
  private Path convertToDefaultPath(Path oldPath, String fileId) {
    return convertPathWithScheme(convertToPhysicalPath(oldPath, fileId), getScheme());
  }

  private Path convertToDefaultScheme(Path oldPath) {
    return convertPathWithScheme(oldPath, getScheme());
  }

  /**
   * Return a physical location for a logical directory
   *
   * @param path logical directory
   * @param fileId fileId
   * @return physical directory where this file is located
   * */
  private Path convertToPhysicalPath(Path path, String fileId) {
    String pathStr = getPathStrWithoutScheme(path.toString());

    if (shouldConvert(pathStr)) {
      String partition = FSUtils.getRelativePartitionPath(new CachingPath(storageStrategy.getBasePath()), path);
      return new Path(storageStrategy.storageLocation(partition, fileId));
    } else {
      return path;
    }
  }

  /** Return physical file path for a logical file path
   *
   * @param filePath logical path to a file
   * @return physical path to a file
   * */
  private Path convertToPhysicalFilePath(Path filePath) {
    String pathStr = getPathStrWithoutScheme(filePath.toString());

    if (shouldConvert(pathStr)) {
      String partition = FSUtils.getRelativePartitionPath(new CachingPath(storageStrategy.getBasePath()),
          getPathStrWithoutScheme(filePath.getParent().toString()));
      String fileName = filePath.getName();
      String fileId = FSUtils.getFileId(fileName);
      Path physicalPath = new Path(String.format("%s/%s", storageStrategy.storageLocation(partition, fileId), fileName));
      LOG.info("wrapperFS, logical: " + filePath);
      LOG.info("wrapperFS, partition: " + partition);
      LOG.info("wrapperFS, physical: " + physicalPath);
      return physicalPath;
    } else {
      return filePath;
    }
  }

  private boolean shouldConvert(String pathStr) {
    return storageStrategy != null
        && !storageStrategy.getClass().getName().equals(StorageStrategy.DEFAULT.value)
        && !pathStr.contains(HoodieTableMetaClient.METAFOLDER_NAME)
        && pathStr.startsWith(storageStrategy.getBasePath())
        && !pathStr.contains(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
  }

  /* ONLY for Spark relation */
  public Path convertPath(Path filePath) {
    return convertToPhysicalFilePath(filePath);
  }

  /**
   * Check if a physical location exist with partition path and file ID
   *
   * @param partitionPath partition path of a file
   * @param fileId fileId
   * @return if this location exist
   * */
  public boolean dirExists(String partitionPath, String fileId) {
    Path physicalPath = new Path(storageStrategy.storageLocation(partitionPath, fileId));
    try {
      // use raw file system
      return getFileSystem(physicalPath).exists(physicalPath);
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to check if dir exists, dir:" + physicalPath, ioe);
    }
  }

  /**
   * Create physical partition folder for a file
   * @param partitionPath partition path of this file. e.g. 2023/02/01
   * @param fileId fileId
   * @return if this path is created successfully
   * */
  public boolean mkPath(String partitionPath, String fileId) {
    Path physicalPath = new Path(storageStrategy.storageLocation(partitionPath, fileId));
    try {
      // use raw file system
      return getFileSystem(physicalPath).mkdirs(physicalPath);
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to make dir " + physicalPath, ioe);
    }
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
    return HDFS.getScheme().equals(getTableFileSystem().getScheme());
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
        fsout = getFileSystem(fullPath).create(fullPath, false);
      }

      if (content.isPresent() && needTempFile) {
        Path parent = fullPath.getParent();
        tmpPath = new Path(parent, fullPath.getName() + TMP_PATH_POSTFIX);
        fsout = getFileSystem(tmpPath).create(tmpPath, false);
        fsout.write(content.get());
      }

      if (content.isPresent() && !needTempFile) {
        fsout = getFileSystem(fullPath).create(fullPath, false);
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
          getFileSystem(tmpPath).rename(tmpPath, fullPath);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to rename " + tmpPath + " to the target " + fullPath, e);
      }
    }
  }

  public FileSystem getFileSystem(Path path) {
    if (path == null
        || storageStrategy == null
        || storageStrategy.getClass().getName().equals(StorageStrategy.DEFAULT.value)) {
      return tableFileSystem;
    }

    // non-default storage strategy
    String pathStr = getPathStrWithoutScheme(path.toString());
    String storagePathWithoutScheme = getPathStrWithoutScheme(storageStrategy.getStoragePath());

    if (shouldConvert(pathStr)
        || pathStr.startsWith(storagePathWithoutScheme)) {
      return storageFileSystem;
    } else if (pathStr.startsWith(storageStrategy.getBasePath())) {
      // meta file or default storage strategy
      return tableFileSystem;
    }

    LOG.error("Can't get the correct FS for path: " + path + ", trying with metaFileSystem");
    return tableFileSystem;
  }

  public FileSystem getTableFileSystem() {
    return tableFileSystem;
  }

  /**
   * Extract partition path with table name, table base path, and full partition path
   *
   * @param tableName table name
   * @param basePath table base path
   * @param fullPartitionPath full partition path.
   *                          When using default storage strategy: base_path/partition_path
   *                          When using non-default storage strategy: storage_location/table_name/partition_path
   * */
  public static String getPartitionPath(String tableName, Path basePath, Path fullPartitionPath) {
    String fullPartitionPathStr = getPathStrWithoutScheme(fullPartitionPath.toString());

    int tableNameStartIndex = fullPartitionPathStr.lastIndexOf(tableName);
    if (tableNameStartIndex == -1) {
      // could be partition metadata, fall back to extract partition with the base path
      return FSUtils.getRelativePartitionPath(basePath, fullPartitionPath);
    }

    // Partition-Path could be empty for non-partitioned tables
    return tableNameStartIndex + tableName.length() == fullPartitionPathStr.length()
        ? ""
        : fullPartitionPathStr.substring(tableNameStartIndex + tableName.length() + 1);
  }

  public static String getPathStrWithoutScheme(String pathStr) {
    int start = 0;
    int slash = pathStr.indexOf(47);
    int colon;
    if (StringUtils.countMatches(pathStr, ":") > 2) {
      colon = pathStr.indexOf(":/");
    } else {
      colon = pathStr.indexOf(58);
    }

    if (colon != -1 && (slash == -1 || colon < slash)) {
      start = colon + 1;
      while (pathStr.startsWith("//", start)) {
        start++;
      }
    } else if (slash != -1) {
      start = slash;
    }

    return pathStr.substring(start);
  }
}
