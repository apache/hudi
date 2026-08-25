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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableMetaClient.TEMPFOLDER_NAME;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Covers {@link RollbackHelperV1} on a table whose base path is NOT on the local filesystem.
 *
 * <p>Every other rollback test runs the table under a local {@code file://} base path, where the
 * default storage URI of {@link HoodieStorageUtils#DEFAULT_URI} happens to be the correct
 * filesystem. That is precisely why a filesystem-resolution defect could survive unnoticed in this
 * class. These tests put the table under an object-store scheme instead, which is how the defect
 * shows up in production as {@code IllegalArgumentException: Wrong FS: s3a://..., expected:
 * file:///}.
 */
class TestRollbackHelperV1 extends HoodieRollbackTestBase {

  /**
   * Matches the production failure. Also list-status friendly per
   * {@link org.apache.hudi.storage.StorageSchemes}, so {@code getPathInfoUnderPartition} takes the
   * same branch it takes for a {@code file} base path, so the storage handle is then the only
   * difference between a passing and a failing rollback.
   */
  private static final String SCHEME = "s3a";
  private static final String BUCKET = "test-bucket";
  private static final byte[] LOG_FILE_CONTENT =
      "not a real log block, only bytes to size".getBytes(StandardCharsets.UTF_8);

  /**
   * The local filesystem exposed under a scheme other than {@code file}, so a test can tell apart
   * storage resolved from a path (correct) and storage resolved from the default URI (wrong), the
   * way s3a and gs do in production without needing a remote object store.
   *
   * <p>{@link RawLocalFileSystem#pathToFile} keeps only the path component of a URI, so a
   * {@code <scheme>://<bucket>/tmp/x} path reads and writes the local file {@code /tmp/x}. What the
   * subclass changes is only the identity the filesystem reports, which is what
   * {@link FileSystem#checkPath} validates every path against.
   */
  public static class NonLocalSchemeLocalFileSystem extends RawLocalFileSystem {
    /**
     * Answer for {@link #getUri()} until {@link #initialize} supplies the real one. The superclass
     * constructor calls {@code getUri()}, which runs before any instance field of this subclass is
     * assigned, so the fallback has to be a static.
     */
    private static final URI UNINITIALIZED_URI = URI.create(SCHEME + ":///");

    private URI uri;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
      super.initialize(name, conf);
      this.uri = URI.create(name.getScheme() + "://"
          + (name.getAuthority() == null ? "" : name.getAuthority()));
      setWorkingDirectory(new Path(this.uri.toString() + Path.SEPARATOR));
    }

    @Override
    public URI getUri() {
      return uri == null ? UNINITIALIZED_URI : uri;
    }

    @Override
    public String getScheme() {
      return getUri().getScheme();
    }

    /**
     * The superclass qualifies the process working directory against {@link #getUri()} from its own
     * constructor. Skip that: this filesystem does not know its real URI yet, and every path these
     * tests use is absolute, so the working directory is never consulted.
     */
    @Override
    protected Path getInitialWorkingDirectory() {
      return new Path(System.getProperty("user.dir"));
    }
  }

  @Override
  protected StoragePath createBasePath() {
    return new StoragePath(SCHEME + "://" + BUCKET + tmpDir + "/" + UUID.randomUUID());
  }

  @Override
  protected StorageConfiguration<?> createStorageConf() {
    Configuration hadoopConf = HoodieTestUtils.getDefaultStorageConf().unwrap();
    // hadoop-aws is not on this module's classpath, so nothing else claims the scheme.
    hadoopConf.setClass("fs." + SCHEME + ".impl", NonLocalSchemeLocalFileSystem.class, FileSystem.class);
    return HadoopFSUtils.getStorageConf(hadoopConf);
  }

  @Override
  @BeforeEach
  void setup() throws IOException {
    super.setup();
    prepareMetaClient(HoodieTableVersion.SIX);
    // Parallelism is read off a mock, which would otherwise hand back 0.
    when(config.getRollbackParallelism()).thenReturn(1);
    when(config.getFinalizeWriteParallelism()).thenReturn(1);
  }

  @AfterEach
  void tearDown() throws IOException {
    storage.deleteDirectory(basePath);
  }

  /**
   * The table really is on a non-local scheme, and the mocked storage really does reach the local
   * files behind it. Without this, a test that resolves the wrong filesystem and a test that
   * resolves the right one could both pass for the wrong reason.
   */
  @Test
  void testTableIsOnANonLocalSchemeThatStillReachesLocalFiles() throws IOException {
    assertEquals(SCHEME, basePath.toUri().getScheme());
    assertEquals(SCHEME, storage.getScheme());
    assertEquals(BUCKET, basePath.toUri().getAuthority());

    StoragePath probe = new StoragePath(basePath, "probe");
    writeBytes(probe, LOG_FILE_CONTENT);
    assertEquals(LOG_FILE_CONTENT.length, storage.getPathInfo(probe).getLength());
    assertTrue(storage.deleteFile(probe));

    // The default-URI storage is bound to the local filesystem no matter what the configuration
    // carries. This is the whole reason the path-aware overload has to be used.
    assertEquals("file", HoodieStorageUtils.getStorage(storage.getConf()).getScheme());
  }

  /**
   * A rollback that is re-attempted after an interrupted one must still resolve storage from the
   * table's own scheme.
   *
   * <p>An interrupted attempt leaves APPEND markers under the rollback instant. The retry reads
   * them back and lists the table's real partition paths to size the log files they point at.
   * Resolving that listing against the default {@code file:///} URI instead of the partition path
   * fails with "Wrong FS". Because a pending rollback reuses the same instant and the same stored
   * plan on every retry, the rollback never completes, and neither does the clean behind it.
   */
  @Test
  void testPerformRollbackAddsBackLogFilesLeftByAnInterruptedAttempt() throws IOException {
    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    String baseInstantTimeOfLogFiles = "001";
    String partition = "partition1";
    String baseFileId = UUID.randomUUID().toString();
    String logFileId = UUID.randomUUID().toString();

    // The base file this rollback deletes. It produces the rollback stat keyed on `partition`, which
    // is the left side of the join that the missing-log-file lookup hangs off.
    StoragePath baseFilePath = createBaseFileToRollback(partition, baseFileId, instantToRollback);

    // A log file an earlier, interrupted rollback attempt appended a command block to, plus the
    // APPEND marker it left behind under the rollback instant. The marker is what makes the
    // recovered log path set non-empty, which is what opens the branch under test.
    String logFileName = FileCreateUtils.logFileName(baseInstantTimeOfLogFiles, logFileId, 1);
    StoragePath logFilePath = new StoragePath(new StoragePath(basePath, partition), logFileName);
    writeBytes(logFilePath, LOG_FILE_CONTENT);
    createAppendMarker(rollbackInstantTime, partition, logFileName);

    when(timeline.lastInstant()).thenReturn(Option.of(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, rollbackInstantTime)));

    // Before the fix this threw IllegalArgumentException: Wrong FS: s3a://... expected: file:///
    List<HoodieRollbackStat> rollbackStats = new RollbackHelperV1(table, config).performRollback(
        new HoodieLocalEngineContext(storage.getConf()),
        rollbackInstantTime,
        INSTANT_GENERATOR.createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
        Collections.singletonList(baseFileRollbackRequest(partition, baseFileId, instantToRollback, baseFilePath)));

    assertEquals(1, rollbackStats.size());
    HoodieRollbackStat stat = rollbackStats.get(0);
    assertEquals(partition, stat.getPartitionPath());
    assertEquals(Collections.singletonList(baseFilePath.toString()), stat.getSuccessDeleteFiles());
    assertEquals(Collections.emptyList(), stat.getFailedDeleteFiles());

    // The recovered log file is reported with the path it actually has on the table's own scheme,
    // and with its real size. A stat that carried a local path or a wrong length would mean the
    // listing had gone to the wrong filesystem.
    assertEquals(
        Collections.singletonMap(logFilePath.toString(), (long) LOG_FILE_CONTENT.length),
        stat.getCommandBlocksCount().entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().getPath().toString(), Map.Entry::getValue)),
        "the log file left by the interrupted attempt should be added back, sized off the table's own filesystem");
  }

  /**
   * With no interrupted attempt there are no APPEND markers, so the recovered log path set is empty
   * and the missing-log-file lookup is skipped altogether. This pins the early return that hid the
   * defect for so long, so that a later change cannot quietly make the lookup unconditional or
   * quietly make it never run.
   */
  @Test
  void testPerformRollbackSkipsMissingLogFileLookupWithoutAnInterruptedAttempt() throws IOException {
    String rollbackInstantTime = "003";
    String instantToRollback = "002";
    String partition = "partition1";
    String baseFileId = UUID.randomUUID().toString();

    StoragePath baseFilePath = createBaseFileToRollback(partition, baseFileId, instantToRollback);

    when(timeline.lastInstant()).thenReturn(Option.of(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, rollbackInstantTime)));

    List<HoodieRollbackStat> rollbackStats = new RollbackHelperV1(table, config).performRollback(
        new HoodieLocalEngineContext(storage.getConf()),
        rollbackInstantTime,
        INSTANT_GENERATOR.createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback),
        Collections.singletonList(baseFileRollbackRequest(partition, baseFileId, instantToRollback, baseFilePath)));

    assertEquals(1, rollbackStats.size());
    HoodieRollbackStat stat = rollbackStats.get(0);
    assertEquals(partition, stat.getPartitionPath());
    assertEquals(Collections.singletonList(baseFilePath.toString()), stat.getSuccessDeleteFiles());
    assertEquals(Collections.emptyMap(), stat.getCommandBlocksCount());
  }

  /**
   * Pins the mechanism itself. {@link RollbackHelperV1#getPathInfoUnderPartition} branches on the
   * scheme it reads off the storage handle, so this runs the lookup once per branch: {@code s3a} is
   * list-status friendly and takes the listing branch, {@code hdfs} is not and takes the per-file
   * branch. Storage resolved from the partition path reads the partition on either branch.
   *
   * <p>Storage resolved from the default URI reads neither. It reports scheme {@code file}, so it
   * always takes the listing branch, and it always takes it against the wrong filesystem. Any
   * future call site that drops the path argument fails here.
   */
  @ParameterizedTest
  @ValueSource(strings = {"s3a", "hdfs"})
  void testPathInfoLookupNeedsStorageResolvedFromThePartitionPath(String scheme) throws IOException {
    Configuration hadoopConf = HoodieTestUtils.getDefaultStorageConf().unwrap();
    hadoopConf.setClass("fs." + scheme + ".impl", NonLocalSchemeLocalFileSystem.class, FileSystem.class);
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf(hadoopConf);

    StoragePath partitionPath = new StoragePath(
        scheme + "://" + BUCKET + tmpDir + "/" + UUID.randomUUID() + "/partition1");
    HoodieStorage pathResolvedStorage = HoodieStorageUtils.getStorage(partitionPath, storageConf);
    pathResolvedStorage.createDirectory(partitionPath);
    String fileName = "file.parquet";
    try (OutputStream out = pathResolvedStorage.create(new StoragePath(partitionPath, fileName))) {
      out.write(LOG_FILE_CONTENT);
    }

    List<Option<StoragePathInfo>> found = RollbackHelperV1.getPathInfoUnderPartition(
        pathResolvedStorage, partitionPath, new HashSet<>(Collections.singletonList(fileName)), true);
    assertEquals(1, found.size());
    assertTrue(found.get(0).isPresent());
    assertEquals(LOG_FILE_CONTENT.length, found.get(0).get().getLength());

    HoodieStorage defaultUriStorage = HoodieStorageUtils.getStorage(storageConf);
    assertEquals("file", defaultUriStorage.getScheme());
    assertThrows(
        IllegalArgumentException.class,
        () -> RollbackHelperV1.getPathInfoUnderPartition(
            defaultUriStorage, partitionPath, new HashSet<>(Collections.singletonList(fileName)), true),
        "storage bound to the default URI must not be able to read a partition on another scheme");
  }

  private HoodieRollbackRequest baseFileRollbackRequest(String partition,
                                                        String fileId,
                                                        String latestBaseInstant,
                                                        StoragePath baseFilePath) {
    return HoodieRollbackRequest.newBuilder()
        .setPartitionPath(partition)
        .setFileId(fileId)
        .setLatestBaseInstant(latestBaseInstant)
        .setFilesToBeDeleted(Collections.singletonList(baseFilePath.toString()))
        .setLogBlocksToBeDeleted(Collections.emptyMap())
        .build();
  }

  /**
   * Writes the APPEND marker an interrupted rollback attempt would have left behind, at the layout
   * {@code <base>/.hoodie/.temp/<rollbackInstant>/<partition>/<logFileName>.marker.APPEND} that
   * {@code MarkerUtils.stripMarkerFolderPrefix} expects.
   */
  private void createAppendMarker(String rollbackInstantTime,
                                  String partition,
                                  String logFileName) throws IOException {
    StoragePath markerPath = new StoragePath(
        new StoragePath(new StoragePath(new StoragePath(basePath, TEMPFOLDER_NAME), rollbackInstantTime), partition),
        logFileName + HoodieTableMetaClient.MARKER_EXTN + "." + IOType.APPEND.name());
    storage.createDirectory(markerPath.getParent());
    storage.create(markerPath).close();
  }

  private void writeBytes(StoragePath path, byte[] content) throws IOException {
    if (!storage.exists(path.getParent())) {
      storage.createDirectory(path.getParent());
    }
    try (OutputStream out = storage.create(path)) {
      out.write(content);
    }
  }
}
