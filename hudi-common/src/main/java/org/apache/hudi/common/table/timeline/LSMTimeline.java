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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.model.HoodieLSMTimelineManifest;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ArchivedInstantReadSchemas;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;

/**
 * Represents the LSM Timeline for the Hoodie table.
 *
 * <p>After several instants are accumulated as a batch on the active timeline, they would be flushed as a parquet file into the LSM timeline.
 * In general the timeline is comprised with parquet files with LSM style file layout. Each new operation to the timeline yields
 * a new snapshot version. Theoretically, there could be multiple snapshot versions on the timeline.
 *
 * <p><h2>The LSM Timeline Layout</h2>
 *
 * <pre>
 *   t111, t112 ... t120 ... ->
 *     \              /
 *        \        /
 *            |
 *            V
 *   t111_t120_0.parquet, t101_t110_0.parquet,...  t11_t20_0.parquet    L0
 *                                  \                    /
 *                                     \              /
 *                                            |
 *                                            V
 *                                    t11_t100_1.parquet                L1
 *
 *      manifest_1, manifest_2, ... manifest_12
 *                                      |
 *                                      V
 *                                  _version_
 * </pre>
 *
 * <p><h2>The LSM Tree Compaction</h2>
 * Use the universal compaction strategy, that is: when N(by default 10) number of parquet files exist in the current layer, they are merged and flush as a compacted file in the next layer.
 * We have no limit for the layer number, assumes there are 10 instants for each file in L0, there could be 100 instants per file in L1,
 * so 3000 instants could be represented as 3 parquets in L2, it is pretty fast if we apply concurrent read.
 *
 * <p>The benchmark shows 1000 instants reading cost about 10 ms.
 *
 * <p><h2>The Archiver & Reader Snapshot Isolation</h2>
 *
 * <p>In order to make snapshot isolation of the LSM timeline write/read, we add two kinds of metadata files for the LSM tree version management:
 * <ol>
 *   <li>Manifest file: Each new file in layer 0 or each compaction would generate a new manifest file, the manifest file records the valid file handles of the latest snapshot;</li>
 *   <li>Version file: A version file is generated right after a new manifest file is formed.</li>
 * </ol>
 *
 * <p><h2>The Reader Workflow</h2>
 * <ul>
 *   <li>read the latest version;</li>
 *   <li>read the manifest file for valid file handles;</li>
 *   <li>read the data files, probably do a data skipping with the parquet file name max min timestamp.</li>
 * </ul>
 *
 * <p><h2>The Legacy Files Cleaning and Read Retention</h2>
 * Only triggers file cleaning after a valid compaction.
 *
 * <p><h3>Clean Strategy</h3></p>
 * Keeps only 3 valid snapshot versions for the reader, that means, a file is kept for at lest 3 archival trigger interval, for default configuration, it is 30 instants time span,
 * which is far longer that the LSM timeline loading time.
 *
 * <p><h3>Instants TTL</h3></p>
 * The timeline reader only reads instants of last limited days. We will by default skip the instants from LSM timeline that are generated long time ago.
 */
public class LSMTimeline {
  private static final Logger LOG = LoggerFactory.getLogger(LSMTimeline.class);

  public static final int LSM_TIMELINE_INSTANT_VERSION_1 = 1;

  private static final String VERSION_FILE_NAME = "_version_";    // _version_
  private static final String MANIFEST_FILE_PREFIX = "manifest_"; // manifest_[N]

  private static final String TEMP_FILE_SUFFIX = ".tmp";

  private static final Pattern ARCHIVE_FILE_PATTERN =
      Pattern.compile("^(\\d+)_(\\d+)_(\\d)\\.parquet");

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  public static Schema getReadSchema(HoodieArchivedTimeline.LoadMode loadMode) {
    switch (loadMode) {
      case TIME:
        return ArchivedInstantReadSchemas.TIMELINE_LSM_READ_SCHEMA_WITH_TIME;
      case ACTION:
        return ArchivedInstantReadSchemas.TIMELINE_LSM_READ_SCHEMA_WITH_ACTION;
      case METADATA:
        return ArchivedInstantReadSchemas.TIMELINE_LSM_READ_SCHEMA_WITH_METADATA;
      case PLAN:
        return ArchivedInstantReadSchemas.TIMELINE_LSM_READ_SCHEMA_WITH_PLAN;
      case FULL:
        return ArchivedInstantReadSchemas.TIMELINE_LSM_READ_SCHEMA_AS_FULL;
      default:
        throw new AssertionError("Unexpected");
    }
  }

  /**
   * Returns whether the given file is located in the filter.
   */
  public static boolean isFileInRange(HoodieArchivedTimeline.TimeRangeFilter filter, String fileName) {
    String minInstant = getMinInstantTime(fileName);
    String maxInstant = getMaxInstantTime(fileName);
    return filter.hasOverlappingInRange(minInstant, maxInstant);
  }

  /**
   * Returns the latest snapshot version.
   */
  public static int latestSnapshotVersion(HoodieTableMetaClient metaClient, StoragePath archivePath) throws IOException {
    StoragePath versionFilePath = getVersionFilePath(archivePath);
    try {
      Option<byte[]> content =
          FileIOUtils.readDataFromPath(metaClient.getStorage(), versionFilePath, true);
      if (content.isPresent()) {
        return Integer.parseInt(fromUTF8Bytes(content.get()));
      }
    } catch (Exception e) {
      // fallback to manifest file listing.
      LOG.warn("Error reading version file {}", versionFilePath, e);
    }

    return allSnapshotVersions(metaClient, archivePath).stream().max(Integer::compareTo).orElse(-1);
  }

  /**
   * Returns all the valid snapshot versions.
   */
  public static List<Integer> allSnapshotVersions(HoodieTableMetaClient metaClient, StoragePath archivePath) throws IOException {
    try {
      return metaClient.getStorage().listDirectEntries(archivePath,
              getManifestFilePathFilter())
          .stream()
          .map(fileStatus -> fileStatus.getPath().getName())
          .map(LSMTimeline::getManifestVersion)
          .collect(Collectors.toList());
    } catch (FileNotFoundException ex) {
      LOG.debug("Archive path {} does not exist", archivePath);
      return Collections.emptyList();
    }
  }

  /**
   * Returns the latest snapshot metadata files.
   */
  public static HoodieLSMTimelineManifest latestSnapshotManifest(HoodieTableMetaClient metaClient, StoragePath archivePath) throws IOException {
    int latestVersion = latestSnapshotVersion(metaClient, archivePath);
    return latestSnapshotManifest(metaClient, latestVersion, archivePath);
  }

  /**
   * Reads the file list from the manifest file for the latest snapshot.
   */
  public static HoodieLSMTimelineManifest latestSnapshotManifest(HoodieTableMetaClient metaClient, int latestVersion, StoragePath archivePath) {
    if (latestVersion < 0) {
      // there is no valid snapshot of the timeline.
      return HoodieLSMTimelineManifest.EMPTY;
    }
    // read and deserialize the valid files.
    byte[] content =
        FileIOUtils.readDataFromPath(metaClient.getStorage(), getManifestFilePath(latestVersion, archivePath)).get();
    try {
      return HoodieLSMTimelineManifest.fromJsonString(fromUTF8Bytes(content), HoodieLSMTimelineManifest.class);
    } catch (Exception e) {
      throw new HoodieException("Error deserializing manifest entries", e);
    }
  }

  /**
   * Returns the full manifest file path with given version number.
   */
  public static StoragePath getManifestFilePath(int snapshotVersion, StoragePath archivePath) {
    return new StoragePath(archivePath, MANIFEST_FILE_PREFIX + snapshotVersion);
  }

  /**
   * Returns the full version file path with given version number.
   */
  public static StoragePath getVersionFilePath(StoragePath archivePath) {
    return new StoragePath(archivePath, VERSION_FILE_NAME);
  }

  /**
   * List all the parquet manifest files.
   */
  public static List<StoragePathInfo> listAllManifestFiles(HoodieTableMetaClient metaClient, StoragePath archivePath)
      throws IOException {
    return metaClient.getStorage().listDirectEntries(
        archivePath, getManifestFilePathFilter());
  }

  /**
   * List all the parquet metadata files.
   */
  public static List<StoragePathInfo> listAllMetaFiles(HoodieTableMetaClient metaClient, StoragePath archivePath) throws IOException {
    return metaClient.getStorage().globEntries(
        new StoragePath(archivePath, "*.parquet"));
  }

  /**
   * Parse the snapshot version from the manifest file name.
   */
  public static int getManifestVersion(String fileName) {
    return Integer.parseInt(fileName.split("_")[1]);
  }

  /**
   * Parse the layer number from the file name.
   */
  public static int getFileLayer(String fileName) {
    try {
      Matcher fileMatcher = ARCHIVE_FILE_PATTERN.matcher(fileName);
      if (fileMatcher.matches()) {
        return Integer.parseInt(fileMatcher.group(3));
      }
    } catch (NumberFormatException e) {
      // log and ignore any format warnings
      LOG.warn("error getting file layout for archived file: {}", fileName, e);
    }

    // return default value in case of any errors
    return 0;
  }

  /**
   * Parse the minimum instant time from the file name.
   */
  public static String getMinInstantTime(String fileName) {
    Matcher fileMatcher = ARCHIVE_FILE_PATTERN.matcher(fileName);
    if (fileMatcher.matches()) {
      return fileMatcher.group(1);
    } else {
      throw new HoodieException("Unexpected archival file name: " + fileName);
    }
  }

  /**
   * Parse the maximum instant time from the file name.
   */
  public static String getMaxInstantTime(String fileName) {
    Matcher fileMatcher = ARCHIVE_FILE_PATTERN.matcher(fileName);
    if (fileMatcher.matches()) {
      return fileMatcher.group(2);
    } else {
      throw new HoodieException("Unexpected archival file name: " + fileName);
    }
  }

  /**
   * Returns whether a file belongs to the specified layer {@code layer} within the LSM layout.
   */
  public static boolean isFileFromLayer(String fileName, int layer) {
    return getFileLayer(fileName) == layer;
  }

  /**
   * Returns a path filter for the manifest files.
   */
  public static StoragePathFilter getManifestFilePathFilter() {
    return path -> path.getName().startsWith(MANIFEST_FILE_PREFIX) && !path.getName().endsWith(TEMP_FILE_SUFFIX);
  }
}
