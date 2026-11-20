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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tells whether the archiver ran while a validation run was in progress.
 *
 * <p>Archival removes instants without leaving one behind, so a mid-run archival would be condemned
 * as {@code CODE_BUG}; the run must abort as inconclusive instead. From table version 8 the signature
 * is {@link LSMTimeline#latestSnapshotVersion}; below it, the listing's {@code (name, length)} pairs,
 * since the v1 archiver appends to the current file. An unreadable archive counts as having run.
 */
public final class ArchivalProbe {

  private static final Logger LOG = LoggerFactory.getLogger(ArchivalProbe.class);

  /** The reading taken when the archive could not be probed. Never equal to a real reading. */
  public static final String UNKNOWN = "unknown";

  /** The first table version whose layout keeps the archive as an LSM tree with a version file. */
  private static final int LSM_LAYOUT_FROM = 8;

  private ArchivalProbe() {
  }

  /**
   * An opaque token for the current state of the table's archive, comparable only for equality.
   * Never throws.
   */
  public static String signature(HoodieTableMetaClient metaClient) {
    try {
      return usesLsmLayout(metaClient)
          ? "lsm:" + LSMTimeline.latestSnapshotVersion(metaClient, metaClient.getArchivePath())
          : legacySignature(metaClient);
    } catch (Exception e) {
      LOG.warn("Could not read the archive of table {} to tell whether archival is running; the "
              + "run will treat this as archival having possibly run", metaClient.getBasePath(), e);
      return UNKNOWN;
    }
  }

  /**
   * Whether the archiver may have run between two readings: true when they differ or either is
   * {@link #UNKNOWN}.
   */
  public static boolean archivalRan(String atPrev, String atAft) {
    return UNKNOWN.equals(atPrev) || UNKNOWN.equals(atAft) || !atPrev.equals(atAft);
  }

  /** What to put in the log line and the failure message, in the operator's terms. */
  public static String describe(String atPrev, String atAft) {
    if (UNKNOWN.equals(atPrev) || UNKNOWN.equals(atAft)) {
      return "the archive could not be read (before=" + atPrev + ", after=" + atAft
          + "), so whether the archiver ran is unknown";
    }
    return "the archive changed from " + atPrev + " to " + atAft
        + ", so instants left the active timeline without leaving anything on it to say so";
  }

  private static boolean usesLsmLayout(HoodieTableMetaClient metaClient) {
    return metaClient.getTableConfig().getTableVersion().versionCode() >= LSM_LAYOUT_FROM;
  }

  /**
   * The pre-LSM archive as {@code name:length} pairs, sorted so two readings of an unchanged
   * archive produce the same token whatever order the listing came back in.
   */
  private static String legacySignature(HoodieTableMetaClient metaClient) {
    StoragePath archivePath = metaClient.getArchivePath();
    List<StoragePathInfo> entries;
    try {
      entries = metaClient.getStorage().listDirectEntries(archivePath);
    } catch (java.io.FileNotFoundException absent) {
      // The first archival creates the directory, so absent is a real, stable never-archived reading.
      return "legacy:absent";
    } catch (Exception e) {
      LOG.warn("Could not list the archive directory {} of table {}", archivePath,
          metaClient.getBasePath(), e);
      return UNKNOWN;
    }
    List<String> parts = new ArrayList<>(entries.size());
    for (StoragePathInfo entry : entries) {
      parts.add(entry.getPath().getName() + ":" + entry.getLength());
    }
    Collections.sort(parts);
    return "legacy:" + parts;
  }
}
