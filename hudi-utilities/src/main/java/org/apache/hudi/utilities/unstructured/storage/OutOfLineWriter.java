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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Writes prepared payload files named in Hudi's base-file form, {@code <fileId>_<writeToken>_<instantTime>.bin}
 * ({@link FSUtils#makeBaseFileName}): a fresh file id per file, the write token of the writing Spark task (partition, stage,
 * attempt; outside a Spark task Hudi's default token {@code 0-0-0}, names staying unique by file id) and the write instant, all
 * independent of the table's data files. The name encodes no record identity. With
 * {@code hoodie.write.blob.markers.enable=true} (the default) each write creates its instant's timeline-server CREATE
 * marker before a single non-overwriting create, and in-flight rollback deletes every marked file. With {@code false} no marker
 * is created: the files of a failed round are left under {@code .hoodie/.blob}, referenced by no row. Files the committed round
 * does not reference stay under {@code .hoodie/.blob} after the commit: those of successful task retries or recomputes, and a
 * failed write's file whose removal failed when the round still commits (a quarantined row); the commit's marker
 * reconciliation skips the blob folder, and reconciling those orphans is out of scope. Restoring a committed instant is
 * unsupported.
 */
@Slf4j
public final class OutOfLineWriter {

  /** Suffix of every out-of-line file name. */
  private static final String FILE_EXTENSION = ".bin";

  private final HoodieStorage storage;
  private final String basePath;
  private final String instantTime;
  private final Option<WriteMarkers> markers;
  private final TaskContextSupplier taskContextSupplier;

  /**
   * Requires timeline-server markers when markers are given, including when the factory would otherwise fall back to direct
   * markers. With no markers (out-of-line markers disabled) no file is marked. File names carry {@code instantTime} and the
   * write token of the Spark task that writes them.
   */
  public OutOfLineWriter(HoodieStorage storage, String basePath, String instantTime, Option<WriteMarkers> markers) {
    this(storage, basePath, instantTime, markers, new SparkTaskContextSupplier());
  }

  @VisibleForTesting
  OutOfLineWriter(HoodieStorage storage, String basePath, String instantTime, Option<WriteMarkers> markers,
                  TaskContextSupplier taskContextSupplier) {
    if (markers.isPresent() && !(markers.get() instanceof TimelineServerBasedWriteMarkers)) {
      throw new HoodieException("Out-of-line writes require TIMELINE_SERVER_BASED markers; direct-marker fallback is refused");
    }
    this.storage = storage;
    this.basePath = basePath;
    this.instantTime = instantTime;
    this.markers = markers;
    this.taskContextSupplier = taskContextSupplier;
  }

  /**
   * Builds the write config and table once, on the driver, from the running write client's effective configuration,
   * including its timeline-server address. Refuses a target below table version 9 before writing, even when the job uses a
   * newer write version, and, with {@code hoodie.write.blob.markers.enable=true}, a configuration whose markers would fall
   * back to direct markers in every task (an HDFS base path, or no embedded or remote timeline server). Tasks create their
   * writers from the returned factory.
   */
  public static Factory forTable(StorageConfiguration<?> storageConf, String basePath, Properties props) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(props).withPath(basePath).build();
    HoodieTable<?, ?, ?, ?> table = HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf));
    HoodieTableVersion version = table.getMetaClient().getTableConfig().getTableVersion();
    if (version.lesserThan(HoodieTableVersion.NINE)) {
      throw new HoodieValidationException("Out-of-line writes require target table version 9 or above: " + basePath
          + " is version " + version.versionCode());
    }
    Factory factory = new Factory(basePath, table);
    factory.checkTimelineServerMarkers();
    return factory;
  }

  /**
   * The driver-built table, serialized into tasks. A task creates its instant's markers from it, as write handles do
   * (HoodieWriteHandle); the markers themselves are not shipped, since their marker folder path does not serialize.
   */
  public static class Factory implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String basePath;
    private final HoodieTable<?, ?, ?, ?> table;

    @VisibleForTesting
    Factory(String basePath, HoodieTable<?, ?, ?, ?> table) {
      this.basePath = basePath;
      this.table = table;
    }

    /**
     * Refuses, on the driver, out-of-line markers that would not be timeline-server based: every task's writer would refuse
     * them ({@link OutOfLineWriter}).
     */
    private void checkTimelineServerMarkers() {
      if (!table.getConfig().isBlobMarkersEnabled()) {
        return;
      }
      MarkerType markerType = table.getConfig().getMarkersType();
      if (markerType != MarkerType.TIMELINE_SERVER_BASED
          || !(WriteMarkersFactory.get(markerType, table, HoodieTimeline.INIT_INSTANT_TS) instanceof TimelineServerBasedWriteMarkers)) {
        throw new HoodieValidationException("Out-of-line writes to " + basePath + " require TIMELINE_SERVER_BASED markers served by "
            + "a timeline server, but the write gets direct markers (" + HoodieWriteConfig.MARKERS_TYPE.key() + "=" + markerType
            + ", an HDFS base path, or no embedded or remote timeline server). Set "
            + HoodieWriteConfig.BLOB_MARKERS_ENABLE.key() + "=false to write out-of-line files without markers");
      }
    }

    /** A writer for {@code instant}, with no table, metaClient or write config built in the task. */
    public OutOfLineWriter forInstant(HoodieStorage storage, String instant) {
      return new OutOfLineWriter(storage, basePath, instant, markers(instant));
    }

    /** The instant's timeline-server markers, or none when {@code hoodie.write.blob.markers.enable} is false. */
    @VisibleForTesting
    Option<WriteMarkers> markers(String instant) {
      if (!table.getConfig().isBlobMarkersEnabled()) {
        return Option.empty();
      }
      MarkerType markerType = table.getConfig().getMarkersType();
      if (markerType != MarkerType.TIMELINE_SERVER_BASED) {
        throw new HoodieException("Out-of-line writes require TIMELINE_SERVER_BASED markers");
      }
      return Option.of(WriteMarkersFactory.get(markerType, table, instant));
    }
  }

  /** A bounded streaming operation; callers classify source and target failures using their existing contracts. */
  @FunctionalInterface
  public interface StreamingWrite {
    void write(OutputStream out) throws IOException;
  }

  /**
   * Generates a fresh relative path ({@code <fileId>_<writeToken>_<instantTime>.bin}: a new file id, the calling task's write
   * token), marks it when markers are enabled, then creates and streams it exactly once. A failed
   * stream or close removes the file best-effort; if removal fails, a rollback of the round deletes it through its marker (with
   * markers disabled it stays orphaned), and a round that still commits leaves it unreferenced. Returns only after the file is
   * closed.
   */
  public String write(String partition, StreamingWrite stream) throws IOException {
    validatePartition(partition);
    String directory = ManagedBlobPaths.FOLDER + (StringUtils.isNullOrEmpty(partition) ? "" : "/" + partition);
    String name = FSUtils.makeBaseFileName(instantTime, FSUtils.makeWriteToken(taskContextSupplier),
        FSUtils.createNewFileId(FSUtils.createNewFileIdPfx(), 0), FILE_EXTENSION);
    String relative = directory + "/" + name;
    if (markers.isPresent() && !markers.get().create(directory, name, IOType.CREATE).isPresent()) {
      throw new HoodieIOException("Timeline server did not create the out-of-line marker for " + relative);
    }
    StoragePath target = new StoragePath(basePath, relative);
    boolean created = false;
    try (OutputStream out = storage.create(target, false)) {
      created = true;
      stream.write(out);
    } catch (Throwable failure) {
      // an Error too (a codec class missing on the executor): the partial file must not outlive the write that failed
      if (created) {
        try {
          storage.deleteFile(target);
        } catch (IOException | RuntimeException cleanup) {
          failure.addSuppressed(cleanup);
          log.warn("Could not remove failed out-of-line file {}: a rollback of the round deletes it through its marker "
              + "(with out-of-line markers disabled it stays orphaned); if the round commits, it stays unreferenced", target, cleanup);
        }
      }
      throw failure;
    }
    return relative;
  }

  /** Keeps generated paths relative and contained; numeric partition segments carry no special meaning. */
  public static void validatePartition(String partition) {
    if (!StringUtils.isNullOrEmpty(partition)) {
      for (String segment : partition.split("/", -1)) {
        if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
          throw new IllegalArgumentException("Out-of-line partition must have relative segments without empty, '.' or '..': " + partition);
        }
      }
    }
  }
}
