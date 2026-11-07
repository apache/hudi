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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.hudi.common.table.checkpoint.CheckpointUtils.createCheckpoint;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.MAX_FILES_PER_BATCH;

/**
 * Selects the next batch of files for {@code UnstructuredFileDFSSource}.
 *
 * <p>Differs from {@link org.apache.hudi.utilities.sources.helpers.DFSPathSelector} in three ways
 * that matter at scale:
 *
 * <ul>
 *   <li>The batch is bounded by file <em>count</em> as well as bytes. A byte budget alone never
 *       bounds the number of files, and file count is what drives driver memory and task count.</li>
 *   <li>The checkpoint carries {@code (modificationTime, lastPath)} rather than a bare timestamp,
 *       so a group of files sharing one modification time can be split across syncs without the
 *       unread remainder being stranded by a {@code mtime > checkpoint} filter. Files arriving in
 *       one bulk upload routinely share a timestamp, and object stores report modification times
 *       at second granularity.</li>
 *   <li>Selected files are returned as a list of {@code (path, size, modificationTime)} rather
 *       than one comma-joined string. Paths legitimately contain commas, and re-splitting on one
 *       corrupts them.</li>
 * </ul>
 */
public class UnstructuredFilePathSelector implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final List<String> IGNORE_FILE_PREFIXES = Arrays.asList(".", "_");

  private final String rootPath;
  private final int maxFilesPerBatch;
  private final transient Configuration hadoopConf;

  public UnstructuredFilePathSelector(TypedProperties props, Configuration hadoopConf) {
    this.rootPath = getStringWithAltKeys(props, DFSPathSelectorConfig.ROOT_INPUT_PATH);
    this.maxFilesPerBatch = getIntWithAltKeys(props, MAX_FILES_PER_BATCH);
    // a non-positive cap would select nothing, leave the checkpoint untouched, and stall every
    // subsequent sync without ever failing
    ValidationUtils.checkArgument(maxFilesPerBatch > 0,
        MAX_FILES_PER_BATCH.key() + " must be positive, got " + maxFilesPerBatch);
    this.hadoopConf = hadoopConf;
  }

  /**
   * One selected file, carrying what the driver already learned from listing so executors do not
   * have to stat it a second time.
   */
  public static class FileEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String path;
    public final long size;
    public final long modificationTime;

    public FileEntry(String path, long size, long modificationTime) {
      this.path = path;
      this.size = size;
      this.modificationTime = modificationTime;
    }
  }

  /**
   * The files chosen for one sync and the checkpoint to record if it commits.
   */
  public static class Batch {
    public final List<FileEntry> files;
    public final Checkpoint checkpoint;

    Batch(List<FileEntry> files, Checkpoint checkpoint) {
      this.files = files;
      this.checkpoint = checkpoint;
    }
  }

  /**
   * Position in the source folder: everything at an earlier modification time, plus everything at
   * this modification time whose path sorts at or before {@code lastPath}, has been ingested.
   */
  @VisibleForTesting
  static class Position {
    final long modificationTime;
    final String lastPath;

    Position(long modificationTime, String lastPath) {
      this.modificationTime = modificationTime;
      this.lastPath = lastPath;
    }

    /** True when this file still needs ingesting. */
    boolean isAfter(FileStatus file) {
      if (file.getModificationTime() != modificationTime) {
        return file.getModificationTime() > modificationTime;
      }
      // same timestamp: only the tail of the group beyond lastPath remains. A checkpoint written
      // by the old timestamp-only format has no lastPath, and treating the whole group as done
      // preserves the previous behaviour rather than re-ingesting it on upgrade.
      return lastPath != null && file.getPath().toString().compareTo(lastPath) > 0;
    }
  }

  @VisibleForTesting
  static Position decode(Option<Checkpoint> checkpoint) {
    if (!checkpoint.isPresent() || checkpoint.get().getCheckpointKey() == null
        || checkpoint.get().getCheckpointKey().isEmpty()) {
      return new Position(Long.MIN_VALUE, null);
    }
    String key = checkpoint.get().getCheckpointKey();
    if (key.startsWith("{")) {
      try {
        JsonNode node = MAPPER.readTree(key);
        JsonNode path = node.get("lastPath");
        return new Position(node.get("mtime").asLong(),
            path == null || path.isNull() ? null : path.asText());
      } catch (IOException e) {
        throw new HoodieIOException("Unreadable checkpoint: " + key, e);
      }
    }
    // legacy timestamp-only checkpoint written by DFSPathSelector
    return new Position(Long.parseLong(key.trim()), null);
  }

  @VisibleForTesting
  static String encode(Position position) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("mtime", position.modificationTime);
    if (position.lastPath == null) {
      node.putNull("lastPath");
    } else {
      node.put("lastPath", position.lastPath);
    }
    return node.toString();
  }

  public Batch selectNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    Position from = decode(lastCheckpoint);
    List<FileStatus> eligible = new ArrayList<>();
    try {
      FileSystem fs = HadoopFSUtils.getFs(rootPath, hadoopConf);
      collectEligible(fs, new Path(rootPath), from, eligible);
    } catch (IOException e) {
      throw new HoodieIOException("Unable to list source root " + rootPath, e);
    }

    // ordering the whole batch by (mtime, path) is what makes a partial group resumable
    eligible.sort(Comparator.comparingLong(FileStatus::getModificationTime)
        .thenComparing(f -> f.getPath().toString()));

    List<FileEntry> selected = new ArrayList<>();
    long bytes = 0;
    Position to = from;
    for (FileStatus file : eligible) {
      if (selected.size() >= maxFilesPerBatch) {
        break;
      }
      if (!selected.isEmpty() && bytes + file.getLen() >= sourceLimit) {
        break;
      }
      String path = file.getPath().toString();
      selected.add(new FileEntry(path, file.getLen(), file.getModificationTime()));
      bytes += file.getLen();
      to = new Position(file.getModificationTime(), path);
    }

    return new Batch(selected, createCheckpoint(encode(to)));
  }

  private void collectEligible(FileSystem fs, Path path, Position from, List<FileStatus> out)
      throws IOException {
    FileStatus[] statuses = fs.listStatus(path, candidate ->
        IGNORE_FILE_PREFIXES.stream().noneMatch(prefix -> candidate.getName().startsWith(prefix)));
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        if (!status.isSymlink()) {
          collectEligible(fs, status.getPath(), from, out);
        }
      } else if (status.getLen() > 0 && from.isAfter(status)) {
        out.add(status);
      }
    }
  }
}
