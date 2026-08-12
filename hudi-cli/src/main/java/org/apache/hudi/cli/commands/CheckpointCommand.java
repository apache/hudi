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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_RESET_KEY_V2;

/**
 * CLI command to inspect and repair ingestion checkpoints stored in commit metadata.
 */
@ShellComponent
@Slf4j
public class CheckpointCommand {

  private static final String[] STREAMER_CHECKPOINT_KEYS = {
      STREAMER_CHECKPOINT_KEY_V1, STREAMER_CHECKPOINT_RESET_KEY_V1,
      STREAMER_CHECKPOINT_KEY_V2, STREAMER_CHECKPOINT_RESET_KEY_V2
  };

  private static final String BACKUP_DIR_NAME = ".checkpoint_backup";

  /**
   * Find the latest ingestion instant that stores a checkpoint in its commit metadata.
   */
  private Option<Pair<HoodieInstant, HoodieCommitMetadata>> findLatestCheckpointInstant(
      HoodieTimeline timeline, String... keys) {
    return TimelineUtils.findLatestInCommitMetadata(
        timeline.filterCompletedInstants(), (instant, metadata) -> {
          boolean hasCheckpoint = Arrays.stream(keys)
              .anyMatch(key -> !StringUtils.isNullOrEmpty(metadata.getMetadata(key)));
          return hasCheckpoint ? Option.of(Pair.of(instant, metadata)) : Option.empty();
        });
  }

  @ShellMethod(key = "checkpoint get", value = "Display the ingestion checkpoint value stored in the latest ingestion commit")
  public String getCheckpoint(
      @ShellOption(value = {"--checkpointKey"}, help = "Checkpoint metadata key to read (defaults to all streamer checkpoint keys)",
          defaultValue = "") final String checkpointKey) {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    try {
      HoodieTimeline timeline = metaClient.getActiveTimeline();
      String[] keys = StringUtils.isNullOrEmpty(checkpointKey) ? STREAMER_CHECKPOINT_KEYS : new String[] {checkpointKey};
      Option<Pair<HoodieInstant, HoodieCommitMetadata>> checkpointInfo = findLatestCheckpointInstant(timeline, keys);
      if (checkpointInfo.isEmpty()) {
        return "No checkpoint found in the active timeline. The table has no ingestion commit with a checkpoint "
            + "metadata key matching: " + String.join(", ", keys);
      }

      HoodieInstant instant = checkpointInfo.get().getLeft();
      HoodieCommitMetadata commitMetadata = checkpointInfo.get().getRight();

      final List<Comparable[]> rows = new ArrayList<>();
      for (String key : keys) {
        String value = commitMetadata.getMetadata(key);
        if (value != null) {
          rows.add(new Comparable[] {key, value});
        }
      }
      if (rows.isEmpty()) {
        rows.add(new Comparable[] {keys.length == 1 ? keys[0] : "checkpoint", ""});
      }

      final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
      final TableHeader header = new TableHeader()
          .addTableHeaderField("Checkpoint-Key")
          .addTableHeaderField("Checkpoint-Value");

      StringBuilder sb = new StringBuilder();
      sb.append("Ingestion checkpoint found in commit ").append(instant.requestedTime())
          .append(" (").append(instant.getAction()).append(", ").append(instant.getState()).append(")\n");
      sb.append("Commit statistics: records=").append(commitMetadata.fetchTotalRecordsWritten())
          .append(", bytes=").append(commitMetadata.fetchTotalBytesWritten())
          .append(", partitions=").append(commitMetadata.fetchTotalPartitionsWritten())
          .append(", filesInserted=").append(commitMetadata.fetchTotalFilesInsert())
          .append(", filesUpdated=").append(commitMetadata.fetchTotalFilesUpdated())
          .append(", errors=").append(commitMetadata.fetchTotalWriteErrors()).append("\n\n");
      sb.append(HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows, ""));
      return sb.toString();
    } catch (IOException e) {
      log.error("Error reading checkpoint", e);
      return "Error reading checkpoint: " + e.getMessage();
    }
  }

  @ShellMethod(key = "checkpoint set", value = "Overwrite the ingestion checkpoint value in the latest completed ingestion commit")
  public String setCheckpoint(
      @ShellOption(value = {"--checkpointValue"}, help = "The new checkpoint value to set") final String checkpointValue,
      @ShellOption(value = {"--isDeltaStreamer"}, help = "Whether to target the DeltaStreamer checkpoint key",
          defaultValue = "true") final boolean isDeltaStreamer,
      @ShellOption(value = {"--customCheckpointKey"}, help = "Custom checkpoint key name (used when isDeltaStreamer=false)",
          defaultValue = "") final String customCheckpointKey,
      @ShellOption(value = {"--force"}, help = "Bypass the safety confirmation warning",
          defaultValue = "false") final boolean force) {
    if (!force) {
      return "This is a destructive operation: it rewrites commit metadata. "
          + "Re-run with --force true to proceed. The original commit file is backed up to "
          + BACKUP_DIR_NAME + "/ before modification.";
    }
    if (StringUtils.isNullOrEmpty(checkpointValue)) {
      return "Error: --checkpointValue cannot be empty.";
    }

    String targetKey;
    if (isDeltaStreamer) {
      targetKey = STREAMER_CHECKPOINT_KEY_V1;
    } else {
      if (StringUtils.isNullOrEmpty(customCheckpointKey)) {
        return "Error: --customCheckpointKey is required when --isDeltaStreamer false.";
      }
      targetKey = customCheckpointKey;
    }

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    try {
      HoodieTimeline timeline = metaClient.getActiveTimeline();
      Option<Pair<HoodieInstant, HoodieCommitMetadata>> checkpointInfo =
          findLatestCheckpointInstant(timeline, STREAMER_CHECKPOINT_KEYS);
      if (checkpointInfo.isEmpty()) {
        return "No ingestion commit with a checkpoint found in the active timeline. Nothing to update.";
      }

      HoodieInstant instant = checkpointInfo.get().getLeft();
      HoodieCommitMetadata commitMetadata = checkpointInfo.get().getRight();

      // Back up the original commit file before modification.
      StoragePath timelinePath = new StoragePath(metaClient.getTimelinePath());
      StoragePath backupDir = new StoragePath(timelinePath, BACKUP_DIR_NAME);
      StoragePath instantFileName = new StoragePath(timelinePath,
          metaClient.getInstantFileNameGenerator().getFileName(instant));
      StoragePath backupPath = new StoragePath(backupDir,
          metaClient.getInstantFileNameGenerator().getFileName(instant));

      if (!metaClient.getStorage().exists(backupDir)) {
        metaClient.getStorage().createDirectory(backupDir);
      }
      String backupCode;
      if (metaClient.getStorage().exists(backupPath)) {
        backupCode = "already-present";
      } else {
        // Manual copy: read the original file and write to backup.
        try (InputStream in = metaClient.getStorage().open(instantFileName);
             OutputStream out = metaClient.getStorage().create(backupPath, false)) {
          byte[] buf = new byte[8192];
          int n;
          while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
          }
        }
        backupCode = "created";
      }

      // Modify the checkpoint value and write back.
      commitMetadata.addMetadata(targetKey, checkpointValue);
      writeCommitMetadata(metaClient, instant, commitMetadata);

      // Verify by re-reading the timeline.
      Option<Pair<HoodieInstant, HoodieCommitMetadata>> verified =
          findLatestCheckpointInstant(metaClient.getActiveTimeline(), targetKey);
      if (verified.isEmpty() || !checkpointValue.equals(verified.get().getRight().getMetadata(targetKey))) {
        return "Checkpoint write could not be verified. "
            + "The original commit file was backed up to " + backupPath + " — restore it manually if needed.";
      }

      return "Checkpoint updated in commit " + instant.requestedTime() + ": " + targetKey + " = " + checkpointValue
          + "\nOriginal commit file backed up to " + backupPath + " (" + backupCode + ").";
    } catch (IOException e) {
      log.error("Error updating checkpoint", e);
      return "Error updating checkpoint: " + e.getMessage();
    }
  }

  private void writeCommitMetadata(HoodieTableMetaClient metaClient, HoodieInstant instant, HoodieCommitMetadata metadata)
      throws IOException {
    Option<HoodieInstantWriter> writerOpt = metaClient.getActiveTimeline().getInstantWriter(Option.of(metadata));
    if (writerOpt.isEmpty()) {
      throw new HoodieException("Unable to create instant writer for " + instant);
    }
    StoragePath instantPath = new StoragePath(new StoragePath(metaClient.getTimelinePath()),
        metaClient.getInstantFileNameGenerator().getFileName(instant));
    try (OutputStream os = metaClient.getStorage().create(instantPath, true)) {
      writerOpt.get().writeToStream(os);
    }
  }
}