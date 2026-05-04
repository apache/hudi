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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.SCHEMA_COMMIT_ACTION;
import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Pure-{@link HoodieSchema} schema-history storage on top of {@code .hoodie/.schema/}.
 *
 * <p>Reads and writes the schema-history files that pin the on-disk evolution
 * trail for tables with {@code hoodie.schema.on.read.enable=true}. The on-disk
 * paths, file-name format ({@code <instantTime>.schemacommit}), and JSON layout
 * are unchanged from prior Hudi versions — old tables remain readable, and tables
 * written today remain readable by old code. The byte-format compatibility is
 * enforced by {@link HoodieSchemaSerDe}.
 */
@Slf4j
public class HoodieSchemaHistoryStorageManager {

  /**
   * Subdirectory name under {@code .hoodie/} that holds the schema-history files.
   * Carried over verbatim from the legacy storage manager so existing tables
   * continue to be located at the same path.
   */
  public static final String SCHEMA_NAME = ".schema";

  private final StoragePath baseSchemaPath;
  private final HoodieStorage storage;
  private HoodieTableMetaClient metaClient;

  public HoodieSchemaHistoryStorageManager(HoodieStorage storage, StoragePath baseTablePath) {
    StoragePath metaPath = new StoragePath(baseTablePath, HoodieTableMetaClient.METAFOLDER_NAME);
    this.baseSchemaPath = new StoragePath(metaPath, SCHEMA_NAME);
    this.storage = storage;
  }

  public HoodieSchemaHistoryStorageManager(HoodieTableMetaClient metaClient) {
    this.baseSchemaPath = new StoragePath(metaClient.getMetaPath(), SCHEMA_NAME);
    this.storage = metaClient.getStorage();
    this.metaClient = metaClient;
  }

  /**
   * Lazy-builds the metaClient when the storage-only constructor was used.
   * Construction of metaClient is non-trivial; deferring it to first need
   * keeps the read-only history-string lookups cheap.
   */
  private HoodieTableMetaClient getMetaClient() {
    if (metaClient == null) {
      metaClient = HoodieTableMetaClient.builder()
          .setBasePath(baseSchemaPath.getParent().getParent().toString())
          .setStorage(storage)
          .setTimeGeneratorConfig(
              HoodieTimeGeneratorConfig.defaultConfig(baseSchemaPath.getParent().getParent().toString()))
          .build();
    }
    return metaClient;
  }

  /**
   * Persists a history-schema JSON blob keyed by the commit instant time. The
   * input is expected to be the layout produced by
   * {@link HoodieSchemaSerDe#toJsonHistory(List)} or
   * {@link HoodieSchemaSerDe#inheritHistory(HoodieSchema, String)}.
   */
  public void persistHistorySchemaStr(String instantTime, String historySchemaStr) {
    cleanResidualFiles();
    HoodieActiveTimeline timeline = getMetaClient().getActiveTimeline();
    HoodieInstant hoodieInstant = metaClient.createNewInstant(
        HoodieInstant.State.REQUESTED, SCHEMA_COMMIT_ACTION, instantTime);
    timeline.createNewInstant(hoodieInstant);
    byte[] writeContent = getUTF8Bytes(historySchemaStr);
    timeline.transitionRequestedToInflight(hoodieInstant, Option.empty());
    // TODO[HUDI-9094]: we should not write raw byte array directly.
    timeline.saveAsComplete(false, metaClient.createNewInstant(
        HoodieInstant.State.INFLIGHT, hoodieInstant.getAction(), hoodieInstant.requestedTime()),
        Option.of(HoodieInstantWriter.convertByteArrayToWriter(writeContent)));
    log.info("persist history schema success on commit time: {}", instantTime);
  }

  /**
   * Removes schema-history files whose instant times are not in the
   * {@code validateCommits} set. Safety mechanism for archival cleanup.
   */
  public void cleanOldFiles(List<String> validateCommits) {
    try {
      if (storage.exists(baseSchemaPath)) {
        List<String> candidateSchemaFiles = storage.listDirectEntries(baseSchemaPath).stream()
            .filter(f -> f.isFile())
            .map(file -> file.getPath().getName()).collect(Collectors.toList());
        List<String> validateSchemaFiles = candidateSchemaFiles.stream()
            .filter(f -> validateCommits.contains(getMetaClient().getInstantFileNameParser().extractTimestamp(f)))
            .collect(Collectors.toList());
        for (int i = 0; i < validateSchemaFiles.size(); i++) {
          storage.deleteFile(new StoragePath(validateSchemaFiles.get(i)));
        }
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Returns the most recent history-schema JSON blob across all commits, or
   * empty string if no schema history exists.
   */
  public String getHistorySchemaStr() {
    return getHistorySchemaStrByGivenValidCommits(Collections.emptyList());
  }

  /**
   * Same as {@link #getHistorySchemaStr()} but only considers files whose
   * instant time appears in {@code validCommits}. Empty / null
   * {@code validCommits} falls back to the meta-client's completed instants.
   */
  public String getHistorySchemaStrByGivenValidCommits(List<String> validCommits) {
    List<String> commitList = validCommits == null || validCommits.isEmpty() ? getValidInstants() : validCommits;
    try {
      if (storage.exists(baseSchemaPath)) {
        List<String> validSchemaFiles = storage.listDirectEntries(baseSchemaPath).stream()
            .filter(f -> f.isFile() && f.getPath().getName().endsWith(SCHEMA_COMMIT_ACTION))
            .map(file -> file.getPath().getName())
            .filter(Objects::nonNull)
            .filter(f -> commitList.contains(getMetaClient().getInstantFileNameParser().extractTimestamp(f)))
            .sorted()
            .collect(Collectors.toList());
        if (!validSchemaFiles.isEmpty()) {
          StoragePath latestFilePath = new StoragePath(baseSchemaPath, validSchemaFiles.get(validSchemaFiles.size() - 1));
          byte[] content;
          try (InputStream is = storage.open(latestFilePath)) {
            content = FileIOUtils.readAsByteArray(is);
            log.info("read history schema success from file: {}", latestFilePath);
            return fromUTF8Bytes(content);
          } catch (IOException e) {
            throw new HoodieIOException("Could not read history schema from " + latestFilePath, e);
          }
        }
      }
    } catch (IOException io) {
      throw new HoodieException(io);
    }
    log.info("failed to read history schema");
    return "";
  }

  /**
   * Resolves a single schema by its version id (typically a commit-instant
   * timestamp). Returns empty if no schema with that version exists in the
   * history blob — including the no-history-on-disk case.
   *
   * <p>Implementation note: parses the latest history blob via
   * {@link HoodieSchemaSerDe} and walks it with
   * {@link HoodieSchemaSerDe#searchSchema(long, TreeMap)}, so the lookup
   * follows "exact match, else greatest entry &lt; versionId" semantics.
   */
  public Option<HoodieSchema> getSchemaByKey(String versionId) {
    String historySchemaStr = getHistorySchemaStr();
    if (historySchemaStr.isEmpty()) {
      return Option.empty();
    }
    TreeMap<Long, HoodieSchema> history = HoodieSchemaSerDe.parseHistorySchemas(historySchemaStr);
    HoodieSchema result = HoodieSchemaSerDe.searchSchema(Long.valueOf(versionId), history);
    return result == null ? Option.empty() : Option.of(result);
  }

  private void cleanResidualFiles() {
    List<String> validateCommits = getValidInstants();
    try {
      if (storage.exists(baseSchemaPath)) {
        List<String> candidateSchemaFiles = storage.listDirectEntries(baseSchemaPath).stream()
            .filter(f -> f.isFile())
            .map(file -> file.getPath().getName()).collect(Collectors.toList());
        List<String> residualSchemaFiles = candidateSchemaFiles.stream()
            .filter(f -> !validateCommits.contains(getMetaClient().getInstantFileNameParser().extractTimestamp(f)))
            .collect(Collectors.toList());
        residualSchemaFiles.forEach(f -> {
          try {
            storage.deleteFile(new StoragePath(getMetaClient().getSchemaFolderName(), f));
          } catch (IOException o) {
            throw new HoodieException(o);
          }
        });
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  private List<String> getValidInstants() {
    return getMetaClient().getCommitsTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .map(f -> f.requestedTime())
        .collect(Collectors.toList());
  }
}
