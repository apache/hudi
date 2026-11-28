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

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.archive.ArchivedRecordProjector;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Projects a single archived timeline record into the <b>bare file names</b> it records, <b>keyed by
 * the wanted file group they belong to</b> -- each group's names being the union of its write stat's
 * {@code path}, its {@code logFiles}, and the keys of its {@code cdcStats}.
 *
 * <p><b>Keyed, and not one flat set, because the flat set is a delete authorisation.</b> A batch's
 * projection covers every file group the batch names, and a consumer asks it whether one commit
 * referenced one file. Unioned flat, a commit that wrote group B and nothing at all for group A
 * hands back a non-empty name set that does not contain A's file -- which reads as "the commit spoke
 * about this group and declined to claim the file", the one archived shape that condemns. Keyed, A's
 * entry is simply absent. The flat form also made the verdict depend on which other groups happened
 * to share a shuffle partition, and no deletion authorisation may depend on that.
 *
 * <p><b>An absent group is evidence only through the completed commit's own record.</b> Alongside
 * the names, a completed record also projects whether it recorded any write stats, read off the
 * commit's whole {@code partitionToWriteStats}, never off the other wanted groups. A completed
 * commit that recorded write stats and names nothing for a group did not write that group at this
 * instant, which is positive proof; one that recorded no write stats claimed nothing, which refuses.
 *
 * <p>The keys are the wanted {@link HoodieFileGroupId}s <b>as the caller supplied them</b>, spelling
 * included, so a consumer looks up the group id it already holds. Normalisation happens on the
 * record's side of the comparison only.
 *
 * <p><b>Why the union, and not {@code path} alone.</b> A write stat's {@code path} is only ever the
 * base file (or, for a delta write stat, the newest log file); it is not the whole name set a
 * commit actually wrote. {@code logFiles} is seeded from the file slice the write started from, so
 * consulting it can only ever turn a would-be DANGLING verdict into a REFERENCED one -- it never
 * over-claims. {@code cdcStats} is the one place CDC file names are recorded at all: nothing else
 * about a commit names them. Projecting {@code path} alone would silently drop both, which is exactly
 * what makes a CDC or non-newest-log file undecidable from the archive even though
 * the commit that wrote it is sitting right there. This projector exists to make the archive answer
 * with the same name set {@code InstantMembershipResolver.namesOfStats} assembles from the active
 * timeline, so the two paths agree.
 *
 * <p><b>Three record shapes, tried in order, never guessed.</b> The record handed to
 * {@link #project} is whatever the archive's own reader schema decoded, and that varies by layout:
 *
 * <ul>
 *   <li>{@code HoodieLSMTimelineInstant} (v9 layout) carries its payload serialised as a
 *       {@code metadata} {@link ByteBuffer} -- a self-describing avro data file, read back with
 *       {@link #pathsFromSerialisedMetadata}.
 *   <li>{@code HoodieArchivedMetaEntry} (v6 layout) nests it under {@code hoodieCommitMetadata} or
 *       {@code hoodieReplaceCommitMetadata} -- both, since a v6 entry can in principle carry either,
 *       and the two are unioned rather than one overwriting the other.
 *   <li>Bare commit metadata -- a {@code HoodieCommitMetadata} or {@code HoodieReplaceCommitMetadata}
 *       record with {@code partitionToWriteStats} directly on it -- for callers that already hold
 *       the decoded metadata and hand it straight to this projector without an envelope.
 * </ul>
 *
 * <p>A record whose schema matches none of the three is a refusal, not a guess: {@link #project}
 * throws {@link IOException} naming the schema.
 *
 * <p>Every partition-key comparison goes through {@link PartitionPaths#relative}, on both the
 * record's key and the wanted file groups' keys, because Hudi commit metadata mixes absolute and
 * relative partition spellings for the same partition and a raw-string comparison answers "not
 * found" for every file in a partition spelled the other way.
 *
 * <p>A record that does not decode as commit metadata at all -- wrong schema, wrong field types --
 * makes {@link #project} throw, never return an empty map: "I could not understand this" and "this
 * commit records nothing here" are different facts, and only the second may ever license treating a
 * file as unreferenced.
 */
public final class CommitPathProjector
    implements ArchivedRecordProjector<ArchivedCommitPaths> {

  /** The field {@code HoodieLSMTimelineInstant} (v9 layout) carries its payload under. */
  static final String LSM_METADATA_FIELD = "metadata";

  /** The fields {@code HoodieArchivedMetaEntry} (v6 layout) carries write stats under. */
  static final List<String> ARCHIVED_META_ENTRY_FIELDS =
      Collections.unmodifiableList(Arrays.asList(
          "hoodieCommitMetadata", "hoodieReplaceCommitMetadata"));

  static final String PARTITION_TO_WRITE_STATS_FIELD = "partitionToWriteStats";
  static final String FILE_ID_FIELD = "fileId";
  static final String PATH_FIELD = "path";
  static final String LOG_FILES_FIELD = "logFiles";
  static final String CDC_STATS_FIELD = "cdcStats";
  static final String ACTION_STATE_FIELD = "actionState";
  static final String COMPLETED_ACTION_STATE = "COMPLETED";

  private final StoragePath basePath;

  /**
   * Normalised partition -> file id -> every wanted group id that spells that pair, precomputed once
   * so the per-record loop is two map lookups rather than a scan of every wanted file group.
   *
   * <p>A {@link Set} of group ids rather than one, because two wanted groups can spell the same
   * partition differently and normalise together. Recording both means a consumer's lookup hits
   * whichever spelling it holds; keeping only one would silently answer "nothing recorded" for the
   * other, which refuses -- safe, but for a reason nobody could see.
   */
  private final Map<String, Map<String, Set<HoodieFileGroupId>>> wantedByPartition;

  public CommitPathProjector(StoragePath basePath, Set<HoodieFileGroupId> wantedGroups) {
    this.basePath = basePath;
    Map<String, Map<String, Set<HoodieFileGroupId>>> byPartition = new HashMap<>();
    for (HoodieFileGroupId groupId : wantedGroups) {
      String partition = PartitionPaths.relative(basePath, groupId.getPartitionPath());
      byPartition
          .computeIfAbsent(partition, unused -> new HashMap<>())
          .computeIfAbsent(groupId.getFileId(), unused -> new LinkedHashSet<>())
          .add(groupId);
    }
    this.wantedByPartition = byPartition;
  }

  /**
   * @return the names this record records, keyed by wanted file group, and, for a completed
   *         record, whether it recorded any write stats. A group the record names nothing for is
   *         <b>absent</b> from the names; only a completed record with write stats can turn that
   *         absence into evidence.
   * @throws IOException when the record carries commit metadata under none of the three known
   *         shapes, or carries one that cannot be read. Never returns an empty projection to stand
   *         in for a decode failure.
   */
  @Override
  public ArchivedCommitPaths project(GenericRecord record) throws IOException {
    return pathsFor(record);
  }

  /**
   * Unions two projections of the same instant into a new value, mutating neither argument and in
   * a way that does not depend on which arrived first (see {@link ArchivedCommitPaths#merge}).
   */
  @Override
  public ArchivedCommitPaths merge(ArchivedCommitPaths first, ArchivedCommitPaths second) {
    return ArchivedCommitPaths.merge(first, second);
  }

  /**
   * Extracts the names a single archived record ascribes to each wanted file group, branching on
   * which field the record's own schema actually has.
   *
   * @throws IOException when the record carries none of the three known payload shapes, or carries
   *         one that cannot be read. Never returns an empty map to stand in for that case.
   */
  private ArchivedCommitPaths pathsFor(GenericRecord record) throws IOException {
    Schema schema = record.getSchema();
    if (schema.getField(LSM_METADATA_FIELD) != null) {
      Object payload = record.get(schema.getField(LSM_METADATA_FIELD).pos());
      if (payload == null) {
        // A completed instant whose meta file was empty: no write stats, which refuses.
        return ArchivedCommitPaths.empty();
      }
      if (!(payload instanceof ByteBuffer)) {
        throw new IOException("archived record " + schema.getFullName() + " carries '"
            + LSM_METADATA_FIELD + "' as " + payload.getClass().getName()
            + ", which this decoder does not understand");
      }
      return pathsFromSerialisedMetadata((ByteBuffer) payload);
    }

    boolean isArchivedMetaEntry = false;
    boolean completed = isCompletedEntry(record);
    ArchivedCommitPaths projected = ArchivedCommitPaths.empty();
    for (String field : ARCHIVED_META_ENTRY_FIELDS) {
      Schema.Field declared = schema.getField(field);
      if (declared == null) {
        continue;
      }
      isArchivedMetaEntry = true;
      Object payload = record.get(declared.pos());
      if (payload == null) {
        continue;
      }
      // A v6 entry can in principle carry both payloads, so they are unioned per group rather than
      // one overwriting the other.
      projected = ArchivedCommitPaths.merge(projected,
          pathsFromDecodedMetadata(payload, completed));
    }
    if (isArchivedMetaEntry) {
      return projected;
    }

    if (schema.getField(PARTITION_TO_WRITE_STATS_FIELD) != null) {
      return pathsFromMetadataRecord(record, true);
    }

    throw new IOException("archived record " + schema.getFullName() + " carries commit metadata "
        + "under none of '" + LSM_METADATA_FIELD + "', " + ARCHIVED_META_ENTRY_FIELDS + ", or '"
        + PARTITION_TO_WRITE_STATS_FIELD + "'; refusing to guess which layout it is");
  }

  /**
   * The v9 payload: a self-describing avro data file. Read with the writer's own schema so that
   * commit, delta commit and replace-commit metadata all decode without being told apart first.
   */
  private ArchivedCommitPaths pathsFromSerialisedMetadata(ByteBuffer payload)
      throws IOException {
    ByteBuffer readable = payload.duplicate();
    byte[] bytes = new byte[readable.remaining()];
    readable.get(bytes);
    if (bytes.length == 0) {
      return ArchivedCommitPaths.empty();
    }
    try (DataFileStream<GenericRecord> stream = new DataFileStream<>(
        new ByteArrayInputStream(bytes), new GenericDatumReader<>())) {
      if (!stream.hasNext()) {
        throw new IOException("serialised commit metadata held no record");
      }
      // The v9 layout archives completed instants only.
      return pathsFromMetadataRecord(stream.next(), true);
    }
  }

  /**
   * The v6 payload, which the schema declares as a nested avro record -- {@code HoodieCommitMetadata}
   * under {@code hoodieCommitMetadata}, or {@code HoodieReplaceCommitMetadata} under
   * {@code hoodieReplaceCommitMetadata} -- per {@code HoodieArchivedMetaEntry.avsc}.
   *
   * <p>Anything else is a refusal rather than a guess: there is deliberately no text branch here,
   * because a JSON parse would be one lenient reader away from turning arbitrary text into an empty
   * write-stat set.
   */
  private ArchivedCommitPaths pathsFromDecodedMetadata(Object payload, boolean completed)
      throws IOException {
    if (payload instanceof IndexedRecord) {
      return pathsFromMetadataRecord((IndexedRecord) payload, completed);
    }
    throw new IOException("archived commit metadata arrived as " + payload.getClass().getName()
        + ", which this decoder does not understand");
  }

  /**
   * Reads {@code partitionToWriteStats} positionally off whichever commit-metadata record this is,
   * so that the avro {@code HoodieCommitMetadata} and {@code HoodieReplaceCommitMetadata} shapes --
   * which reference the same shared {@code HoodieWriteStat} record -- are both served without
   * naming either class.
   */
  private ArchivedCommitPaths pathsFromMetadataRecord(IndexedRecord metadata, boolean completed)
      throws IOException {
    Schema.Field statsField = metadata.getSchema().getField(PARTITION_TO_WRITE_STATS_FIELD);
    if (statsField == null) {
      throw new IOException("commit metadata record " + metadata.getSchema().getFullName()
          + " has no '" + PARTITION_TO_WRITE_STATS_FIELD + "' field");
    }
    Object partitions = metadata.get(statsField.pos());
    if (partitions == null) {
      return ArchivedCommitPaths.empty();
    }
    if (!(partitions instanceof Map)) {
      throw new IOException("'" + PARTITION_TO_WRITE_STATS_FIELD + "' arrived as "
          + partitions.getClass().getName() + " rather than a map");
    }

    Map<HoodieFileGroupId, Set<String>> namesByGroup = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : ((Map<?, ?>) partitions).entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      // Both sides normalised, so a commit that spells this partition absolutely still matches. A
      // raw comparison answers "not found" for EVERY file in such a partition, and downstream that
      // reads as a dangling verdict for the lot.
      String recordPartition = PartitionPaths.relative(basePath, entry.getKey().toString());
      Map<String, Set<HoodieFileGroupId>> wantedInPartition = wantedByPartition.get(recordPartition);
      if (wantedInPartition == null) {
        continue;
      }
      if (!(entry.getValue() instanceof Collection)) {
        throw new IOException("write stats for partition " + entry.getKey()
            + " arrived as " + (entry.getValue() == null ? "null" : entry.getValue().getClass().getName())
            + " rather than a list");
      }
      for (Object stat : (Collection<?>) entry.getValue()) {
        if (!(stat instanceof IndexedRecord)) {
          throw new IOException("a write stat arrived as "
              + (stat == null ? "null" : stat.getClass().getName()) + " rather than a record");
        }
        IndexedRecord statRecord = (IndexedRecord) stat;
        Object statFileId = fieldOf(statRecord, FILE_ID_FIELD);
        if (statFileId == null) {
          continue;
        }
        Set<HoodieFileGroupId> groups = wantedInPartition.get(statFileId.toString());
        if (groups == null) {
          continue;
        }
        Set<String> names = new LinkedHashSet<>();
        addName(names, fieldOf(statRecord, PATH_FIELD));
        addNames(names, optionalFieldOf(statRecord, LOG_FILES_FIELD));
        addCdcNames(names, optionalFieldOf(statRecord, CDC_STATS_FIELD));
        for (HoodieFileGroupId group : groups) {
          namesByGroup.computeIfAbsent(group, unused -> new LinkedHashSet<>()).addAll(names);
        }
      }
    }
    if (!completed) {
      return ArchivedCommitPaths.namesOnly(namesByGroup);
    }
    return ArchivedCommitPaths.completed(namesByGroup, !((Map<?, ?>) partitions).isEmpty());
  }

  /**
   * Whether a v6 {@code HoodieArchivedMetaEntry} is the completed record of its instant. An entry
   * whose schema has no {@code actionState} is not taken as completed, which only ever refuses.
   */
  private static boolean isCompletedEntry(GenericRecord record) {
    Schema.Field state = record.getSchema().getField(ACTION_STATE_FIELD);
    if (state == null) {
      return false;
    }
    Object value = record.get(state.pos());
    return value != null && COMPLETED_ACTION_STATE.equals(value.toString());
  }

  /** Strict: {@code fileId} and {@code path} are declared on every write-stat schema Hudi has ever
   *  written, so a missing one is a real decode failure, not schema evolution. */
  private static Object fieldOf(IndexedRecord record, String field) throws IOException {
    Schema.Field declared = record.getSchema().getField(field);
    if (declared == null) {
      throw new IOException("write stat record " + record.getSchema().getFullName()
          + " has no '" + field + "' field");
    }
    return record.get(declared.pos());
  }

  /** Absent from the schema is not an error: an archived record decodes against ITS OWN writer
   *  schema, and {@code logFiles}/{@code cdcStats} were added to {@code HoodieWriteStat} over time.
   *  A strict read would make every instant archived by an older Hudi INDETERMINATE. */
  private static Object optionalFieldOf(IndexedRecord record, String field) {
    Schema.Field declared = record.getSchema().getField(field);
    return declared == null ? null : record.get(declared.pos());
  }

  /**
   * Bare names, never paths: the suspect arrives from a storage listing, and a recorded path is
   * relative to the table root.
   */
  private static void addName(Set<String> names, Object path) {
    if (path == null) {
      return;
    }
    String value = path.toString();
    if (value.isEmpty()) {
      return;
    }
    names.add(new StoragePath(value).getName());
  }

  /**
   * @param logFiles a {@code Collection<?>}, or {@code null} when the field is absent from the
   *                 write stat's schema or is genuinely null for this stat -- both mean "nothing to
   *                 add". A value that is present but not a collection is a decode failure: throw,
   *                 rather than silently treating an unreadable field as an empty one.
   */
  private static void addNames(Set<String> names, Object logFiles) throws IOException {
    if (logFiles == null) {
      return;
    }
    if (!(logFiles instanceof Collection)) {
      throw new IOException("'" + LOG_FILES_FIELD + "' arrived as " + logFiles.getClass().getName()
          + " rather than a collection");
    }
    for (Object logFile : (Collection<?>) logFiles) {
      addName(names, logFile);
    }
  }

  /**
   * @param cdcStats a {@code Map<?, ?>}, or {@code null} when the field is absent from the write
   *                 stat's schema or is genuinely null for this stat -- both mean "nothing to add".
   *                 A value that is present but not a map is a decode failure: throw, rather than
   *                 silently treating an unreadable field as an empty one.
   */
  private static void addCdcNames(Set<String> names, Object cdcStats) throws IOException {
    if (cdcStats == null) {
      return;
    }
    if (!(cdcStats instanceof Map)) {
      throw new IOException("'" + CDC_STATS_FIELD + "' arrived as " + cdcStats.getClass().getName()
          + " rather than a map");
    }
    for (Object cdcPath : ((Map<?, ?>) cdcStats).keySet()) {
      addName(names, cdcPath);
    }
  }
}
