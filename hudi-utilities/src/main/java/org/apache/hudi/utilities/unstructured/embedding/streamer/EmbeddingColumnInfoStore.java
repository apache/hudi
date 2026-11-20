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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.exception.HoodiePreCommitRefusalException;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableVersion;
import org.apache.hudi.utilities.streamer.CompletedWrite;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stream-owned model history in one auxiliary JSON file ({@value #RELATIVE_PATH}), plus a commit-time identity fence
 * ({@value #FENCE_PATH}). Start checks are read-only. The history is published only after a completed table commit. The fence
 * is replaced inside the data commit, under the commit's own lock when the writer's configuration requires one, and by a manual
 * publication ({@link #publishAfterCommit}) on a fenced table. Every operation but the in-commit one takes the configured table
 * lock itself, and publication rereads history and fence under that lock.
 *
 * <p>One table has one stream writer. The fence closes the window in which history publication fails after a successful data
 * commit and a later round could otherwise accept a different model of the same dimension for that column.
 * {@link #validateBeforeCommit}, called inside the data commit, rechecks the round against the committed identities and
 * replaces one fence file ({@value #FENCE_PATH}) holding the confirmed committed state and this commit's candidate state.
 * It runs under the table lock when the writer's configuration requires one; a writer without a lock provider runs it unlocked.
 * A candidate becomes committed authority only once its instant completed, so a failed commit reserves nothing.
 */
public class EmbeddingColumnInfoStore {
  /** Governs the parser of the auxiliary history file, independently of blob layout and table schema versions. */
  public static final ConfigProperty<String> LAYOUT_VERSION = UnstructuredTableVersion.EMBEDDING_COLUMN_INFO_LAYOUT_VERSION;
  /** The {@link #LAYOUT_VERSION} this build writes and reads; written with the table's other pins when the table is created. */
  public static final String CURRENT_LAYOUT_VERSION = UnstructuredTableVersion.CURRENT_EMBEDDING_COLUMN_INFO_LAYOUT_VERSION;
  public static final String RELATIVE_PATH = ".hoodie/.aux/embedding-column-info.json";
  private static final String SCRATCH_DIRECTORY = ".hoodie/.temp/embedding-column-info";
  /** The commit-time identity fence: {layout_version: 1, committed: snapshot or null, pending: {instant, snapshot}}. */
  static final String FENCE_PATH = ".hoodie/.aux/embedding-column-fence.json";
  /** The fence file's own layout version, independent of the history file's {@link #LAYOUT_VERSION}. */
  private static final int FENCE_LAYOUT_VERSION = 1;
  private static final String FENCE_SCRATCH_DIRECTORY = ".hoodie/.temp/embedding-column-fence";
  /** The {@link #replaceAtomically} step before the swap of the fence and of the history: none. */
  private static final Runnable NOTHING_BEFORE_SWAP = () -> { };
  private static final String FIELD_COMMITTED = "committed";
  private static final String FIELD_PENDING = "pending";
  private static final String FIELD_SNAPSHOT = "snapshot";
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingColumnInfoStore.class);
  private static final ObjectMapper JSON = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final String FIELD_REVISION = "revision";
  private static final String FIELD_INSTANT = "instant";
  private static final String FIELD_LAYOUT_VERSION = "layout_version";
  private static final String EVENT_ADD = "add";
  private static final String EVENT_RETIRE = "retire";
  private static final String EVENT_RECOVER = "recover";
  private static final String FIELD_DIMENSION = "dimension";
  private static final String FIELD_EVENTS = "events";
  private static final String FIELD_COLUMN_NAME = "column_name";
  private static final String FIELD_EVENT = "event";
  private static final String FIELD_IDENTITY = "identity";
  private static final String FIELD_SEQUENCE = "sequence";
  private static final String FIELD_PROVIDER = "provider";
  private static final String FIELD_MODEL_NAME = "model_name";
  private static final String FIELD_MODEL_VERSION = "model_version";
  private static final String FIELD_INPUT_TRANSFORM_VERSION = "input_transform_version";
  private static final String FIELD_ADDED_AT_INSTANT = "added_at_instant";
  private static final String VECTOR_COLUMN = "VECTOR column ";
  private static final Set<String> SNAPSHOT_FIELDS = fields(FIELD_LAYOUT_VERSION, FIELD_REVISION, FIELD_EVENTS);
  private static final Set<String> EVENT_FIELDS = fields(FIELD_SEQUENCE, FIELD_COLUMN_NAME, FIELD_EVENT, FIELD_INSTANT, FIELD_ADDED_AT_INSTANT, FIELD_IDENTITY);
  private static final Set<String> IDENTITY_FIELDS = fields(FIELD_PROVIDER, FIELD_MODEL_NAME, FIELD_DIMENSION, FIELD_MODEL_VERSION,
      FIELD_INPUT_TRANSFORM_VERSION);
  private static final Set<String> FENCE_FIELDS = fields(FIELD_LAYOUT_VERSION, FIELD_COMMITTED, FIELD_PENDING);
  private static final Set<String> PENDING_FIELDS = fields(FIELD_INSTANT, FIELD_SNAPSHOT);

  private final String basePath;
  // The table writer passes its configured path verbatim; lock providers may key on that exact string.
  private final String writerLockBasePath;
  private final StorageConfiguration<?> storageConf;
  private final TypedProperties props;
  private final HoodieStorage storage;
  private final HoodieStorage auxiliaryStorage;
  private final StoragePath path;
  private final EmbeddingColumnInfoAtomicSwap swap;
  private final Set<String> warned = ConcurrentHashMap.newKeySet();

  /** Uses the table's configured lock provider and a backend with complete-file atomic replacement. */
  public EmbeddingColumnInfoStore(String basePath, StorageConfiguration<?> storageConf, TypedProperties props) {
    this(basePath, storageConf, props, new EmbeddingColumnInfoAtomicSwap());
  }

  EmbeddingColumnInfoStore(String basePath, StorageConfiguration<?> storageConf, TypedProperties props,
                           EmbeddingColumnInfoAtomicSwap swap) {
    this.basePath = new StoragePath(basePath).toString();
    this.writerLockBasePath = basePath;
    this.storageConf = storageConf.newInstance();
    this.props = TypedProperties.copy(props);
    this.storage = HoodieStorageUtils.getStorage(new StoragePath(this.basePath), this.storageConf);
    this.auxiliaryStorage = EmbeddingColumnInfoAtomicSwap.metadataStorage(storage);
    this.path = new StoragePath(this.basePath, RELATIVE_PATH);
    this.swap = Objects.requireNonNull(swap, "swap");
  }

  /** Checks configured model columns, without requesting logical retirements. */
  public Update checkAtStart(Map<String, Identity> configured, Map<String, Integer> existingVectorColumns) {
    return checkAtStart(configured, existingVectorColumns, Collections.emptySet());
  }

  /**
   * Refuses present mismatches, missing authoritative identities on existing job-owned VECTOR columns, retired-name reuse and invalid
   * retirements before prep. A surviving committed fence can validate and restore a missing auxiliary history. Inactive columns remain
   * active unless explicitly retired. Retirement is logical and need not remove a physical schema field. Repeating a
   * retirement request is a no-op; configuring a model under that retired name remains forbidden. Backend support is checked
   * with an owned temporary probe; this start check never publishes identity metadata.
   */
  public Update checkAtStart(Map<String, Identity> configured, Map<String, Integer> existingVectorColumns,
                             Set<String> retiredColumns) {
    return checkAtStart(configured, existingVectorColumns, retiredColumns, ConcurrentHashMap.newKeySet());
  }

  /** Refreshes one job's effective model columns without forgetting model ownership observed by its earlier rounds. */
  Update refreshAtStart(Map<String, Identity> configured, Map<String, Integer> existingVectorColumns,
                        Set<String> retiredColumns, Update previous) {
    if (!basePath.equals(previous.basePath)) {
      throw invalid("Update belongs to another table");
    }
    return checkAtStart(configured, existingVectorColumns, retiredColumns, previous.owned);
  }

  private Update checkAtStart(Map<String, Identity> configured, Map<String, Integer> existingVectorColumns,
                              Set<String> retiredColumns, Set<String> ownedColumns) {
    return locked(meta -> {
      Snapshot snapshot = read(meta);
      Snapshot committed = readFence(meta, snapshot);
      Update update = new Update(basePath, configured, existingVectorColumns, retiredColumns, ownedColumns);
      validate(committed, update);
      if (!update.owned.isEmpty() || !retiredColumns.isEmpty()) {
        swap.checkSupported(auxiliaryStorage, path);
      }
      warnMissingHistory(snapshot, committed, update);
      return update;
    });
  }

  /**
   * The manual publication path, for a caller that committed without the commit-time fence (no {@link #validateBeforeCommit}):
   * after a successful table commit, appends new identities and requested retirements to the committed history, and
   * on a fenced table records the result in the fence too. The streamer publishes with {@link #publishCommittedState} instead,
   * which only writes out what the fence already holds. Repeating an already-applied update is a no-op. Pre-publication
   * failures preserve the previous bytes. Object-store close acknowledgement failures may occur after the complete new object
   * is durable; visibility remains old or new.
   */
  public void publishAfterCommit(String instant, Update update) {
    requireInstant(instant, FIELD_INSTANT);
    if (!basePath.equals(update.basePath)) {
      throw invalid("Update belongs to another table");
    }
    locked(meta -> publishLocked(meta, instant, update));
  }

  /**
   * Under the table lock, appends this update's events to the committed history and writes it when anything changed. On a
   * fenced table the fence takes the result too, so a manual publication stays part of the one committed authority.
   */
  private Void publishLocked(HoodieTableMetaClient meta, String instant, Update update) throws IOException {
    requireCompleted(meta, instant);
    Snapshot published = read(meta);
    Snapshot snapshot = readFence(meta, published);
    validate(snapshot, update);
    Snapshot updated = append(snapshot, update, instant);
    // an empty snapshot has no layout 1 form (revision 0): with nothing committed there is nothing to fence
    if (!updated.events.isEmpty() && auxiliaryStorage.exists(new StoragePath(basePath, FENCE_PATH))) {
      writeFence(updated, updated, instant);
    }
    if (!Arrays.equals(serialize(updated), serialize(published))) {
      write(updated);
    }
    return null;
  }

  /**
   * Called by the driver's commit callback while the data commit holds the table lock (when the writer's configuration requires
   * one); never acquires a lock itself. Rechecks {@code update} against the committed identities, including a committed fence
   * candidate whose history publication failed after commit, and fences the resulting state as this commit's candidate.
   * This preserves column identity across the single-writer publication window described above. A conflict fails the commit
   * before its instant completes, with a {@link HoodiePreCommitRefusalException}.
   *
   * <p>Cost: every data commit whose candidate state has an identity (from the first commit that adds a model column on)
   * replaces the fence (one small file, created and atomically swapped) while it holds the lock, so a later round can tell
   * whether this candidate completed even if its history publication failed.
   */
  public void validateBeforeCommit(HoodieTableMetaClient committingMetaClient, String instant, Update update) {
    requireInstant(instant, FIELD_INSTANT);
    if (!basePath.equals(new StoragePath(committingMetaClient.getBasePath().toString()).toString()) || !basePath.equals(update.basePath)) {
      throw invalid("Embedding identity validation belongs to another table");
    }
    try {
      HoodieTableMetaClient fresh = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf.newInstance()).build();
      Snapshot committed = readFence(fresh, read(fresh));
      try {
        // A concurrent writer may have added a model column after this round started. An empty table's create schema only
        // describes planned columns, so only a completed table can refresh physical ownership here.
        Update current = update;
        if (fresh.isTimelineNonEmpty()) {
          current = new TableSchemaResolver(fresh).getTableSchemaIfPresent(false)
              .map(schema -> update.withExistingColumns(EmbeddingColumnInfoConfig.vectorColumns(schema))).orElse(update);
        }
        validate(committed, current);
      } catch (IllegalArgumentException conflict) {
        // the streamer rolls a refused instant back, so its files do not wait for the failed-writes cleaner
        throw new HoodiePreCommitRefusalException(conflict.getMessage(), conflict);
      }
      Snapshot candidate = append(committed, update, instant);
      if (!candidate.events.isEmpty()) {
        writeFence(committed, candidate, instant);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Cannot fence embedding model identity before commit " + instant, e);
    }
  }

  /**
   * After a successful table commit, publishes the committed state the fence holds (this commit's and any whose callback was
   * delayed). Never replaces newer published state with older: the committed state already contains it.
   */
  public void publishCommittedState(String instant, Update update) {
    requireInstant(instant, FIELD_INSTANT);
    if (!basePath.equals(update.basePath)) {
      throw invalid("Update belongs to another table");
    }
    locked(meta -> {
      requireCompleted(meta, instant);
      Snapshot published = read(meta);
      Snapshot committed = readFence(meta, published);
      if (!committed.events.isEmpty() && !Arrays.equals(serialize(published), serialize(committed))) {
        write(committed);
      }
      return null;
    });
  }

  private static void requireCompleted(HoodieTableMetaClient meta, String instant) {
    if (!CompletedWrite.exists(meta, instant)) {
      throw invalid("Cannot publish embedding column information before completed table instant " + instant);
    }
  }

  /** {@code snapshot} with this update's retire and add events appended; the same snapshot when nothing changes. */
  private static Snapshot append(Snapshot snapshot, Update update, String instant) {
    List<Event> appended = new ArrayList<>(snapshot.events);
    long sequence = snapshot.events.isEmpty() ? 0 : snapshot.events.get(snapshot.events.size() - 1).sequence;
    sequence = appendRetirements(snapshot, update, instant, appended, sequence);
    appendConfiguredColumns(snapshot, update, instant, appended, sequence);
    if (appended.size() == snapshot.events.size()) {
      return snapshot;
    }
    return new Snapshot(next(snapshot.revision), appended);
  }

  /** Replaces the fence with the confirmed committed state and this commit's candidate, as one complete file. */
  private void writeFence(Snapshot committed, Snapshot candidate, String instant) throws IOException {
    ObjectNode envelope = JSON.createObjectNode();
    envelope.put(FIELD_LAYOUT_VERSION, FENCE_LAYOUT_VERSION);
    if (committed.events.isEmpty()) {
      envelope.putNull(FIELD_COMMITTED);
    } else {
      envelope.set(FIELD_COMMITTED, JSON.readTree(serialize(committed)));
    }
    ObjectNode pending = envelope.putObject(FIELD_PENDING);
    pending.put(FIELD_INSTANT, instant);
    pending.set(FIELD_SNAPSHOT, JSON.readTree(serialize(candidate)));
    replaceAtomically(new StoragePath(basePath, FENCE_PATH), FENCE_SCRATCH_DIRECTORY, JSON.writeValueAsBytes(envelope),
        "embedding column fence", NOTHING_BEFORE_SWAP);
  }

  /**
   * The committed identity state: the fence's candidate when its instant completed, else its confirmed committed state, merged
   * with {@code published} history; {@code published} alone on a table without a fence. Malformed fences refuse.
   */
  private Snapshot readFence(HoodieTableMetaClient meta, Snapshot published) throws IOException {
    StoragePath fence = new StoragePath(basePath, FENCE_PATH);
    EmbeddingColumnInfoAtomicSwap.checkLocalChecksum(auxiliaryStorage, fence);
    if (!auxiliaryStorage.exists(fence)) {
      return published;
    }
    JsonNode root;
    try (InputStream input = auxiliaryStorage.open(fence)) {
      root = JSON.readTree(input);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Malformed embedding identity fence JSON at " + fence, e);
    }
    exactFields(root, FENCE_FIELDS, FENCE_FIELDS, "identity fence");
    if (positiveLong(root.get(FIELD_LAYOUT_VERSION), "identity fence " + FIELD_LAYOUT_VERSION) != FENCE_LAYOUT_VERSION) {
      throw invalid("Unsupported embedding identity fence layout version");
    }
    Snapshot committed = root.get(FIELD_COMMITTED).isNull() ? published : parseSnapshot(root.get(FIELD_COMMITTED));
    JsonNode pending = root.get(FIELD_PENDING);
    exactFields(pending, PENDING_FIELDS, PENDING_FIELDS, "pending identity");
    String instant = text(pending.get(FIELD_INSTANT), "pending instant");
    requireInstant(instant, "pending instant");
    Snapshot candidate = parseSnapshot(pending.get(FIELD_SNAPSHOT));
    return mergePublished(CompletedWrite.exists(meta, instant) ? candidate : committed, published);
  }

  /**
   * {@code authority} plus what only {@code published} history holds: columns a manual publication added and retirements; a
   * column both name with different identities refuses.
   */
  private static Snapshot mergePublished(Snapshot authority, Snapshot published) throws IOException {
    List<Event> merged = new ArrayList<>(authority.events);
    long sequence = merged.isEmpty() ? 0 : merged.get(merged.size() - 1).sequence;
    Set<String> missing = new HashSet<>(published.latest.keySet());
    missing.removeAll(authority.latest.keySet());
    for (Event event : published.events) {
      if (missing.contains(event.column)) {
        merged.add(new Event(next(sequence), event.column, event.action, event.instant, event.addedAt, event.identity));
        sequence++;
      }
    }
    for (Map.Entry<String, Event> entry : published.latest.entrySet()) {
      Event previous = authority.latest.get(entry.getKey());
      Event event = entry.getValue();
      if (previous == null) {
        continue;
      }
      if (!previous.identity.equals(event.identity)) {
        throw invalid(VECTOR_COLUMN + event.column + " has conflicting committed model identities");
      }
      if (EVENT_RETIRE.equals(event.action) && !EVENT_RETIRE.equals(previous.action)) {
        merged.add(new Event(next(sequence), event.column, EVENT_RETIRE, event.instant, previous.addedAt, previous.identity));
        sequence++;
      }
    }
    // A delayed callback may merge the same older fence again. Keep complete published history when nothing changed.
    if (published.revision >= authority.revision
        && Arrays.equals(serialize(new Snapshot(published.revision, merged)), serialize(published))) {
      return published;
    }
    if (merged.size() == authority.events.size() && authority.revision >= published.revision) {
      return authority;
    }
    return new Snapshot(next(Math.max(authority.revision, published.revision)), merged);
  }

  /** Appends a retire event for each requested column not already retired; returns the last sequence used. */
  private static long appendRetirements(Snapshot snapshot, Update update, String instant, List<Event> appended, long sequence) {
    long last = sequence;
    for (String column : update.retired) {
      Event previous = snapshot.latest.get(column);
      if (!EVENT_RETIRE.equals(previous.action)) {
        appended.add(new Event(next(last), column, EVENT_RETIRE, instant, previous.addedAt, previous.identity));
        last++;
      }
    }
    return last;
  }

  /** Appends an add event for each configured column absent from the history. */
  private static void appendConfiguredColumns(Snapshot snapshot, Update update, String instant, List<Event> appended,
                                              long sequence) {
    long last = sequence;
    for (Map.Entry<String, Identity> entry : update.configured.entrySet()) {
      if (!snapshot.latest.containsKey(entry.getKey())) {
        appended.add(new Event(next(last), entry.getKey(), EVENT_ADD, instant, instant, entry.getValue()));
        last++;
      }
    }
  }

  private void validate(Snapshot snapshot, Update update) {
    update.owned.addAll(snapshot.latest.keySet());
    for (String column : update.existing.keySet()) {
      if (update.owned.contains(column) && !snapshot.latest.containsKey(column)) {
        throw invalid(VECTOR_COLUMN + column + " has no model identity information in the history or committed fence; use a new table");
      }
    }
    for (Map.Entry<String, Identity> entry : update.configured.entrySet()) {
      validateConfigured(snapshot, entry.getKey(), entry.getValue(), update.existing.get(entry.getKey()));
    }
    for (String column : update.retired) {
      validateRetired(snapshot, column);
    }
  }

  /** A configured column: its physical dimension and its identity history agree with the configuration. */
  private void validateConfigured(Snapshot snapshot, String column, Identity configured, Integer physicalDimension) {
    if (physicalDimension != null && physicalDimension != configured.dimension) {
      throw invalid(VECTOR_COLUMN + column + " has dimension " + physicalDimension
          + ", configured model requires " + configured.dimension);
    }
    Event current = snapshot.latest.get(column);
    if (current != null) {
      if (EVENT_RETIRE.equals(current.action)) {
        throw invalid(VECTOR_COLUMN + column + " is retired; use a new physical column name");
      }
      if (!current.identity.equals(configured)) {
        throw invalid(VECTOR_COLUMN + column + " has a different model identity; use a new physical column name");
      }
    }
  }

  /**
   * Warns once per column that exists physically and has a committed identity in the fence but none in the auxiliary history:
   * validation used the fence, and publication after a successful commit restores the history. A column the fence does not
   * know either is refused by {@link #validate} before this warning step.
   */
  private void warnMissingHistory(Snapshot published, Snapshot committed, Update update) {
    for (String column : update.configured.keySet()) {
      if (update.existing.containsKey(column) && !published.latest.containsKey(column) && committed.latest.containsKey(column)
          && warned.add(column)) {
        LOG.warn("Embedding VECTOR column {} has no model identity information in auxiliary history; validated against the "
            + "committed identity fence and recovering the history after a successful commit", column);
      }
    }
  }

  /** A column to retire has a known, non-recovered identity history. */
  private static void validateRetired(Snapshot snapshot, String column) {
    Event current = snapshot.latest.get(column);
    if (current == null) {
      throw invalid("Cannot retire " + VECTOR_COLUMN + column + " without known active identity history");
    }
    if (current.addedAt == null) {
      throw invalid("Cannot retire recovered " + VECTOR_COLUMN + column
          + " with unknown creation instant: layout 1 permits null added_at_instant only on recover events");
    }
  }

  private Snapshot read(HoodieTableMetaClient meta) throws IOException {
    String version = meta.getTableConfig().getString(LAYOUT_VERSION);
    if (!CURRENT_LAYOUT_VERSION.equals(version)) {
      throw invalid("Unsupported " + LAYOUT_VERSION.key() + "=" + (version == null ? "none" : version)
          + "; supported layout version is " + CURRENT_LAYOUT_VERSION + ", written when the table is created");
    }
    boolean exists = auxiliaryStorage.exists(path);
    EmbeddingColumnInfoAtomicSwap.checkLocalChecksum(auxiliaryStorage, path);
    if (!exists) {
      return new Snapshot(0, Collections.emptyList());
    }
    JsonNode root;
    try (InputStream input = auxiliaryStorage.open(path)) {
      root = JSON.readTree(input);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Malformed embedding column info JSON at " + path, e);
    }
    return parseSnapshot(root);
  }

  /** Parses one strict layout 1 snapshot: the history file's content, also held by the fence. */
  private static Snapshot parseSnapshot(JsonNode root) {
    exactFields(root, SNAPSHOT_FIELDS, SNAPSHOT_FIELDS, "snapshot");
    if (positiveLong(root.get(FIELD_LAYOUT_VERSION), FIELD_LAYOUT_VERSION) != 1) {
      throw invalid("Embedding column info layout_version disagrees with table property 1");
    }
    long revision = positiveLong(root.get(FIELD_REVISION), FIELD_REVISION);
    JsonNode events = root.get(FIELD_EVENTS);
    if (!events.isArray()) {
      throw invalid("events must be an array");
    }
    List<Event> parsed = new ArrayList<>();
    Map<String, Event> latest = new HashMap<>();
    long sequence = 0;
    for (JsonNode node : events) {
      Event event = parseEvent(node);
      if (event.sequence <= sequence) {
        throw invalid("Event sequences must be unique and strictly increasing");
      }
      checkFollows(latest.get(event.column), event);
      latest.put(event.column, event);
      parsed.add(event);
      sequence = event.sequence;
    }
    return new Snapshot(revision, parsed);
  }

  /** A retire event follows a live event of the same identity and creation instant; any other event is the column's first. */
  private static void checkFollows(Event previous, Event event) {
    if (EVENT_RETIRE.equals(event.action)) {
      if (previous == null || EVENT_RETIRE.equals(previous.action) || !previous.identity.equals(event.identity)
          || !Objects.equals(previous.addedAt, event.addedAt)) {
        throw invalid("Invalid retirement history for VECTOR column " + event.column);
      }
    } else if (previous != null) {
      throw invalid(VECTOR_COLUMN + event.column + " has repeated creation or reuses a retired name");
    }
  }

  private static Event parseEvent(JsonNode node) {
    exactFields(node, EVENT_FIELDS, EVENT_FIELDS, FIELD_EVENT);
    long sequence = positiveLong(node.get(FIELD_SEQUENCE), FIELD_SEQUENCE);
    String column = text(node.get(FIELD_COLUMN_NAME), FIELD_COLUMN_NAME);
    String action = text(node.get(FIELD_EVENT), FIELD_EVENT);
    if (!Arrays.asList(EVENT_ADD, EVENT_RETIRE, EVENT_RECOVER).contains(action)) {
      throw invalid("Unknown column event " + action);
    }
    String instant = text(node.get(FIELD_INSTANT), FIELD_INSTANT);
    requireInstant(instant, FIELD_INSTANT);
    JsonNode birth = node.get(FIELD_ADDED_AT_INSTANT);
    String addedAt = birth.isNull() ? null : text(birth, FIELD_ADDED_AT_INSTANT);
    if (addedAt == null && !EVENT_RECOVER.equals(action)) {
      throw invalid("Null added_at_instant is permitted only for recover events");
    }
    if (addedAt != null) {
      requireInstant(addedAt, FIELD_ADDED_AT_INSTANT);
    }
    if (EVENT_ADD.equals(action) && !instant.equals(addedAt)) {
      throw invalid("An add event must retain its actual creation instant");
    }
    JsonNode identity = node.get(FIELD_IDENTITY);
    exactFields(identity, IDENTITY_FIELDS, fields(FIELD_PROVIDER, FIELD_MODEL_NAME, FIELD_DIMENSION, FIELD_INPUT_TRANSFORM_VERSION),
        FIELD_IDENTITY);
    long dimension = positiveLong(identity.get(FIELD_DIMENSION), FIELD_DIMENSION);
    if (dimension > Integer.MAX_VALUE) {
      throw invalid("dimension exceeds integer range");
    }
    Identity model = new Identity(text(identity.get(FIELD_PROVIDER), FIELD_PROVIDER), text(identity.get(FIELD_MODEL_NAME), FIELD_MODEL_NAME),
        (int) dimension, identity.has(FIELD_MODEL_VERSION) ? text(identity.get(FIELD_MODEL_VERSION), FIELD_MODEL_VERSION) : null,
        text(identity.get(FIELD_INPUT_TRANSFORM_VERSION), FIELD_INPUT_TRANSFORM_VERSION));
    return new Event(sequence, column, action, instant, addedAt, model);
  }

  private void write(Snapshot snapshot) throws IOException {
    replaceAtomically(path, SCRATCH_DIRECTORY, serialize(snapshot), "embedding column info", NOTHING_BEFORE_SWAP);
  }

  /**
   * Replaces {@code target} with {@code bytes} as one complete file: writes them to a new scratch file under
   * {@code scratchDirectory}, runs {@code beforeSwap}, then swaps the scratch file into place. The scratch file is removed in
   * every case; a failure to remove it is logged as {@code label} scratch.
   */
  private void replaceAtomically(StoragePath target, String scratchDirectory, byte[] bytes, String label, Runnable beforeSwap)
      throws IOException {
    StoragePath directory = new StoragePath(basePath, scratchDirectory);
    auxiliaryStorage.createDirectory(directory);
    auxiliaryStorage.createDirectory(target.getParent());
    StoragePath scratch = new StoragePath(directory, UUID.randomUUID() + ".json");
    try {
      try (OutputStream output = auxiliaryStorage.create(scratch, false)) {
        output.write(bytes);
      }
      beforeSwap.run();
      swap.replace(auxiliaryStorage, scratch, target);
    } finally {
      try {
        auxiliaryStorage.deleteFile(scratch);
      } catch (IOException cleanupFailure) {
        LOG.warn("Cannot remove owned {} scratch {}", label, scratch, cleanupFailure);
      }
    }
  }

  private static byte[] serialize(Snapshot snapshot) throws IOException {
    ObjectNode root = JSON.createObjectNode();
    root.put(FIELD_LAYOUT_VERSION, 1);
    root.put(FIELD_REVISION, snapshot.revision);
    ArrayNode events = root.putArray(FIELD_EVENTS);
    for (Event event : snapshot.events) {
      ObjectNode node = events.addObject();
      node.put(FIELD_SEQUENCE, event.sequence);
      node.put(FIELD_COLUMN_NAME, event.column);
      node.put(FIELD_EVENT, event.action);
      node.put(FIELD_INSTANT, event.instant);
      if (event.addedAt == null) {
        node.putNull(FIELD_ADDED_AT_INSTANT);
      } else {
        node.put(FIELD_ADDED_AT_INSTANT, event.addedAt);
      }
      ObjectNode identity = node.putObject(FIELD_IDENTITY);
      identity.put(FIELD_PROVIDER, event.identity.provider);
      identity.put(FIELD_MODEL_NAME, event.identity.modelName);
      identity.put(FIELD_DIMENSION, event.identity.dimension);
      if (event.identity.modelVersion != null) {
        identity.put(FIELD_MODEL_VERSION, event.identity.modelVersion);
      }
      identity.put(FIELD_INPUT_TRANSFORM_VERSION, event.identity.inputTransformVersion);
    }
    return JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
  }

  private <T> T locked(Operation<T> operation) {
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf.newInstance()).build();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(props).withPath(writerLockBasePath)
        .forTable(meta.getTableConfig().getTableName()).build();
    if (config.getLockProviderClass() == null) {
      if (config.getWriteConcurrencyMode() != WriteConcurrencyMode.SINGLE_WRITER) {
        throw invalid("Embedding column information requires a configured table lock provider for " + config.getWriteConcurrencyMode());
      }
      config.setValue(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME, InProcessLockProvider.class.getName());
    }
    try (LockManager manager = new LockManager(config, storage)) {
      manager.lock();
      try {
        // Table properties may have changed while this operation waited for the lock.
        return operation.run(HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf.newInstance()).build());
      } finally {
        manager.unlock();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read or publish embedding column information at " + path, e);
    }
  }

  private interface Operation<T> {
    T run(HoodieTableMetaClient meta) throws IOException;
  }

  private static Set<String> fields(String... names) {
    return new HashSet<>(Arrays.asList(names));
  }

  private static void exactFields(JsonNode node, Set<String> allowed, Set<String> required, String label) {
    if (node == null || !node.isObject()) {
      throw invalid(label + " must be an object");
    }
    Iterator<String> keys = node.fieldNames();
    while (keys.hasNext()) {
      String key = keys.next();
      if (!allowed.contains(key)) {
        throw invalid("Unknown " + label + " field " + key);
      }
    }
    for (String key : required) {
      if (!node.has(key)) {
        throw invalid("Missing " + label + " field " + key);
      }
    }
  }

  private static long positiveLong(JsonNode node, String label) {
    if (node == null || !node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() < 1) {
      throw invalid(label + " must be a positive integer");
    }
    return node.longValue();
  }

  private static String text(JsonNode node, String label) {
    if (node == null || !node.isTextual()) {
      throw invalid(label + " must be a nonempty string");
    }
    return nonempty(node.textValue(), label);
  }

  private static String nonempty(String value, String label) {
    if (value == null || value.trim().isEmpty() || !value.equals(value.trim())) {
      throw invalid(label + " must be a nonempty canonical string without surrounding whitespace");
    }
    return value;
  }

  private static void requireInstant(String instant, String label) {
    nonempty(instant, label);
    if (!instant.matches("\\d+")) {
      throw invalid(label + " must be a Hudi requested-instant string");
    }
  }

  private static long next(long value) {
    if (value == Long.MAX_VALUE) {
      throw invalid("Embedding column info counter exhausted");
    }
    return value + 1;
  }

  private static IllegalArgumentException invalid(String message) {
    return new IllegalArgumentException(message);
  }

  /** In-memory intent only; round views share ownership learned from committed history, never a persisted sidecar. */
  public static final class Update implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String basePath;
    private final Map<String, Identity> configured;
    private final Map<String, Integer> existing;
    private final Set<String> retired;
    private final Set<String> owned;

    private Update(String basePath, Map<String, Identity> configured, Map<String, Integer> existing, Set<String> retired,
                   Set<String> ownedColumns) {
      this.basePath = basePath;
      this.configured = Collections.unmodifiableMap(new TreeMap<>(configured));
      this.existing = Collections.unmodifiableMap(new TreeMap<>(existing));
      this.retired = Collections.unmodifiableSet(new TreeSet<>(retired));
      // Keep observations shared across cached round views; VECTOR type alone does not establish ownership.
      this.owned = ownedColumns;
      this.owned.addAll(configured.keySet());
      for (Map.Entry<String, Identity> entry : this.configured.entrySet()) {
        nonempty(entry.getKey(), FIELD_COLUMN_NAME);
        Objects.requireNonNull(entry.getValue(), "model identity");
        if (this.retired.contains(entry.getKey())) {
          throw invalid(VECTOR_COLUMN + entry.getKey() + " cannot be configured and retired in the same round");
        }
      }
      this.retired.forEach(column -> nonempty(column, FIELD_COLUMN_NAME));
      this.existing.forEach((column, dimension) -> {
        nonempty(column, FIELD_COLUMN_NAME);
        if (dimension == null || dimension < 1) {
          throw invalid("VECTOR dimension must be positive for column " + column);
        }
      });
    }

    /** Refreshes physical columns from this round's committed schema without repeating the job's startup checks. */
    Update withExistingColumns(Map<String, Integer> columns) {
      return new Update(basePath, configured, columns, retired, owned);
    }
  }

  /**
   * The only allow-listed model fields accepted by the history writer; no connection/configuration map is retained. The
   * input-transform version is the compute-side transform applied to an input before the model call (the in-memory image
   * resize for a model that embeds images, {@link #NO_INPUT_TRANSFORM} otherwise): the same model behind another transform
   * produces other vectors, so it is a different identity.
   */
  public static final class Identity implements Serializable {
    private static final long serialVersionUID = 2L;
    /** The input-transform version of a model whose inputs are sent as they are (text). */
    public static final String NO_INPUT_TRANSFORM = "none";
    private final String provider;
    private final String modelName;
    private final int dimension;
    private final String modelVersion;
    private final String inputTransformVersion;

    /** The identity of a model whose inputs are sent as they are ({@link #NO_INPUT_TRANSFORM}); for tests. */
    @VisibleForTesting
    Identity(String provider, String modelName, int dimension, String modelVersion) {
      this(provider, modelName, dimension, modelVersion, NO_INPUT_TRANSFORM);
    }

    public Identity(String provider, String modelName, int dimension, String modelVersion, String inputTransformVersion) {
      this.provider = nonempty(provider, FIELD_PROVIDER);
      this.modelName = nonempty(modelName, FIELD_MODEL_NAME);
      if (dimension < 1) {
        throw invalid("dimension must be positive");
      }
      this.dimension = dimension;
      this.modelVersion = modelVersion == null ? null : nonempty(modelVersion, FIELD_MODEL_VERSION);
      this.inputTransformVersion = nonempty(inputTransformVersion, FIELD_INPUT_TRANSFORM_VERSION);
    }

    public String getProvider() {
      return provider;
    }

    public String getModelName() {
      return modelName;
    }

    public int getDimension() {
      return dimension;
    }

    public String getModelVersion() {
      return modelVersion;
    }

    public String getInputTransformVersion() {
      return inputTransformVersion;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Identity)) {
        return false;
      }
      Identity that = (Identity) other;
      return dimension == that.dimension && provider.equals(that.provider) && modelName.equals(that.modelName)
          && Objects.equals(modelVersion, that.modelVersion) && inputTransformVersion.equals(that.inputTransformVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(provider, modelName, dimension, modelVersion, inputTransformVersion);
    }
  }

  private static final class Event {
    private final long sequence;
    private final String column;
    private final String action;
    private final String instant;
    private final String addedAt;
    private final Identity identity;

    private Event(long sequence, String column, String event, String instant, String addedAt, Identity identity) {
      this.sequence = sequence;
      this.column = column;
      this.action = event;
      this.instant = instant;
      this.addedAt = addedAt;
      this.identity = identity;
    }
  }

  private static final class Snapshot {
    private final long revision;
    private final List<Event> events;
    private final Map<String, Event> latest = new LinkedHashMap<>();

    private Snapshot(long revision, List<Event> events) {
      this.revision = revision;
      this.events = events;
      events.forEach(event -> latest.put(event.column, event));
    }
  }
}
