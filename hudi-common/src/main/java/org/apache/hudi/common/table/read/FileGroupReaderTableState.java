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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.utils.InternalSchemaUtils;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.storage.StoragePath;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.TreeMap;

/**
 * The table-level state a file group reader needs, in place of a {@link HoodieTableMetaClient}.
 *
 * <p>A reader needs the table config and base path for every read. Only two cases need more: log blocks of tables
 * before version 8 are checked against the committed instants, and schema-on-read resolves the schema a log block
 * or file was written with. A state built with {@link #snapshotOf} answers the first from instants captured where the
 * timeline is already loaded, so every reader of one query sees the same snapshot, and carries the meta client only
 * for the second. {@link #fromMetaClient} keeps the older behavior of loading both lazily from a meta client.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class FileGroupReaderTableState implements Serializable {

  private static final long serialVersionUID = 1L;

  @Getter
  private final StoragePath basePath;
  @Getter
  private final HoodieTableConfig tableConfig;
  private final SerializableSupplier<CommittedInstants> committedInstantsSupplier;
  private final InternalSchemaResolver internalSchemaResolver;
  private transient volatile CommittedInstants committedInstants;

  /**
   * Captures the state from a meta client whose timeline is loaded, with only what its readers will consult.
   *
   * @param readsLogFiles          whether the readers read log files; committed instants are captured only then, and
   *                               only for tables before version 8
   * @param resolvesSchemaVersions whether the readers resolve schema-on-read versions; only then the state carries the
   *                               meta client, with its active timeline loaded, so the readers search the schema
   *                               history through {@link InternalSchemaCache} without loading the table config or the
   *                               timeline
   */
  public static FileGroupReaderTableState snapshotOf(HoodieTableMetaClient metaClient, boolean readsLogFiles,
                                                     boolean resolvesSchemaVersions) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Option<CommittedInstants> committed = readsLogFiles && tableConfig.getTableVersion().lesserThan(HoodieTableVersion.EIGHT)
        ? Option.of(CommittedInstants.fromCommitsTimeline(metaClient.getCommitsTimeline()))
        : Option.empty();
    String basePath = String.valueOf(metaClient.getBasePath());
    InternalSchemaResolver schemaResolver;
    if (resolvesSchemaVersions) {
      // Loaded here so that every copy of the state carries it
      metaClient.getActiveTimeline();
      schemaResolver = new MetaClientSchemaResolver(metaClient);
    } else {
      schemaResolver = new UnavailableSchemaResolver(basePath);
    }
    return new FileGroupReaderTableState(metaClient.getBasePath(), tableConfig,
        new CapturedCommittedInstants(basePath, committed.orElse(null)), schemaResolver);
  }

  /**
   * Creates the state from already captured values, e.g. after transporting them as plain data.
   *
   * @param committedInstants required to read log files of tables before version 8
   * @param schemaHistory     serialized schema history (see {@link SerDeHelper#parseSchemas}), required for
   *                          schema-on-read
   */
  public static FileGroupReaderTableState of(StoragePath basePath,
                                             HoodieTableConfig tableConfig,
                                             Option<CommittedInstants> committedInstants,
                                             Option<String> schemaHistory) {
    return new FileGroupReaderTableState(basePath, tableConfig,
        new CapturedCommittedInstants(String.valueOf(basePath), committedInstants.orElse(null)),
        schemaHistory.isPresent() ? new SchemaHistoryResolver(schemaHistory.get()) : new UnavailableSchemaResolver(String.valueOf(basePath)));
  }

  /**
   * Creates the state backed by a meta client: committed instants and schema versions are loaded from it on first
   * use. The state holds the meta client and carries it, timeline included, when serialized; a state shipped to
   * executors must be built with {@link #snapshotOf} instead.
   */
  public static FileGroupReaderTableState fromMetaClient(HoodieTableMetaClient metaClient) {
    return new FileGroupReaderTableState(metaClient.getBasePath(), metaClient.getTableConfig(),
        new MetaClientCommittedInstants(metaClient), new MetaClientSchemaResolver(metaClient));
  }

  /**
   * Whether the instant of a log block counts as committed; only consulted for tables before version 8.
   */
  public boolean isCommitted(String instantTime) {
    CommittedInstants committed = committedInstants;
    if (committed == null) {
      // Readers of one executor share a broadcast state, so the instants are loaded once for all of them
      synchronized (this) {
        committed = committedInstants;
        if (committed == null) {
          committed = committedInstantsSupplier.get();
          committedInstants = committed;
        }
      }
    }
    return committed.isCommitted(instantTime);
  }

  /**
   * The schema of the given schema version, i.e. the newest schema at or before the version.
   */
  public InternalSchema getInternalSchema(long versionId) {
    return internalSchemaResolver.resolve(versionId);
  }

  @FunctionalInterface
  private interface InternalSchemaResolver extends Serializable {
    InternalSchema resolve(long versionId);
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  private static final class CapturedCommittedInstants implements SerializableSupplier<CommittedInstants> {
    private static final long serialVersionUID = 1L;

    private final String basePath;
    // Null when the instants were not captured.
    private final CommittedInstants committedInstants;

    @Override
    public CommittedInstants get() {
      if (committedInstants == null) {
        throw new IllegalStateException("Committed instants were not captured for table " + basePath);
      }
      return committedInstants;
    }
  }

  /**
   * Loads the committed instants from the meta client, which travels with this supplier when it is serialized.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  private static final class MetaClientCommittedInstants implements SerializableSupplier<CommittedInstants> {
    private static final long serialVersionUID = 1L;

    private final HoodieTableMetaClient metaClient;

    @Override
    public CommittedInstants get() {
      return CommittedInstants.fromCommitsTimeline(metaClient.getCommitsTimeline());
    }
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  private static final class MetaClientSchemaResolver implements InternalSchemaResolver {
    private static final long serialVersionUID = 1L;

    private final HoodieTableMetaClient metaClient;

    @Override
    public InternalSchema resolve(long versionId) {
      return InternalSchemaCache.searchSchemaAndCache(versionId, metaClient);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  private static final class SchemaHistoryResolver implements InternalSchemaResolver {
    private static final long serialVersionUID = 1L;

    private final String schemaHistory;
    private transient volatile TreeMap<Long, InternalSchema> schemas;

    @Override
    public InternalSchema resolve(long versionId) {
      TreeMap<Long, InternalSchema> parsed = schemas;
      if (parsed == null) {
        // Readers of one executor share a broadcast state, so the history is parsed once for all of them
        synchronized (this) {
          parsed = schemas;
          if (parsed == null) {
            parsed = StringUtils.isNullOrEmpty(schemaHistory) ? new TreeMap<>() : SerDeHelper.parseSchemas(schemaHistory);
            schemas = parsed;
          }
        }
      }
      return InternalSchemaUtils.searchSchema(versionId, parsed);
    }
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  private static final class UnavailableSchemaResolver implements InternalSchemaResolver {
    private static final long serialVersionUID = 1L;

    private final String basePath;

    @Override
    public InternalSchema resolve(long versionId) {
      throw new IllegalStateException("Schema versions were not captured for table " + basePath);
    }
  }
}
