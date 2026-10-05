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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.CommittedInstants;
import org.apache.hudi.common.table.read.FileGroupReaderTableState;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * The table state a {@link HoodieFileGroupReaderBasedRecordReader} needs, captured once per table when the splits
 * are listed and carried by each split, so that record readers neither build a meta client nor resolve the table
 * schema or the latest commit per split.
 *
 * <p>Splits carry the state as compressed plain data (table properties, latest commit, schema, committed instants)
 * behind a format byte, encoded once per state and shared by all its splits. A state whose encoding exceeds
 * {@link #MAX_ENCODED_BYTES} is shipped without the schema, or not at all, and readers then resolve what is missing.
 */
public final class HiveReaderTableState {

  private static final Logger LOG = LoggerFactory.getLogger(HiveReaderTableState.class);

  /**
   * Upper bound of the encoded state every split carries.
   */
  static final int MAX_ENCODED_BYTES = 16 * 1024;

  private static final byte NO_STATE = 0;
  private static final byte FORMAT_V1 = 1;

  private final String basePath;
  private final String latestCommitTime;
  private final Map<String, String> tableProperties;
  // null when not shipped, the record reader then resolves it itself
  private final String tableSchema;
  // present for merge-on-read reads of tables before version 8 only, the only reads that check log blocks with it
  private final Option<CommittedInstants> committedInstants;
  private final byte[] encoded;
  private volatile FileGroupReaderTableState tableState;

  private HiveReaderTableState(String basePath, String latestCommitTime, Map<String, String> tableProperties,
                               String tableSchema, Option<CommittedInstants> committedInstants, byte[] encoded) {
    this.basePath = basePath;
    this.latestCommitTime = latestCommitTime;
    this.tableProperties = tableProperties;
    this.tableSchema = tableSchema;
    this.committedInstants = committedInstants;
    this.encoded = encoded;
  }

  /**
   * Returns a capturer of the states of one table, or one that captures nothing when the record readers of the job
   * do not use the state.
   */
  public static Capturer capturer(HoodieTableMetaClient metaClient, JobConf job) {
    return new Capturer(HoodieInputFormatUtils.shouldUseFilegroupReader(job) ? metaClient : null);
  }

  /**
   * Builds the state from its parts, encoding it within {@code maxEncodedBytes}: without the schema if the whole
   * state does not fit, and not at all if even that does not fit.
   */
  @VisibleForTesting
  static Option<HiveReaderTableState> create(String basePath, String latestCommitTime, Map<String, String> tableProperties,
                                             String tableSchema, Option<CommittedInstants> committedInstants,
                                             int maxEncodedBytes) {
    Map<String, String> shippedProperties = new TreeMap<>(tableProperties);
    // The create schema is the largest property and readers do not use it
    shippedProperties.remove(HoodieTableConfig.CREATE_SCHEMA.key());
    byte[] encoded = encode(basePath, latestCommitTime, shippedProperties, tableSchema, committedInstants);
    String shippedSchema = tableSchema;
    if (encoded.length > maxEncodedBytes && shippedSchema != null) {
      shippedSchema = null;
      encoded = encode(basePath, latestCommitTime, shippedProperties, null, committedInstants);
    }
    if (encoded.length > maxEncodedBytes) {
      LOG.warn("The table state of {} takes {} bytes encoded, more than {}, splits will resolve it themselves",
          basePath, encoded.length, maxEncodedBytes);
      return Option.empty();
    }
    return Option.of(new HiveReaderTableState(basePath, latestCommitTime, shippedProperties, shippedSchema,
        committedInstants, encoded));
  }

  /**
   * The requested time of the latest completed commit, or an empty string if the table has none.
   */
  public static String latestCompletedCommitTime(HoodieTableMetaClient metaClient) {
    return metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant()
        .map(HoodieInstant::requestedTime).orElse(EMPTY_STRING);
  }

  /**
   * Returns the state carried by the split, if any.
   */
  public static Option<HiveReaderTableState> of(InputSplit split) {
    if (split instanceof RealtimeSplit) {
      return ((RealtimeSplit) split).getReaderTableState();
    }
    if (split instanceof FileSplitWithReaderTableState) {
      return Option.ofNullable(((FileSplitWithReaderTableState) split).getReaderTableState());
    }
    return Option.empty();
  }

  /**
   * The state to read the split's file group with.
   */
  public FileGroupReaderTableState getTableState() {
    FileGroupReaderTableState state = tableState;
    if (state == null) {
      HoodieTableConfig tableConfig = new HoodieTableConfig();
      tableConfig.getProps().putAll(tableProperties);
      state = FileGroupReaderTableState.of(new StoragePath(basePath), tableConfig, committedInstants, Option.empty());
      tableState = state;
    }
    return state;
  }

  public String getLatestCommitTime() {
    return latestCommitTime;
  }

  /**
   * The table schema as of {@link #getLatestCommitTime()}, if it was shipped.
   */
  public Option<String> getTableSchema() {
    return Option.ofNullable(tableSchema);
  }

  @VisibleForTesting
  byte[] getEncoded() {
    return encoded;
  }

  /**
   * Writes the state as part of a split.
   */
  public static void write(Option<HiveReaderTableState> state, DataOutput out) throws IOException {
    if (!state.isPresent()) {
      out.writeByte(NO_STATE);
      return;
    }
    out.writeByte(FORMAT_V1);
    out.writeInt(state.get().encoded.length);
    out.write(state.get().encoded);
  }

  /**
   * Reads the state written by {@link #write}. A split written without the state, or with a format this reader does
   * not know, reads as one without the state.
   */
  public static Option<HiveReaderTableState> read(DataInput in) throws IOException {
    byte format;
    try {
      format = in.readByte();
    } catch (EOFException e) {
      return Option.empty();
    }
    if (format == NO_STATE) {
      return Option.empty();
    }
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    if (format != FORMAT_V1) {
      LOG.warn("Ignoring the table state of a split written in unknown format {}", format);
      return Option.empty();
    }
    return Option.of(decode(bytes));
  }

  private static byte[] encode(String basePath, String latestCommitTime, Map<String, String> tableProperties,
                               String tableSchema, Option<CommittedInstants> committedInstants) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(new DeflaterOutputStream(bytes))) {
      writeString(basePath, out);
      writeString(latestCommitTime, out);
      out.writeInt(tableProperties.size());
      for (Map.Entry<String, String> property : tableProperties.entrySet()) {
        writeString(property.getKey(), out);
        writeString(property.getValue(), out);
      }
      writeNullableString(tableSchema, out);
      out.writeBoolean(committedInstants.isPresent());
      if (committedInstants.isPresent()) {
        writeStrings(committedInstants.get().getCompletedInstants(), out);
        writeStrings(committedInstants.get().getInflightInstants(), out);
        writeNullableString(committedInstants.get().getTimelineStartInstant().orElse(null), out);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to encode the table state of " + basePath, e);
    }
    return bytes.toByteArray();
  }

  private static HiveReaderTableState decode(byte[] encoded) throws IOException {
    try (DataInputStream in = new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(encoded)))) {
      String basePath = readString(in);
      String latestCommitTime = readString(in);
      int propertyCount = in.readInt();
      Map<String, String> tableProperties = new HashMap<>(propertyCount * 2);
      for (int i = 0; i < propertyCount; i++) {
        String key = readString(in);
        tableProperties.put(key, readString(in));
      }
      String tableSchema = readNullableString(in);
      Option<CommittedInstants> committedInstants = Option.empty();
      if (in.readBoolean()) {
        List<String> completed = readStrings(in);
        List<String> inflight = readStrings(in);
        committedInstants = Option.of(CommittedInstants.of(completed, inflight, Option.ofNullable(readNullableString(in))));
      }
      return new HiveReaderTableState(basePath, latestCommitTime, tableProperties, tableSchema, committedInstants, encoded);
    }
  }

  private static void writeString(String value, DataOutput out) throws IOException {
    byte[] bytes = getUTF8Bytes(value);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return fromUTF8Bytes(bytes);
  }

  private static void writeNullableString(String value, DataOutput out) throws IOException {
    out.writeBoolean(value != null);
    if (value != null) {
      writeString(value, out);
    }
  }

  private static String readNullableString(DataInput in) throws IOException {
    return in.readBoolean() ? readString(in) : null;
  }

  private static void writeStrings(Collection<String> values, DataOutput out) throws IOException {
    out.writeInt(values.size());
    for (String value : values) {
      writeString(value, out);
    }
  }

  private static List<String> readStrings(DataInput in) throws IOException {
    int count = in.readInt();
    List<String> values = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      values.add(readString(in));
    }
    return values;
  }

  /**
   * Captures the states of one table while its files are listed, once per commit time the files are read as of,
   * from the timeline the files are listed with.
   */
  public static final class Capturer {
    private final HoodieTableMetaClient metaClient;
    private final Map<String, Option<HiveReaderTableState>> states = new HashMap<>();
    private Map<String, String> tableProperties;
    private String latestCompletedCommitTime;

    private Capturer(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
    }

    /**
     * The state for base files read as of the latest completed commit; base files alone never need the committed
     * instants.
     */
    public Option<HiveReaderTableState> forLatestCommit() {
      if (metaClient == null) {
        return Option.empty();
      }
      if (latestCompletedCommitTime == null) {
        latestCompletedCommitTime = latestCompletedCommitTime(metaClient);
      }
      return capture(latestCompletedCommitTime, false);
    }

    /**
     * The state for merge-on-read file slices read as of the given commit.
     */
    public Option<HiveReaderTableState> forFileSlicesAsOf(String commitTime) {
      return metaClient == null ? Option.empty() : capture(commitTime, true);
    }

    private Option<HiveReaderTableState> capture(String commitTime, boolean readsLogFiles) {
      return states.computeIfAbsent(commitTime + (readsLogFiles ? "/logs" : ""), key -> {
        if (tableProperties == null) {
          Properties props = metaClient.getTableConfig().getProps();
          tableProperties = new HashMap<>();
          props.stringPropertyNames().forEach(name -> tableProperties.put(name, props.getProperty(name)));
        }
        String tableSchema = null;
        try {
          tableSchema = new TableSchemaResolver(metaClient).getTableSchema(commitTime).toString();
        } catch (Exception e) {
          LOG.warn("Unable to resolve the schema of table {} as of {}, record readers will resolve it",
              metaClient.getBasePath(), commitTime, e);
        }
        Option<CommittedInstants> committedInstants = readsLogFiles
            && metaClient.getTableType() == HoodieTableType.MERGE_ON_READ
            && metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)
            ? Option.of(committedInstantsAsOf(metaClient.getCommitsTimeline(), commitTime))
            : Option.empty();
        return create(metaClient.getBasePath().toString(), commitTime, tableProperties, tableSchema, committedInstants,
            MAX_ENCODED_BYTES);
      });
    }

    /**
     * Log blocks after the commit a file slice is read as of are skipped before the committed instants are checked,
     * so only the instants up to it are kept.
     */
    private static CommittedInstants committedInstantsAsOf(HoodieTimeline commitsTimeline, String commitTime) {
      CommittedInstants all = CommittedInstants.fromCommitsTimeline(commitsTimeline);
      if (StringUtils.isNullOrEmpty(commitTime)) {
        return all;
      }
      CommittedInstants upToCommit = CommittedInstants.fromCommitsTimeline(commitsTimeline.findInstantsBeforeOrEquals(commitTime));
      return CommittedInstants.of(new ArrayList<>(upToCommit.getCompletedInstants()),
          new ArrayList<>(upToCommit.getInflightInstants()), all.getTimelineStartInstant());
    }
  }
}
