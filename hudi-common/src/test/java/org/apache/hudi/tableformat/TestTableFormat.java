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

package org.apache.hudi.tableformat;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.BaseFileColumnResolver;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFileRowLineage;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.TableMetadataFactory;
import org.apache.hudi.storage.StoragePath;

import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Test implementation of HoodieTableFormat that records all Hoodie instants in memory.
 * Used for functional testing of HoodieTableFormat.
 *
 * <p>With {@link #ROW_LINEAGE_ENABLED} set in the table config, the table format also tracks row lineage: when
 * a commit completes, it assigns the next sequence number to the commit and to every base file the commit
 * added, and the next range of row IDs, one per record, to each of these base files. It declares two format meta
 * columns: the row ID and the last updated sequence number of a row. A rewrite stores them null for a new or
 * updated row, which gets a new row lineage, and for a row carried over unchanged, it stores the value its base
 * file stores, or derives it: the first row ID of the base file plus the position of the row in the file, and the
 * sequence number of the base file.
 *
 * <p>With {@link #INSTANT_TIME_COLUMN_ENABLED} set in the table config, the table format declares a format meta
 * column with a value on write: the instant time of the write that wrote a row into a base file as new or changed.
 * A row carried over unchanged keeps the value its base file stores, or gets the instant time of the base file. It
 * also declares a first instant time column with a value on update: a row written as a changed version of a row of
 * the base file keeps the value of that row, and is otherwise maintained like the instant time column.
 */
@NoArgsConstructor
public class TestTableFormat implements HoodieTableFormat {

  public static final String ROW_LINEAGE_ENABLED = HoodieTableConfig.TABLE_FORMAT.key() + ".test.row.lineage.enabled";
  public static final String ROW_ID_COLUMN = "_test_row_id";
  public static final String LAST_UPDATED_SEQUENCE_NUMBER_COLUMN = "_test_last_updated_sequence_number";
  public static final String INSTANT_TIME_COLUMN_ENABLED = HoodieTableConfig.TABLE_FORMAT.key() + ".test.instant.time.column.enabled";
  public static final String INSTANT_TIME_COLUMN = "_test_instant_time";
  public static final String FIRST_INSTANT_TIME_COLUMN = "_test_first_instant_time";

  private static final Map<String, List<HoodieInstant>> RECORDED_INSTANTS = new ConcurrentHashMap<>();
  private static final Map<String, TableRowLineage> ROW_LINEAGE = new ConcurrentHashMap<>();

  private boolean rowLineageEnabled;
  private boolean instantTimeColumnEnabled;

  public static List<HoodieInstant> getRecordedInstants(String basePath) {
    return RECORDED_INSTANTS.getOrDefault(basePath, Collections.emptyList());
  }

  /**
   * Returns the row lineage values assigned to a base file of the table, if a completed commit added the file.
   */
  public static Option<BaseFileRowLineage> getBaseFileRowLineage(String basePath, String baseFileName) {
    TableRowLineage tableRowLineage = ROW_LINEAGE.get(basePath);
    return tableRowLineage == null ? Option.empty() : Option.ofNullable(tableRowLineage.baseFiles.get(baseFileName));
  }

  /**
   * Makes the table format lose the row lineage values it assigned to the base files of the table.
   */
  public static void forgetBaseFileRowLineage(String basePath) {
    TableRowLineage tableRowLineage = ROW_LINEAGE.get(basePath);
    if (tableRowLineage != null) {
      tableRowLineage.baseFiles.clear();
    }
  }

  /**
   * Stores settings of the table format in the table config of the table, from which {@link #init} reads them, and
   * returns a reloaded meta client of the table.
   */
  public static HoodieTableMetaClient setTableFormatConfigs(HoodieTableMetaClient metaClient, Map<String, String> configs) {
    Properties properties = new Properties();
    properties.putAll(configs);
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), properties);
    return HoodieTableMetaClient.reload(metaClient);
  }

  public static void tearDown() {
    RECORDED_INSTANTS.clear();
    ROW_LINEAGE.clear();
  }

  @Override
  public String getName() {
    return "test-format";
  }

  @Override
  public void init(Properties properties) {
    rowLineageEnabled = Boolean.parseBoolean(properties.getProperty(ROW_LINEAGE_ENABLED, "false"));
    instantTimeColumnEnabled = Boolean.parseBoolean(properties.getProperty(INSTANT_TIME_COLUMN_ENABLED, "false"));
  }

  @Override
  public List<FormatMetaColumn> getFormatMetaColumns() {
    List<FormatMetaColumn> columns = new ArrayList<>();
    if (rowLineageEnabled) {
      columns.add(new RowLineageColumn(ROW_ID_COLUMN, true));
      columns.add(new RowLineageColumn(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN, false));
    }
    if (instantTimeColumnEnabled) {
      columns.add(new InstantTimeColumn(INSTANT_TIME_COLUMN, false));
      columns.add(new InstantTimeColumn(FIRST_INSTANT_TIME_COLUMN, true));
    }
    return columns;
  }

  @Override
  public void commit(HoodieCommitMetadata commitMetadata, HoodieInstant completedInstant, 
                    HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, 
                    FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.putIfAbsent(metaClient.getBasePath().toString(), new CopyOnWriteArrayList<>());
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(completedInstant);
    if (rowLineageEnabled) {
      ROW_LINEAGE.computeIfAbsent(metaClient.getBasePath().toString(), basePath -> new TableRowLineage()).assign(commitMetadata);
    }
  }

  @Override
  public void clean(HoodieCleanMetadata cleanMetadata, HoodieInstant completedInstant, 
                   HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, 
                   FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(completedInstant);
  }

  @Override
  public void archive(Supplier<List<HoodieInstant>> archivedInstants, HoodieEngineContext engineContext,
                      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).removeAll(archivedInstants.get());
  }

  @Override
  public void rollback(HoodieInstant instantToRollback, HoodieEngineContext engineContext,
                      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).remove(instantToRollback);
  }

  @Override
  public void completedRollback(HoodieInstant rollbackInstant, HoodieEngineContext engineContext, 
                               HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.putIfAbsent(metaClient.getBasePath().toString(), new CopyOnWriteArrayList<>());
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(rollbackInstant);
  }

  @Override
  public void savepoint(HoodieInstant savepointInstant, HoodieEngineContext engineContext, 
                       HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(savepointInstant);
  }

  @Override
  public void restore(HoodieInstant restoreCompletedInstant, HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(restoreCompletedInstant);
  }

  @Override
  public TimelineFactory getTimelineFactory() {
    return new TestTimelineFactory(null);
  }

  @Override
  public TableMetadataFactory getMetadataFactory() {
    return TestTableMetadataFactory.getInstance();
  }

  private static HoodieSchemaField nullableLongField(String name) {
    return HoodieSchemaField.of(name, HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE);
  }

  /**
   * A row lineage column: null for a new or updated row, and for a row carried over unchanged, the value its base
   * file stores, or the one derived from the row lineage values assigned to the base file.
   */
  private static class RowLineageColumn implements FormatMetaColumn {
    private final String name;
    private final boolean rowId;

    private RowLineageColumn(String name, boolean rowId) {
      this.name = name;
      this.rowId = rowId;
    }

    @Override
    public HoodieSchemaField field() {
      return nullableLongField(name);
    }

    @Override
    public Object valueOnWrite(String instantTime) {
      return null;
    }

    @Override
    public BaseFileColumnResolver resolverFor(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
      BaseFileRowLineage baseFileRowLineage = getBaseFileRowLineage(metaClient.getBasePath().toString(), baseFile.getFileName())
          .orElseThrow(() -> new HoodieException("Table format test-format declares row lineage columns but has no row lineage for base file "
              + baseFile.getPath() + ", so the records carried over from it cannot keep their row lineage"));
      long firstRowId = baseFileRowLineage.getFirstRowId();
      long sequenceNumber = baseFileRowLineage.getSequenceNumber();
      return rowId
          ? (rowPosition, storedValue) -> storedValue != null ? storedValue : (Object) (firstRowId + rowPosition)
          : (rowPosition, storedValue) -> storedValue != null ? storedValue : (Object) sequenceNumber;
    }
  }

  /**
   * The instant time of the write that wrote a row into a base file as new or changed, as a long. With
   * {@code keptOnUpdate}, a row written as a changed version of a row of the base file keeps the value of that row.
   */
  private static class InstantTimeColumn implements FormatMetaColumn {
    private final String name;
    private final boolean keptOnUpdate;

    private InstantTimeColumn(String name, boolean keptOnUpdate) {
      this.name = name;
      this.keptOnUpdate = keptOnUpdate;
    }

    @Override
    public HoodieSchemaField field() {
      return nullableLongField(name);
    }

    @Override
    public Object valueOnWrite(String instantTime) {
      return Long.parseLong(instantTime);
    }

    @Override
    public Object valueOnUpdate(String instantTime, Object replacedValue) {
      return keptOnUpdate ? replacedValue : valueOnWrite(instantTime);
    }

    @Override
    public BaseFileColumnResolver resolverFor(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
      long baseFileInstantTime = Long.parseLong(baseFile.getCommitTime());
      return (rowPosition, storedValue) -> storedValue != null ? storedValue : (Object) baseFileInstantTime;
    }
  }

  /**
   * The row lineage state of a table: the next row ID and sequence number to assign, and the values assigned to
   * each base file by name.
   */
  private static class TableRowLineage {
    private final Map<String, BaseFileRowLineage> baseFiles = new ConcurrentHashMap<>();
    private long nextRowId = 0;
    private long lastSequenceNumber = 0;

    private synchronized void assign(HoodieCommitMetadata commitMetadata) {
      long sequenceNumber = ++lastSequenceNumber;
      commitMetadata.getWriteStats().stream()
          .filter(writeStat -> FSUtils.isBaseFile(new StoragePath(writeStat.getPath())))
          .forEach(writeStat -> {
            baseFiles.put(new StoragePath(writeStat.getPath()).getName(), new BaseFileRowLineage(nextRowId, sequenceNumber));
            nextRowId += writeStat.getNumWrites();
          });
    }
  }
}
