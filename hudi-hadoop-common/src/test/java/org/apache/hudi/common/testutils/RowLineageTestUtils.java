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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.BaseFileRowLineage;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.tableformat.TestTableFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.tableformat.TestTableFormat.LAST_UPDATED_SEQUENCE_NUMBER_COLUMN;
import static org.apache.hudi.tableformat.TestTableFormat.ROW_ID_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reads the row lineage columns of {@link TestTableFormat} straight from the parquet base files of a table, and
 * derives the row lineage of a record that stores none the way the table format does: from the values the table
 * format assigned to the base file and the position of the record in the file.
 *
 * <p>A row lineage is a pair of the row ID and the last updated sequence number.
 */
public class RowLineageTestUtils {

  public static final Pair<Long, Long> NO_ROW_LINEAGE = Pair.of(null, null);

  public static MessageType readParquetSchema(StorageConfiguration<?> storageConf, HoodieBaseFile baseFile) throws IOException {
    Configuration conf = storageConf.unwrapAs(Configuration.class);
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(baseFile.getPath()), conf))) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
  }

  public static boolean hasRowLineageColumns(StorageConfiguration<?> storageConf, HoodieBaseFile baseFile) throws IOException {
    MessageType schema = readParquetSchema(storageConf, baseFile);
    return schema.containsField(ROW_ID_COLUMN) || schema.containsField(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN);
  }

  /**
   * Returns the row lineage the base file stores for each of its records by record key, in the order of the
   * file. A value the file does not store is null.
   */
  public static Map<String, Pair<Long, Long>> readStoredRowLineage(StorageConfiguration<?> storageConf, HoodieBaseFile baseFile) throws IOException {
    boolean hasRowLineageColumns = hasRowLineageColumns(storageConf, baseFile);
    if (hasRowLineageColumns) {
      // The row lineage columns follow the columns of the table
      List<String> columns = readParquetSchema(storageConf, baseFile).getFields().stream().map(Type::getName).collect(Collectors.toList());
      assertEquals(Arrays.asList(ROW_ID_COLUMN, LAST_UPDATED_SEQUENCE_NUMBER_COLUMN), columns.subList(columns.size() - 2, columns.size()));
    }
    Map<String, Pair<Long, Long>> rowLineage = new LinkedHashMap<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(baseFile.getPath()))
        .withConf(storageConf.unwrapAs(Configuration.class)).build()) {
      Group row;
      while ((row = reader.read()) != null) {
        String recordKey = row.getString(HoodieRecord.RECORD_KEY_METADATA_FIELD, 0);
        Pair<Long, Long> previous = rowLineage.put(recordKey, Pair.of(
            hasRowLineageColumns ? readLong(row, ROW_ID_COLUMN) : null,
            hasRowLineageColumns ? readLong(row, LAST_UPDATED_SEQUENCE_NUMBER_COLUMN) : null));
        assertNull(previous, "duplicate record key " + recordKey);
      }
    }
    return rowLineage;
  }

  private static Long readLong(Group row, String column) {
    return row.getFieldRepetitionCount(column) == 0 ? null : (Long) row.getLong(column, 0);
  }

  /**
   * Returns the row lineage of each record in the base file by record key, the way the table format derives it:
   * the stored value, or for a value the file does not store, the first row ID of the base file plus the
   * position of the record in the file, and the sequence number of the base file.
   */
  public static Map<String, Pair<Long, Long>> deriveRowLineage(StorageConfiguration<?> storageConf, String basePath, HoodieBaseFile baseFile) throws IOException {
    BaseFileRowLineage baseFileRowLineage = TestTableFormat.getBaseFileRowLineage(basePath, baseFile.getFileName()).get();
    Map<String, Pair<Long, Long>> rowLineage = new LinkedHashMap<>();
    long position = 0;
    for (Map.Entry<String, Pair<Long, Long>> stored : readStoredRowLineage(storageConf, baseFile).entrySet()) {
      Long rowId = stored.getValue().getLeft() != null ? stored.getValue().getLeft() : (Long) (baseFileRowLineage.getFirstRowId() + position);
      Long sequenceNumber = stored.getValue().getRight() != null ? stored.getValue().getRight() : (Long) baseFileRowLineage.getSequenceNumber();
      rowLineage.put(stored.getKey(), Pair.of(rowId, sequenceNumber));
      position++;
    }
    return rowLineage;
  }

  /**
   * Asserts, for every record that is in the base file before a rewrite and in the base file after it, that the
   * row lineage of the record is unchanged exactly when its {@code _hoodie_commit_seqno} is unchanged: a rewrite
   * keeps the meta fields of a record it carries over unchanged, and generates them for a record it writes anew.
   * Requires a table that populates the meta fields.
   *
   * @param expectedUnchanged the expected number of records with an unchanged commit sequence number
   * @param expectedChanged   the expected number of records with a changed commit sequence number
   */
  public static void assertRowLineageChangesWithCommitSeqNo(StorageConfiguration<?> storageConf, String basePath, HoodieBaseFile before,
                                                            HoodieBaseFile after, int expectedUnchanged, int expectedChanged) throws IOException {
    Map<String, String> commitSeqNosBefore = readCommitSeqNos(storageConf, before);
    Map<String, String> commitSeqNosAfter = readCommitSeqNos(storageConf, after);
    Map<String, Pair<Long, Long>> rowLineageBefore = deriveRowLineage(storageConf, basePath, before);
    Map<String, Pair<Long, Long>> rowLineageAfter = deriveRowLineage(storageConf, basePath, after);
    int unchanged = 0;
    int changed = 0;
    for (String key : commitSeqNosBefore.keySet()) {
      if (!commitSeqNosAfter.containsKey(key)) {
        continue;
      }
      boolean commitSeqNoUnchanged = commitSeqNosBefore.get(key).equals(commitSeqNosAfter.get(key));
      assertEquals(commitSeqNoUnchanged, rowLineageBefore.get(key).equals(rowLineageAfter.get(key)),
          () -> "Record " + key + " has the commit sequence numbers " + commitSeqNosBefore.get(key) + " and " + commitSeqNosAfter.get(key)
              + ", and the row lineage " + rowLineageBefore.get(key) + " and " + rowLineageAfter.get(key));
      if (commitSeqNoUnchanged) {
        unchanged++;
      } else {
        changed++;
      }
    }
    assertEquals(expectedUnchanged, unchanged);
    assertEquals(expectedChanged, changed);
  }

  private static Map<String, String> readCommitSeqNos(StorageConfiguration<?> storageConf, HoodieBaseFile baseFile) throws IOException {
    Map<String, String> commitSeqNos = new LinkedHashMap<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(baseFile.getPath()))
        .withConf(storageConf.unwrapAs(Configuration.class)).build()) {
      Group row;
      while ((row = reader.read()) != null) {
        commitSeqNos.put(row.getString(HoodieRecord.RECORD_KEY_METADATA_FIELD, 0), row.getString(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, 0));
      }
    }
    return commitSeqNos;
  }

  /**
   * Asserts that the new and updated records of a rewritten base file have a row lineage of that base file with
   * row IDs no record had before the rewrite, and that every record of the file has a row ID of its own.
   *
   * @param rowLineage         the derived row lineage of the records in the rewritten base file
   * @param changedKeys        the keys of the records the rewrite inserted or updated
   * @param previousRowLineage the derived row lineage of the records before the rewrite
   */
  public static void assertNewRowLineage(String basePath, HoodieBaseFile baseFile, Map<String, Pair<Long, Long>> rowLineage,
                                         Set<String> changedKeys, Collection<Pair<Long, Long>> previousRowLineage) {
    BaseFileRowLineage baseFileRowLineage = TestTableFormat.getBaseFileRowLineage(basePath, baseFile.getFileName()).get();
    Set<Long> previousRowIds = previousRowLineage.stream().map(Pair::getLeft).collect(Collectors.toSet());
    assertFalse(changedKeys.isEmpty());
    for (String key : changedKeys) {
      Pair<Long, Long> changed = rowLineage.get(key);
      assertEquals(baseFileRowLineage.getSequenceNumber(), changed.getRight());
      assertTrue(changed.getLeft() >= baseFileRowLineage.getFirstRowId()
          && changed.getLeft() < baseFileRowLineage.getFirstRowId() + rowLineage.size(), () -> "Unexpected row ID: " + changed);
      assertFalse(previousRowIds.contains(changed.getLeft()), () -> "Row ID is not new: " + changed);
    }
    assertEquals(rowLineage.size(), rowLineage.values().stream().map(Pair::getLeft).distinct().count());
  }
}
