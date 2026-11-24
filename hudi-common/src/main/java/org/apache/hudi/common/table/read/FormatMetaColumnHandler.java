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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.BaseFileColumnResolver;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Maintains the format meta columns, see {@link FormatMetaColumn}, of the records a rewrite reads from a file
 * group and writes to a new base file.
 *
 * <p>A record carried over from the base file the way the file stores it gets the value the table format resolves
 * from the position of the record in the file and the value the file stores for it. A changed version of the last
 * record read from the base file gets the value on update of the column for the value resolved for that record, see
 * {@link FormatMetaColumn#valueOnUpdate}, and any other record the rewrite inserts or changes gets the value on write
 * of the column.
 */
public final class FormatMetaColumnHandler<T> {
  private final RecordContext<T> recordContext;
  private final List<FormatMetaColumn> columns;
  private final String instantTime;
  private final List<String> columnNames;
  private final Object[] valuesOnWrite;
  // Resolvers of the values for the records of the base file, empty without a base file
  private final List<BaseFileColumnResolver> baseFileResolvers;
  // Position in the base file of the next record read from it
  private long baseFilePosition = 0;
  // The last record read from the base file with its schema, its key once needed, and the values resolved for it
  private T lastBaseFileRecord;
  private HoodieSchema lastBaseFileRecordSchema;
  private String lastBaseFileRecordKey;
  private Object[] lastBaseFileValues;
  // Positions of the columns in the schema of the records of the base file, resolved on the first record
  private int[] baseFileRecordPositions;
  // Positions of the columns in the schema of the last changed record, -1 for a column the schema does not have
  private Integer changedRecordSchemaId;
  private int[] changedRecordPositions;
  // Positions of the columns in the schema of the last record given values on write
  private HoodieSchema writtenRecordSchema;
  private int[] writtenRecordPositions;

  FormatMetaColumnHandler(RecordContext<T> recordContext, List<FormatMetaColumn> columns,
                          List<BaseFileColumnResolver> baseFileResolvers, String instantTime) {
    this.recordContext = recordContext;
    this.columns = columns;
    this.instantTime = instantTime;
    this.columnNames = columns.stream().map(column -> column.field().name()).collect(Collectors.toList());
    this.valuesOnWrite = columns.stream().map(column -> column.valueOnWrite(instantTime)).toArray();
    this.baseFileResolvers = baseFileResolvers;
  }

  /**
   * Returns a handler of the given format meta columns for a rewrite of a file group with the given base file, or
   * an empty option without format meta columns. Fails if the table format cannot resolve the values of the
   * columns for the base file.
   *
   * @param recordContext record context of the records
   * @param columns       the format meta columns, or an empty list
   * @param metaClient    meta client of the table
   * @param baseFile      the base file of the file group, if any
   * @param instantTime   instant time of the rewrite, for the values on write
   */
  public static <T> Option<FormatMetaColumnHandler<T>> create(RecordContext<T> recordContext, List<FormatMetaColumn> columns,
                                                              HoodieTableMetaClient metaClient, Option<HoodieBaseFile> baseFile, String instantTime) {
    if (columns.isEmpty()) {
      return Option.empty();
    }
    List<BaseFileColumnResolver> baseFileResolvers = baseFile.isPresent()
        ? columns.stream().map(column -> column.resolverFor(metaClient, baseFile.get())).collect(Collectors.toList())
        : Collections.emptyList();
    return Option.of(new FormatMetaColumnHandler<>(recordContext, columns, baseFileResolvers, instantTime));
  }

  Set<String> getColumnNames() {
    return new LinkedHashSet<>(columnNames);
  }

  /**
   * Resolves the format meta columns of the records of the base file.
   *
   * @param baseFileRecords iterator over all the records of the base file, in the order of the file
   * @param schema          schema of the records, which has the format meta columns
   * @return an iterator over the same records with the format meta columns resolved
   */
  ClosableIterator<T> resolveBaseFileRecords(ClosableIterator<T> baseFileRecords, HoodieSchema schema) {
    return new ClosableIterator<T>() {
      @Override
      public boolean hasNext() {
        return baseFileRecords.hasNext();
      }

      @Override
      public T next() {
        return resolveNextBaseFileRecord(baseFileRecords.next(), schema);
      }

      @Override
      public void close() {
        baseFileRecords.close();
      }
    };
  }

  /**
   * Resolves the format meta columns of the next record of the base file. Every record of the base file has to be
   * passed, in the order of the file.
   *
   * @param record the next record of the base file
   * @param schema schema of the record, which has the format meta columns
   * @return the record with the format meta columns resolved, which is the given record when it can be updated in place
   */
  public T resolveNextBaseFileRecord(T record, HoodieSchema schema) {
    if (baseFileRecordPositions == null) {
      ValidationUtils.checkArgument(!baseFileResolvers.isEmpty(), "The format meta columns of a base file cannot be resolved without the base file");
      baseFileRecordPositions = getPositions(schema);
      ValidationUtils.checkArgument(Arrays.stream(baseFileRecordPositions).allMatch(pos -> pos >= 0),
          () -> "The format meta columns " + columnNames + " have to be read from the base file, got: " + schema);
      lastBaseFileValues = new Object[columnNames.size()];
    }
    long position = baseFilePosition++;
    T result = record;
    for (int i = 0; i < columnNames.size(); i++) {
      Object storedValue = recordContext.getValue(result, schema, columnNames.get(i));
      Object value = baseFileResolvers.get(i).valueAt(position, storedValue);
      if (value != storedValue) {
        result = recordContext.setValue(result, baseFileRecordPositions[i], value);
      }
      lastBaseFileValues[i] = value;
    }
    lastBaseFileRecord = result;
    lastBaseFileRecordSchema = schema;
    lastBaseFileRecordKey = null;
    return result;
  }

  /**
   * Returns the values of the format meta columns for a record written as new or changed: the values on update for
   * the values of the last record read from the base file if the record has its key, so is a changed version of it,
   * and the values on write otherwise. A file group reader merges a record of the base file and returns the result
   * before it reads the next one, so a record it returns as changed has the key of the last record read from the
   * base file exactly when it replaces that record.
   */
  private Object[] valuesFor(String recordKey) {
    if (lastBaseFileRecord == null) {
      return valuesOnWrite;
    }
    if (lastBaseFileRecordKey == null) {
      lastBaseFileRecordKey = recordContext.getRecordKey(lastBaseFileRecord, lastBaseFileRecordSchema);
    }
    if (!lastBaseFileRecordKey.equals(recordKey)) {
      return valuesOnWrite;
    }
    Object[] values = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      values[i] = columns.get(i).valueOnUpdate(instantTime, lastBaseFileValues[i]);
    }
    return values;
  }

  /**
   * Sets the format meta columns of the record to their values on update or on write, see {@link #valuesFor}, if
   * the record is not returned the way it is stored in the base file.
   */
  void setValuesOnWriteIfChanged(BufferedRecord<T> record) {
    if (record.getHoodieOperation() == null || record.isDelete() || record.isEmpty() || record.getSchemaId() == null) {
      return;
    }
    if (!record.getSchemaId().equals(changedRecordSchemaId)) {
      changedRecordPositions = getPositions(recordContext.getSchemaFromBufferRecord(record));
      changedRecordSchemaId = record.getSchemaId();
    }
    Object[] values = valuesFor(record.getRecordKey());
    T result = record.getRecord();
    for (int i = 0; i < columnNames.size(); i++) {
      if (changedRecordPositions[i] >= 0) {
        result = recordContext.setValue(result, changedRecordPositions[i], values[i]);
      }
    }
    record.replaceRecord(result);
  }

  /**
   * Sets the format meta columns of a record written as new or changed, which are null, to their non-null values on
   * update or on write, see {@link #valuesFor}. A handle that writes the records a file group reader returns calls
   * it on the handler of the reader for each record before it reads the next one.
   *
   * @param recordKey key of the record
   * @param record    the record
   * @param schema    schema of the record, which has the format meta columns
   * @return the record with the values set, which is the given record when it can be updated in place
   */
  public T setNonNullValuesOnWrite(String recordKey, T record, HoodieSchema schema) {
    if (schema != writtenRecordSchema) {
      writtenRecordPositions = getPositions(schema);
      writtenRecordSchema = schema;
    }
    Object[] values = valuesFor(recordKey);
    T result = record;
    for (int i = 0; i < columnNames.size(); i++) {
      if (values[i] != null) {
        ValidationUtils.checkArgument(writtenRecordPositions[i] >= 0, () -> "The record has no format meta columns " + columnNames + ": " + schema);
        result = recordContext.setValue(result, writtenRecordPositions[i], values[i]);
      }
    }
    return result;
  }

  private int[] getPositions(HoodieSchema schema) {
    int[] positions = new int[columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      Option<HoodieSchemaField> field = schema.getField(columnNames.get(i));
      positions[i] = field.isPresent() ? field.get().pos() : -1;
    }
    return positions;
  }
}
