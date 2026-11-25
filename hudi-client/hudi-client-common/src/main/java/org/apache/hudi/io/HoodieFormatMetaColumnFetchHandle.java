/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.common.BaseFileColumnResolver;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieAvroFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Fetches the values of the format meta columns, see {@link FormatMetaColumn}, for records of a base file given by
 * their positions in the file, as the table format resolves them from the values the file stores. It reads only the
 * columns the file stores, and nothing but the footer when the file stores none of them.
 */
public class HoodieFormatMetaColumnFetchHandle<T, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  private final HoodieBaseFile baseFile;
  private final List<FormatMetaColumn> columns;

  public HoodieFormatMetaColumnFetchHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O> hoodieTable,
                                           Pair<String, HoodieBaseFile> partitionPathBaseFilePair, List<FormatMetaColumn> columns) {
    super(config, hoodieTable, Pair.of(partitionPathBaseFilePair.getLeft(), partitionPathBaseFilePair.getRight().getFileId()));
    this.baseFile = partitionPathBaseFilePair.getRight();
    this.columns = columns;
  }

  /**
   * Returns the values of the format meta columns for the records at the given positions of the base file. Fails if
   * the table format cannot resolve the values for the base file.
   *
   * @param positions distinct positions of records in the base file, in ascending order
   * @return the values for each position, in the order of the positions, each in the order of the columns
   */
  public List<Object[]> valuesAt(long[] positions) {
    List<BaseFileColumnResolver> resolvers = columns.stream()
        .map(column -> column.resolverFor(hoodieTable.getMetaClient(), baseFile)).collect(Collectors.toList());
    Object[][] storedValues = readStoredValues(positions);
    List<Object[]> values = new ArrayList<>(positions.length);
    for (int i = 0; i < positions.length; i++) {
      Object[] recordValues = new Object[columns.size()];
      for (int j = 0; j < columns.size(); j++) {
        recordValues[j] = resolvers.get(j).valueAt(positions[i], storedValues[i][j]);
      }
      values.add(recordValues);
    }
    return values;
  }

  /**
   * Reads the values the base file stores for the format meta columns of the records at the given positions, null
   * for a column the file does not store.
   */
  private Object[][] readStoredValues(long[] positions) {
    Object[][] storedValues = new Object[positions.length][columns.size()];
    try (HoodieAvroFileReader reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(hoodieTable.getStorage())
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(config, baseFile.getStoragePath())) {
      HoodieSchema fileSchema = reader.getSchema();
      List<String> storedColumns = columns.stream().map(column -> column.field().name())
          .filter(name -> fileSchema.getField(name).isPresent()).collect(Collectors.toList());
      if (storedColumns.isEmpty() || positions.length == 0) {
        return storedValues;
      }
      HoodieSchema requestedSchema = HoodieSchemaUtils.generateProjectionSchema(fileSchema, storedColumns);
      int[] fieldPositions = columns.stream()
          .mapToInt(column -> requestedSchema.getField(column.field().name()).map(field -> field.pos()).orElse(-1)).toArray();
      try (ClosableIterator<IndexedRecord> records = reader.getIndexedRecordIterator(requestedSchema, requestedSchema)) {
        long position = 0;
        int next = 0;
        while (next < positions.length && records.hasNext()) {
          IndexedRecord record = records.next();
          if (position == positions[next]) {
            for (int j = 0; j < columns.size(); j++) {
              storedValues[next][j] = fieldPositions[j] < 0 ? null : toJavaValue(record.get(fieldPositions[j]));
            }
            next++;
          }
          position++;
        }
      }
      return storedValues;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read the format meta columns of base file " + baseFile.getPath(), e);
    }
  }

  private static Object toJavaValue(Object value) {
    return value instanceof Utf8 ? value.toString() : value;
  }
}
