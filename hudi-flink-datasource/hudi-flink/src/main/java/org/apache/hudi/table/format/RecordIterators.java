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

package org.apache.hudi.table.format;

import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.table.format.cow.ParquetSplitReaderUtil;
import org.apache.hudi.util.RowDataProjection;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * Factory clazz for record iterators.
 */
public abstract class RecordIterators {

  public static ClosableIterator<RowData> getParquetRecordIterator(
      InternalSchemaManager internalSchemaManager,
      boolean utcTimestamp,
      boolean caseSensitive,
      Configuration conf,
      String[] fieldNames,
      DataType[] fieldTypes,
      Map<String, Object> partitionSpec,
      int[] selectedFields,
      int batchSize,
      Path path,
      long splitStart,
      long splitLength) throws IOException {
    InternalSchema mergeSchema = internalSchemaManager.getMergeSchema(path.getName());
    if (mergeSchema.isEmptySchema()) {
      return new ParquetSplitRecordIterator(
          ParquetSplitReaderUtil.genPartColumnarRowReader(
              utcTimestamp,
              caseSensitive,
              conf,
              fieldNames,
              fieldTypes,
              partitionSpec,
              selectedFields,
              batchSize,
              path,
              splitStart,
              splitLength));
    } else {
      CastMap castMap = internalSchemaManager.getCastMap(mergeSchema, fieldNames, fieldTypes, selectedFields);
      Option<RowDataProjection> castProjection = castMap.toRowDataProjection(selectedFields);
      ClosableIterator<RowData> itr = new ParquetSplitRecordIterator(
          ParquetSplitReaderUtil.genPartColumnarRowReader(
              utcTimestamp,
              caseSensitive,
              conf,
              internalSchemaManager.getMergeFieldNames(mergeSchema, fieldNames), // the reconciled field names
              castMap.getFileFieldTypes(),                                     // the reconciled field types
              partitionSpec,
              selectedFields,
              batchSize,
              path,
              splitStart,
              splitLength));
      if (castProjection.isPresent()) {
        return new SchemaEvolvedRecordIterator(itr, castProjection.get());
      } else {
        return itr;
      }
    }
  }
}
