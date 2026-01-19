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

package org.apache.hudi.source.reader.function;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.reader.DefaultHoodieBatchReader;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.util.List;

/**
 * Reader function implementation for Copy On Write table.
 */
public class CopyOnWriteSplitReaderFunction implements SplitReaderFunction<RowData> {
  private final StorageConfiguration<?> conf;
  private final Configuration flinkConf;
  private final InternalSchemaManager internalSchemaManager;
  private final DataType dataType;
  private final HoodieSchema requestedSchema;
  private final List<ExpressionPredicates.Predicate> predicates;
  private ClosableIterator<RowData> currentReader;

  public CopyOnWriteSplitReaderFunction(
      StorageConfiguration<?> conf,
      Configuration flinkConf,
      InternalSchemaManager internalSchemaManager,
      DataType dataType,
      HoodieSchema requestedSchema,
      List<ExpressionPredicates.Predicate> predicates) {
    this.conf = conf;
    this.flinkConf = flinkConf;
    this.internalSchemaManager = internalSchemaManager;
    this.dataType = dataType;
    this.requestedSchema = requestedSchema;
    this.predicates = predicates;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> read(HoodieSourceSplit split) {
    ValidationUtils.checkArgument(split.getBasePath().get() != null, "Base path of COW Hoodie Source Split can't be null");
    try {
      StoragePath path = new StoragePath(split.getBasePath().get());
      currentReader = RecordIterators.getParquetRecordIterator(conf, internalSchemaManager, dataType, requestedSchema, path, predicates);
      DefaultHoodieBatchReader<RowData> defaultBatchReader = new DefaultHoodieBatchReader<RowData>(flinkConf);
      return defaultBatchReader.batch(split, currentReader);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from file group: " + split.getFileId(), e);
    }
  }

  @Override
  public void close() throws Exception {
    if (currentReader != null) {
      currentReader.close();
    }
  }
}
