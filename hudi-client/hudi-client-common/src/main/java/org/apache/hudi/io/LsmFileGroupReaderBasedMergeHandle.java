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

package org.apache.hudi.io;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.read.HoodieRecordReader;
import org.apache.hudi.common.table.read.lsm.HoodieLsmFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A merge handle that uses the LSM file-group reader to merge sorted runs.
 *
 * <p>The incoming records, base file records, and native parquet log records are expected to be
 * sorted by record key. The LSM reader performs a k-way merge and emits sorted output.
 */
public class LsmFileGroupReaderBasedMergeHandle<T, I, K, O> extends FileGroupReaderBasedMergeHandle<T, I, K, O> {

  public LsmFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                            Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                            TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
  }

  public LsmFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                            Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                            TaskContextSupplier taskContextSupplier, HoodieBaseFile baseFile, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, baseFile, keyGeneratorOpt);
  }

  public LsmFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                            Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                                            HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier,
                                            Option<BaseKeyGenerator> keyGeneratorOpt) {
    this(config, instantTime, hoodieTable, keyToNewRecords.values().stream()
        .sorted(Comparator.comparing(HoodieRecord::getRecordKey)).iterator(), partitionPath, fileId,
        taskContextSupplier, dataFileToBeMerged, keyGeneratorOpt);
  }

  public LsmFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                            CompactionOperation compactionOperation, TaskContextSupplier taskContextSupplier,
                                            HoodieReaderContext<T> readerContext, String maxInstantTime,
                                            HoodieRecord.HoodieRecordType enginRecordType) {
    super(config, instantTime, hoodieTable, compactionOperation, taskContextSupplier, readerContext, maxInstantTime, enginRecordType);
  }

  @Override
  protected HoodieRecordReader<T> getFileGroupReader(boolean usePosition, Option<InternalSchema> internalSchemaOption, TypedProperties props,
                                                     Option<Stream<HoodieLogFile>> logFileStreamOpt, Iterator<HoodieRecord<T>> incomingRecordsItr) {
    HoodieLsmFileGroupReader.HoodieLsmFileGroupReaderBuilder<T> fileGroupBuilder = HoodieLsmFileGroupReader.<T>builder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(maxInstantTime)
        .withPartitionPath(partitionPath)
        .withBaseFileOption(Option.ofNullable(baseFileToMerge))
        .withDataSchema(writeSchemaWithMetaFields)
        .withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchemaOpt(internalSchemaOption)
        .withProps(props)
        .withFileGroupUpdateCallback(createCallback());

    if (logFileStreamOpt.isPresent()) {
      fileGroupBuilder.withLogFiles(logFileStreamOpt.get());
    } else {
      fileGroupBuilder.withRecordIterator(incomingRecordsItr);
    }
    return fileGroupBuilder.build();
  }
}
