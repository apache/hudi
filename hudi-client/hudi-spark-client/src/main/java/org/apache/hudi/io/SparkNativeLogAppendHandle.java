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

package org.apache.hudi.io;

import org.apache.hudi.SparkFileFormatInternalRecordContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.Iterator;
import java.util.Map;

/**
 * Native log append handle that uses Spark's internal-row context when the write config requires Spark records.
 */
public class SparkNativeLogAppendHandle<T, I, K, O> extends HoodieNativeLogAppendHandle<T, I, K, O> {

  public SparkNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                    String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                                    TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier);
  }

  public SparkNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                    String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                                    TaskContextSupplier taskContextSupplier, Map<HeaderMetadataType, String> header) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, header);
  }

  public SparkNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                    String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
  }

  @Override
  protected RecordContext<?> getRecordContextForNativeLog() {
    if (config.getRecordMerger().getRecordType() == HoodieRecord.HoodieRecordType.SPARK) {
      // HoodieSparkEngineContext is not serializable. On executors the table carries a HoodieLocalEngineContext,
      // which always returns an Avro context. Use the serializable Spark field-accessor context only for SPARK
      // writes so delete records have the InternalRow type expected by the configured native file writer.
      return SparkFileFormatInternalRecordContext.getFieldAccessorInstance();
    }
    // AVRO writes must keep the default context; forcing InternalRow would mismatch the configured writer.
    return super.getRecordContextForNativeLog();
  }
}
