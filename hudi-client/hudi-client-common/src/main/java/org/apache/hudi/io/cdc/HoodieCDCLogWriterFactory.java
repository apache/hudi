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

package org.apache.hudi.io.cdc;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.MergeUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.CommonClientUtils;

import org.apache.avro.generic.IndexedRecord;

import java.util.function.Supplier;

/**
 * Creates CDC log writers for Avro merge handles.
 */
public final class HoodieCDCLogWriterFactory {

  private HoodieCDCLogWriterFactory() {
  }

  public static <T, I, K, O> HoodieCDCLogWriter<IndexedRecord> createAvroCDCLogWriter(
      String instantTime,
      HoodieWriteConfig config,
      HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath,
      HoodieStorage storage,
      HoodieSchema writerSchema,
      String fileId,
      String writeToken,
      LogFileCreationCallback logCreationCallback,
      TaskContextSupplier taskContextSupplier,
      Supplier<HoodieLogFormat.Writer> logWriterSupplier) {
    HoodieTableConfig tableConfig = hoodieTable.getMetaClient().getTableConfig();
    if (CommonClientUtils.shouldWriteNativeLogs(config)) {
      return new HoodieAvroNativeCDCLogger(
          instantTime,
          config,
          tableConfig,
          partitionPath,
          storage,
          writerSchema,
          FSUtils.constructAbsolutePath(hoodieTable.getMetaClient().getBasePath(), partitionPath),
          fileId,
          writeToken,
          logCreationCallback,
          taskContextSupplier);
    }
    return new HoodieCDCLogger(
        instantTime,
        config,
        tableConfig,
        partitionPath,
        storage,
        writerSchema,
        logWriterSupplier.get(),
        MergeUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config));
  }
}
