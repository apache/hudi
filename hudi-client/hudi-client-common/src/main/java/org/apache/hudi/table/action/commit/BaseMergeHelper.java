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

package org.apache.hudi.table.action.commit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.utils.MergingIterator;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Helper to read records from previous version of base file and run Merge.
 */
public abstract class BaseMergeHelper<T extends HoodieRecordPayload, I, K, O> {

  /**
   * Read records from previous version of base file and merge.
   * @param table Hoodie Table
   * @param upsertHandle Merge Handle
   * @throws IOException in case of error
   */
  public abstract void runMerge(HoodieTable<T, I, K, O> table, HoodieMergeHandle<T, I, K, O> upsertHandle) throws IOException;

  protected GenericRecord transformRecordBasedOnNewSchema(GenericRecord record, Schema writerSchema) {
    return HoodieAvroUtils.rewriteRecord(record, writerSchema);
  }

  /**
   * Create Parquet record iterator that provides a stitched view of record read from skeleton and bootstrap file.
   * Skeleton file is a representation of the bootstrap file inside the table, with just the bare bone fields needed
   * for indexing, writing and other functionality.
   *
   */
  protected Iterator<GenericRecord> getMergingIterator(HoodieTable<T, I, K, O> table, HoodieMergeHandle<T, I, K, O> mergeHandle,
                                                                                               HoodieBaseFile baseFile, HoodieAvroFileReader reader,
                                                                                               Schema readSchema, boolean externalSchemaTransformation) throws IOException {
    Path externalFilePath = new Path(baseFile.getBootstrapBaseFile().get().getPath());
    Configuration bootstrapFileConfig = new Configuration(table.getHadoopConf());
    HoodieAvroFileReader bootstrapReader = HoodieFileReaderFactory.<GenericRecord>getFileReader(bootstrapFileConfig, externalFilePath);
    Schema bootstrapReadSchema;
    if (externalSchemaTransformation) {
      bootstrapReadSchema = bootstrapReader.getSchema();
    } else {
      bootstrapReadSchema = mergeHandle.getWriterSchema();
    }

    return new MergingIterator<>(reader.getRecordIterator(readSchema), bootstrapReader.getRecordIterator(bootstrapReadSchema),
        (inputRecordPair) -> HoodieAvroUtils.stitchRecords((GenericRecord) inputRecordPair.getLeft(), (GenericRecord) inputRecordPair.getRight(), mergeHandle.getWriterSchemaWithMetaFields()));
  }

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  protected static class UpdateHandler extends BoundedInMemoryQueueConsumer<GenericRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    protected UpdateHandler(HoodieMergeHandle upsertHandle) {
      this.upsertHandle = upsertHandle;
    }

    @Override
    protected void consumeOneRecord(GenericRecord record) {
      upsertHandle.write(record);
    }

    @Override
    protected void finish() {}

    @Override
    protected Void getResult() {
      return null;
    }
  }
}
