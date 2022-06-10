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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import scala.collection.immutable.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Iterator;

public class FlinkMergeHelper<T> extends BaseMergeHelper<T, List<HoodieRecord<T>>,
    List<HoodieKey>, List<WriteStatus>> {

  private FlinkMergeHelper() {
  }

  private static class MergeHelperHolder {
    private static final FlinkMergeHelper FLINK_MERGE_HELPER = new FlinkMergeHelper();
  }

  public static FlinkMergeHelper newInstance() {
    return FlinkMergeHelper.MergeHelperHolder.FLINK_MERGE_HELPER;
  }

  @Override
  public void runMerge(HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                       HoodieMergeHandle<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> mergeHandle) throws IOException {
    final GenericDatumWriter<GenericRecord> gWriter;
    final GenericDatumReader<GenericRecord> gReader;
    Schema readSchema;

    final boolean externalSchemaTransformation = table.getConfig().shouldUseExternalSchemaTransformation();
    HoodieBaseFile baseFile = mergeHandle.baseFileForMerge();
    if (externalSchemaTransformation || baseFile.getBootstrapBaseFile().isPresent()) {
      readSchema = HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), mergeHandle.getOldFilePath()).getSchema();
      gWriter = new GenericDatumWriter<>(readSchema);
      gReader = new GenericDatumReader<>(readSchema, mergeHandle.getWriterSchemaWithMetaFields());
    } else {
      gReader = null;
      gWriter = null;
      readSchema = mergeHandle.getWriterSchemaWithMetaFields();
    }

    BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
    Configuration cfgForHoodieFile = new Configuration(table.getHadoopConf());
    HoodieAvroFileReader reader = HoodieFileReaderFactory.getFileReader(cfgForHoodieFile, mergeHandle.getOldFilePath());
    try {
      final Iterator<HoodieRecord> readerIterator;
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        readerIterator = getMergingIterator(table, mergeHandle, baseFile, reader, readSchema, externalSchemaTransformation);
      } else {
        readerIterator = reader.getRecordIterator(readSchema, HoodieAvroIndexedRecord::new);
      }

      ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<>();
      ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();
      wrapper = new BoundedInMemoryExecutor(table.getConfig().getWriteBufferLimitBytes(), new IteratorBasedQueueProducer<>(readerIterator),
          Option.of(new UpdateHandler(mergeHandle)), record -> {
        if (!externalSchemaTransformation) {
          return record;
        }
        // TODO Other type of record need to change
        return transformRecordBasedOnNewSchema(gReader, gWriter, encoderCache, decoderCache, (GenericRecord) ((HoodieRecord)record).getData());
      });
      wrapper.execute();
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      // HUDI-2875: mergeHandle is not thread safe, we should totally terminate record inputting
      // and executor firstly and then close mergeHandle.
      if (reader != null) {
        reader.close();
      }
      if (null != wrapper) {
        wrapper.shutdownNow();
        wrapper.awaitTermination();
      }
      mergeHandle.close();
    }
  }
}
