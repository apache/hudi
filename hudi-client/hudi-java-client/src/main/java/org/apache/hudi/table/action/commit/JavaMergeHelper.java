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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class JavaMergeHelper<T> extends BaseMergeHelper<T, List<HoodieRecord<T>>,
    List<HoodieKey>, List<WriteStatus>> {

  private JavaMergeHelper() {
  }

  private static class MergeHelperHolder {
    private static final JavaMergeHelper JAVA_MERGE_HELPER = new JavaMergeHelper();
  }

  public static JavaMergeHelper newInstance() {
    return JavaMergeHelper.MergeHelperHolder.JAVA_MERGE_HELPER;
  }

  @Override
  public void runMerge(HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                       HoodieMergeHandle<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> upsertHandle) throws IOException {
    final boolean externalSchemaTransformation = table.getConfig().shouldUseExternalSchemaTransformation();
    Configuration cfgForHoodieFile = new Configuration(table.getHadoopConf());
    HoodieMergeHandle<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> mergeHandle = upsertHandle;
    HoodieBaseFile baseFile = mergeHandle.baseFileForMerge();

    // Support schema evolution
    Schema readSchema;
    // These two schema used to replace gWriter and gReader.
    // In previous logic, avro record is serialized by gWriter and then is deserialized by gReader.
    // Now we converge this logic in record#rewrite.
    Schema readerSchema;
    Schema writerSchema;
    HoodieFileReader reader = HoodieFileReaderFactory.getReaderFactory(table.getConfig().getRecordMerger().getRecordType()).getFileReader(cfgForHoodieFile, mergeHandle.getOldFilePath());
    if (externalSchemaTransformation || baseFile.getBootstrapBaseFile().isPresent()) {
      readSchema = reader.getSchema();
      writerSchema = readSchema;
      readerSchema = mergeHandle.getWriterSchemaWithMetaFields();
    } else {
      readerSchema = null;
      writerSchema = null;
      readSchema = mergeHandle.getWriterSchemaWithMetaFields();
    }

    BoundedInMemoryExecutor<HoodieRecord, HoodieRecord, Void> wrapper = null;
    try {
      final Iterator<HoodieRecord> readerIterator;
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        readerIterator = getMergingIterator(table, mergeHandle, baseFile, reader, readSchema, externalSchemaTransformation);
      } else {
        readerIterator = reader.getRecordIterator(readSchema);
      }

      wrapper = new BoundedInMemoryExecutor<>(table.getConfig().getWriteBufferLimitBytes(), new IteratorBasedQueueProducer<>(readerIterator),
          Option.of(new UpdateHandler(mergeHandle)), record -> {
        HoodieRecord recordCopy = record.copy();
        if (!externalSchemaTransformation) {
          return recordCopy;
        }
        try {
          return recordCopy.rewriteRecord(writerSchema, new Properties(), readerSchema);
        } catch (IOException e) {
          throw new HoodieException(e);
        }
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
