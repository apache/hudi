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

import org.apache.avro.SchemaCompatibility;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class HoodieMergeHelper<T> extends
    BaseMergeHelper<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {

  private HoodieMergeHelper() {
  }

  private static class MergeHelperHolder {
    private static final HoodieMergeHelper HOODIE_MERGE_HELPER = new HoodieMergeHelper<>();
  }

  public static HoodieMergeHelper newInstance() {
    return MergeHelperHolder.HOODIE_MERGE_HELPER;
  }

  @Override
  public void runMerge(HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
                       HoodieMergeHandle<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> mergeHandle) throws IOException {
    final boolean externalSchemaTransformation = table.getConfig().shouldUseExternalSchemaTransformation();
    Configuration cfgForHoodieFile = new Configuration(table.getHadoopConf());
    HoodieBaseFile baseFile = mergeHandle.baseFileForMerge();

    final GenericDatumWriter<GenericRecord> gWriter;
    final GenericDatumReader<GenericRecord> gReader;
    Schema readSchema;
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
    HoodieFileReader reader = HoodieFileReaderFactory.getFileReader(cfgForHoodieFile, mergeHandle.getOldFilePath());

    Option<InternalSchema> querySchemaOpt = SerDeHelper.fromJson(table.getConfig().getInternalSchema());
    boolean needToReWriteRecord = false;
    Map<String, String> renameCols = new HashMap<>();
    // TODO support bootstrap
    if (querySchemaOpt.isPresent() && !baseFile.getBootstrapBaseFile().isPresent()) {
      // check implicitly add columns, and position reorder(spark sql may change cols order)
      InternalSchema querySchema = AvroSchemaEvolutionUtils.evolveSchemaFromNewAvroSchema(readSchema, querySchemaOpt.get(), true);
      long commitInstantTime = Long.valueOf(FSUtils.getCommitTime(mergeHandle.getOldFilePath().getName()));
      InternalSchema writeInternalSchema = InternalSchemaCache.searchSchemaAndCache(commitInstantTime, table.getMetaClient(), table.getConfig().getInternalSchemaCacheEnable());
      if (writeInternalSchema.isEmptySchema()) {
        throw new HoodieException(String.format("cannot find file schema for current commit %s", commitInstantTime));
      }
      List<String> colNamesFromQuerySchema = querySchema.getAllColsFullName();
      List<String> colNamesFromWriteSchema = writeInternalSchema.getAllColsFullName();
      List<String> sameCols = colNamesFromWriteSchema.stream()
              .filter(f -> colNamesFromQuerySchema.contains(f)
                      && writeInternalSchema.findIdByName(f) == querySchema.findIdByName(f)
                      && writeInternalSchema.findIdByName(f) != -1
                      && writeInternalSchema.findType(writeInternalSchema.findIdByName(f)).equals(querySchema.findType(writeInternalSchema.findIdByName(f)))).collect(Collectors.toList());
      readSchema = AvroInternalSchemaConverter
          .convert(new InternalSchemaMerger(writeInternalSchema, querySchema, true, false, false).mergeSchema(), readSchema.getName());
      Schema writeSchemaFromFile = AvroInternalSchemaConverter.convert(writeInternalSchema, readSchema.getName());
      needToReWriteRecord = sameCols.size() != colNamesFromWriteSchema.size()
              || SchemaCompatibility.checkReaderWriterCompatibility(readSchema, writeSchemaFromFile).getType() == org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
      if (needToReWriteRecord) {
        renameCols = InternalSchemaUtils.collectRenameCols(writeInternalSchema, querySchema);
      }
    }

    try {
      final Iterator<HoodieRecord> readerIterator;
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        readerIterator = getMergingIterator(table, mergeHandle, baseFile, reader, readSchema, externalSchemaTransformation);
      } else {
        if (needToReWriteRecord) {
          readerIterator = new RewriteIterator(reader.getRecordIterator(), readSchema, readSchema, table.getConfig().getProps(), renameCols);
        } else {
          readerIterator = reader.getRecordIterator(readSchema);
        }
      }

      ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<>();
      ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();
      wrapper = new BoundedInMemoryExecutor(table.getConfig().getWriteBufferLimitBytes(), readerIterator,
          new UpdateHandler(mergeHandle), record -> {
        if (!externalSchemaTransformation) {
          return record;
        }
        // TODO Other type of record need to change
        return transformRecordBasedOnNewSchema(gReader, gWriter, encoderCache, decoderCache, (GenericRecord) ((HoodieRecord)record).getData());
      }, table.getPreExecuteRunnable());
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

  class RewriteIterator implements ClosableIterator<HoodieRecord> {

    private final ClosableIterator<HoodieRecord> iter;
    private final Schema newSchema;
    private final Schema recordSchema;
    private final Properties prop;
    private final Map<String, String> renameCols;

    public RewriteIterator(ClosableIterator<HoodieRecord> iter, Schema newSchema, Schema recordSchema, Properties prop, Map<String, String> renameCols) {
      this.iter = iter;
      this.newSchema = newSchema;
      this.recordSchema = recordSchema;
      this.prop = prop;
      this.renameCols = renameCols;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public HoodieRecord next() {
      try {
        return iter.next().rewriteRecordWithNewSchema(recordSchema, prop, newSchema, renameCols);
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public void close() {
      iter.close();
    }
  }
}
