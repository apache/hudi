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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

// TODO unify w/ Flink, Java impls
public class HoodieMergeHelper<T extends HoodieRecordPayload> extends
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
    HoodieWriteConfig writeConfig = table.getConfig();
    HoodieBaseFile baseFile = mergeHandle.baseFileForMerge();

    Schema writerSchema = mergeHandle.getWriterSchemaWithMetaFields();
    Schema readSchema;

    final GenericDatumWriter<GenericRecord> gWriter;
    final GenericDatumReader<GenericRecord> gReader;

    Configuration hadoopConf = new Configuration(table.getHadoopConf());
    HoodieFileReader<GenericRecord> reader = HoodieFileReaderFactory.getFileReader(hadoopConf, mergeHandle.getOldFilePath());

    boolean shouldRewriteInWriterSchema =
        writeConfig.shouldUseExternalSchemaTransformation() || baseFile.getBootstrapBaseFile().isPresent();
    if (shouldRewriteInWriterSchema) {
      readSchema = reader.getSchema();
      gWriter = new GenericDatumWriter<>(readSchema);
      gReader = new GenericDatumReader<>(readSchema, writerSchema);
    } else {
      gReader = null;
      gWriter = null;
      readSchema = writerSchema;
    }


    Option<Pair<Schema, Map<String, String>>> r = tryEvolveSchema(readSchema, baseFile, writeConfig, table.getMetaClient());
    boolean needToReWriteRecord;
    Map<String, String> renameCols;

    if (r.isPresent()) {
      needToReWriteRecord = true;
      renameCols = r.get().getRight();
      readSchema = r.get().getKey();
    } else {
      needToReWriteRecord = false;
      renameCols = Collections.emptyMap();
    }

    BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
    try {
      final Iterator<GenericRecord> readerIterator;
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        readerIterator = getMergingIterator(table, mergeHandle, baseFile, reader, readSchema, shouldRewriteInWriterSchema);
      } else if (needToReWriteRecord) {
        readerIterator = HoodieAvroUtils.rewriteRecordWithNewSchema(reader.getRecordIterator(), readSchema, renameCols);
      } else {
        readerIterator = reader.getRecordIterator(readSchema);
      }

      ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<>();
      ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();
      wrapper = new BoundedInMemoryExecutor(writeConfig.getWriteBufferLimitBytes(), readerIterator,
          new UpdateHandler(mergeHandle), record -> {
        if (shouldRewriteInWriterSchema) {
          return transformRecordBasedOnNewSchema(gReader, gWriter, encoderCache, decoderCache, (GenericRecord) record);
        }
        return record;
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

  private Option<Pair<Schema, Map<String, String>>> tryEvolveSchema(Schema readerSchema,
                                                                    HoodieBaseFile baseFile,
                                                                    HoodieWriteConfig writeConfig,
                                                                    HoodieTableMetaClient metaClient) {
    Option<InternalSchema> querySchemaOpt = SerDeHelper.fromJson(writeConfig.getInternalSchema());
    // TODO support bootstrap
    if (querySchemaOpt.isPresent() && !baseFile.getBootstrapBaseFile().isPresent()) {
      // check implicitly add columns, and position reorder(spark sql may change cols order)
      InternalSchema querySchema = AvroSchemaEvolutionUtils.reconcileSchema(readerSchema, querySchemaOpt.get());
      long commitInstantTime = Long.parseLong(baseFile.getCommitTime());
      InternalSchema writeInternalSchema = InternalSchemaCache.searchSchemaAndCache(commitInstantTime, metaClient, writeConfig.getInternalSchemaCacheEnable());
      if (writeInternalSchema.isEmptySchema()) {
        throw new HoodieException(String.format("cannot find file schema for current commit %s", commitInstantTime));
      }
      List<String> colNamesFromQuerySchema = querySchema.getAllColsFullName();
      List<String> colNamesFromWriteSchema = writeInternalSchema.getAllColsFullName();
      List<String> sameCols = colNamesFromWriteSchema.stream()
          .filter(f -> {
            int writerSchemaFieldId = writeInternalSchema.findIdByName(f);
            int querySchemaFieldId = querySchema.findIdByName(f);

            return colNamesFromQuerySchema.contains(f)
                && writerSchemaFieldId == querySchemaFieldId
                && writerSchemaFieldId != -1
                && Objects.equals(writeInternalSchema.findType(writerSchemaFieldId), querySchema.findType(querySchemaFieldId));
          })
          .collect(Collectors.toList());
      Schema newReaderSchema = AvroInternalSchemaConverter
          .convert(new InternalSchemaMerger(writeInternalSchema, querySchema, true, false, false).mergeSchema(), readerSchema.getName());
      Schema writeSchemaFromFile = AvroInternalSchemaConverter.convert(writeInternalSchema, newReaderSchema.getName());
      boolean needToReWriteRecord = sameCols.size() != colNamesFromWriteSchema.size()
          || SchemaCompatibility.checkReaderWriterCompatibility(newReaderSchema, writeSchemaFromFile).getType() == org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
      if (needToReWriteRecord) {
        Map<String, String> renameCols = InternalSchemaUtils.collectRenameCols(writeInternalSchema, querySchema);
        return Option.of(Pair.of(newReaderSchema, renameCols));
      } else {
        return Option.empty();
      }
    } else {
      return Option.empty();
    }
  }
}
