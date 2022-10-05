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
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.AvroSchemaUtils.isProjectionOf;
import static org.apache.hudi.avro.HoodieAvroUtils.rewriteRecordWithNewSchema;

public class HoodieMergeHelper<T extends HoodieRecordPayload> extends BaseMergeHelper {

  private HoodieMergeHelper() {
  }

  private static class MergeHelperHolder {
    private static final HoodieMergeHelper HOODIE_MERGE_HELPER = new HoodieMergeHelper<>();
  }

  public static HoodieMergeHelper newInstance() {
    return MergeHelperHolder.HOODIE_MERGE_HELPER;
  }

  @Override
  public void runMerge(HoodieTable<?, ?, ?, ?> table,
                       HoodieMergeHandle<?, ?, ?, ?> mergeHandle) throws IOException {
    HoodieWriteConfig writeConfig = table.getConfig();
    HoodieBaseFile baseFile = mergeHandle.baseFileForMerge();

    Configuration hadoopConf = new Configuration(table.getHadoopConf());
    HoodieFileReader<GenericRecord> reader = HoodieFileReaderFactory.getFileReader(hadoopConf, mergeHandle.getOldFilePath());

    Schema writerSchema = mergeHandle.getWriterSchemaWithMetaFields();
    Schema readerSchema = reader.getSchema();

    // NOTE: Here we check whether the writer schema is simply a projection of the reader one
    boolean isProjection = isProjectionOf(readerSchema, writerSchema);

    boolean shouldRewriteInWriterSchema = writeConfig.shouldUseExternalSchemaTransformation()
        || !isProjection
        || baseFile.getBootstrapBaseFile().isPresent();

    // In case Advanced Schema Evolution is enabled we might need to rewrite currently
    // persisted records to adhere to an evolved schema
    Option<Function<GenericRecord, GenericRecord>> schemaEvolutionTransformerOpt =
        composeSchemaEvolutionTransformer(writerSchema, baseFile, writeConfig, table.getMetaClient());

    BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;

    try {
      Iterator<GenericRecord> recordIterator;

      ClosableIterator<GenericRecord> baseFileRecordIterator = reader.getRecordIterator(readerSchema);
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        Path bootstrapFilePath = new Path(baseFile.getBootstrapBaseFile().get().getPath());
        recordIterator = getMergingIterator(table, mergeHandle, bootstrapFilePath, baseFileRecordIterator);
      } else if (schemaEvolutionTransformerOpt.isPresent()) {
        recordIterator = new MappingIterator<>(baseFileRecordIterator,
            schemaEvolutionTransformerOpt.get());
      } else {
        recordIterator = baseFileRecordIterator;
      }

      wrapper = new BoundedInMemoryExecutor(writeConfig.getWriteBufferLimitBytes(), recordIterator,
          new UpdateHandler(mergeHandle), record -> {
        if (shouldRewriteInWriterSchema) {
          return rewriteRecordWithNewSchema((GenericRecord) record, writerSchema);
        } else {
          return record;
        }
      }, table.getPreExecuteRunnable());
      wrapper.execute();
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      // HUDI-2875: mergeHandle is not thread safe, we should totally terminate record inputting
      // and executor firstly and then close mergeHandle.
      reader.close();
      if (null != wrapper) {
        wrapper.shutdownNow();
        wrapper.awaitTermination();
      }
      mergeHandle.close();
    }
  }

  private Option<Function<GenericRecord, GenericRecord>> composeSchemaEvolutionTransformer(Schema writerSchema,
                                                                                           HoodieBaseFile baseFile,
                                                                                           HoodieWriteConfig writeConfig,
                                                                                           HoodieTableMetaClient metaClient) {
    Option<InternalSchema> querySchemaOpt = SerDeHelper.fromJson(writeConfig.getInternalSchema());
    // TODO support bootstrap
    if (querySchemaOpt.isPresent() && !baseFile.getBootstrapBaseFile().isPresent()) {
      // check implicitly add columns, and position reorder(spark sql may change cols order)
      InternalSchema querySchema = AvroSchemaEvolutionUtils.reconcileSchema(writerSchema, querySchemaOpt.get());
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
      InternalSchema mergedSchema = new InternalSchemaMerger(writeInternalSchema, querySchema,
          true, false, false).mergeSchema();
      Schema newWriterSchema = AvroInternalSchemaConverter.convert(mergedSchema, writerSchema.getName());
      Schema writeSchemaFromFile = AvroInternalSchemaConverter.convert(writeInternalSchema, newWriterSchema.getName());
      boolean needToReWriteRecord = sameCols.size() != colNamesFromWriteSchema.size()
          || SchemaCompatibility.checkReaderWriterCompatibility(newWriterSchema, writeSchemaFromFile).getType() == org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
      if (needToReWriteRecord) {
        Map<String, String> renameCols = InternalSchemaUtils.collectRenameCols(writeInternalSchema, querySchema);
        return Option.of(record -> rewriteRecordWithNewSchema(record, newWriterSchema, renameCols));
      } else {
        return Option.empty();
      }
    } else {
      return Option.empty();
    }
  }
}
