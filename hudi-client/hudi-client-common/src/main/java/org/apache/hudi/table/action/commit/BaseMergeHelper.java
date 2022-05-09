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

import org.apache.hudi.client.utils.MergingIterator;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Helper to read records from previous version of base file and run Merge.
 */
public abstract class BaseMergeHelper<T, I, K, O> {

  /**
   * Read records from previous version of base file and merge.
   * @param table Hoodie Table
   * @param upsertHandle Merge Handle
   * @throws IOException in case of error
   */
  public abstract void runMerge(HoodieTable<T, I, K, O> table, HoodieMergeHandle<T, I, K, O> upsertHandle) throws IOException;

  protected HoodieRecord transformRecordBasedOnNewSchema(GenericDatumReader<GenericRecord> gReader, GenericDatumWriter<GenericRecord> gWriter,
                                                               ThreadLocal<BinaryEncoder> encoderCache, ThreadLocal<BinaryDecoder> decoderCache,
                                                               GenericRecord gRec) {
    ByteArrayOutputStream inStream = null;
    try {
      inStream = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(inStream, encoderCache.get());
      encoderCache.set(encoder);
      gWriter.write(gRec, encoder);
      encoder.flush();

      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inStream.toByteArray(), decoderCache.get());
      decoderCache.set(decoder);
      GenericRecord transformedRec = gReader.read(null, decoder);
      return new HoodieAvroIndexedRecord(transformedRec);
    } catch (IOException e) {
      throw new HoodieException(e);
    } finally {
      try {
        inStream.close();
      } catch (IOException ioe) {
        throw new HoodieException(ioe.getMessage(), ioe);
      }
    }
  }

  /**
   * Create Parquet record iterator that provides a stitched view of record read from skeleton and bootstrap file.
   * Skeleton file is a representation of the bootstrap file inside the table, with just the bare bone fields needed
   * for indexing, writing and other functionality.
   */
  protected Iterator<HoodieRecord> getMergingIterator(HoodieTable<T, I, K, O> table,
                                                      HoodieMergeHandle<T, I, K, O> mergeHandle,
                                                      HoodieBaseFile baseFile,
                                                      HoodieFileReader reader,
                                                      Schema readerSchema,
                                                      boolean externalSchemaTransformation) throws IOException {
    Path externalFilePath = new Path(baseFile.getBootstrapBaseFile().get().getPath());
    Configuration bootstrapFileConfig = new Configuration(table.getHadoopConf());
    HoodieFileReader bootstrapReader = HoodieFileReaderFactory.getFileReader(bootstrapFileConfig, externalFilePath);

    Schema bootstrapReadSchema;
    if (externalSchemaTransformation) {
      bootstrapReadSchema = bootstrapReader.getSchema();
    } else {
      bootstrapReadSchema = mergeHandle.getWriterSchema();
    }

    return new MergingIterator<>(
        reader.getRecordIterator(readerSchema, HoodieAvroIndexedRecord::new),
        bootstrapReader.getRecordIterator(bootstrapReadSchema, HoodieAvroIndexedRecord::new),
        (oneRecord, otherRecord) -> mergeRecords(oneRecord, otherRecord, readerSchema, mergeHandle.getWriterSchemaWithMetaFields()));
  }

  @Nonnull
  private static HoodieRecord mergeRecords(HoodieRecord one, HoodieRecord other, Schema readerSchema, Schema writerSchema) {
    try {
      return one.mergeWith(other, readerSchema, writerSchema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to merge records", e);
    }
  }

  protected static HoodieRecord.Mapper createHoodieRecordMapper(HoodieTable<?, ?, ?, ?> table) {
    return createHoodieRecordMapper(table, Option.empty());
  }

  protected static HoodieRecord.Mapper createHoodieRecordMapper(HoodieTable<?, ?, ?, ?> table, Option<String> partitionPathOpt) {
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    HoodieWriteConfig writeConfig = table.getConfig();

    String payloadClassFQN = tableConfig.getPayloadClass();
    String preCombineField = tableConfig.getPreCombineField();
    boolean allowOperationMetadataField = writeConfig.allowOperationMetadataField();

    boolean populateMetaFields = tableConfig.populateMetaFields();
    String recordKeyFieldName = tableConfig.getRecordKeyFieldProp();
    String recordPartitionFieldName = tableConfig.getPartitionFieldProp();

    return (record) ->
        populateMetaFields
            ? SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) record, payloadClassFQN, preCombineField,
            Pair.of(recordKeyFieldName, recordPartitionFieldName), allowOperationMetadataField, partitionPathOpt)
            : SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) record, payloadClassFQN, preCombineField,
            allowOperationMetadataField, partitionPathOpt);
  }

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  protected static class UpdateHandler extends BoundedInMemoryQueueConsumer<HoodieRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    protected UpdateHandler(HoodieMergeHandle upsertHandle) {
      this.upsertHandle = upsertHandle;
    }

    @Override
    protected void consumeOneRecord(HoodieRecord record) {
      upsertHandle.write(record);
    }

    @Override
    protected void finish() {
    }

    @Override
    protected Void getResult() {
      return null;
    }
  }
}
