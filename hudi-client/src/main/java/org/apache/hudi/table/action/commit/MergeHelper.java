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

import java.io.ByteArrayOutputStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.utils.MergingParquetIterator;
import org.apache.hudi.client.utils.ParquetReaderIterator;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Iterator;

/**
 * Helper to read records from previous version of parquet and run Merge.
 */
public class MergeHelper {

  /**
   * Read records from previous version of base file and merge.
   * @param table Hoodie Table
   * @param upsertHandle Merge Handle
   * @param <T>
   * @throws IOException in case of error
   */
  public static <T extends HoodieRecordPayload<T>> void runMerge(HoodieTable<T> table,
      HoodieMergeHandle<T> upsertHandle) throws IOException {
    final boolean externalchemaTransformation = table.getConfig().shouldUseExternalSchemaTransformation();
    Configuration configForHudiFile = new Configuration(table.getHadoopConf());
    HoodieBaseFile baseFile = upsertHandle.getPrevBaseFile();

    final GenericDatumWriter<GenericRecord> gWriter;
    final GenericDatumReader<GenericRecord> gReader;
    if (externalchemaTransformation || baseFile.getExternalBaseFile().isPresent()) {
      MessageType usedParquetSchema = ParquetUtils.readSchema(table.getHadoopConf(), upsertHandle.getOldFilePath());
      Schema lastWrittenSchema = new AvroSchemaConverter().convert(usedParquetSchema);
      AvroReadSupport.setAvroReadSchema(configForHudiFile, lastWrittenSchema);
      gWriter = new GenericDatumWriter<>(lastWrittenSchema);
      gReader = new GenericDatumReader<>(lastWrittenSchema, upsertHandle.getWriterSchemaWithMetafields());
    } else {
      gReader = null;
      gWriter = null;
      AvroReadSupport.setAvroReadSchema(configForHudiFile, upsertHandle.getWriterSchemaWithMetafields());
    }

    BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
    ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(upsertHandle.getOldFilePath()).withConf(configForHudiFile).build();
    ParquetReader<GenericRecord> externalFileReader = null;
    try {
      final Iterator<GenericRecord> readerIterator;
      if (baseFile.getExternalBaseFile().isPresent()) {
        readerIterator = getStitchedParquetIterator(table, upsertHandle, baseFile, reader,
            externalchemaTransformation);
      } else {
        readerIterator = new ParquetReaderIterator(reader);
      }

      ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<>();
      ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();
      wrapper = new SparkBoundedInMemoryExecutor(table.getConfig(), readerIterator,
          new UpdateHandler(upsertHandle), record -> {
        if (!externalchemaTransformation) {
          return record;
        }
        return transformRecordBasedOnNewSchema(gReader, gWriter, encoderCache, decoderCache,
            (GenericRecord)record);

      });
      wrapper.execute();
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      if (reader != null) {
        reader.close();
      }
      upsertHandle.close();
      if (null != wrapper) {
        wrapper.shutdownNow();
      }
    }
  }

  private static GenericRecord transformRecordBasedOnNewSchema(
      GenericDatumReader<GenericRecord> gReader, GenericDatumWriter<GenericRecord> gWriter,
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
      return transformedRec;
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
   * Create Parquet record iterator that provides a stitched view of record read from skeleton and external file.
   * @param table  Hoodie Table
   * @param upsertHandle Merge Handle
   * @param baseFile Base File
   * @param reader  Record Reader corresponding to skeleton file
   * @param externalchemaTransformation Disable Parquet managed avro schema evolution
   * @param <T>
   * @return
   * @throws IOException in case of error reading files
   */
  private static <T extends HoodieRecordPayload<T>> Iterator<GenericRecord> getStitchedParquetIterator(
      HoodieTable<T> table, HoodieMergeHandle<T> upsertHandle, HoodieBaseFile baseFile,
      ParquetReader<GenericRecord> reader,  boolean externalchemaTransformation) throws IOException {
    Path externalFilePath = new Path(baseFile.getExternalBaseFile().get().getPath());
    Configuration configForExternalFile = new Configuration(table.getHadoopConf());
    if (externalchemaTransformation) {
      MessageType usedExtParquetSchema =  ParquetUtils.readSchema(configForExternalFile, externalFilePath);
      Schema lastExtWrittenSchema = new AvroSchemaConverter().convert(usedExtParquetSchema);
      AvroReadSupport.setAvroReadSchema(configForExternalFile, lastExtWrittenSchema);
    } else {
      AvroReadSupport.setAvroReadSchema(configForExternalFile, upsertHandle.getOriginalSchema());
    }
    ParquetReader<GenericRecord> externalFileReader = AvroParquetReader.<GenericRecord>builder(externalFilePath).withConf(configForExternalFile).build();
    return new MergingParquetIterator<>(new ParquetReaderIterator<>(reader),
        new ParquetReaderIterator<>(externalFileReader), (inputRecordPair) -> HoodieAvroUtils.stitchRecords(
        inputRecordPair.getLeft(), inputRecordPair.getRight(), upsertHandle.getWriterSchemaWithMetafields()));
  }

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  private static class UpdateHandler extends BoundedInMemoryQueueConsumer<GenericRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    private UpdateHandler(HoodieMergeHandle upsertHandle) {
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
