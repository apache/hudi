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

package org.apache.hudi.common.table.log;

import org.apache.hudi.avro.model.HoodieDeleteRecord;
import org.apache.hudi.avro.model.HoodieDeleteRecordList;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.avro.HoodieAvroUtils.wrapValueIntoAvro;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

/**
 * A util converter used to generate serializer for log blocks content, including data blocks
 * and delete blocks. This is used in scenarios that the block contents bytes are eagerly generated
 * instead of buffering records in list inside block for more memory efficiency, for example,
 * flink streaming ingestion uses record iterator based on managed memory to build data block.
 */
public class BlockBytesConverter {

  /**
   * Get the content serializer for data blocks to serialize the records iterator into bytes.
   */
  public static BlockContentSerializer<HoodieRecord> getDataBlockSerializer(
      HoodieLogBlockType blockType,
      HoodieStorage storage,
      HoodieRecord.HoodieRecordType recordType,
      Schema writerSchema,
      Schema readerSchema,
      String keyField,
      Map<String, String> paramsMap) {
    switch (blockType) {
      case PARQUET_DATA_BLOCK:
        return new ParquetBlockContentSerializer(
            Objects.requireNonNull(storage),
            Objects.requireNonNull(recordType),
            Objects.requireNonNull(writerSchema),
            Objects.requireNonNull(readerSchema),
            Objects.requireNonNull(keyField),
            Objects.requireNonNull(paramsMap));
      default:
        throw new UnsupportedOperationException(String.format("BlockContentSerializer for %s is not supported yet", blockType));
    }
  }

  /**
   * Get the content serializer for delete blocks to serialize the records iterator into bytes.
   */
  public static BlockContentSerializer<DeleteRecord> getDeleteBlockSerializer() {
    return new DeleteBlockContentSerializer();
  }

  /**
   * An implementation of {@link BlockContentSerializer} for Parquet data block, which uses the underlying Parquet
   * writer to generate bytes for the given record iterator.
   */
  public static class ParquetBlockContentSerializer implements BlockContentSerializer<HoodieRecord> {
    private final HoodieStorage storage;
    private final Schema writerSchema;
    private final Schema readerSchema;
    private final String keyField;
    private final Map<String, String> paramsMap;
    private final HoodieRecordType recordType;
    private Map<String, HoodieColumnRangeMetadata<Comparable>> columnStatsMeta;

    public ParquetBlockContentSerializer(
        HoodieStorage storage,
        HoodieRecord.HoodieRecordType recordType,
        Schema writerSchema,
        Schema readerSchema,
        String keyField,
        Map<String, String> paramsMap) {
      this.storage = storage;
      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
      this.recordType = recordType;
      this.keyField = keyField;
      this.paramsMap = paramsMap;
    }

    @Override
    public byte[] serialize(Iterator<HoodieRecord> recordIterator) throws IOException {
      columnStatsMeta = new HashMap<>();
      return HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(PARQUET)
          .serializeRecordsToLogBlock(
              storage,
              recordIterator,
              recordType,
              writerSchema,
              readerSchema,
              keyField,
              paramsMap,
              columnMeta -> columnStatsMeta.putAll(columnMeta));
    }

    @Override
    public Map<String, HoodieColumnRangeMetadata<Comparable>> getColumnStatsMeta() {
      return columnStatsMeta;
    }
  }

  /**
   * An implementation of {@link BlockContentSerializer} for delete block, which uses Avro as serde for the given
   * delete records.
   */
  public static class DeleteBlockContentSerializer implements BlockContentSerializer<DeleteRecord> {
    /**
     * These static builders are added to avoid performance issue in Avro 1.10.
     * You can find more details in HoodieAvroUtils, HUDI-3834, and AVRO-3048.
     */
    private static final Lazy<HoodieDeleteRecordList.Builder> HOODIE_DELETE_RECORD_LIST_BUILDER_STUB =
        Lazy.lazily(HoodieDeleteRecordList::newBuilder);
    private static final Lazy<HoodieDeleteRecord.Builder> HOODIE_DELETE_RECORD_BUILDER_STUB =
        Lazy.lazily(HoodieDeleteRecord::newBuilder);

    @Override
    public byte[] serialize(Iterator<DeleteRecord> recordIterator) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(baos);
      output.writeInt(HoodieDeleteBlock.version);
      byte[] bytesToWrite = serializeRecords(recordIterator);
      output.writeInt(bytesToWrite.length);
      output.write(bytesToWrite);
      return baos.toByteArray();
    }

    private byte[] serializeRecords(Iterator<DeleteRecord> recordIterator) throws IOException {
      DatumWriter<HoodieDeleteRecordList> writer = new SpecificDatumWriter<>(HoodieDeleteRecordList.class);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      // Serialization for log block version 3 and above
      HoodieDeleteRecordList.Builder recordListBuilder = HOODIE_DELETE_RECORD_LIST_BUILDER_STUB.get();
      HoodieDeleteRecord.Builder recordBuilder = HOODIE_DELETE_RECORD_BUILDER_STUB.get();
      MappingIterator<DeleteRecord, HoodieDeleteRecord> deleteItr = new MappingIterator<>(recordIterator,
          record -> HoodieDeleteRecord.newBuilder(recordBuilder)
              .setRecordKey(record.getRecordKey())
              .setPartitionPath(record.getPartitionPath())
              .setOrderingVal(wrapValueIntoAvro(record.getOrderingValue()))
              .build());
      List<HoodieDeleteRecord> deleteRecordList = new ArrayList<>();
      deleteItr.forEachRemaining(deleteRecordList::add);
      writer.write(HoodieDeleteRecordList.newBuilder(recordListBuilder)
          .setDeleteRecordList(deleteRecordList)
          .build(), encoder);
      encoder.flush();
      return baos.toByteArray();
    }

    @Override
    public Map<String, HoodieColumnRangeMetadata<Comparable>> getColumnStatsMeta() {
      return Collections.emptyMap();
    }
  }

  /**
   * {@code BlockContentSerializer} is used to serialize log block content into bytes,
   * and collect column statistics in the meantime.
   */
  public interface BlockContentSerializer<T> {

    byte[] serialize(Iterator<T> dataList) throws IOException;

    Map<String, HoodieColumnRangeMetadata<Comparable>> getColumnStatsMeta();
  }
}
