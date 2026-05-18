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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieDeleteRecord;
import org.apache.hudi.avro.model.HoodieDeleteRecordList;
import org.apache.hudi.common.fs.SizeAwareDataInputStream;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.util.Lazy;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroWrapperUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.avro.HoodieAvroWrapperUtils.wrapValueIntoAvro;

/**
 * Delete block contains a list of keys to be deleted from scanning the blocks so far.
 */
public class HoodieDeleteBlock extends HoodieLogBlock {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieDeleteBlock.class);
  /**
   * These static builders are added to avoid performance issue in Avro 1.10.
   * You can find more details in HoodieAvroUtils, HUDI-3834, and AVRO-3048.
   */
  private static final Lazy<HoodieDeleteRecordList.Builder> HOODIE_DELETE_RECORD_LIST_BUILDER_STUB =
      Lazy.lazily(HoodieDeleteRecordList::newBuilder);
  private static final Lazy<HoodieDeleteRecord.Builder> HOODIE_DELETE_RECORD_BUILDER_STUB =
      Lazy.lazily(HoodieDeleteRecord::newBuilder);

  // Records to delete, sorted based on the record position if writing record position to the log block header
  private DeleteRecord[] recordsToDelete;

  public HoodieDeleteBlock(List<Pair<DeleteRecord, Long>> recordsToDelete,
                           Map<HeaderMetadataType, String> header) {
    this(Option.empty(), null, false, Option.empty(), header, new HashMap<>());
    addRecordPositionsIfRequired(recordsToDelete, Pair::getRight);
    this.recordsToDelete = recordsToDelete.stream().map(Pair::getLeft).toArray(DeleteRecord[]::new);
  }

  public HoodieDeleteBlock(Option<byte[]> content, Supplier<SeekableDataInputStream> inputStreamSupplier, boolean readBlockLazily,
                           Option<HoodieLogBlockContentLocation> blockContentLocation, Map<HeaderMetadataType, String> header,
                           Map<FooterMetadataType, String> footer) {
    super(header, footer, blockContentLocation, content, inputStreamSupplier, readBlockLazily);
  }

  @Override
  public ByteArrayOutputStream getContentBytes(HoodieStorage storage) throws IOException {
    // In case this method is called before realizing keys from content
    Option<ByteArrayOutputStream> baosOpt = getContentAsByteStream();
    if (baosOpt.isPresent()) {
      return baosOpt.get();
    } else if (readBlockLazily && recordsToDelete == null) {
      // read block lazily
      getRecordsToDelete();
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    output.writeInt(version);
    byte[] bytesToWrite = (version <= 2) ? serializeV2() : serializeV3();
    output.writeInt(bytesToWrite.length);
    output.write(bytesToWrite);
    return baos;
  }

  public DeleteRecord[] getRecordsToDelete() {
    try {
      if (recordsToDelete == null) {
        if (!getContent().isPresent() && readBlockLazily) {
          // read content from disk
          inflate();
        }
        SizeAwareDataInputStream dis =
            new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(getContent().get())));
        int version = dis.readInt();
        int dataLength = dis.readInt();
        byte[] data = new byte[dataLength];
        dis.readFully(data);
        this.recordsToDelete = deserialize(version, data);
        deflate();
      }
      return recordsToDelete;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to generate keys to delete from block content", io);
    }
  }

  private byte[] serializeV2() throws IOException {
    // Serialization for log block version 2
    return SerializationUtils.serialize(getRecordsToDelete());
  }

  private byte[] serializeV3() throws IOException {
    DatumWriter<HoodieDeleteRecordList> writer = new SpecificDatumWriter<>(HoodieDeleteRecordList.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    // Serialization for log block version 3 and above
    HoodieDeleteRecordList.Builder recordListBuilder = HOODIE_DELETE_RECORD_LIST_BUILDER_STUB.get();
    HoodieDeleteRecord.Builder recordBuilder = HOODIE_DELETE_RECORD_BUILDER_STUB.get();
    List<HoodieDeleteRecord> deleteRecordList = Arrays.stream(getRecordsToDelete())
        .map(record -> HoodieDeleteRecord.newBuilder(recordBuilder)
          .setRecordKey(record.getRecordKey())
          .setPartitionPath(record.getPartitionPath())
          .setOrderingVal(wrapValueIntoAvro(record.getOrderingValue()))
          .build())
        .collect(Collectors.toList());
    writer.write(HoodieDeleteRecordList.newBuilder(recordListBuilder)
        .setDeleteRecordList(deleteRecordList)
        .build(), encoder);
    encoder.flush();
    return baos.toByteArray();
  }

  private static DeleteRecord[] deserialize(int version, byte[] data) throws IOException {
    if (version == 1) {
      // legacy version
      HoodieKey[] keys = SerializationUtils.deserialize(data);
      return Arrays.stream(keys).map(DeleteRecord::create).toArray(DeleteRecord[]::new);
    } else if (version == 2) {
      return SerializationUtils.deserialize(data);
    } else {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(HoodieDeleteRecordList.SCHEMA$);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, 0, data.length, null);
      List<HoodieDeleteRecord> deleteRecordList = HoodieAvroUtils.convertToSpecificRecord(HoodieDeleteRecordList.class, reader.read(null, decoder))
          .getDeleteRecordList();
      return deleteRecordList.stream()
          .map(record -> DeleteRecord.create(
              record.getRecordKey(),
              record.getPartitionPath(),
              unwrapAvroValueWrapper(record.getOrderingVal())))
          .toArray(DeleteRecord[]::new);
    }
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.DELETE_BLOCK;
  }

}
