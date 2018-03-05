/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log.block;

import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.storage.SizeAwareDataInputStream;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * DataBlock contains a list of records serialized using Avro.
 * The Datablock contains
 * 1. Total number of records in the block
 * 2. Size of a record
 * 3. Actual avro serialized content of the record
 */
public class HoodieAvroDataBlock extends HoodieLogBlock {

  private List<IndexedRecord> records;
  private byte [] content;
  private Schema schema;

  public HoodieAvroDataBlock(List<IndexedRecord> records,
                             Map<HeaderMetadataType, String> header,
                             Map<HeaderMetadataType, String> footer) {
    super(header, footer, null);
    this.records = records;
    this.schema = Schema.parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
  }

  public HoodieAvroDataBlock(List<IndexedRecord> records, Map<HeaderMetadataType, String> header) {
    this(records, header, new HashMap<>());
  }

  private HoodieAvroDataBlock(byte [] content,
                              Schema readerSchema, Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(header, footer, Optional.empty());
    this.schema = readerSchema;
    this.content = content;
  }

  private HoodieAvroDataBlock(HoodieLogBlockContentLocation blockContentLocation,
                              Schema readerSchema, Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(header, footer, Optional.of(blockContentLocation));
    this.schema = readerSchema;
  }

  @Deprecated
  /**
   * This constructor is retained to provide backwards compatibility to HoodieArchivedLogs
   * which were written using HoodieLogFormat V1
   */
  public HoodieAvroDataBlock(List<IndexedRecord> records, Schema schema) {
    super(new HashMap<>(), new HashMap<>() , Optional.empty());
    this.records = records;
    this.schema = schema;
  }

  @Override
  public byte[] getContentBytes() throws IOException {

    Schema schema = Schema.parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);


    // 1. Write total number of records
    output.writeInt(records.size());

    // 2. Write the records
    Iterator<IndexedRecord> itr = records.iterator();
    while (itr.hasNext()) {
      IndexedRecord s = itr.next();
      ByteArrayOutputStream temp = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(temp, null);
      try {
        // Encode the record into bytes
        writer.write(s, encoder);
        encoder.flush();

        // Get the size of the bytes
        int size = temp.toByteArray().length;
        // Write the record size
        output.writeInt(size);
        // Write the content
        output.write(temp.toByteArray());
        itr.remove();
      } catch (IOException e) {
        throw new HoodieIOException("IOException converting HoodieAvroDataBlock to bytes", e);
      }
    }

    output.close();
    return baos.toByteArray();
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.AVRO_DATA_BLOCK;
  }

  public List<IndexedRecord> getRecords() {
    if(records == null) {
      try {
        fromBytes();
      } catch(IOException io) {
        throw new HoodieIOException("Unable to convert bytes content to records", io);
      }
    }
    return records;
  }

  public Schema getSchema() {
    return schema;
  }

  public static HoodieLogBlock getBlock(HoodieLogFile logFile,
                                        long position,
                                        long blockSize,
                                        Schema readerSchema,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) {

    return new HoodieAvroDataBlock(new HoodieLogBlockContentLocation(logFile, position, blockSize)
        , readerSchema, header, footer);

  }

  public static HoodieLogBlock getBlock(byte [] content,
                                        Schema readerSchema,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) {

    return new HoodieAvroDataBlock(content, readerSchema, header, footer);

  }

  //TODO (na) - Break down content into smaller chunks of byte [] to be GC as they are used
  //TODO (na) - Implement a recordItr instead of recordList
  private void fromBytes() throws IOException {

    if(content == null) {
      throw new HoodieException("Content is empty, use HoodieLazyBlockReader to read contents lazily");
      // read content from disk
    }
    SizeAwareDataInputStream dis = new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(content)));

    // 1. Read the schema written out
    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    // If readerSchema was not present, use writerSchema
    if (schema == null) {
      schema = writerSchema;
    }

    //TODO : (na) lazily create IndexedRecords only when required
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, schema);
    // 2. Get the total records
    int totalRecords = dis.readInt();
    List<IndexedRecord> records = new ArrayList<>(totalRecords);

    // 3. Read the content
    for (int i=0;i<totalRecords;i++) {
      int recordLength = dis.readInt();
      Decoder decoder = DecoderFactory.get().binaryDecoder(content, dis.getNumberOfBytesRead(), recordLength, null);
      IndexedRecord record = reader.read(null, decoder);
      records.add(record);
      dis.skipBytes(recordLength);
    }
    dis.close();
    this.records = records;
    // Free up content to be GC'd
    this.content = null;
  }

  /*****************************************************DEPRECATED METHODS**********************************************/
  @Deprecated
  /**
   * This method is retained to provide backwards compatibility to HoodieArchivedLogs which were written using HoodieLogFormat V1
   */
  public static HoodieLogBlock fromBytes(byte[] content, Schema readerSchema) throws IOException {

    SizeAwareDataInputStream dis = new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(content)));

    // 1. Read the schema written out
    int schemaLength = dis.readInt();
    byte[] compressedSchema = new byte[schemaLength];
    dis.readFully(compressedSchema, 0, schemaLength);
    Schema writerSchema = new Schema.Parser().parse(HoodieAvroUtils.decompress(compressedSchema));

    if (readerSchema == null) {
      readerSchema = writerSchema;
    }

    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    // 2. Get the total records
    int totalRecords = dis.readInt();
    List<IndexedRecord> records = new ArrayList<>(totalRecords);

    // 3. Read the content
    for (int i=0;i<totalRecords;i++) {
      int recordLength = dis.readInt();
      Decoder decoder = DecoderFactory.get().binaryDecoder(content, dis.getNumberOfBytesRead(), recordLength, null);
      IndexedRecord record = reader.read(null, decoder);
      records.add(record);
      dis.skipBytes(recordLength);
    }
    dis.close();
    return new HoodieAvroDataBlock(records, readerSchema);
  }

  @Deprecated
  @VisibleForTesting
  public byte[] getBytes(Schema schema) throws IOException {

    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);

    // 2. Compress and Write schema out
    byte[] schemaContent = HoodieAvroUtils.compress(schema.toString());
    output.writeInt(schemaContent.length);
    output.write(schemaContent);

    // 3. Write total number of records
    output.writeInt(records.size());

    // 4. Write the records
    Iterator<IndexedRecord> itr = records.iterator();
    while (itr.hasNext()) {
      IndexedRecord s = itr.next();
      ByteArrayOutputStream temp = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(temp, null);
      try {
        // Encode the record into bytes
        writer.write(s, encoder);
        encoder.flush();

        // Get the size of the bytes
        int size = temp.toByteArray().length;
        // Write the record size
        output.writeInt(size);
        // Write the content
        output.write(temp.toByteArray());
        itr.remove();
      } catch (IOException e) {
        throw new HoodieIOException("IOException converting HoodieAvroDataBlock to bytes", e);
      }
    }

    output.close();
    return baos.toByteArray();
  }

}
