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

import com.uber.hoodie.common.storage.SizeAwareDataInputStream;
import com.uber.hoodie.common.util.HoodieAvroUtils;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * DataBlock contains a list of records serialized using Avro.
 * The Datablock contains
 * 1. Metadata for a block
 * 2. Compressed Writer Schema length
 * 3. Compressed Writer Schema content
 * 4. Total number of records in the block
 * 5. Size of a record
 * 6. Actual avro serialized content of the record
 */
public class HoodieAvroDataBlock extends HoodieLogBlock {

  private List<IndexedRecord> records;
  private Schema schema;
  private byte [] content;

  public HoodieAvroDataBlock(List<IndexedRecord> records,
                             Schema schema, Map<LogMetadataType, String> metadata) {
    super(metadata);
    this.records = records;
    this.schema = schema;
  }

  public HoodieAvroDataBlock(List<IndexedRecord> records, Schema schema) {
    this(records, schema, null);
  }

  private HoodieAvroDataBlock(byte [] content, Schema schema, Map<LogMetadataType, String> metadata) {
    super(metadata);
    this.schema = schema;
    this.content = content;
  }

  public List<IndexedRecord> getRecords() {
    if(records == null) {
      try {
        readRecordsFromBytes();
      } catch(IOException io) {
        throw new HoodieIOException("Unable to convert bytes content to records", io);
      }
    }
    return records;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public byte[] getBytes() throws IOException {

    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);

    // 1. Write out metadata
    if (super.getLogMetadata() != null) {
      output.write(HoodieLogBlock.getLogMetadataBytes(super.getLogMetadata()));
    }

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

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.AVRO_DATA_BLOCK;
  }

  //TODO (na) - Break down content into smaller chunks of byte [] to be GC as they are used
  public static HoodieLogBlock fromBytes(byte[] content, Schema readerSchema, boolean readMetadata) throws IOException {

    SizeAwareDataInputStream dis = new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(content)));
    Map<LogMetadataType, String> metadata = null;
    // 1. Read the metadata written out, if applicable
    if (readMetadata) {
      metadata = readMetadata(dis);
    }
    // 2. Read the schema written out
    Schema writerSchema = readWriterSchema(dis);
    if (readerSchema == null) {
      readerSchema = writerSchema;
    }
    return new HoodieAvroDataBlock(content, readerSchema, metadata);
  }

  private static Map<LogMetadataType, String> readMetadata(SizeAwareDataInputStream dis) throws IOException {
    return HoodieLogBlock.getLogMetadata(dis);
  }

  private static Schema readWriterSchema(SizeAwareDataInputStream dis) throws IOException {
    int schemaLength = dis.readInt();
    byte[] compressedSchema = new byte[schemaLength];
    dis.readFully(compressedSchema, 0, schemaLength);
    Schema writerSchema = new Schema.Parser().parse(HoodieAvroUtils.decompress(compressedSchema));
    return writerSchema;
  }

  private void readRecordsFromBytes() throws IOException {
    SizeAwareDataInputStream dis =
        new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(this.content)));
    if(super.getLogMetadata() != null) {
      readMetadata(dis);
    }
    Schema writerSchema = readWriterSchema(dis);

    // 3. Get the total records
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, schema);
    int totalRecords = dis.readInt();
    List<IndexedRecord> records = new ArrayList<>(totalRecords);

    // 4. Read the content
    for (int i=0;i<totalRecords;i++) {
      int recordLength = dis.readInt();
      Decoder decoder = DecoderFactory.get().binaryDecoder(content, dis.getNumberOfBytesRead(), recordLength, null);
      IndexedRecord record = reader.read(null, decoder);
      records.add(record);
      dis.skipBytes(recordLength);
    }
    dis.close();
    this.records = records;
    // Null out content and release memory
    this.content = null;
  }
}
