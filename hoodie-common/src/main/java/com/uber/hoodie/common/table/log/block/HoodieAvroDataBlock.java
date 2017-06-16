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

import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * DataBlock contains a list of records serialized using Avro.
 * The Datablock contains
 * 1. Compressed Writer Schema length
 * 2. Compressed Writer Schema content
 * 3. Total number of records in the block
 * 4. Size of a record
 * 5. Actual avro serialized content of the record
 */
public class HoodieAvroDataBlock implements HoodieLogBlock {

  private List<IndexedRecord> records;
  private Schema schema;

  public HoodieAvroDataBlock(List<IndexedRecord> records, Schema schema) {
    this.records = records;
    this.schema = schema;
  }

  public List<IndexedRecord> getRecords() {
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

    // 1. Compress and Write schema out
    byte[] schemaContent = HoodieAvroUtils.compress(schema.toString());
    output.writeInt(schemaContent.length);
    output.write(schemaContent);

    // 2. Write total number of records
    output.writeInt(records.size());

    // 3. Write the records
    records.forEach(s -> {
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
      } catch (IOException e) {
        throw new HoodieIOException("IOException converting HoodieAvroDataBlock to bytes", e);
      }
    });

    output.close();
    return baos.toByteArray();
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.AVRO_DATA_BLOCK;
  }

  public static HoodieLogBlock fromBytes(byte[] content, Schema readerSchema) throws IOException {
    // 1. Read the schema written out
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(content));
    int schemaLength = dis.readInt();
    byte[] compressedSchema = new byte[schemaLength];
    dis.readFully(compressedSchema, 0, schemaLength);
    Schema writerSchema = new Schema.Parser().parse(HoodieAvroUtils.decompress(compressedSchema));

    if(readerSchema == null) {
      readerSchema = writerSchema;
    }

    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    // 2. Get the total records
    int totalRecords = dis.readInt();
    List<IndexedRecord> records = new ArrayList<>(totalRecords);

    // 3. Read the content
    for(int i=0;i<totalRecords;i++) {
      // TODO - avoid bytes copy
      int recordLength = dis.readInt();
      byte[] recordData = new byte[recordLength];
      dis.readFully(recordData, 0, recordLength);
      Decoder decoder = DecoderFactory.get().binaryDecoder(recordData, null);
      IndexedRecord record = reader.read(null, decoder);
      records.add(record);
    }

    dis.close();
    return new HoodieAvroDataBlock(records, readerSchema);
  }
}
