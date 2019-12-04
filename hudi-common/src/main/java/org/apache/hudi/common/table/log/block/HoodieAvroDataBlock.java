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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.storage.SizeAwareDataInputStream;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.fs.FSDataInputStream;

import javax.annotation.Nonnull;

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

/**
 * DataBlock contains a list of records serialized using Avro. The Datablock contains 1. Data Block version 2. Total
 * number of records in the block 3. Size of a record 4. Actual avro serialized content of the record
 */
public class HoodieAvroDataBlock extends HoodieLogBlock {

  private List<IndexedRecord> records;
  private Schema schema;
  private ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<>();
  private ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();

  public HoodieAvroDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header,
      @Nonnull Map<HeaderMetadataType, String> footer) {
    super(header, footer, Option.empty(), Option.empty(), null, false);
    this.records = records;
    this.schema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
  }

  public HoodieAvroDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header) {
    this(records, header, new HashMap<>());
  }

  private HoodieAvroDataBlock(Option<byte[]> content, @Nonnull FSDataInputStream inputStream, boolean readBlockLazily,
      Option<HoodieLogBlockContentLocation> blockContentLocation, Schema readerSchema,
      @Nonnull Map<HeaderMetadataType, String> headers, @Nonnull Map<HeaderMetadataType, String> footer) {
    super(headers, footer, blockContentLocation, content, inputStream, readBlockLazily);
    this.schema = readerSchema;
  }

  public static HoodieLogBlock getBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
      boolean readBlockLazily, long position, long blockSize, long blockEndpos, Schema readerSchema,
      Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {

    return new HoodieAvroDataBlock(content, inputStream, readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)), readerSchema, header,
        footer);

  }

  @Override
  public byte[] getContentBytes() throws IOException {

    // In case this method is called before realizing records from content
    if (getContent().isPresent()) {
      return getContent().get();
    } else if (readBlockLazily && !getContent().isPresent() && records == null) {
      // read block lazily
      createRecordsFromContentBytes();
    }

    Schema schema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);

    // 1. Write out the log block version
    output.writeInt(HoodieLogBlock.version);

    // 2. Write total number of records
    output.writeInt(records.size());

    // 3. Write the records
    Iterator<IndexedRecord> itr = records.iterator();
    while (itr.hasNext()) {
      IndexedRecord s = itr.next();
      ByteArrayOutputStream temp = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(temp, encoderCache.get());
      encoderCache.set(encoder);
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
    if (records == null) {
      try {
        // in case records are absent, read content lazily and then convert to IndexedRecords
        createRecordsFromContentBytes();
      } catch (IOException io) {
        throw new HoodieIOException("Unable to convert content bytes to records", io);
      }
    }
    return records;
  }

  public Schema getSchema() {
    // if getSchema was invoked before converting byte [] to records
    if (records == null) {
      getRecords();
    }
    return schema;
  }

  // TODO (na) - Break down content into smaller chunks of byte [] to be GC as they are used
  // TODO (na) - Implement a recordItr instead of recordList
  private void createRecordsFromContentBytes() throws IOException {

    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    SizeAwareDataInputStream dis =
        new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(getContent().get())));

    // 1. Read version for this data block
    int version = dis.readInt();
    HoodieAvroDataBlockVersion logBlockVersion = new HoodieAvroDataBlockVersion(version);

    // Get schema from the header
    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    // If readerSchema was not present, use writerSchema
    if (schema == null) {
      schema = writerSchema;
    }

    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, schema);
    // 2. Get the total records
    int totalRecords = 0;
    if (logBlockVersion.hasRecordCount()) {
      totalRecords = dis.readInt();
    }
    List<IndexedRecord> records = new ArrayList<>(totalRecords);

    // 3. Read the content
    for (int i = 0; i < totalRecords; i++) {
      int recordLength = dis.readInt();
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(getContent().get(), dis.getNumberOfBytesRead(),
          recordLength, decoderCache.get());
      decoderCache.set(decoder);
      IndexedRecord record = reader.read(null, decoder);
      records.add(record);
      dis.skipBytes(recordLength);
    }
    dis.close();
    this.records = records;
    // Free up content to be GC'd, deflate
    deflate();
  }

  //----------------------------------------------------------------------------------------
  //                                  DEPRECATED METHODS
  //----------------------------------------------------------------------------------------

  /**
   * This constructor is retained to provide backwards compatibility to HoodieArchivedLogs which were written using
   * HoodieLogFormat V1.
   */
  @Deprecated
  @VisibleForTesting
  public HoodieAvroDataBlock(List<IndexedRecord> records, Schema schema) {
    super(new HashMap<>(), new HashMap<>(), Option.empty(), Option.empty(), null, false);
    this.records = records;
    this.schema = schema;
  }

  /**
   * This method is retained to provide backwards compatibility to HoodieArchivedLogs which were written using
   * HoodieLogFormat V1.
   */
  @Deprecated
  public static HoodieLogBlock getBlock(byte[] content, Schema readerSchema) throws IOException {

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
    for (int i = 0; i < totalRecords; i++) {
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
