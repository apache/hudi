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

import org.apache.hudi.common.fs.SizeAwareDataInputStream;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockContentLocation;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;

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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieAvroDataBlock contains a list of records serialized using Avro. It is used with the Parquet base file format.
 */
public class HoodieAvroDataBlock extends HoodieDataBlock {

  private final ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<>();

  public HoodieAvroDataBlock(FSDataInputStream inputStream,
                             Option<byte[]> content,
                             boolean readBlockLazily,
                             HoodieLogBlockContentLocation logBlockContentLocation,
                             Option<Schema> readerSchema,
                             Map<HeaderMetadataType, String> header,
                             Map<HeaderMetadataType, String> footer,
                             String keyField) {
    super(content, inputStream, readBlockLazily, Option.of(logBlockContentLocation), readerSchema, header, footer, keyField, false);
  }

  public HoodieAvroDataBlock(@Nonnull List<HoodieRecord> records,
      @Nonnull Map<HeaderMetadataType, String> header,
      @Nonnull String keyField
  ) {
    super(records, header, new HashMap<>(), keyField);
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.AVRO_DATA_BLOCK;
  }

  @Override
  protected byte[] serializeRecords(List<HoodieRecord> records) throws IOException {
    Schema schema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);

    // 1. Write out the log block version
    output.writeInt(HoodieLogBlock.version);

    // 2. Write total number of records
    output.writeInt(records.size());

    // 3. Write the records
    for (HoodieRecord<?> s : records) {
      ByteArrayOutputStream temp = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(temp, encoderCache.get());
      encoderCache.set(encoder);
      try {
        // Encode the record into bytes
        // Spark Record not support write avro log
        IndexedRecord data = s.toIndexedRecord(schema, new Properties()).get().getData();
        writer.write(data, encoder);
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
    }
    encoderCache.remove();
    output.close();
    return baos.toByteArray();
  }

  // TODO (na) - Break down content into smaller chunks of byte [] to be GC as they are used
  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException {
    checkState(this.readerSchema != null, "Reader's schema has to be non-null");
    checkArgument(type != HoodieRecordType.SPARK, "Not support read avro to spark record");
    // TODO AvroSparkReader need
    RecordIterator iterator = RecordIterator.getInstance(this, content);
    return new MappingIterator<>(iterator, data -> (HoodieRecord<T>) new HoodieAvroIndexedRecord(data));
  }

  private static class RecordIterator implements ClosableIterator<IndexedRecord> {
    private byte[] content;
    private final SizeAwareDataInputStream dis;
    private final GenericDatumReader<IndexedRecord> reader;
    private final ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();

    private int totalRecords = 0;
    private int readRecords = 0;

    private RecordIterator(Schema readerSchema, Schema writerSchema, byte[] content) throws IOException {
      this.content = content;

      this.dis = new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(this.content)));

      // 1. Read version for this data block
      int version = this.dis.readInt();
      if (new HoodieAvroDataBlockVersion(version).hasRecordCount()) {
        this.totalRecords = this.dis.readInt();
      }

      this.reader = new GenericDatumReader<>(writerSchema, readerSchema);
    }

    public static RecordIterator getInstance(HoodieAvroDataBlock dataBlock, byte[] content) throws IOException {
      // Get schema from the header
      Schema writerSchema = new Schema.Parser().parse(dataBlock.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
      return new RecordIterator(dataBlock.readerSchema, writerSchema, content);
    }

    @Override
    public void close() {
      try {
        this.dis.close();
        this.decoderCache.remove();
        this.content = null;
      } catch (IOException e) {
        // ignore
      }
    }

    @Override
    public boolean hasNext() {
      return readRecords < totalRecords;
    }

    @Override
    public IndexedRecord next() {
      try {
        int recordLength = this.dis.readInt();
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(this.content, this.dis.getNumberOfBytesRead(),
                recordLength, this.decoderCache.get());
        this.decoderCache.set(decoder);
        IndexedRecord record = this.reader.read(null, decoder);
        this.dis.skipBytes(recordLength);
        this.readRecords++;
        return record;
      } catch (IOException e) {
        throw new HoodieIOException("Unable to convert bytes to record.", e);
      }
    }
  }

  //----------------------------------------------------------------------------------------
  //                                  DEPRECATED METHODS
  //
  // These methods were only supported by HoodieAvroDataBlock and have been deprecated. Hence,
  // these are only implemented here even though they duplicate the code from HoodieAvroDataBlock.
  //----------------------------------------------------------------------------------------

  /**
   * This constructor is retained to provide backwards compatibility to HoodieArchivedLogs which were written using
   * HoodieLogFormat V1.
   */
  @Deprecated
  public HoodieAvroDataBlock(List<HoodieRecord> records, Schema schema) {
    super(records, Collections.singletonMap(HeaderMetadataType.SCHEMA, schema.toString()), new HashMap<>(), HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  public static HoodieAvroDataBlock getBlock(byte[] content, Schema readerSchema) throws IOException {
    return getBlock(content, readerSchema, InternalSchema.getEmptyInternalSchema());
  }

  /**
   * This method is retained to provide backwards compatibility to HoodieArchivedLogs which were written using
   * HoodieLogFormat V1.
   */
  @Deprecated
  public static HoodieAvroDataBlock getBlock(byte[] content, Schema readerSchema, InternalSchema internalSchema) throws IOException {

    SizeAwareDataInputStream dis = new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(content)));

    // 1. Read the schema written out
    int schemaLength = dis.readInt();
    byte[] compressedSchema = new byte[schemaLength];
    dis.readFully(compressedSchema, 0, schemaLength);
    Schema writerSchema = new Schema.Parser().parse(decompress(compressedSchema));

    if (readerSchema == null) {
      readerSchema = writerSchema;
    }

    if (!internalSchema.isEmptySchema()) {
      readerSchema = writerSchema;
    }

    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    // 2. Get the total records
    int totalRecords = dis.readInt();
    List<HoodieRecord> records = new ArrayList<>(totalRecords);

    // 3. Read the content
    for (int i = 0; i < totalRecords; i++) {
      int recordLength = dis.readInt();
      Decoder decoder = DecoderFactory.get().binaryDecoder(content, dis.getNumberOfBytesRead(), recordLength, null);
      IndexedRecord record = reader.read(null, decoder);
      records.add(new HoodieAvroIndexedRecord(record));
      dis.skipBytes(recordLength);
    }
    dis.close();
    return new HoodieAvroDataBlock(records, readerSchema);
  }

  private static byte[] compress(String text) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      OutputStream out = new DeflaterOutputStream(baos);
      out.write(text.getBytes(StandardCharsets.UTF_8));
      out.close();
    } catch (IOException e) {
      throw new HoodieIOException("IOException while compressing text " + text, e);
    }
    return baos.toByteArray();
  }

  private static String decompress(byte[] bytes) {
    InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = in.read(buffer)) > 0) {
        baos.write(buffer, 0, len);
      }
      return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new HoodieIOException("IOException while decompressing text", e);
    }
  }

  @Deprecated
  public byte[] getBytes(Schema schema) throws IOException {

    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);

    // 1. Compress and Write schema out
    byte[] schemaContent = compress(schema.toString());
    output.writeInt(schemaContent.length);
    output.write(schemaContent);

    List<HoodieRecord<?>> records = new ArrayList<>();
    try (ClosableIterator<HoodieRecord<Object>> recordItr = getRecordIterator(HoodieRecordType.AVRO)) {
      recordItr.forEachRemaining(records::add);
    }

    // 2. Write total number of records
    output.writeInt(records.size());

    // 3. Write the records
    Iterator<HoodieRecord<?>> itr = records.iterator();
    while (itr.hasNext()) {
      IndexedRecord s = itr.next().toIndexedRecord(schema, new Properties()).get().getData();
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
