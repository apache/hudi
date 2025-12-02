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

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.SizeAwareDataInputStream;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.parquet.schema.AvroSchemaRepair;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.apache.hudi.avro.HoodieAvroUtils.recordNeedsRewriteForExtendedAvroTypePromotion;
import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieAvroDataBlock contains a list of records serialized using Avro. It is used with the Parquet base file format.
 */
public class HoodieAvroDataBlock extends HoodieDataBlock {

  public HoodieAvroDataBlock(Supplier<SeekableDataInputStream> inputStreamSupplier,
                             Option<byte[]> content,
                             boolean readBlockLazily,
                             HoodieLogBlockContentLocation logBlockContentLocation,
                             Option<Schema> readerSchema,
                             Map<HeaderMetadataType, String> header,
                             Map<FooterMetadataType, String> footer,
                             String keyField) {
    super(content, inputStreamSupplier, readBlockLazily, Option.of(logBlockContentLocation), readerSchema, header, footer, keyField, false);
  }

  public HoodieAvroDataBlock(@Nonnull List<HoodieRecord> records,
                             @Nonnull Map<HeaderMetadataType, String> header,
                             @Nonnull String keyField) {
    super(records, header, new HashMap<>(), keyField);
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.AVRO_DATA_BLOCK;
  }

  @Override
  protected ByteArrayOutputStream serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException {
    Schema schema = AvroSchemaCache.intern(new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA)));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(baos)) {
      // 1. Write out the log block version
      output.writeInt(HoodieLogBlock.version);

      // 2. Write total number of records
      output.writeInt(records.size());

      // 3. Write the records
      Properties props = initProperties(storage.getConf());
      for (HoodieRecord<?> s : records) {
        try {
          // Encode the record into bytes
          // Spark Record not support write avro log
          ByteArrayOutputStream data = s.getAvroBytes(schema, props);
          // Write the record size
          output.writeInt(data.size());
          // Write the content
          data.writeTo(output);
        } catch (IOException e) {
          throw new HoodieIOException("IOException converting HoodieAvroDataBlock to bytes", e);
        }
      }
    }
    return baos;
  }

  // TODO (na) - Break down content into smaller chunks of byte [] to be GC as they are used
  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException {
    checkState(this.readerSchema != null, "Reader's schema has to be non-null");
    checkArgument(type != HoodieRecordType.SPARK, "Not support read avro to spark record");
    // TODO AvroSparkReader need
    RecordIterator iterator = RecordIterator.getInstance(this, content, true);
    return new CloseableMappingIterator<>(iterator, data -> (HoodieRecord<T>) new HoodieAvroIndexedRecord(data));
  }

  /**
   * Streaming deserialization of records.
   *
   * @param inputStream The input stream from which to read the records.
   * @param contentLocation The location within the input stream where the content starts.
   * @param bufferSize The size of the buffer to use for reading the records.
   * @return A ClosableIterator over HoodieRecord<T>.
   * @throws IOException If there is an error reading or deserializing the records.
   */
  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(
          SeekableDataInputStream inputStream,
          HoodieLogBlockContentLocation contentLocation,
          HoodieRecordType type,
          int bufferSize
  ) throws IOException {
    StreamingRecordIterator iterator = StreamingRecordIterator.getInstance(this, inputStream, contentLocation, bufferSize);
    return new CloseableMappingIterator<>(iterator, data -> (HoodieRecord<T>) new HoodieAvroIndexedRecord(data));
  }

  @Override
  protected <T> ClosableIterator<T> deserializeRecords(HoodieReaderContext<T> readerContext, byte[] content) throws IOException {
    checkState(this.readerSchema != null, "Reader's schema has to be non-null");
    RecordIterator iterator = RecordIterator.getInstance(this, content, readerContext.enableLogicalTimestampFieldRepair());
    return new CloseableMappingIterator<>(iterator, data -> readerContext.getRecordContext().convertAvroRecord(data));
  }

  private static class RecordIterator implements ClosableIterator<IndexedRecord> {
    private byte[] content;
    private final SizeAwareDataInputStream dis;
    private final GenericDatumReader<IndexedRecord> reader;
    private final ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();
    private Option<HoodieSchema> promotedSchema = Option.empty();
    private int totalRecords = 0;
    private int readRecords = 0;

    private RecordIterator(HoodieSchema readerSchema, HoodieSchema writerSchema, byte[] content, boolean enableLogicalTimestampFieldRepair) throws IOException {
      this.content = content;

      this.dis = new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(this.content)));

      // 1. Read version for this data block
      int version = this.dis.readInt();
      if (new HoodieAvroDataBlockVersion(version).hasRecordCount()) {
        this.totalRecords = this.dis.readInt();
      }

      // writer schema could refer to table schema.
      // avoid this for MDT for sure.
      // and for tables having no logical ts column.
      Schema repairedWriterSchema = enableLogicalTimestampFieldRepair
          ? AvroSchemaRepair.repairLogicalTypes(writerSchema.toAvroSchema(), readerSchema.toAvroSchema()) : writerSchema.toAvroSchema();
      if (recordNeedsRewriteForExtendedAvroTypePromotion(repairedWriterSchema, readerSchema.toAvroSchema())) {
        this.reader = new GenericDatumReader<>(repairedWriterSchema, repairedWriterSchema);
        this.promotedSchema = Option.of(readerSchema);
      } else {
        this.reader = new GenericDatumReader<>(repairedWriterSchema, readerSchema.toAvroSchema());
      }
    }

    public static RecordIterator getInstance(HoodieAvroDataBlock dataBlock, byte[] content, boolean enableLogicalTimestampFieldRepair) throws IOException {
      return new RecordIterator(dataBlock.readerSchema, dataBlock.getSchemaFromHeader(), content, enableLogicalTimestampFieldRepair);
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
        if (this.promotedSchema.isPresent()) {
          return HoodieAvroUtils.rewriteRecordWithNewSchema(record, this.promotedSchema.get().toAvroSchema());
        }
        return record;
      } catch (IOException e) {
        throw new HoodieIOException("Unable to convert bytes to record.", e);
      }
    }
  }

  /**
   * {@code StreamingRecordIterator} is an iterator for reading records from a Hoodie log block in streaming manner.
   * It decodes the given input stream into Avro records with optional schema promotion.
   *
   * <p>This iterator ensures that the buffer has enough data for each record and handles buffer setup,
   * including compaction and resizing when necessary.
   */
  private static class StreamingRecordIterator implements ClosableIterator<IndexedRecord> {
    private static final int RECORD_LENGTH_BYTES = 4;
    // The minimum buffer size in bytes
    private static final int MIN_BUFFER_SIZE = RECORD_LENGTH_BYTES;
    private final SeekableDataInputStream inputStream;
    private final GenericDatumReader<IndexedRecord> reader;
    private final ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();
    private Option<HoodieSchema> promotedSchema = Option.empty();
    private int totalRecords = 0;
    private int readRecords = 0;
    private ByteBuffer buffer;

    private StreamingRecordIterator(HoodieSchema readerSchema, HoodieSchema writerSchema, SeekableDataInputStream inputStream,
        HoodieLogBlockContentLocation contentLocation, int bufferSize) throws IOException {
      // Negative values should not be used because they are generally considered to indicate the operation of closing stream reading,
      // in order to avoid confusing users into thinking that stream reading can be closed.
      checkArgument(bufferSize > 0, "Buffer size must be greater than zero");
      bufferSize = Math.max(bufferSize, MIN_BUFFER_SIZE);

      this.inputStream = inputStream;

      // Seek to the start of the block
      this.inputStream.seek(contentLocation.getContentPositionInLogFile());

      // Read version for this data block
      int version = this.inputStream.readInt();
      if (new HoodieAvroDataBlockVersion(version).hasRecordCount()) {
        this.totalRecords = this.inputStream.readInt();
      }

      Schema repairedWriterSchema = AvroSchemaRepair.repairLogicalTypes(writerSchema.toAvroSchema(), readerSchema.toAvroSchema());
      if (recordNeedsRewriteForExtendedAvroTypePromotion(repairedWriterSchema, readerSchema.toAvroSchema())) {
        this.reader = new GenericDatumReader<>(repairedWriterSchema, repairedWriterSchema);
        this.promotedSchema = Option.of(readerSchema);
      } else {
        this.reader = new GenericDatumReader<>(repairedWriterSchema, readerSchema.toAvroSchema());
      }

      this.buffer = ByteBuffer.allocate(Math.min(bufferSize, Math.toIntExact(contentLocation.getBlockSize())));
      // The buffer defaults to read mode
      this.buffer.flip();
    }

    public static StreamingRecordIterator getInstance(HoodieAvroDataBlock dataBlock, SeekableDataInputStream inputStream,
        HoodieLogBlockContentLocation contentLocation, int bufferSize) throws IOException {
      return new StreamingRecordIterator(dataBlock.readerSchema, dataBlock.getSchemaFromHeader(), inputStream, contentLocation, bufferSize);
    }

    @Override
    public void close() {
      this.decoderCache.remove();
      this.buffer = null;
      try {
        this.inputStream.close();
      } catch (IOException ex) {
        throw new HoodieIOException("Failed to close input stream", ex);
      }
    }

    @Override
    public boolean hasNext() {
      return readRecords < totalRecords;
    }

    @Override
    public IndexedRecord next() {
      try {
        ensureBufferHasData(RECORD_LENGTH_BYTES);

        // Read the record length
        int recordLength = buffer.getInt();

        // Ensure buffer is large enough and has enough data
        ensureBufferHasData(recordLength);

        // Decode the record
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buffer.array(), buffer.position(), recordLength, this.decoderCache.get());
        this.decoderCache.set(decoder);
        IndexedRecord record = this.reader.read(null, decoder);
        buffer.position(buffer.position() + recordLength);
        this.readRecords++;
        if (this.promotedSchema.isPresent()) {
          return HoodieAvroUtils.rewriteRecordWithNewSchema(record, this.promotedSchema.get().toAvroSchema());
        }
        return record;
      } catch (IOException e) {
        throw new HoodieIOException("Unable to convert bytes to record", e);
      }
    }

    /**
     * Ensures that the buffer contains at least the specified amount of data.
     *
     * <p>This method checks if the buffer has the required amount of data. If not, it attempts to fill the buffer
     * by reading more data from the input stream. If the buffer's capacity is insufficient, it allocates a larger buffer.
     * If the end of the input stream is reached before the required amount of data is available, an exception is thrown.
     *
     * @param dataLength the amount of data (in bytes) that must be available in the buffer.
     * @throws IOException if an I/O error occurs while reading from the input stream.
     * @throws HoodieException if the end of the input stream is reached before the required amount of data is available.
     */
    private void ensureBufferHasData(int dataLength) throws IOException {
      // Check if the current buffer has enough space to read the required data length
      if (buffer.capacity() - buffer.position() < dataLength) {
        buffer.compact();
        // Reset the buffer to read mode
        buffer.flip();
      }

      // Check again if the buffer still doesn't have enough space after compaction
      if (buffer.capacity() - buffer.position() < dataLength) {
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.position() + dataLength);
        newBuffer.put(buffer);
        // Reset the new buffer to read mode
        newBuffer.flip();
        buffer = newBuffer;
      }

      while (buffer.remaining() < dataLength) {
        boolean hasMoreData = fillBuffer();
        if (!hasMoreData && buffer.remaining() < dataLength) {
          throw new HoodieException("Unable to read enough data from the input stream to fill the buffer");
        }
      }
    }

    /**
     * Attempts to fill the buffer with more data from the input stream.
     *
     * <p>This method reads data from the input stream into the buffer, starting at the current limit
     * and reading up to the capacity of the buffer. If the end of the input stream is reached,
     * it returns false. Otherwise, it updates the buffer's limit to reflect the new data and returns true.
     *
     * @return true if data was successfully read into the buffer; false if the end of the input stream was reached.
     * @throws IOException if an I/O error occurs while reading from the input stream.
     */
    private boolean fillBuffer() throws IOException {
      int bytesRead = inputStream.read(buffer.array(), buffer.limit(), buffer.capacity() - buffer.limit());
      if (bytesRead == -1) {
        return false;
      }

      buffer.limit(buffer.limit() + bytesRead);
      return true;
    }
  }

  protected Properties initProperties(StorageConfiguration<?> storageConfig) {
    return CollectionUtils.emptyProps();
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
      try (OutputStream out = new DeflaterOutputStream(baos)) {
        out.write(getUTF8Bytes(text));
      }
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
      return fromUTF8Bytes(baos.toByteArray());
    } catch (IOException e) {
      throw new HoodieIOException("IOException while decompressing text", e);
    }
  }

  @Deprecated
  public byte[] getBytes(Schema schema) throws IOException {

    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(baos)) {
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

          // Write the record size
          output.writeInt(temp.size());
          // Write the content
          temp.writeTo(output);
          itr.remove();
        } catch (IOException e) {
          throw new HoodieIOException("IOException converting HoodieAvroDataBlock to bytes", e);
        }
      }
    }
    return baos.toByteArray();
  }
}
