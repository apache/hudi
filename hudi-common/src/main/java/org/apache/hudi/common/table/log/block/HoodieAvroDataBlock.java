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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.SizeAwareDataInputStream;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

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

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieAvroDataBlock contains a list of records serialized using Avro. It is used with the Parquet base file format.
 */
public class HoodieAvroDataBlock extends HoodieDataBlock {

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
    Iterator<HoodieRecord> itr = records.iterator();
    while (itr.hasNext()) {
      try {
        // Encode the record into bytes
        byte[] bs = HoodieAvroUtils.recordToBytes(itr.next(), schema).get();
        // Get the size of the bytes
        // Write the record size
        output.writeInt(bs.length);
        // Write the content
        output.write(bs);
      } catch (IOException e) {
        throw new HoodieIOException("IOException converting HoodieAvroDataBlock to bytes", e);
      }
    }
    output.close();
    return baos.toByteArray();
  }

  // TODO (na) - Break down content into smaller chunks of byte [] to be GC as they are used
  // TODO (na) - Implement a recordItr instead of recordList
  @Override
  protected List<HoodieRecord> deserializeRecords(byte[] content, HoodieRecordMapper mapper) throws IOException {
    checkState(readerSchema != null, "Reader's schema has to be non-null");

    SizeAwareDataInputStream dis =
        new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(content)));

    // 1. Read version for this data block
    int version = dis.readInt();
    HoodieAvroDataBlockVersion logBlockVersion = new HoodieAvroDataBlockVersion(version);

    // Get schema from the header
    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    // 2. Get the total records
    int totalRecords;
    if (logBlockVersion.hasRecordCount()) {
      totalRecords = dis.readInt();
    } else {
      totalRecords = 0;
    }

    List<HoodieRecord> records = new ArrayList<>(totalRecords);

    // 3. Read the content
    for (int i = 0; i < totalRecords; i++) {
      int recordLength = dis.readInt();
      int offset = dis.getNumberOfBytesRead();

      // NOTE: To avoid copying bytes b/w buffers, we don't read from stream but instead
      //       read directly from the underlying buffer.
      //       Interface could be optimized further by leveraging {@code ByteBuffer} instead of raw
      //       byte arrays instead
      GenericRecord avroRecordPayload =
          HoodieAvroUtils.bytesToAvro(content, offset, recordLength, writerSchema, readerSchema);

      records.add(mapper.apply(avroRecordPayload));

      // Skip record in the stream
      dis.skipBytes(recordLength);
    }

    dis.close();

    return records;
  }

  //----------------------------------------------------------------------------------------
  //                                  DEPRECATED METHODS
  //
  // These methods were only supported by HoodieAvroDataBlock and have been deprecated. Hence,
  // these are only implemented here even though they duplicate the code from HoodieAvroDataBlock.
  //----------------------------------------------------------------------------------------

  // /**
  //  * This constructor is retained to provide backwards compatibility to HoodieArchivedLogs which were written using
  //  * HoodieLogFormat V1.
  //  */
  // @Deprecated
  // public HoodieAvroDataBlock(List<IndexedRecord> records, Schema schema) {
  //   super(records, Collections.singletonMap(HeaderMetadataType.SCHEMA, schema.toString()), new HashMap<>(), HoodieRecord.RECORD_KEY_METADATA_FIELD);
  // }
  //
  // /**
  //  * This method is retained to provide backwards compatibility to HoodieArchivedLogs which were written using
  //  * HoodieLogFormat V1.
  //  */
  // @Deprecated
  // public static HoodieAvroDataBlock getBlock(byte[] content, Schema readerSchema) throws IOException {
  //
  //   SizeAwareDataInputStream dis = new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(content)));
  //
  //   // 1. Read the schema written out
  //   int schemaLength = dis.readInt();
  //   byte[] compressedSchema = new byte[schemaLength];
  //   dis.readFully(compressedSchema, 0, schemaLength);
  //   Schema writerSchema = new Schema.Parser().parse(decompress(compressedSchema));
  //
  //   if (readerSchema == null) {
  //     readerSchema = writerSchema;
  //   }
  //
  //   GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
  //   // 2. Get the total records
  //   int totalRecords = dis.readInt();
  //   List<IndexedRecord> records = new ArrayList<>(totalRecords);
  //
  //   // 3. Read the content
  //   for (int i = 0; i < totalRecords; i++) {
  //     int recordLength = dis.readInt();
  //     Decoder decoder = DecoderFactory.get().binaryDecoder(content, dis.getNumberOfBytesRead(), recordLength, null);
  //     IndexedRecord record = reader.read(null, decoder);
  //     records.add(record);
  //     dis.skipBytes(recordLength);
  //   }
  //   dis.close();
  //   return new HoodieAvroDataBlock(records, readerSchema);
  // }
  //
  // private static byte[] compress(String text) {
  //   ByteArrayOutputStream baos = new ByteArrayOutputStream();
  //   try {
  //     OutputStream out = new DeflaterOutputStream(baos);
  //     out.write(text.getBytes(StandardCharsets.UTF_8));
  //     out.close();
  //   } catch (IOException e) {
  //     throw new HoodieIOException("IOException while compressing text " + text, e);
  //   }
  //   return baos.toByteArray();
  // }
  //
  // private static String decompress(byte[] bytes) {
  //   InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
  //   ByteArrayOutputStream baos = new ByteArrayOutputStream();
  //   try {
  //     byte[] buffer = new byte[8192];
  //     int len;
  //     while ((len = in.read(buffer)) > 0) {
  //       baos.write(buffer, 0, len);
  //     }
  //     return new String(baos.toByteArray(), StandardCharsets.UTF_8);
  //   } catch (IOException e) {
  //     throw new HoodieIOException("IOException while decompressing text", e);
  //   }
  // }
}
