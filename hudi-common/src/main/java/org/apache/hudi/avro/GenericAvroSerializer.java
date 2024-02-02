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

package org.apache.hudi.avro;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;


/**
 * Custom serializer used for generic Avro containers.
 * <p>
 * Heavily adapted from:
 * <p>
 * <a href="https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/serializer/GenericAvroSerializer.scala">GenericAvroSerializer.scala</a>
 * <p>
 * As {@link org.apache.hudi.common.util.SerializationUtils} is not shared between threads and does not concern any
 * shuffling operations, compression and decompression cache is omitted as network IO is not a concern.
 * <p>
 * Unlike Spark's implementation, the class and constructor is not initialized with a predefined map of avro schemas.
 * This is the case as schemas to read and write are not known beforehand.
 *
 * @param <D> the subtype of [[GenericContainer]] handled by this serializer
 */
public class GenericAvroSerializer<D extends GenericContainer> extends Serializer<D> {

  // reuses the same datum reader/writer since the same schema will be used many times
  private final HashMap<Schema, DatumWriter<D>> writerCache = new HashMap<>();
  private final HashMap<Schema, DatumReader<D>> readerCache = new HashMap<>();

  // cache results of Schema to bytes result as the same schema will be used many times
  private final HashMap<Schema, byte[]> encodeCache = new HashMap<>();
  private final HashMap<ByteBuffer, Schema> schemaCache = new HashMap<>();

  private byte[] getSchemaBytes(Schema schema) {
    if (encodeCache.containsKey(schema)) {
      return encodeCache.get(schema);
    } else {
      byte[] schemaBytes = getUTF8Bytes(schema.toString());
      encodeCache.put(schema, schemaBytes);
      return schemaBytes;
    }
  }

  private Schema getSchema(byte[] schemaBytes) {
    ByteBuffer schemaByteBuffer = ByteBuffer.wrap(schemaBytes);
    if (schemaCache.containsKey(schemaByteBuffer)) {
      return schemaCache.get(schemaByteBuffer);
    } else {
      String schema = fromUTF8Bytes(schemaBytes);
      Schema parsedSchema = new Schema.Parser().parse(schema);
      schemaCache.put(schemaByteBuffer, parsedSchema);
      return parsedSchema;
    }
  }

  private DatumWriter<D> getDatumWriter(Schema schema) {
    DatumWriter<D> writer;
    if (writerCache.containsKey(schema)) {
      writer = writerCache.get(schema);
    } else {
      writer = new GenericDatumWriter<>(schema);
      writerCache.put(schema, writer);
    }
    return writer;
  }

  private DatumReader<D> getDatumReader(Schema schema) {
    DatumReader<D> reader;
    if (readerCache.containsKey(schema)) {
      reader = readerCache.get(schema);
    } else {
      reader = new GenericDatumReader<>(schema);
      readerCache.put(schema, reader);
    }
    return reader;
  }

  private void serializeDatum(D datum, Output output) throws IOException {
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
    Schema schema = datum.getSchema();
    byte[] schemaBytes = getSchemaBytes(schema);
    output.writeInt(schemaBytes.length);
    output.writeBytes(schemaBytes);
    getDatumWriter(schema).write(datum, encoder);
    encoder.flush();
  }

  private D deserializeDatum(Input input) throws IOException {
    int schemaBytesLen = input.readInt();
    byte[] schemaBytes = input.readBytes(schemaBytesLen);
    Schema schema = getSchema(schemaBytes);
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, null);
    return getDatumReader(schema).read(null, decoder);
  }

  @Override
  public void write(Kryo kryo, Output output, D datum) {
    try {
      serializeDatum(datum, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public D read(Kryo kryo, Input input, Class<D> datumClass) {
    try {
      return deserializeDatum(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
