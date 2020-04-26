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

package org.apache.hudi.utilities.serde;

import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.serde.config.HoodieKafkaAvroDeserializationConfig;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AbstractHoodieKafkaAvroDeserializer {

  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private boolean useSpecificAvroReader = false;
  private Schema sourceSchema;

  public AbstractHoodieKafkaAvroDeserializer(VerifiableProperties properties) {
    this.sourceSchema = new Schema.Parser().parse(properties.props().getProperty(FilebasedSchemaProvider.Config.SOURCE_SCHEMA_PROP));
  }

  protected void configure(HoodieKafkaAvroDeserializationConfig config) {
    useSpecificAvroReader = config
      .getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
  }

  protected Object deserialize(byte[] payload) throws SerializationException {
    return deserialize(null, null, payload, sourceSchema);
  }

  protected Object deserialize(String topic, Boolean isKey, byte[] payload, Schema readerSchema) {
    try {
      ByteBuffer buffer = this.getByteBuffer(payload);
      int id = buffer.getInt();
      int length = buffer.limit() - 1 - 4;
      Object result;
      if (sourceSchema.getType().equals(Schema.Type.BYTES)) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, length);
        result = bytes;
      } else {
        int start = buffer.position() + buffer.arrayOffset();
        DatumReader reader = this.getDatumReader(sourceSchema, readerSchema);
        Object object = reader.read(null, this.decoderFactory.binaryDecoder(buffer.array(), start, length, null));
        if (sourceSchema.getType().equals(Schema.Type.STRING)) {
          object = object.toString();
        }

        result = object;
      }
      return result;
    } catch (IOException ioe) {
      throw new SerializationException("Error deserializing payload: ", ioe);
    }
  }

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != 0) {
      throw new SerializationException("Unknown magic byte!");
    } else {
      return buffer;
    }
  }

  private DatumReader getDatumReader(Schema writerSchema, Schema readerSchema) {
    if (this.useSpecificAvroReader) {
      return new SpecificDatumReader(writerSchema, readerSchema);
    } else {
      return new GenericDatumReader(writerSchema, readerSchema);
    }
  }
}
