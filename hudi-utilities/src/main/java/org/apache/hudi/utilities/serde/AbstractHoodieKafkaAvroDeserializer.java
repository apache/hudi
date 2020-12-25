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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.serde.config.HoodieKafkaAvroDeserializationConfig;

import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.utilities.serde.config.HoodieKafkaAvroDeserializationConfig.SCHEMA_PROVIDER_CLASS_PROP;

public class AbstractHoodieKafkaAvroDeserializer {

  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private boolean useSpecificAvroReader = false;
  private Schema sourceSchema;
  private Schema targetSchema;

  public AbstractHoodieKafkaAvroDeserializer(VerifiableProperties properties) {
    // this.sourceSchema = new Schema.Parser().parse(properties.props().getProperty(FilebasedSchemaProvider.Config.SOURCE_SCHEMA_PROP));
    TypedProperties typedProperties = new TypedProperties();
    copyProperties(typedProperties, properties.props());
    try {
      SchemaProvider schemaProvider = UtilHelpers.createSchemaProvider(
          typedProperties.getString(SCHEMA_PROVIDER_CLASS_PROP), typedProperties, null);
      this.sourceSchema = Objects.requireNonNull(schemaProvider).getSourceSchema();
      this.targetSchema = Objects.requireNonNull(schemaProvider).getTargetSchema();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void copyProperties(TypedProperties typedProperties, Properties properties) {
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      typedProperties.put(entry.getKey(), entry.getValue());
    }
  }

  protected void configure(HoodieKafkaAvroDeserializationConfig config) {
    useSpecificAvroReader = config
        .getBoolean(HoodieKafkaAvroDeserializationConfig.SPECIFIC_AVRO_READER_CONFIG);
  }

  protected Object deserialize(byte[] payload) throws SerializationException {
    return deserialize(null, null, payload, targetSchema);
  }

  /**
   * Just like single-parameter version but accepts an Avro schema to use for reading.
   *
   * @param payload serialized data
   * @param readerSchema schema to use for Avro read (optional, enables Avro projection)
   * @return the deserialized object
   */
  protected Object deserialize(byte[] payload, Schema readerSchema) throws SerializationException {
    return deserialize(null, null, payload, readerSchema);
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
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error deserializing payload: ", e);
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
