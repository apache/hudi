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

package org.apache.hudi.utilities.decoders;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.kafka.common.errors.SerializationException;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Extending {@link KafkaAvroSchemaDeserializer} as we need to be able to inject reader schema during deserialization.
 */
public class KafkaAvroSchemaDeserializer extends KafkaAvroDeserializer {

  private static final String SCHEMA_PROVIDER_CLASS_PROP = "hoodie.deltastreamer.schemaprovider.class";

  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private Schema sourceSchema;

  public KafkaAvroSchemaDeserializer() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    try {
      TypedProperties props = getConvertToTypedProperties(configs);
      SchemaProvider schemaProvider = UtilHelpers.createSchemaProvider(
          props.getString(SCHEMA_PROVIDER_CLASS_PROP), props, null);
      sourceSchema = Objects.requireNonNull(schemaProvider).getSourceSchema();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Pretty much copy-paste from the {@link AbstractKafkaAvroDeserializer} except line 87:
   * DatumReader reader = new GenericDatumReader(schema, sourceSchema);
   * <p>
   * We need to inject reader schema during deserialization or later stages of the pipeline break.
   *
   * @param includeSchemaAndVersion
   * @param topic
   * @param isKey
   * @param payload
   * @param readerSchema
   * @return
   * @throws SerializationException
   */
  @Override
  protected Object deserialize(
      boolean includeSchemaAndVersion,
      String topic,
      Boolean isKey,
      byte[] payload,
      Schema readerSchema)
      throws SerializationException {
    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle
    // this case.
    if (payload == null) {
      return null;
    }
    int id = -1;
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      id = buffer.getInt();
      Schema schema = schemaRegistry.getByID(id);

      int length = buffer.limit() - 1 - idSize;
      final Object result;
      if (schema.getType().equals(Schema.Type.BYTES)) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, length);
        result = bytes;
      } else {
        int start = buffer.position() + buffer.arrayOffset();
        DatumReader reader = new GenericDatumReader(schema, sourceSchema);
        Object object =
            reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

        if (schema.getType().equals(Schema.Type.STRING)) {
          object = object.toString(); // Utf8 -> String
        }
        result = object;
      }

      if (includeSchemaAndVersion) {
        // Annotate the schema with the version. Note that we only do this if the schema +
        // version are requested, i.e. in Kafka Connect converters. This is critical because that
        // code *will not* rely on exact schema equality. Regular deserializers *must not* include
        // this information because it would return schemas which are not equivalent.
        //
        // Note, however, that we also do not fill in the connect.version field. This allows the
        // Converter to let a version provided by a Kafka Connect source take priority over the
        // schema registry's ordering (which is implicit by auto-registration time rather than
        // explicit from the Connector).
        Integer version = schemaRegistry.getVersion(getSubjectName(topic, isKey), schema);
        if (schema.getType() == Schema.Type.UNION) {
          // Can't set additional properties on a union schema since it's just a list, so set it
          // on the first non-null entry
          for (Schema memberSchema : schema.getTypes()) {
            if (memberSchema.getType() != Schema.Type.NULL) {
              memberSchema.addProp(
                  SCHEMA_REGISTRY_SCHEMA_VERSION_PROP,
                  JsonNodeFactory.instance.numberNode(version));
              break;
            }
          }
        } else {
          schema.addProp(
              SCHEMA_REGISTRY_SCHEMA_VERSION_PROP, JsonNodeFactory.instance.numberNode(version));
        }
        if (schema.getType().equals(Schema.Type.RECORD)) {
          return result;
        } else {
          return new NonRecordContainer(schema, result);
        }
      } else {
        return result;
      }
    } catch (IOException | RuntimeException e) {
      // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
      throw new SerializationException("Error deserializing Avro message for id " + id, e);
    } catch (RestClientException e) {
      throw new SerializationException("Error retrieving Avro schema for id " + id, e);
    }
  }

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer;
  }

  private TypedProperties getConvertToTypedProperties(Map<String, ?> configs) {
    TypedProperties typedProperties = new TypedProperties();
    for (Entry<String, ?> entry : configs.entrySet()) {
      typedProperties.put(entry.getKey(), entry.getValue());
    }
    return typedProperties;
  }
}
