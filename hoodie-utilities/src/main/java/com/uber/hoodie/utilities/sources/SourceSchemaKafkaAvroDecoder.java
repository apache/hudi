package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.utilities.UtilHelpers;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

/** A Kafka decoder that uses the source schema for read. */
public class SourceSchemaKafkaAvroDecoder extends AbstractKafkaAvroDeserializer
    implements Decoder<Object> {

  private static final String SCHEMA_PROVIDER_CLASS_PROP = "hoodie.deltastreamer.schemaprovider.class";

  private final Schema sourceSchema;

  public SourceSchemaKafkaAvroDecoder(VerifiableProperties props) {
    this.configure(new KafkaAvroDeserializerConfig(props.props()));

    TypedProperties typedProperties = new TypedProperties();
    copyProperties(typedProperties, props.props());

    try {
      SchemaProvider schemaProvider =
          UtilHelpers.createSchemaProvider(
              props.getString(SCHEMA_PROVIDER_CLASS_PROP), typedProperties);
      sourceSchema = Objects.requireNonNull(schemaProvider).getSourceSchema();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    return deserialize(bytes);
  }

  @Override
  protected Object deserialize(
      boolean includeSchemaAndVersion,
      String topic,
      Boolean isKey,
      byte[] payload,
      Schema readerSchema)
      throws SerializationException {
    if (readerSchema != null) {
      return super.deserialize(includeSchemaAndVersion, topic, isKey, payload, readerSchema);
    }

    return super.deserialize(includeSchemaAndVersion, topic, isKey, payload, sourceSchema);
  }

  private static void copyProperties(TypedProperties typedProperties, Properties properties) {
    for (Entry<Object, Object> entry : properties.entrySet()) {
      typedProperties.put(entry.getKey(), entry.getValue());
    }
  }
}
