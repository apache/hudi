package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.AvroConversionUtils;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.types.StructType;

/**
 * Convert a list of Kafka ConsumerRecord<String, String> to GenericRecord.
 */
public abstract class KafkaAvroConverter implements Serializable {
  // Use GenericRecord as schema wrapper to leverage Spark Kyro serialization.
  
  private final StructType avroSchema;
  private final String structName;

  public KafkaAvroConverter(StructType schema, String name) {
    avroSchema = schema;
    structName = name;
  }

  public Schema getSchema() {
    return AvroConversionUtils.convertStructTypeToAvroSchema(avroSchema, structName,
        "hudi." + structName);
  }

  public Iterator<GenericRecord> apply(Iterator<ConsumerRecord<Object, Object>> records) {
    if (!records.hasNext()) {
      return Collections.emptyIterator();
    } else {
      return StreamSupport
          .stream(Spliterators.spliteratorUnknownSize(records, Spliterator.ORDERED), false)
          // Ignore tombstone record
          .filter((r) -> r.value() != null)
          .map((r) -> transform((String)r.key(), (String)r.value())).iterator();
    }
  }

  protected abstract GenericRecord transform(String key, String value);
}
