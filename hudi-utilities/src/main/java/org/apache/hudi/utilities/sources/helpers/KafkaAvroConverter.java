package org.apache.hudi.utilities.sources.helpers;

import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Convert a list of Kafka ConsumerRecord<String, String> to GenericRecord.
 */
public abstract class KafkaAvroConverter {
  // Use GenericRecord as schema wrapper to leverage Spark Kyro serialization.
  private final GenericRecord schemaHolder;

  public KafkaAvroConverter(Schema schema) {
    schemaHolder = new GenericData.Record(schema);
  }

  public Schema getSchema() {
    return schemaHolder.getSchema();
  }

  public Iterator<GenericRecord> apply(Iterator<ConsumerRecord> records) {
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
