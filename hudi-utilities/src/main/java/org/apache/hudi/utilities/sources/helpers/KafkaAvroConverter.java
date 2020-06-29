package org.apache.hudi.utilities.sources.helpers;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Convert a list of Kafka ConsumerRecord<String, String> to GenericRecord.
 */
public abstract class KafkaAvroConverter implements Serializable {
  private final String schemaStr;

  public KafkaAvroConverter(String schemaStr) {
    this.schemaStr = schemaStr;
  }

  private Schema getSchema() {
    return new Schema.Parser().parse(schemaStr);
  }

  public Iterator<GenericRecord> apply(Iterator<ConsumerRecord<Object, Object>> records) {
    if (!records.hasNext()) {
      return Collections.emptyIterator();
    } else {
      final Schema schema = getSchema();
      return StreamSupport
          .stream(Spliterators.spliteratorUnknownSize(records, Spliterator.ORDERED), false)
          // Ignore tombstone record
          .filter((r) -> r.value() != null)
          .map((r) -> transform(schema, (String) r.key(), (String) r.value())).iterator();
    }
  }

  protected abstract GenericRecord transform(Schema schema, String key, String value);
}
