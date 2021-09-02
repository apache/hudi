package org.apache.hudi.connect.writers;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Base Hudi Writer that manages reading the raw Kafka records and
 * converting them to {@link HoodieRecord}s that can be written to Hudi by
 * the derived implementations of this class.
 */
public abstract class AbstractHudiConnectWriter implements ConnectWriter<WriteStatus> {

  public static final String KAFKA_AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
  public static final String KAFKA_JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
  public static final String KAFKA_STRING_CONVERTER = "org.apache.kafka.connect.storage.StringConverter";
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHudiConnectWriter.class);

  private final HudiConnectConfigs connectConfigs;
  private final KeyGenerator keyGenerator;
  private final SchemaProvider schemaProvider;

  public AbstractHudiConnectWriter(HudiConnectConfigs connectConfigs,
                                   KeyGenerator keyGenerator,
                                   SchemaProvider schemaProvider) {
    this.connectConfigs = connectConfigs;
    this.keyGenerator = keyGenerator;
    this.schemaProvider = schemaProvider;
  }

  @Override
  public void writeRecord(SinkRecord record) throws IOException {
    AvroConvertor convertor = new AvroConvertor(schemaProvider.getSourceSchema());
    Option<GenericRecord> avroRecord;
    switch (connectConfigs.getKafkaValueConverter()) {
      case KAFKA_AVRO_CONVERTER:
        avroRecord = Option.of((GenericRecord) record.value());
        break;
      case KAFKA_STRING_CONVERTER:
        avroRecord = Option.of(convertor.fromJson((String) record.value()));
        break;
      case KAFKA_JSON_CONVERTER:
        throw new UnsupportedEncodingException("Currently JSON objects are not supported");
      default:
        throw new IOException("Unsupported Kafka Format type (" + connectConfigs.getKafkaValueConverter() + ")");
    }

    HoodieRecord hudiRecord = new HoodieRecord<>(keyGenerator.getKey(avroRecord.get()), new HoodieAvroPayload(avroRecord));
    writeHudiRecord(hudiRecord);
  }

  @Override
  public List<WriteStatus> close() {
    return flushHudiRecords();
  }

  protected abstract void writeHudiRecord(HoodieRecord<HoodieAvroPayload> record);

  protected abstract List<WriteStatus> flushHudiRecords();
}
