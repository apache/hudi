package org.apache.hudi.connect.writers;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class AbstractHudiConnectWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHudiConnectWriter.class);

  private final HudiConnectConfigs connectConfigs;
  private final KeyGenerator keyGenerator;
  private final SchemaProvider schemaProvider;
  private final ObjectMapper mapper;

  public AbstractHudiConnectWriter(HudiConnectConfigs connectConfigs,
                                   KeyGenerator keyGenerator,
                                   SchemaProvider schemaProvider) {
    this.connectConfigs = connectConfigs;
    this.keyGenerator = keyGenerator;
    this.schemaProvider = schemaProvider;
    this.mapper = new ObjectMapper();
  }

  public void writeRecord(SinkRecord record) throws IOException {
    AvroConvertor convertor = new AvroConvertor(schemaProvider.getSourceSchema());
    Option<GenericRecord> avroRecord;
    switch (connectConfigs.getKafkaValueConverter()) {
      case "io.confluent.connect.avro.AvroConverter":
        avroRecord = Option.of((GenericRecord) record.value());
        break;
      case "org.apache.kafka.connect.json.JsonConverter":
        avroRecord = Option.of(convertor.fromJson(mapper.writeValueAsString(record.value())));
        break;
      case "org.apache.kafka.connect.storage.StringConverter":
        avroRecord = Option.of(convertor.fromJson((String) record.value()));
        break;
      default:
        throw new IOException("Unsupported Kafka Format type (" + connectConfigs.getKafkaValueConverter() + ")");
    }

    LOG.error("WNI VIMP " + avroRecord.get().toString());
    HoodieRecord hudiRecord = new HoodieRecord<>(keyGenerator.getKey(avroRecord.get()), new HoodieAvroPayload(avroRecord));
    writeHudiRecord(hudiRecord);
  }

  public List<WriteStatus> close() throws IOException {
    return flushHudiRecords();
  }

  protected abstract void writeHudiRecord(HoodieRecord<HoodieAvroPayload> record);

  protected abstract List<WriteStatus> flushHudiRecords();
}
