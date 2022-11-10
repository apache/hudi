package org.apache.hudi.common.model;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link TestHoodieRecordPayload}.
 */
public class TestHoodieRecordPayload {
  private Schema tableSchema;
  private Schema recordSchema;
  private Properties props;

  @BeforeEach
  public void setUp() throws Exception {
    tableSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)
    ));

    props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition");

  }

  @ParameterizedTest
  @ValueSource(booleans = true)
  public void testGetInsertValueAfterDropPartitionFields(boolean dropPartitionFields) throws IOException {
    props.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), String.valueOf(dropPartitionFields));
    recordSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)));
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("id", "1");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);
    Option<GenericRecord> recordOption = Option.of(record);
    HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(recordOption);
    assertEquals(hoodieAvroPayload.getInsertValue(tableSchema, props), recordOption);
  }

  @ParameterizedTest
  @ValueSource(booleans = false)
  public void testGetInsertValueWithPartitionFields(boolean dropPartitionFields) throws IOException {
    props.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), String.valueOf(dropPartitionFields));
    recordSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)));
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("id", "1");
    record.put("partition", "001");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);
    Option<GenericRecord> recordOption = Option.of(record);
    HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(recordOption);
    assertEquals(hoodieAvroPayload.getInsertValue(tableSchema, props), recordOption);
  }
}
