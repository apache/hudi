package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link OverwritePrecombineAvroPayload}.
 */
public class TestOverwritePrecombineAvroPayload {

  private Schema schema;

  @BeforeEach
  public void setUp() throws Exception {
    schema = Schema.createRecord(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
            new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", ""),
            new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
            new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)
    ));
  }

  @Test
  public void testActiveRecords() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "2");
    record2.put("partition", "");
    record2.put("ts", 1L);
    record2.put("_hoodie_is_deleted", false);

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "2");
    record3.put("partition", "partition0");
    record3.put("ts", 1L);
    record3.put("_hoodie_is_deleted", false);

    OverwriteWithLatestAvroPayload payload1 = new OverwritePrecombineAvroPayload(record1, 1);
    OverwriteWithLatestAvroPayload payload2 = new OverwritePrecombineAvroPayload(record2, 2);
    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    assertEquals(payload1.combineAndGetUpdateValue(record2, schema).get(), record1);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema).get(), record2);

    assertEquals(HoodieAvroUtils.bytesToAvro(payload1.preCombine(payload2, schema).recordBytes, schema),
            record3);
  }
}
