package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;
import static org.apache.hudi.io.storage.HoodieOrcConfig.AVRO_SCHEMA_METADATA_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieOrcWriter {
  final Path filePath = new Path("/tmp/f1_1-0-1_000.orc");

  @BeforeEach
  @AfterEach
  public void clearTest() {
    File file = new File(filePath.toString());
    file.delete();
  }

  private HoodieOrcWriter createOrcWriter(Schema avroSchema) throws IOException {
    String instantTime = "000";
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.001, -1, BloomFilterTypeCode.SIMPLE.name());
    HoodieOrcConfig config = new HoodieOrcConfig(new Configuration(), CompressionKind.ZLIB, 64 * 1024 * 1024, 120 * 1024 * 1024, 120 * 1024 * 1024, filter);
    TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
    return new HoodieOrcWriter(instantTime, filePath, config, avroSchema, mockTaskContextSupplier);
  }

  @Test
  public void testWritePrimitive() throws IOException {
    Schema avroSchema = new Schema.Parser().parse("{\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"default\": null,\n" +
        "      \"name\": \"long_value\",\n" +
        "      \"type\": [\n" +
        "        \"null\",\n" +
        "        \"long\"\n" +
        "      ]\n" +
        "    },\n" +
        "    {\n" +
        "      \"default\": null,\n" +
        "      \"name\": \"string_value\",\n" +
        "      \"type\": [\n" +
        "        \"null\",\n" +
        "        \"string\"\n" +
        "      ]\n" +
        "    }\n" +
        "  ],\n" +
        "  \"name\": \"test\",\n" +
        "  \"namespace\": \"test\",\n" +
        "  \"type\": \"record\"\n" +
        "}");
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("long_value", 123L);
    record.put("string_value", "test");
    writer.writeAvro("unit-test-key", record);
    writer.close();

    Reader orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(new Configuration()));
    assertEquals("struct<long_value:bigint,string_value:string>", orcReader.getSchema().toString());
    assertEquals(1, orcReader.getNumberOfRows());
    assertEquals(4, orcReader.getMetadataKeys().size());
    assertTrue(orcReader.getMetadataKeys().contains(HOODIE_MIN_RECORD_KEY_FOOTER));
    ByteBuffer minRecordKey = orcReader.getMetadataValue(HOODIE_MIN_RECORD_KEY_FOOTER);
    assertEquals(StandardCharsets.US_ASCII.decode(minRecordKey).toString(), "unit-test-key");
    assertTrue(orcReader.getMetadataKeys().contains(HOODIE_MAX_RECORD_KEY_FOOTER));
    ByteBuffer maxRecordKey = orcReader.getMetadataValue(HOODIE_MIN_RECORD_KEY_FOOTER);
    assertEquals(StandardCharsets.US_ASCII.decode(maxRecordKey).toString(), "unit-test-key");
    assertTrue(orcReader.getMetadataKeys().contains(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
    ByteBuffer filterBuffer = orcReader.getMetadataValue(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    String filterStr = StandardCharsets.US_ASCII.decode(filterBuffer).toString();
    BloomFilter filter = BloomFilterFactory.fromString(filterStr, BloomFilterTypeCode.SIMPLE.name());
    assertTrue(filter.mightContain("unit-test-key"));
    assertFalse(filter.mightContain("non-existent-key"));
    assertTrue(orcReader.getMetadataKeys().contains(AVRO_SCHEMA_METADATA_KEY));
    ByteBuffer schemaBuffer = orcReader.getMetadataValue(AVRO_SCHEMA_METADATA_KEY);
    assertEquals(avroSchema.toString(), StandardCharsets.US_ASCII.decode(schemaBuffer).toString());
    assertEquals(CompressionKind.ZLIB.name(), orcReader.getCompressionKind().toString());
  }
}

