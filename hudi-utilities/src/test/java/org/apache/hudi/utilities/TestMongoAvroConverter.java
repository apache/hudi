package org.apache.hudi.utilities;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.utilities.sources.helpers.MongoAvroConverter;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestMongoAvroConverter {

  @Test
  public void testDocumentId() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Path sampleSchemaPath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleSchema.avsc");
    String sampleSchemaStr = new String(Files.readAllBytes(sampleSchemaPath));
    Path sampleKeyPath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleOplogKey.json");
    String sampleKeyStr = new String(Files.readAllBytes(sampleKeyPath));
    Schema schema = parser.parse(sampleSchemaStr);
    MongoAvroConverter transformer = new MongoAvroConverter(schema);
    String createSampleId = "55555505d648da1824d45a1d";
    assertEquals(createSampleId, transformer.getDocumentId(sampleKeyStr));
  }

  @Test
  public void testKeyValueTransform() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Path sampleSchemaPath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleSchema.avsc");
    String sampleSchemaStr = new String(Files.readAllBytes(sampleSchemaPath));
    Path sampleUpdateValuePath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleOplogUpdate.json");
    String sampleUpdateValueStr = new String(Files.readAllBytes(sampleUpdateValuePath));
    Path sampleCreateValuePath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleOplogCreate.json");
    String sampleCreateValueStr = new String(Files.readAllBytes(sampleCreateValuePath));
    Path sampleKeyPath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleOplogKey.json");
    String sampleKeyStr = new String(Files.readAllBytes(sampleKeyPath));

    Schema schema = parser.parse(sampleSchemaStr);
    MongoAvroConverter transformer = new MongoAvroConverter(schema);
    GenericRecord resultUpdate = transformer.transform(sampleKeyStr, sampleUpdateValueStr);
    GenericRecord resultCreate = transformer.transform(sampleKeyStr, sampleCreateValueStr);

    String updateSampleId = "55555505d648da1824d45a1d";
    String updateSampleOp = "u";
    Long updateSampleTsMs = 1587409166000L;
    String updateSamplePatch = "{\"$v\": 1,\"$set\": {\"e\": false,\"l\": {\"$date\":1587409165984}}}";

    assertEquals(updateSampleId, resultUpdate.get("_id"));
    assertEquals(updateSampleOp, resultUpdate.get("_op"));
    assertEquals(updateSampleTsMs, resultUpdate.get("_ts_ms"));
    assertEquals(updateSamplePatch, resultUpdate.get("_patch"));

    String createSampleId = "55555505d648da1824d45a1d";
    String createSampleOp = "c";
    Long createSampleTsMs = 1587403470000L;
    String createSampleIpc = "USD";
    Boolean createSampleLfd = false;
    Boolean createSampleCb = false;
    String createSampleIfc = "USD";
    Double createSampleCs = 2.55;
    Long createSampleQ = 1L;
    List<Long> createSampleTestField = Arrays.asList(1L, 2L, 3L, 4L);

    assertEquals(createSampleId, resultCreate.get("_id"));
    assertEquals(createSampleOp, resultCreate.get("_op"));
    assertEquals(createSampleTsMs, resultCreate.get("_ts_ms"));
    assertNull(resultCreate.get("_patch"));
    assertEquals(createSampleIpc, resultCreate.get("incentive_payment_currency"));
    assertEquals(createSampleLfd, resultCreate.get("logistic_fee_deducted"));
    assertEquals(createSampleCb, resultCreate.get("chargeback"));
    assertEquals(createSampleIfc, resultCreate.get("incentive_fine_currency"));
    assertEquals(createSampleCs, resultCreate.get("_internal_merchant_cost"));
    assertEquals(createSampleQ, resultCreate.get("quantity"));
    assertEquals(createSampleTestField, resultCreate.get("testfield"));
  }

}
