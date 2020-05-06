package org.apache.hudi.utilities;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.utilities.transform.MongoAvroConverter;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class TestMongoAvroConverter {

  @Test
  public void testReadKey() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Path sampleSchemaPath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleSchema.avsc");
    String sampleSchemaStr = new String(Files.readAllBytes(sampleSchemaPath));
    Path sampleKeyPath = Paths.get("src/test/resources/unitTest/TestMongoAvroConverterSampleOplogKey.json");
    String sampleKeyStr = new String(Files.readAllBytes(sampleKeyPath));
    Schema schema = parser.parse(sampleSchemaStr);
    MongoAvroConverter transformer = new MongoAvroConverter(schema);
    String createSampleId = "55555505d648da1824d45a1d";
    assertEquals(createSampleId, transformer.readKey(sampleKeyStr));


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
    GenericRecord resultUpdate = transformer.keyValueTransform(sampleKeyStr, sampleUpdateValueStr);
    GenericRecord resultCreate = transformer.keyValueTransform(sampleKeyStr, sampleCreateValueStr);

    String updateSampleId = "55555505d648da1824d45a1d";
    String updateSampleOp = "u";
    Long updateSampleTsMs = 1587506573735L;
    String updateSamplePatch = "{\"$v\": 1,\"$set\": {\"e\": false,\"l\": {\"$date\":1587409165984}}}";

    assertEquals(updateSampleId, resultUpdate.get("id"));
    assertEquals(updateSampleOp, resultUpdate.get("op"));
    assertEquals(updateSampleTsMs, resultUpdate.get("ts_ms"));
    assertEquals(updateSamplePatch, resultUpdate.get("patch"));

    String createSampleId = "55555505d648da1824d45a1d";
    String createSampleOp = "c";
    Long createSampleTsMs = 1587506600617L;
    String createSamplePatch = "null";
    String createSampleIpc = "USD";
    Boolean createSampleLfd = false;
    Boolean createSampleCb = false;
    String createSampleIfc = "USD";
    Double createSampleCs = 2.55;
    Long createSampleQ = 1L;
    List<Long> createSampleTestField = Arrays.asList(1L, 2L, 3L, 4L);

    assertEquals(createSampleId, resultCreate.get("id"));
    assertEquals(createSampleOp, resultCreate.get("op"));
    assertEquals(createSampleTsMs, resultCreate.get("ts_ms"));
    assertEquals(createSamplePatch, resultCreate.get("patch"));
    assertEquals(createSampleIpc, resultCreate.get("ipc"));
    assertEquals(createSampleLfd, resultCreate.get("lfd"));
    assertEquals(createSampleCb, resultCreate.get("cb"));
    assertEquals(createSampleIfc, resultCreate.get("ifc"));
    assertEquals(createSampleCs, resultCreate.get("cs"));
    assertEquals(createSampleQ, resultCreate.get("q"));
    assertEquals(createSampleTestField, resultCreate.get("testfield"));
  }

}
