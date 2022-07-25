package org.apache.hudi.common.testutils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

// Utility for the schema of S3 events listed here (https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html)
public class S3EventsSchemaUtils {
  public static final String DEFAULT_STRING_VALUE = "default_string";

  public static String generateSchemaString() {
    return generateS3EventSchema().toString();
  }

  public static Schema generateObjInfoSchema() {
    Schema objInfo = SchemaBuilder.record("objInfo")
        .fields()
        .requiredString("key")
        .requiredLong("size")
        .endRecord();
    return objInfo;
  }

  public static GenericRecord generateObjInfoRecord(String key, Long size) {
    GenericRecord rec = new GenericData.Record(generateObjInfoSchema());
    rec.put("key", key);
    rec.put("size", size);
    return rec;
  }

  public static Schema generateS3MetadataSchema() {
    Schema s3Metadata = SchemaBuilder.record("s3Metadata")
        .fields()
        .requiredString("configurationId")
        .name("object")
        .type(generateObjInfoSchema())
        .noDefault()
        .endRecord();
    return s3Metadata;
  }

  public static GenericRecord generateS3MetadataRecord(GenericRecord objRecord) {
    GenericRecord rec = new GenericData.Record(generateS3MetadataSchema());
    rec.put("configurationId", DEFAULT_STRING_VALUE);
    rec.put("object", objRecord);
    return rec;
  }

  public static Schema generateS3EventSchema() {
    Schema s3Event = SchemaBuilder.record("s3Event")
        .fields()
        .requiredString("eventSource")
        .requiredString("eventName")
        .name("s3")
        .type(generateS3MetadataSchema())
        .noDefault()
        .endRecord();
    return s3Event;
  }

  public static GenericRecord generateS3EventRecord(String rowKey, GenericRecord objRecord) {
    GenericRecord rec = new GenericData.Record(generateS3EventSchema());
    rec.put("_row_key", rowKey);
    rec.put("eventSource", DEFAULT_STRING_VALUE);
    rec.put("eventName", DEFAULT_STRING_VALUE);
    rec.put("s3", generateS3MetadataRecord(objRecord));
    return rec;
  }
}
