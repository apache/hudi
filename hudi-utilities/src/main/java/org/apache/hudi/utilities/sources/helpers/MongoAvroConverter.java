package org.apache.hudi.utilities.sources.helpers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.utilities.mongo.Operation;
import org.apache.hudi.utilities.mongo.SchemaUtils;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.codecs.BsonObjectIdCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;
import org.json.JSONObject;

public class MongoAvroConverter extends KafkaAvroConverter {

  // Internal OpLog fields
  private static final String AFTER_OPLOGFIELD = "after";
  private static final String OP_OPLOGFIELD = "op";
  private static final String TS_MS_OPLOGFIELD = "ts_ms";
  private static final String PATCH_OPLOGFIELD = "patch";
  private static final String ID_OPLOGFIELD = "id";
  private static final String PAYLOAD_OPLOGFIELD = "payload";
  private static final String SOURCE_OPLOGFIELD = "source";

  public MongoAvroConverter(Schema avroSchema) {
    super(avroSchema);
  }

  @Override
  public GenericRecord transform(String key, String value) {
    GenericRecord genericRecord = new Record(this.getSchema());
    genericRecord.put(SchemaUtils.ID_FIELD, getDocumentId(key));

    // Payload field may be absent when value.converter.schemas.enable=false
    JSONObject valueJson = new JSONObject(value);
    if (valueJson.has(PAYLOAD_OPLOGFIELD)) {
      valueJson = valueJson.getJSONObject(PAYLOAD_OPLOGFIELD);
    }

    // Op field is mandatory
    Operation op = Operation.forCode(valueJson.getString(OP_OPLOGFIELD));
    genericRecord.put(SchemaUtils.OP_FIELD, op.code());

    // Read timestamp from source field
    genericRecord.put(SchemaUtils.TS_MS_FIELD, getTimeMs(valueJson));

    // Insert/Read operation has after field
    if (op.equals(Operation.CREATE) || op.equals(Operation.READ)) {
      BsonDocument after = BsonDocument.parse(valueJson.getString(AFTER_OPLOGFIELD));
      SchemaUtils.extractAvroValues(after, genericRecord);
    }

    // Update operation has patch field
    if (op.equals(Operation.UPDATE)) {
      genericRecord.put(SchemaUtils.PATCH_FIELD, valueJson.optString(PATCH_OPLOGFIELD));
    }

    return genericRecord;
  }

  private static long getTimeMs(JSONObject valueJson) {
    // Source field is mandatory
    JSONObject source = valueJson.getJSONObject(SOURCE_OPLOGFIELD);
    long timeMs = source.optLong(TS_MS_OPLOGFIELD);
    if (timeMs != 0) {
      return timeMs;
    }
    return valueJson.optLong(TS_MS_OPLOGFIELD);
  }

  private static BsonObjectId getObjectId(String idJson) {
    return new BsonObjectIdCodec().decode(new JsonReader(idJson), DecoderContext.builder().build());
  }

  public static String getDocumentId(String key) {
    // Payload field may be absent when key.converter.schemas.enable=false
    JSONObject keyJson = new JSONObject(key);
    if (keyJson.has(PAYLOAD_OPLOGFIELD)) {
      keyJson = keyJson.getJSONObject(PAYLOAD_OPLOGFIELD);
    }
    return getObjectId(keyJson.getString(ID_OPLOGFIELD)).getValue().toString();
  }
}
