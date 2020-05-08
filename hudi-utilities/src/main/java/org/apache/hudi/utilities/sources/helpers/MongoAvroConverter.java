package org.apache.hudi.utilities.sources.helpers;

import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.utilities.mongo.Operation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.codecs.BsonObjectIdCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriterSettings;
import org.json.JSONObject;

public class MongoAvroConverter extends KafkaAvroConverter {

  private static final Logger LOG = LogManager.getLogger(MongoAvroConverter.class);
  @SuppressWarnings("deprecation") // Backward compatible with libraries
  private static final JsonWriterSettings STRICT_JSON = JsonWriterSettings.builder()
      .outputMode(JsonMode.STRICT).build();
  // Oplog fields
  private static final String AFTER_OPLOGFIELD = "after";
  private static final String OP_OPLOGFIELD = "op";
  private static final String TS_MS_OPLOGFIELD = "ts_ms";
  private static final String PATCH_OPLOGFIELD = "patch";
  private static final String ID_OPLOGFIELD = "id";
  private static final String PAYLOAD_OPLOGFIELD = "payload";
  private static final String SOURCE_OPLOGFIELD = "source";
  // Avro schema field names
  private static final String ID_FIELD = "_id";
  private static final String OP_FIELD = "_op";
  private static final String TS_MS_FIELD = "_ts_ms";
  private static final String PATCH_FIELD = "_patch";

  public MongoAvroConverter(Schema avroSchema) {
    super(avroSchema);
  }

  @Override
  public GenericRecord transform(String key, String value) {
    GenericRecord genericRecord = new Record(this.getSchema());
    genericRecord.put(ID_FIELD, getDocumentId(key));

    // Payload field may be absent when value.converter.schemas.enable=false
    JSONObject valueJson = new JSONObject(value);
    if (valueJson.has(PAYLOAD_OPLOGFIELD)) {
      valueJson = valueJson.getJSONObject(PAYLOAD_OPLOGFIELD);
    }

    // Op field is mandatory
    Operation op = Operation.forCode(valueJson.getString(OP_OPLOGFIELD));
    genericRecord.put(OP_FIELD, op.code());

    // Read timestamp from source field
    genericRecord.put(TS_MS_FIELD, getTimeMs(valueJson));

    // Insert/Read operation has after field
    if (op.equals(Operation.CREATE) || op.equals(Operation.READ)) {
      BsonDocument after = BsonDocument.parse(valueJson.getString(AFTER_OPLOGFIELD));
      for (Schema.Field field : getSchema().getFields()) {
        if (field.name().equals(ID_FIELD)) {
          continue;
        }

        BsonValue fieldValue = getFieldValue(field, after);
        if (!fieldValue.isNull()) {
          Schema schema = getNonNull(field.schema());
          try {
            Object targetValue = typeTransform(field.name(), fieldValue, schema);
            genericRecord.put(field.name(), targetValue);
          } catch (Exception e) {
            logFieldTypeError(field.name(),  schema.getType().getName(), fieldValue);
          }
        }
      }
    }

    // Update operation has patch field
    if (op.equals(Operation.UPDATE)) {
      genericRecord.put(PATCH_FIELD, valueJson.optString(PATCH_OPLOGFIELD));
    }

    return genericRecord;
  }

  public Object typeTransform(String fieldName, BsonValue valueBson, Schema schema) {
    if (valueBson.isNull()) {
      return null;
    }

    Type targetType = schema.getType();
    Object value = null;
    boolean nullOk = false;
    if (targetType.equals(Type.LONG)) {
      if (valueBson.isNumber()) {
        value = Long.valueOf(valueBson.asNumber().longValue());
      } else if (valueBson.isDateTime()) {
        value = Long.valueOf(valueBson.asDateTime().getValue());
      } else if (valueBson.isTimestamp()) {
        value = Long.valueOf(1000L * valueBson.asTimestamp().getTime());
      } else if (valueBson.isString()) {
        value = Long.parseLong(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.INT)) {
      if (valueBson.isNumber()) {
        value = Integer.valueOf(valueBson.asNumber().intValue());
      } else if (valueBson.isString()) {
        value = Integer.parseInt(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.FLOAT)) {
      if (valueBson.isNumber()) {
        value = Float.valueOf((float) valueBson.asNumber().doubleValue());
      } else if (valueBson.isString()) {
        value = Float.parseFloat(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.DOUBLE)) {
      if (valueBson.isNumber()) {
        value = Double.valueOf(valueBson.asNumber().doubleValue());
      } else if (valueBson.isString()) {
        value = Double.parseDouble(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.BOOLEAN)) {
      if (valueBson.isBoolean()) {
        value = Boolean.valueOf(valueBson.asBoolean().getValue());
      } else if (valueBson.isString()) {
        value = Boolean.parseBoolean(valueBson.asString().getValue());
      }
    } else if (targetType.equals(Type.ARRAY) && valueBson.isArray()) {
      BsonArray array = valueBson.asArray();
      if (array.isEmpty()) {
        nullOk = true;
      } else {
        ArrayList<Object> list = new ArrayList<>(array.size());
        for (BsonValue element : array.getValues()) {
          list.add(typeTransform(fieldName, element, schema.getElementType()));
        }
        value = list;
      }
    } else if (targetType.equals(Type.STRING)) {
      if (valueBson.isString()) {
        value = valueBson.asString().getValue();
      } else if (valueBson.isObjectId()) {
        value = valueBson.asObjectId().getValue().toString();
      } else if (valueBson.isDocument()) {
        value = valueBson.asDocument().toJson(STRICT_JSON);
      }
    }
    if (value == null && !nullOk) {
      logFieldTypeError(fieldName, targetType.getName(), valueBson);
    }
    return value;
  }

  private static Schema getNonNull(Schema schema) {
    if (schema.getType().equals(Type.UNION)) {
      for (Schema element : schema.getTypes()) {
        if (!element.getType().equals(Type.NULL)) {
          return element;
        }
      }
    }
    return schema;
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

  private static BsonValue getFieldValue(Schema.Field field, BsonDocument after) {
    for (String aliasName : field.aliases()) {
      BsonValue value = after.get(aliasName, BsonNull.VALUE);
      if (!value.isNull()) {
        return value;
      }
    }
    return after.get(field.name(), BsonNull.VALUE);
  }

  private void logFieldTypeError(String fieldName, String typeName, BsonValue fieldValue) {
    LOG.error(String.format("Field (%s) value cannot be cast to %s: %s", fieldName, typeName,
        fieldValue.toString()));
  }
}
