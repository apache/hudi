package org.apache.hudi.utilities.sources.helpers;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.utilities.mongo.Operation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

public class MongoAvroConverter extends KafkaAvroConverter {
  private static final Logger LOG = LogManager.getLogger(MongoAvroConverter.class);
  private static final String AFTER_OPLOGFIELD = "after";
  private static final String OP_OPLOGFIELD = "op";
  private static final String TS_MS_OPLOGFIELD = "ts_ms";
  private static final String PATCH_OPLOGFIELD = "patch";
  private static final String ID_OPLOGFIELD = "id";
  private static final String PAYLOAD_OPLOGFIELD = "payload";
  private static final String OP_FIELD = "oplog_op";
  private static final String TS_MS_FIELD = "oplog_ts_ms";
  private static final String PATCH_FIELD = "oplog_patch";
  private static final String ID_FIELD = "_id";

  public MongoAvroConverter(Schema avroSchema) {
    super(avroSchema);
  }

  @Override
  public GenericRecord transform(String key, String value) {
    JSONObject valueJson = new JSONObject(value);
    GenericRecord genericRecord = new Record(this.getSchema());

    JSONObject payload = valueJson.getJSONObject(PAYLOAD_OPLOGFIELD);

    Operation op = Operation.forCode(payload.getString(OP_OPLOGFIELD));
    genericRecord.put(OP_FIELD, op.code());

    if (payload.has(AFTER_OPLOGFIELD)
            && (op.equals(Operation.CREATE) || op.equals(Operation.READ))) {
      JSONObject after = new JSONObject(payload.getString(AFTER_OPLOGFIELD));
      for (Schema.Field field : this.getSchema().getFields()) {
        String fieldName = field.name();
        String matchedFieldName = matchField(field, after);
        if (matchedFieldName != null) {
          try {
            Schema fieldSchema = field.schema();
            Object fieldValue = after.get(matchedFieldName);
            Object targetValue = typeTransform(fieldValue, fieldSchema);
            genericRecord.put(fieldName, targetValue);
          } catch (Exception e) {
            LOG.info("Conversion error: " + fieldName
                    + " value: " + after.get(matchedFieldName).toString());
          }
        }
      }
    }
    if (payload.has(PATCH_OPLOGFIELD)) {
      String patch = payload.getString(PATCH_OPLOGFIELD);
      genericRecord.put(PATCH_FIELD, patch);
    }

    long timeMs = Long.parseLong(payload.getString(TS_MS_OPLOGFIELD));
    genericRecord.put(TS_MS_FIELD, timeMs);

    genericRecord.put(ID_FIELD, readKey(key));

    return genericRecord;
  }

  public Object typeTransform(Object valueJson, Schema schema) throws Exception {
    if (isOptional(schema)) {
      if (valueJson == null || valueJson.toString().equals("null")) {
        return null;
      } else {
        schema = getNonNull(schema);
      }
    }
    Type targetType = schema.getType();
    Object value;
    if (targetType.equals(Type.LONG)) {
      value = Long.parseLong(valueJson.toString());
    } else if (targetType.equals(Type.INT)) {
      value = Integer.parseInt(valueJson.toString());
    } else if (targetType.equals(Type.FLOAT)) {
      value = Float.parseFloat(valueJson.toString());
    } else if (targetType.equals(Type.DOUBLE)) {
      value = Double.parseDouble(valueJson.toString());
    } else if (targetType.equals(Type.BOOLEAN)) {
      value = Boolean.parseBoolean(valueJson.toString());
    } else if (targetType.equals(Type.ARRAY) && valueJson instanceof JSONArray) {
      Schema elementSchema = schema.getElementType();
      List<Object> listRes = new ArrayList<>();
      JSONArray valueJsonArray = (JSONArray) valueJson;
      for (int i = 0; i < valueJsonArray.length(); i++) {
        listRes.add(typeTransform(valueJsonArray.get(i), elementSchema));
      }
      value = listRes;
    } else if (targetType.equals(Type.STRING)) {
      value = valueJson.toString();
      // TODO: Handle ObjectId
    } else {
      throw new Exception("invalid format");
    }
    return value;
  }

  private static boolean isOptional(Schema schema) {
    return (schema.getType().equals(Type.UNION)
            && schema.getTypes().size() == 2
            && (schema.getTypes().get(0).getType().equals(Type.NULL)
            || schema.getTypes().get(1).getType().equals(Type.NULL)));
  }

  private static Schema getNonNull(Schema schema) {
    Schema nonNullSchema;
    if (schema.getTypes().get(0).getType().equals(Type.NULL)) {
      nonNullSchema = schema.getTypes().get(1);
    } else {
      nonNullSchema = schema.getTypes().get(0);
    }
    return nonNullSchema;
  }

  public static String readKey(String key) {
    JSONObject keyJson = new JSONObject(key);
    JSONObject idJson = new JSONObject(keyJson.getJSONObject(PAYLOAD_OPLOGFIELD).getString(ID_OPLOGFIELD));
    String id = idJson.getString("$oid");
    return id;
  }

  public static String matchField(Schema.Field field, JSONObject after) {
    String matchedAliasName = null;
    for (String aliasName: field.aliases()) {
      if (after.has(aliasName)) {
        matchedAliasName = aliasName;
      }
    }
    if (matchedAliasName == null && after.has(field.name())) {
      matchedAliasName = field.name();
    }
    return matchedAliasName;
  }
}
