/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.mongo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

public class MongoAvroPayload extends BaseAvroPayload implements
    HoodieRecordPayload<MongoAvroPayload> {

  private static final Logger LOG = LogManager.getLogger(MongoAvroPayload.class);

  private static final String SET_FIELD = "$set";
  private static final String UNSET_FIELD = "$unset";

  public MongoAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public MongoAvroPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, (record1) -> 0); // natural order
  }

  @Override
  public MongoAvroPayload preCombine(MongoAvroPayload another, Schema schema) {
    Pair<GenericRecord, Operation> combinedValue;
    try {
      GenericRecord oneValue = (GenericRecord) getInsertValue(schema).get();
      GenericRecord anotherValue = (GenericRecord) another.getInsertValue(schema).get();
      // Combine the record values
      if (another.orderingVal.compareTo(orderingVal) > 0) {
        combinedValue = combineRecords(oneValue, anotherValue);
      } else {
        combinedValue = combineRecords(anotherValue, oneValue);
      }
    } catch (IOException e) {
      throw new HoodieIOException("MongoAvroPayload::preCombine error", e);
    }
    return new MongoAvroPayload(combinedValue.getKey(),
        (Long) combinedValue.getKey().get(SchemaUtils.TS_MS_FIELD));
  }

  /**
   * Merges the records with the same key, ordered by based on an ordering field.
   */
  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
      throws IOException {
    Pair<GenericRecord, Operation> combinedValue = combineRecords((GenericRecord) currentValue,
        (GenericRecord) getInsertValue(schema).get());
    // Return empty record for deletion operation
    if (combinedValue.getValue().equals(Operation.DELETE)) {
      return Option.empty();
    } else {
      return Option.of(combinedValue.getKey());
    }
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    return Option.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));
  }

  /**
   * Patch current record value with updates.
   */
  private static void patchRecord(GenericRecord curValue, BsonDocument patch) {
    // Merge $set and $unset
    BsonDocument updates = new BsonDocument();
    combineSetAndUnset(patch, updates);

    // Process $set and $unset entries
    for (Entry<String, BsonValue> entry : updates.entrySet()) {
      final String[] fieldPath = entry.getKey().split("\\.");
      // Ignore fields which are not in the schema.
      Schema.Field field = SchemaUtils.getFieldByName(fieldPath[0], curValue.getSchema());
      if (field == null) {
        continue;
      }
      // Updating top level fields
      if (fieldPath.length == 1) {
        curValue.put(field.pos(),
            SchemaUtils.typeTransform(fieldPath[0], entry.getValue(), field.schema()));
        continue;
      }
      // Construct BsonArray or BsonDocument from merged value.
      // Operations on non-viable paths are ignored as idempotence requires that any other update
      // modifiers must still be applied. For example, consider applying the following updates
      // twice to an initially empty document:
      // {$set: {c: 0, a: { b: 1 }}}
      // {$set: {'a.b': 0, c: 1}}
      // {$set: {a: {}}}
      // Setting 'a.b' will fail the second time possibly due to clock skew, but we must
      // still set 'c'.
      try {
        BsonValue fieldValue = getBsonDocumentOrArray(fieldPath, curValue.get(field.pos()),
            field.schema());
        if (fieldValue.isArray() || fieldValue.isDocument()) {
          // Processing array or nested document
          updateBsonValue(fieldPath, fieldValue, entry.getValue());
          curValue.put(field.pos(),
              SchemaUtils.typeTransform(fieldPath[0], fieldValue, field.schema()));
        }
      } catch (Exception e) {
        LOG.warn("Value cannot be set for field path " + entry.getKey());
      }
    }
  }

  /**
   * Combine $set and $unset from patch document.
   */
  private static void combineSetAndUnset(BsonDocument patch, BsonDocument updates) {
    if (patch.containsKey(SET_FIELD)) {
      for (Entry<String, BsonValue> valueEntry : patch.getDocument(SET_FIELD).entrySet()) {
        updates.put(valueEntry.getKey(), valueEntry.getValue());
      }
    }
    if (patch.containsKey(UNSET_FIELD)) {
      for (Entry<String, BsonValue> valueEntry : patch.getDocument(UNSET_FIELD).entrySet()) {
        // No-Op if unset of a key is false
        if (!valueEntry.getValue().asBoolean().getValue()) {
          continue;
        }
        updates.put(valueEntry.getKey(), BsonNull.VALUE);
      }
    }
  }

  /**
   * Combine and rewrite patches.
   */
  private static BsonDocument combinePatches(BsonDocument curPatch, BsonDocument newPatch) {
    BsonDocument updates = new BsonDocument();
    combineSetAndUnset(curPatch, updates);
    combineSetAndUnset(newPatch, updates);
    BsonDocument allPatch = new BsonDocument();
    allPatch.put(SET_FIELD, updates);
    return allPatch;
  }

  /**
   * Make a shallow copy of record.
   */
  private static GenericRecord copyRecord(GenericRecord record) {
    GenericRecord newRecord = new GenericData.Record(record.getSchema());
    for (Schema.Field field : record.getSchema().getFields()) {
      Object value = record.get(field.pos());
      if (value != null) {
        newRecord.put(field.pos(), value);
      }
    }
    return newRecord;
  }

  /**
   * Combine current and new record values. Note current record may not be using latest schema.
   */
  private static Pair<GenericRecord, Operation> combineRecords(
      GenericRecord curValue, GenericRecord newValue) {
    Operation newOp = Operation.forCode(newValue.get(SchemaUtils.OP_FIELD).toString());
    // Latest deletion/insert/read operations on documents win.
    if (newOp.equals(Operation.DELETE) || !newOp.equals(Operation.UPDATE)) {
      return Pair.of(newValue, newOp);
    }

    // Collection update can replace a document entirely.
    BsonDocument newPatch = BsonDocument.parse(newValue.get(SchemaUtils.PATCH_FIELD).toString());
    if (!newPatch.containsKey(SET_FIELD) && !newPatch.containsKey(UNSET_FIELD)) {
      // Update Mongo Operation either has a '$set' or '$unset' for partial updates, or '_id' is
      // expected for full document replacement.
      if (!newPatch.containsKey(SchemaUtils.ID_FIELD)) {
        LOG.error("_id field is expected for document replacement");
      }
      // Copy or overwrite fields to be CREATE OpLog.
      GenericRecord mergedValue = new GenericData.Record(newValue.getSchema());
      mergedValue.put(SchemaUtils.ID_FIELD, newValue.get(SchemaUtils.ID_FIELD));
      mergedValue.put(SchemaUtils.OP_FIELD, Operation.CREATE.code());
      mergedValue.put(SchemaUtils.TS_MS_FIELD, newValue.get(SchemaUtils.TS_MS_FIELD));
      // Extract user defined fields
      SchemaUtils.extractAvroValues(newPatch, mergedValue);
      return Pair.of(mergedValue, Operation.CREATE);
    }

    // Handle hoodie test cases.
    if (curValue == null) {
      return Pair.of(newValue, newOp);
    }

    // Ignore update OpLog for non-existent key.
    Operation curOp = Operation.forCode(curValue.get(SchemaUtils.OP_FIELD).toString());
    if (curOp.equals(Operation.READ) || curOp.equals(Operation.CREATE)) {
      // Rewrite current value with latest schema
      if (curValue.getSchema() != newValue.getSchema()) {
        curValue = HoodieAvroUtils.rewriteRecordWithOnlyNewSchemaFields(
            curValue, newValue.getSchema());
      } else {  // Make a shallow copy of current value
        curValue = copyRecord(curValue);
      }
      patchRecord(curValue, newPatch);
      curValue.put(SchemaUtils.TS_MS_FIELD, newValue.get(SchemaUtils.TS_MS_FIELD));
      return Pair.of(curValue, curOp);
    } else if (curOp.equals(Operation.UPDATE)) {
      // Update newValue with combined patches.
      BsonDocument curPatch = BsonDocument.parse(curValue.get(SchemaUtils.PATCH_FIELD).toString());
      newValue.put(SchemaUtils.PATCH_FIELD,
          combinePatches(curPatch, newPatch).toJson(SchemaUtils.STRICT_JSON));
      return Pair.of(newValue, Operation.UPDATE);
    } else {
      return Pair.of(newValue, newOp);
    }
  }

  /**
   * Get Bson document or array value from Java object. Return Null if the object is not of
   * composite type.
   */
  private static BsonValue getBsonDocumentOrArray(String[] fieldPath, Object object,
      Schema schema) {
    assert (fieldPath.length > 1);
    BsonValue value = getBsonValue(fieldPath, 1, object, schema);
    if (value.isString()) {
      value = BsonDocument.parse(value.asString().getValue());
    }
    return value;
  }

  /**
   * Get Bson array from Avro array object.
   */
  private static BsonArray toBsonArray(String[] fieldPath, int pos, List value,
      Schema schema) {
    BsonArray array = new BsonArray();
    for (int i = 0; i < value.size(); i++) {
      array.add(getBsonValue(fieldPath, pos, value.get(i), schema));
    }
    return array;
  }

  /**
   * Get Bson value from Java object.
   */
  private static BsonValue getBsonValue(String[] fieldPath, int pos, Object object, Schema schema) {
    if (object == null) {
      return BsonNull.VALUE;
    }

    Schema targetSchema = SchemaUtils.getNonNull(schema);
    Type targetType = targetSchema.getType();
    if (targetType.equals(Type.STRING) && object instanceof CharSequence) {
      if (pos < fieldPath.length) {
        return BsonDocument.parse(object.toString());
      } else {
        return new BsonString(object.toString());
      }
    } else if (targetType.equals(Type.ARRAY) && object instanceof List) {
      return toBsonArray(fieldPath, pos + 1, (List) object, targetSchema.getElementType());
    } else if (targetType.equals(Type.LONG) && (object instanceof Long)) {
      return new BsonInt64(((Long) object).longValue());
    } else if (targetType.equals(Type.INT) && (object instanceof Integer)) {
      return new BsonInt32(((Integer) object).intValue());
    } else if (targetType.equals(Type.FLOAT) && (object instanceof Float)) {
      return new BsonDouble(((Float) object).floatValue());
    } else if (targetType.equals(Type.DOUBLE) && (object instanceof Double)) {
      return new BsonDouble(((Double) object).doubleValue());
    } else if (targetType.equals(Type.BOOLEAN) && (object instanceof Boolean)) {
      return new BsonBoolean(((Boolean) object).booleanValue());
    } else if (targetType.equals(Type.BYTES) && (object instanceof ByteBuffer)) {
      return new BsonBinary(((ByteBuffer) object).array());
    }
    return BsonNull.VALUE;
  }

  /**
   * Update value at specified field path.
   *
   * @param fieldPath     Field path to update value
   * @param topLevelValue Top level field value
   * @param newValue      New value corresponding to the field path
   */
  private static void updateBsonValue(String[] fieldPath,
      BsonValue topLevelValue, BsonValue newValue) {
    BsonValue lastValue = topLevelValue;
    for (int pos = 1; pos < fieldPath.length; pos++) {
      String fieldName = fieldPath[pos];
      if (lastValue.isArray() != allDigits(fieldName)) {
        break;
      }
      if (lastValue.isArray()) {
        if (pos < fieldPath.length - 1) {
          lastValue = lastValue.asArray().get(Integer.parseInt(fieldName));
        } else {
          lastValue.asArray().set(Integer.parseInt(fieldName), newValue);
        }
      } else if (lastValue.isDocument()) {
        if (pos < fieldPath.length - 1) {
          lastValue = lastValue.asDocument().get(fieldName, BsonNull.VALUE);
        } else if (!newValue.isNull()) {
          lastValue.asDocument().put(fieldName, newValue);
        } else {
          lastValue.asDocument().remove(fieldName);
        }
      }
    }
  }

  private static boolean allDigits(String fieldName) {
    return fieldName.chars().allMatch(Character::isDigit);
  }
}
