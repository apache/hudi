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

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.hudi.common.model.PartialUpdateAvroPayload.isRecordNewer;

/**
 * Payload clazz that is used for partial updates to Hudi Table based on fields contained in new column.
 *
 * <p>Controlled partial update Logic:
 * <pre>
 *  1. #preCombine
 *  For records with the same record key in one batch,
 *  sort them based on ordering value so they are merged in-order
 *  by overwriting the previous record for fields specified in _hoodie_change_cols.
 *  During precombine phase, we merge the fields specified in _hoodie_change_cols
 *
 *  2. #combineAndGetUpdateValue
 *  For every incoming record with existing record in storage (same record key)
 *  Checks whether incoming record's ordering value is larger than the existing record.
 *  If yes, overwrites the existing one for fields specified in _hoodie_change_cols
 *  and returns a merged record.
 *  We null out _hoodie_change_cols when saving to storage to save space.
 *
 *
 *  Illustration with simple data.
 *  let's say the order field is 'ts' and schema is :
 *  {
 *    [
 *      {"name":"id","type":"string"},
 *      {"name":"ts","type":"long"},
 *      {"name":"name","type":"string"},
 *      {"name":"price","type":"string"},
 *      {"name":"_hoodie_change_cols", "type":"string"}
 *    ]
 *  }
 *
 *  case 1 - precombine
 *  Upsert data one:
 *      id      ts      name    price     _hoodie_change_cols
 *      1       4       name_1  price_1   name
 *  Upsert data two:
 *      id      ts      name    price     _hoodie_change_cols
 *      1       3       name_2  price_2   price
 *  Upsert data three:
 *      id      ts      name    price     _hoodie_change_cols
 *      1       5       name_3  price_3   price
 *
 *  Final record to be merged after sorted #precombine phase:
 *      id      ts      name    price     _hoodie_change_cols
 *      1       5       name_1  price_3   price,name
 *
 *  case 2 - combineAndGetUpdateValue
 *  Current data:
 *      id      ts      name    price     _hoodie_change_cols
 *      1       2       name_1  null      null
 *  Insert data:
 *      id      ts      name    price     _hoodie_change_cols
 *      1       5       name_2  price_3   price,name
 *
 *  Result data after #combineAndGetUpdateValue:
 *      id      ts      name    price     _hoodie_change_cols
 *      1       5       name_2  price_3   null
 *</pre>
 */
public class OverwriteWithLatestPartialUpdatesAvroPayload extends OverwriteWithLatestAvroPayload {

  private static final Logger LOG = LoggerFactory.getLogger(OverwriteWithLatestPartialUpdatesAvroPayload.class);

  private static final String HOODIE_CHANGE_COLS = "_hoodie_change_columns";

  public OverwriteWithLatestPartialUpdatesAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public OverwriteWithLatestPartialUpdatesAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  public OverwriteWithLatestPartialUpdatesAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Schema schema, Properties properties) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }

    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
      throw new HoodieException("Records need to be sorted before precombine for OverwriteWithLatestPartialUpdatesAvroPayload");
    }

    try {
      GenericRecord oldRecord = HoodieAvroUtils.bytesToAvro(oldValue.recordBytes, schema);
      Option<IndexedRecord> mergedRecord = mergeOldRecord(oldRecord, schema, false, false);
      if (mergedRecord.isPresent()) {
        return new OverwriteWithLatestPartialUpdatesAvroPayload((GenericRecord) mergedRecord.get(), this.orderingVal);
      }
    } catch (Exception ex) {
      return this;
    }
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return mergeOldRecord(currentValue, schema, false, true);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
    return mergeOldRecord(currentValue, schema, isRecordNewer(orderingVal, currentValue, prop), true);
  }

  protected Option<IndexedRecord> mergeOldRecord(IndexedRecord oldRecord,
                                                 Schema schema,
                                                 boolean isOldRecordNewer,
                                                 boolean nullChangeCols) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);

    if (!recordOption.isPresent()) {
      // use natural order for delete record
      return Option.empty();
    }

    if (isOldRecordNewer) {
      // Records out of order can result in a dropped partial update
      LOG.error("Out of order record. Partial update could be missed for " + recordOption.get());
      return Option.of(oldRecord);
    } else {
      return mergeRecords(schema, (GenericRecord) recordOption.get(), (GenericRecord) oldRecord, nullChangeCols);
    }
  }

  protected Option<IndexedRecord> mergeRecords(Schema schema, GenericRecord newRecord, GenericRecord previousRecord, boolean nullChangeCols) {
    if (isDeleteRecord(newRecord)) {
      return Option.empty();
    } else {
      final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
      Set<String> oldChangeColSet = getChangeColSetFromRecord(schema, previousRecord);
      Set<String> changeColSet = getChangeColSetFromRecord(schema, newRecord);
      schema.getFields().forEach(field -> setFieldFromChangeCols(newRecord, previousRecord, builder, field, changeColSet));

      // Null change cols for combineAndGetUpdateValue, merge change cols for precombine
      if (nullChangeCols) {
        nullChangeColField(builder);
      } else {
        changeColSet.addAll(oldChangeColSet);
        builder.set(HOODIE_CHANGE_COLS, String.join(",", changeColSet));
      }
      return Option.of(builder.build());
    }
  }

  private Set<String> getChangeColSetFromRecord(Schema schema, GenericRecord record) {
    Set<String> changeColSet = new HashSet<>();

    Schema.Field changeCols = schema.getField(HOODIE_CHANGE_COLS);
    Object changeColValue = record.get(HOODIE_CHANGE_COLS);
    if (changeCols == null) {
      throw new HoodieException("When using OverwriteWithLatestPartialUpdatesAvroPayload the column \""
          + HOODIE_CHANGE_COLS + " \" must be included in schema.");
    } else if (changeColValue instanceof Utf8) {
      List<String> strChangeColumns = Arrays.asList(changeColValue.toString().split(","));
      changeColSet.addAll(strChangeColumns);
    }

    return changeColSet;
  }

  private void setFieldFromChangeCols(
      GenericRecord baseRecord,
      GenericRecord mergedRecord,
      GenericRecordBuilder builder,
      Schema.Field field,
      Set<String> changeColSet) {
    Object value = baseRecord.get(field.name());
    value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
    if (changeColSet.contains(field.name())) {
      builder.set(field, value);
    } else {
      builder.set(field, mergedRecord.get(field.name()));
    }
  }

  // null out change column field to reduce record space
  private void nullChangeColField(GenericRecordBuilder builder) {
    builder.set(HOODIE_CHANGE_COLS, null);
  }
}
