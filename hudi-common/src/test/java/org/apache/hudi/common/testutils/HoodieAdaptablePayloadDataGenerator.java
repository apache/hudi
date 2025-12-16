/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

public class HoodieAdaptablePayloadDataGenerator {

  public static final HoodieSchema SCHEMA = SchemaTestUtil.getSchemaFromResource(HoodieAdaptablePayloadDataGenerator.class, "/adaptable-payload.avsc");
  public static final HoodieSchema SCHEMA_WITH_METAFIELDS = HoodieSchemaUtils.addMetadataFields(SCHEMA);
  public static final String SCHEMA_STR = SCHEMA.toString();

  public static Properties getKeyGenProps(Class<?> payloadClass) {
    String orderingField = new RecordGen(payloadClass).getOrderingField();
    Properties props = new Properties();
    props.put("hoodie.datasource.write.recordkey.field", "id");
    props.put("hoodie.datasource.write.partitionpath.field", "pt");
    props.put(HoodieTableConfig.ORDERING_FIELDS.key(), orderingField);
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    props.put(HoodieTableConfig.PARTITION_FIELDS.key(), "pt");
    props.put(HoodieTableConfig.ORDERING_FIELDS.key(), orderingField);
    return props;
  }

  public static Properties getPayloadProps(Class<?> payloadClass) {
    String orderingField = new RecordGen(payloadClass).getOrderingField();
    Properties props = new Properties();
    props.put("hoodie.compaction.payload.class", payloadClass.getName());
    props.put("hoodie.payload.event.time.field", orderingField);
    props.put("hoodie.payload.ordering.field", orderingField);
    return props;
  }

  public static List<HoodieRecord> getInserts(int n, String partition, long ts, Class<?> payloadClass) throws IOException {
    return getInserts(n, new String[] {partition}, ts, payloadClass);
  }

  public static List<HoodieRecord> getInserts(int n, String[] partitions, long ts, Class<?> payloadClass) throws IOException {
    List<HoodieRecord> inserts = new ArrayList<>();
    RecordGen recordGen = new RecordGen(payloadClass);
    for (GenericRecord r : getInserts(n, partitions, ts, recordGen)) {
      inserts.add(getHoodieRecord(r, recordGen.getPayloadClass()));
    }
    return inserts;
  }

  private static List<GenericRecord> getInserts(int n, String[] partitions, long ts, RecordGen recordGen) {
    return IntStream.range(0, n).mapToObj(id -> {
      String pt = partitions.length == 0 ? "" : partitions[id % partitions.length];
      return getInsert(id, pt, ts, recordGen);
    }).collect(Collectors.toList());
  }

  private static GenericRecord getInsert(int id, String pt, long ts, RecordGen recordGen) {
    GenericRecord r = new GenericData.Record(SCHEMA.toAvroSchema());
    r.put("id", id);
    r.put("pt", pt);
    return recordGen.populateForInsert(r, ts);
  }

  public static List<HoodieRecord> getUpdates(List<HoodieRecord> baseRecords, long ts, Class<?> payloadClass) throws IOException {
    RecordGen recordGen = new RecordGen(payloadClass);
    List<HoodieRecord> updates = new ArrayList<>();
    Properties props = new Properties();
    for (HoodieRecord r : baseRecords) {
      GenericRecord gr = (GenericRecord) r.toIndexedRecord(SCHEMA.toAvroSchema(), props).get().getData();
      GenericRecord updated = getUpdate(Integer.parseInt(gr.get("id").toString()), gr.get("pt").toString(), ts, recordGen);
      updates.add(getHoodieRecord(updated, recordGen.getPayloadClass()));
    }
    return updates;
  }

  public static List<HoodieRecord> getUpdates(List<HoodieRecord> baseRecords, String newPartition, long ts, Class<?> payloadClass) throws IOException {
    RecordGen recordGen = new RecordGen(payloadClass);
    List<HoodieRecord> updates = new ArrayList<>();
    Properties props = new Properties();
    for (HoodieRecord r : baseRecords) {
      GenericRecord gr = (GenericRecord) r.toIndexedRecord(SCHEMA.toAvroSchema(), props).get().getData();
      GenericRecord updated = getUpdate(Integer.parseInt(gr.get("id").toString()), newPartition, ts, recordGen);
      updates.add(getHoodieRecord(updated, recordGen.getPayloadClass()));
    }
    return updates;
  }

  private static GenericRecord getUpdate(int id, String pt, long ts, RecordGen recordGen) {
    GenericRecord r = new GenericData.Record(SCHEMA.toAvroSchema());
    r.put("id", id);
    r.put("pt", pt);
    return recordGen.populateForUpdate(r, ts);
  }

  public static List<HoodieRecord> getDeletes(List<HoodieRecord> baseRecords, long ts, Class<?> payloadClass) throws IOException {
    RecordGen recordGen = new RecordGen(payloadClass);
    List<HoodieRecord> deletes = new ArrayList<>();
    Properties props = new Properties();
    for (HoodieRecord r : baseRecords) {
      GenericRecord gr = (GenericRecord) r.toIndexedRecord(SCHEMA.toAvroSchema(), props).get().getData();
      GenericRecord deleted = getDelete(Integer.parseInt(gr.get("id").toString()), gr.get("pt").toString(), ts, recordGen);
      deletes.add(getHoodieRecord(deleted, recordGen.getPayloadClass()));
    }
    return deletes;
  }

  public static List<HoodieRecord> getDeletesWithNewPartition(List<HoodieRecord> baseRecords, String newPartition, long ts, Class<?> payloadClass)
      throws IOException {
    RecordGen recordGen = new RecordGen(payloadClass);
    List<HoodieRecord> deletes = new ArrayList<>();
    Properties props = new Properties();
    for (HoodieRecord r : baseRecords) {
      GenericRecord gr = (GenericRecord) r.toIndexedRecord(SCHEMA.toAvroSchema(), props).get().getData();
      GenericRecord deleted = getDelete(Integer.parseInt(gr.get("id").toString()), newPartition, ts, recordGen);
      deletes.add(getHoodieRecord(deleted, recordGen.getPayloadClass()));
    }
    return deletes;
  }

  public static List<HoodieRecord> getDeletesWithEmptyPayload(List<HoodieRecord> baseRecords) {
    List<HoodieRecord> deletes = new ArrayList<>();
    for (HoodieRecord r : baseRecords) {
      deletes.add(getHoodieRecordForEmptyPayload(r.getKey()));
    }
    return deletes;
  }

  public static List<HoodieRecord> getDeletesWithEmptyPayloadAndNewPartition(List<HoodieRecord> baseRecords, String newPartition) {
    List<HoodieRecord> deletes = new ArrayList<>();
    for (HoodieRecord r : baseRecords) {
      deletes.add(getHoodieRecordForEmptyPayload(new HoodieKey(r.getRecordKey(), newPartition)));
    }
    return deletes;
  }

  private static GenericRecord getDelete(int id, String pt, long ts, RecordGen recordGen) {
    GenericRecord r = new GenericData.Record(SCHEMA.toAvroSchema());
    r.put("id", id);
    r.put("pt", pt);
    return recordGen.populateForDelete(r, ts);
  }

  private static HoodieRecord getHoodieRecordForEmptyPayload(HoodieKey key) {
    return new HoodieAvroRecord(key, new EmptyHoodieRecordPayload());
  }

  private static HoodieRecord getHoodieRecord(GenericRecord r, Class<?> payloadClass) throws IOException {
    if (payloadClass == EmptyHoodieRecordPayload.class) {
      return getHoodieRecordForEmptyPayload(new HoodieKey(r.get("id").toString(), r.get("pt").toString()));
    }
    return new HoodieAvroIndexedRecord(r)
        .prependMetaFields(
            SCHEMA.toAvroSchema(),
            SCHEMA_WITH_METAFIELDS.toAvroSchema(),
            new MetadataValues().setRecordKey(r.get("id").toString()).setPartitionPath(r.get("pt").toString()),
            new Properties())
        .wrapIntoHoodieRecordPayloadWithParams(
            SCHEMA_WITH_METAFIELDS.toAvroSchema(),
            getPayloadProps(payloadClass),
            Option.empty(),
            false,
            Option.empty(),
            false,
            Option.of(SCHEMA.toAvroSchema()));
  }

  public static class RecordGen {

    public static final Set<Class<?>> SUPPORTED_PAYLOAD_CLASSES = new HashSet<>(Arrays.asList(
        OverwriteWithLatestAvroPayload.class,
        OverwriteNonDefaultsWithLatestAvroPayload.class,
        PartialUpdateAvroPayload.class,
        DefaultHoodieRecordPayload.class,
        AWSDmsAvroPayload.class,
        MySqlDebeziumAvroPayload.class,
        PostgresDebeziumAvroPayload.class
    ));

    private final Class<?> payloadClass;
    private final String orderingField;

    public RecordGen(Class<?> payloadClass) {
      checkArgument(SUPPORTED_PAYLOAD_CLASSES.contains(payloadClass));
      this.payloadClass = payloadClass;
      if (payloadClass == MySqlDebeziumAvroPayload.class) {
        orderingField = "_event_seq";
      } else if (payloadClass == PostgresDebeziumAvroPayload.class) {
        orderingField = "_event_lsn";
      } else {
        orderingField = "ts";
      }
    }

    public Class<?> getPayloadClass() {
      return payloadClass;
    }

    public String getOrderingField() {
      return orderingField;
    }

    GenericRecord populateForInsert(GenericRecord r, long ts) {
      r.put(orderingField, ts);
      if (payloadClass == AWSDmsAvroPayload.class) {
        r.put(AWSDmsAvroPayload.OP_FIELD, "I");
      }
      return r;
    }

    GenericRecord populateForUpdate(GenericRecord r, long ts) {
      r.put(orderingField, ts);
      if (payloadClass == AWSDmsAvroPayload.class) {
        r.put(AWSDmsAvroPayload.OP_FIELD, "U");
      }
      return r;
    }

    GenericRecord populateForDelete(GenericRecord r, long ts) {
      r.put(orderingField, ts);
      if (payloadClass == MySqlDebeziumAvroPayload.class) {
        r.put(DebeziumConstants.FLATTENED_OP_COL_NAME, DebeziumConstants.DELETE_OP);
      } else if (payloadClass == PostgresDebeziumAvroPayload.class) {
        r.put(DebeziumConstants.FLATTENED_OP_COL_NAME, DebeziumConstants.DELETE_OP);
      } else if (payloadClass == AWSDmsAvroPayload.class) {
        r.put(AWSDmsAvroPayload.OP_FIELD, "D");
      } else {
        r.put(HOODIE_IS_DELETED_FIELD, true);
      }
      return r;
    }
  }
}
