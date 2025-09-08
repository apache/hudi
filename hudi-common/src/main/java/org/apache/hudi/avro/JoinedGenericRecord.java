package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

public class JoinedGenericRecord implements GenericRecord {

  private final GenericRecord dataRecord;
  private final Map<String, Object> metaFields;
  private final List<String> metaFieldNames;
  private final Schema schema;

  public JoinedGenericRecord(GenericRecord dataRecord, Map<String, Object> metaFields, List<String> metaFieldNames, Schema schema) {
    this.dataRecord = dataRecord;
    this.metaFields = metaFields;
    this.metaFieldNames = metaFieldNames;
    this.schema = schema;
  }

  @Override
  public void put(String key, Object v) {
    if (metaFieldNames.contains(key)) {
      metaFields.put(key, v);
    } else {
      dataRecord.put(key, v);
    }
  }

  @Override
  public Object get(String key) {
    if (metaFieldNames.contains(key)) {
      return metaFields.get(key);
    } else {
      return dataRecord.get(key);
    }
  }

  @Override
  public void put(int i, Object v) {
    if (i < metaFieldNames.size()) {
      metaFields.put(metaFieldNames.get(i), v);
    } else {
      dataRecord.put(i - metaFieldNames.size(), v);
    }
  }

  @Override
  public Object get(int i) {
    if (i < metaFieldNames.size()) {
      return metaFields.get(metaFieldNames.get(i));
    } else {
      return dataRecord.get(i - metaFieldNames.size());
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
