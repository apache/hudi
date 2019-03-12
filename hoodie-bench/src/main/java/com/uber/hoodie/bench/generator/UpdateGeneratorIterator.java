package com.uber.hoodie.bench.generator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class UpdateGeneratorIterator implements Iterator<GenericRecord> {

  // Use the full payload generator as default
  private GenericRecordFullPayloadGenerator generator;
  private List<String> blackListedFields;
  // iterator
  private Iterator<GenericRecord> itr;

  public UpdateGeneratorIterator(Iterator<GenericRecord> itr, String schemaStr, List<String> partitionPathFieldNames,
      List<String> recordKeyFieldNames, int minPayloadSize) {
    this.itr = itr;
    this.blackListedFields = new ArrayList<>();
    this.blackListedFields.addAll(partitionPathFieldNames);
    this.blackListedFields.addAll(recordKeyFieldNames);
    Schema schema = new Schema.Parser().parse(schemaStr);
    this.generator = new GenericRecordFullPayloadGenerator(schema, minPayloadSize);
  }

  @Override
  public boolean hasNext() {
    return itr.hasNext();
  }

  @Override
  public GenericRecord next() {
    GenericRecord newRecord = itr.next();
    return this.generator.randomize(newRecord, this.blackListedFields);
  }

}
