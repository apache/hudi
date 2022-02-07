package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;

/**
 * Stubbed implementation of {@link HoodieRecordPayload} used to signal that it should simply be
 * skipped
 */
public class SkipHoodieRecordPayload implements HoodieRecordPayload<EmptyHoodieRecordPayload> {

  public SkipHoodieRecordPayload() {
  }

  public SkipHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
  }

  @Override
  public EmptyHoodieRecordPayload preCombine(EmptyHoodieRecordPayload oldValue) {
    return oldValue;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) {
    return Option.empty();
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) {
    return Option.empty();
  }

  @Override
  public boolean canBeIgnored() {
    return true;
  }
}
