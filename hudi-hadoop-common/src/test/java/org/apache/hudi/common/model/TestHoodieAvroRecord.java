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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.read.DeleteContext;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieAvroRecord {

  private static final String DELETE_ON_WRITE = "test.delete.on.write";

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("TestRecord", null, null,
      Collections.singletonList(HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING))));

  private static final DeleteContext DELETE_CONTEXT = new DeleteContext(new TypedProperties(), SCHEMA).withReaderSchema(SCHEMA);

  /**
   * A payload that is a delete only with the properties of the write, as {@code ExpressionPayload} is with those of the
   * append handle.
   */
  private static class WriteTimeDeletePayload extends OverwriteWithLatestAvroPayload {
    private final boolean decidesDeleteOnWrite;

    WriteTimeDeletePayload(boolean decidesDeleteOnWrite) {
      super(new GenericRecordBuilder(SCHEMA.toAvroSchema()).set("field1", "value1").build(), 0);
      this.decidesDeleteOnWrite = decidesDeleteOnWrite;
    }

    @Override
    public boolean isDeleted(Schema schema, Properties props) {
      return Boolean.parseBoolean(props.getProperty(DELETE_ON_WRITE, "false"));
    }

    @Override
    public boolean decidesDeleteOnWrite() {
      return decidesDeleteOnWrite;
    }
  }

  private static HoodieAvroRecord<WriteTimeDeletePayload> record(boolean decidesDeleteOnWrite, Boolean isDelete) {
    return new HoodieAvroRecord<>(new HoodieKey("key1", "p1"), new WriteTimeDeletePayload(decidesDeleteOnWrite), null, 0, isDelete);
  }

  private static TypedProperties writeProps(boolean isDelete) {
    TypedProperties props = new TypedProperties();
    props.setProperty(DELETE_ON_WRITE, String.valueOf(isDelete));
    return props;
  }

  @Test
  void testCachedFalseIsDecidedAgainOnWrite() {
    // Created with false, as the record creation of a MERGE INTO without a record key does
    assertTrue(record(true, false).isDelete(DELETE_CONTEXT, writeProps(true)));

    // False cached by a check before the write, as the index tagging does
    HoodieAvroRecord<WriteTimeDeletePayload> record = record(true, null);
    assertFalse(record.isDelete(DELETE_CONTEXT, writeProps(false)));
    assertTrue(record.isDelete(DELETE_CONTEXT, writeProps(true)));
  }

  @Test
  void testCachedTrueIsKept() {
    assertTrue(record(true, true).isDelete(DELETE_CONTEXT, writeProps(false)));
  }

  @Test
  void testCachedFalseIsKeptForOtherPayloads() {
    assertFalse(record(false, false).isDelete(DELETE_CONTEXT, writeProps(true)));

    HoodieAvroRecord<WriteTimeDeletePayload> record = record(false, null);
    assertFalse(record.isDelete(DELETE_CONTEXT, writeProps(false)));
    assertFalse(record.isDelete(DELETE_CONTEXT, writeProps(true)));
  }

  @Test
  void testCachedFalseIsKeptAfterDeflate() {
    HoodieAvroRecord<WriteTimeDeletePayload> record = record(true, false);
    record.deflate();
    assertFalse(record.isDelete(DELETE_CONTEXT, writeProps(true)));
  }
}
