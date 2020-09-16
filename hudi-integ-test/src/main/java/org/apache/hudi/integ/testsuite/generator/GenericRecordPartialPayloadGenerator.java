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

package org.apache.hudi.integ.testsuite.generator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * This is a GenericRecord payload generator that generates partial generic records {@link GenericRecord}. A partial
 * records is one that has some fields of the schema NULL or NOT PRESENT. This payload enables us to simulate
 * creation of partial records which are possible in many cases, especially for database change logs.
 */
public class GenericRecordPartialPayloadGenerator extends GenericRecordFullPayloadGenerator {

  public GenericRecordPartialPayloadGenerator(Schema schema) {
    super(schema);
  }

  public GenericRecordPartialPayloadGenerator(Schema schema, int minPayloadSize) {
    super(schema, minPayloadSize);
  }

  @Override
  protected GenericRecord convert(Schema schema) {
    GenericRecord record = super.convertPartial(schema);
    return record;
  }

  private void setNull(GenericRecord record) {
    for (Schema.Field field : record.getSchema().getFields()) {
      // A random boolean decides whether this field of the generic record should be present or absent.
      // Using this we can set only a handful of fields in the record and generate partial records
      boolean setNull = random.nextBoolean();
      if (setNull) { // TODO : DO NOT SET THE RECORD KEY FIELDS TO NULL
        record.put(field.name(), null);
      } else {
        if (record.get(field.name()) instanceof GenericData.Record) {
          setNull((GenericData.Record) record.get(field.name()));
        }
      }
    }
  }

  @Override
  public boolean validate(GenericRecord record) {
    return validate((Object) record);
  }

  // Atleast 1 entry should be null
  private boolean validate(Object object) {
    if (object == null) {
      return true;
    } else if (object instanceof GenericRecord) {
      for (Schema.Field field : ((GenericRecord) object).getSchema().getFields()) {
        boolean ret = validate(((GenericRecord) object).get(field.name()));
        if (ret) {
          return ret;
        }
      }
    }
    return false;
  }

}

