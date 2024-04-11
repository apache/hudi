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

package org.apache.hudi.aws.sync;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class HoodieNestedDataGenerator extends HoodieDataGenerator {

  @Override
  public String getAvroSchemaString() {
    return "{\"type\": \"record\",\"name\": \"triprec\",\"fields\": [ "
        + "{\"name\": \"ts\",\"type\": \"long\"},"
        + "{\"name\": \"uuid\", \"type\": \"string\"},"
        + "{\"name\": \"rider\", \"type\": \"string\"},"
        + "{\"name\": \"driver\", \"type\": \"string\"},"
        + "{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"AddressUSRecord\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"state\",\"type\":\"string\"}]}}"
        + "]}";
  }

  @Override
  public GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName, long timestamp) {
    GenericRecord rec = new GenericData.Record(getAvroSchema());
    rec.put("uuid", rowKey);
    rec.put("ts", timestamp);
    rec.put("rider", riderName);
    rec.put("driver", driverName);
    GenericRecord address = new GenericData.Record(getAvroSchema().getField("address").schema());
    address.put("city", "paris");
    address.put("state", "france");
    rec.put("address", address);
    return rec;
  }
}
