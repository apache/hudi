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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceUtils;
import org.junit.Test;

import java.time.LocalDate;

import static org.junit.Assert.assertEquals;

public class DataSourceUtilsTest {

  @Test
  public void testAvroRecordsFieldConversion() {
    // There are fields event_date1, event_date2, event_date3 with logical type as Date. event_date1 & event_date3 are
    // of UNION schema type, which is a union of null and date type in different orders. event_date2 is non-union
    // date type
    String avroSchemaString  = "{\"type\": \"record\"," + "\"name\": \"events\"," + "\"fields\": [ "
        + "{\"name\": \"event_date1\", \"type\" : [{\"type\" : \"int\", \"logicalType\" : \"date\"}, \"null\"]},"
        + "{\"name\": \"event_date2\", \"type\" : {\"type\": \"int\", \"logicalType\" : \"date\"}},"
        + "{\"name\": \"event_date3\", \"type\" : [\"null\", {\"type\" : \"int\", \"logicalType\" : \"date\"}]},"
        + "{\"name\": \"event_name\", \"type\": \"string\"},"
        + "{\"name\": \"event_organizer\", \"type\": \"string\"}"
        + "]}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("event_date1", 18000);
    record.put("event_date2", 18001);
    record.put("event_date3", 18002);
    record.put("event_name", "Hudi Meetup");
    record.put("event_organizer", "Hudi PMC");

    assertEquals(LocalDate.ofEpochDay(18000).toString(), DataSourceUtils.getNestedFieldValAsString(record, "event_date1",
        true));
    assertEquals(LocalDate.ofEpochDay(18001).toString(), DataSourceUtils.getNestedFieldValAsString(record, "event_date2",
        true));
    assertEquals(LocalDate.ofEpochDay(18002).toString(), DataSourceUtils.getNestedFieldValAsString(record, "event_date3",
        true));
    assertEquals("Hudi Meetup", DataSourceUtils.getNestedFieldValAsString(record, "event_name", true));
    assertEquals("Hudi PMC", DataSourceUtils.getNestedFieldValAsString(record, "event_organizer", true));
  }
}
