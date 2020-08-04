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

package org.apache.hudi;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDataSourceUtils {

  @Mock
  private HoodieWriteClient hoodieWriteClient;

  @Mock
  private JavaRDD<HoodieRecord> hoodieRecords;

  @Captor
  private ArgumentCaptor<Option> optionCaptor;
  private HoodieWriteConfig config;

  @BeforeEach
  public void setUp() {
    config = HoodieWriteConfig.newBuilder().withPath("/").build();
  }

  @Test
  public void testAvroRecordsFieldConversion() {
    // There are fields event_date1, event_date2, event_date3 with logical type as Date. event_date1 & event_date3 are
    // of UNION schema type, which is a union of null and date type in different orders. event_date2 is non-union
    // date type
    String avroSchemaString = "{\"type\": \"record\"," + "\"name\": \"events\"," + "\"fields\": [ "
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

    assertEquals(LocalDate.ofEpochDay(18000).toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_date1",
        true));
    assertEquals(LocalDate.ofEpochDay(18001).toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_date2",
        true));
    assertEquals(LocalDate.ofEpochDay(18002).toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_date3",
        true));
    assertEquals("Hudi Meetup", HoodieAvroUtils.getNestedFieldValAsString(record, "event_name", true));
    assertEquals("Hudi PMC", HoodieAvroUtils.getNestedFieldValAsString(record, "event_organizer", true));
  }

  @Test
  public void testDoWriteOperationWithoutUserDefinedBulkInsertPartitioner() throws HoodieException {
    when(hoodieWriteClient.getConfig()).thenReturn(config);

    DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
            DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    verify(hoodieWriteClient, times(1)).bulkInsert(any(hoodieRecords.getClass()), anyString(),
            optionCaptor.capture());
    assertThat(optionCaptor.getValue(), is(equalTo(Option.empty())));
  }

  @Test
  public void testDoWriteOperationWithNonExistUserDefinedBulkInsertPartitioner() throws HoodieException {
    setAndVerifyHoodieWriteClientWith("NonExistClassName");

    Exception exception = assertThrows(HoodieException.class, () -> {
      DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
              DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    });

    assertThat(exception.getMessage(), containsString("Could not create UserDefinedBulkInsertPartitioner"));
  }

  @Test
  public void testDoWriteOperationWithUserDefinedBulkInsertPartitioner() throws HoodieException {
    setAndVerifyHoodieWriteClientWith(NoOpBulkInsertPartitioner.class.getName());

    DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
        DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    verify(hoodieWriteClient, times(1)).bulkInsert(any(hoodieRecords.getClass()), anyString(),
        optionCaptor.capture());
    assertThat(optionCaptor.getValue().get(), is(instanceOf(NoOpBulkInsertPartitioner.class)));
  }

  private void setAndVerifyHoodieWriteClientWith(final String partitionerClassName) {
    config = HoodieWriteConfig.newBuilder().withPath(config.getBasePath())
        .withUserDefinedBulkInsertPartitionerClass(partitionerClassName)
        .build();
    when(hoodieWriteClient.getConfig()).thenReturn(config);

    assertThat(config.getUserDefinedBulkInsertPartitionerClass(), is(equalTo(partitionerClassName)));
  }

  public static class NoOpBulkInsertPartitioner<T extends HoodieRecordPayload>
      implements BulkInsertPartitioner<T> {

    @Override
    public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
      return records;
    }

    @Override
    public boolean arePartitionRecordsSorted() {
      return false;
    }
  }
}
