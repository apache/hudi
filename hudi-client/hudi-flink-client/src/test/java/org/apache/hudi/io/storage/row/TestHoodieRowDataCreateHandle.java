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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieRowDataCreateHandle}.
 */
public class TestHoodieRowDataCreateHandle extends HoodieFlinkClientTestHarness {

  private static final String PARTITION_PATH = "2024/10/21";
  private static final String FILE_ID = "file-1";
  private static final String INSTANT_TIME = "20241021120000";
  private static final int TASK_PARTITION_ID = 0;
  private static final long TASK_ID = 1;
  private static final long TASK_EPOCH_ID = 1;

  @BeforeEach
  public void setUp() throws IOException {
    initPath();
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  /**
   * Adds Hoodie metadata fields to the row type, matching production behavior in BulkInsertWriterHelper.
   */
  private static RowType addMetadataFields(RowType rowType, boolean withOperationField) {
    List<RowType.RowField> mergedFields = new ArrayList<>();

    LogicalType metadataFieldType = DataTypes.STRING().getLogicalType();
    mergedFields.add(new RowType.RowField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, metadataFieldType));
    mergedFields.add(new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, metadataFieldType));
    mergedFields.add(new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, metadataFieldType));
    mergedFields.add(new RowType.RowField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, metadataFieldType));
    mergedFields.add(new RowType.RowField(HoodieRecord.FILENAME_METADATA_FIELD, metadataFieldType));

    if (withOperationField) {
      mergedFields.add(new RowType.RowField(HoodieRecord.OPERATION_METADATA_FIELD, metadataFieldType));
    }

    mergedFields.addAll(rowType.getFields());
    return new RowType(mergedFields);
  }

  @Test
  public void testEventTimeFieldIndexWithDoubleType() throws Exception {
    // Schema with DOUBLE event_time field
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("event_time", DataTypes.DOUBLE()),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    // Add metadata fields like production code does
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "event_time");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(props)
        .withEmbeddedTimelineServerEnabled(false)
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    assertNotNull(handle);
    // Verify the handle was created successfully with event time configured
  }

  @Test
  public void testEventTimeFieldIndexWithWrongType() throws Exception {
    // Schema with STRING event_time field (not DOUBLE)
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("event_time", DataTypes.VARCHAR(20)),  // STRING, not DOUBLE
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "event_time");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(props)
        .withEmbeddedTimelineServerEnabled(false)
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    // Should create handle but log warning about unsupported type
    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    assertNotNull(handle);
  }

  @Test
  public void testEventTimeFieldIndexWithMissingField() throws Exception {
    // Schema without event_time field
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "event_time");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(props)
        .withEmbeddedTimelineServerEnabled(false)
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    // Should create handle but log warning about missing field
    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    assertNotNull(handle);
  }

  @Test
  public void testEventTimeFieldIndexWithNoConfiguration() throws Exception {
    // Schema with a DOUBLE field but no event_time configured
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("timestamp", DataTypes.DOUBLE()),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        // No event time field configured
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    assertNotNull(handle);
  }

  @Test
  public void testEventTimeExtractionWithValidDoubleField() throws Exception {
    // Schema with DOUBLE event_time field
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("event_time", DataTypes.DOUBLE()),  // epoch seconds
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "event_time");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(props)
        .withEmbeddedTimelineServerEnabled(false)
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    // Create test records with event time
    double eventTimeSeconds1 = 1729512000.0; // 2024-10-21 12:00:00
    double eventTimeSeconds2 = 1729515600.0; // 2024-10-21 13:00:00

    GenericRowData row1 = new GenericRowData(5);
    row1.setField(0, StringData.fromString("id1"));
    row1.setField(1, StringData.fromString("Alice"));
    row1.setField(2, 25);
    row1.setField(3, eventTimeSeconds1);
    row1.setField(4, StringData.fromString(PARTITION_PATH));

    GenericRowData row2 = new GenericRowData(5);
    row2.setField(0, StringData.fromString("id2"));
    row2.setField(1, StringData.fromString("Bob"));
    row2.setField(2, 30);
    row2.setField(3, eventTimeSeconds2);
    row2.setField(4, StringData.fromString(PARTITION_PATH));

    // Write records
    handle.write("id1", PARTITION_PATH, row1);
    handle.write("id2", PARTITION_PATH, row2);

    // Get write status and verify event time
    WriteStatus writeStatus = handle.close();
    assertNotNull(writeStatus);
    assertNotNull(writeStatus.getStat());

    // Verify min/max event times are populated
    Long minEventTime = writeStatus.getStat().getMinEventTime();
    Long maxEventTime = writeStatus.getStat().getMaxEventTime();

    assertNotNull(minEventTime, "Min event time should be populated");
    assertNotNull(maxEventTime, "Max event time should be populated");

    // Verify the values are correct (converted from seconds to millis)
    assertEquals((long) (eventTimeSeconds1 * 1000), minEventTime.longValue(),
        "Min event time should match first record");
    assertEquals((long) (eventTimeSeconds2 * 1000), maxEventTime.longValue(),
        "Max event time should match second record");

    // Verify min <= max
    assertTrue(minEventTime <= maxEventTime, "Min event time should be <= max event time");
  }

  @Test
  public void testEventTimeExtractionWithBigintMillis() throws Exception {
    // Schema with BIGINT event_time field (millis)
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("event_time", DataTypes.BIGINT()),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "event_time");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(props)
        .withEmbeddedTimelineServerEnabled(false)
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    long eventTimeMillis1 = 1729512000000L;
    long eventTimeMillis2 = 1729515600000L;

    GenericRowData row1 = new GenericRowData(5);
    row1.setField(0, StringData.fromString("id1"));
    row1.setField(1, StringData.fromString("Alice"));
    row1.setField(2, 25);
    row1.setField(3, eventTimeMillis1);
    row1.setField(4, StringData.fromString(PARTITION_PATH));

    GenericRowData row2 = new GenericRowData(5);
    row2.setField(0, StringData.fromString("id2"));
    row2.setField(1, StringData.fromString("Bob"));
    row2.setField(2, 30);
    row2.setField(3, eventTimeMillis2);
    row2.setField(4, StringData.fromString(PARTITION_PATH));

    handle.write("id1", PARTITION_PATH, row1);
    handle.write("id2", PARTITION_PATH, row2);

    WriteStatus writeStatus = handle.close();
    assertNotNull(writeStatus);
    Long minEventTime = writeStatus.getStat().getMinEventTime();
    Long maxEventTime = writeStatus.getStat().getMaxEventTime();
    assertNotNull(minEventTime);
    assertNotNull(maxEventTime);
    assertEquals(eventTimeMillis1, minEventTime.longValue());
    assertEquals(eventTimeMillis2, maxEventTime.longValue());
  }

  @Test
  public void testEventTimeExtractionWithNullValues() throws Exception {
    // Schema with DOUBLE event_time field
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("event_time", DataTypes.DOUBLE()),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "event_time");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(props)
        .withEmbeddedTimelineServerEnabled(false)
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    // Create record with null event time
    GenericRowData row = new GenericRowData(5);
    row.setField(0, StringData.fromString("id1"));
    row.setField(1, StringData.fromString("Alice"));
    row.setField(2, 25);
    row.setField(3, null);  // null event time
    row.setField(4, StringData.fromString(PARTITION_PATH));

    // Write record
    handle.write("id1", PARTITION_PATH, row);

    // Get write status
    WriteStatus writeStatus = handle.close();
    assertNotNull(writeStatus);
    assertNotNull(writeStatus.getStat());

    // Verify event times are null when input is null
    assertNull(writeStatus.getStat().getMinEventTime(),
        "Min event time should be null when input is null");
    assertNull(writeStatus.getStat().getMaxEventTime(),
        "Max event time should be null when input is null");
  }

  @Test
  public void testEventTimeExtractionWithoutConfiguration() throws Exception {
    // Schema with DOUBLE field but no event_time configured
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("timestamp", DataTypes.DOUBLE()),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    RowType baseRowType = (RowType) dataType.getLogicalType();
    RowType rowTypeWithMetadata = addMetadataFields(baseRowType, false);

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        // No event time field configured
        .build();

    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, context, metaClient);

    HoodieRowDataCreateHandle handle = new HoodieRowDataCreateHandle(
        table, config, PARTITION_PATH, FILE_ID, INSTANT_TIME,
        TASK_PARTITION_ID, TASK_ID, TASK_EPOCH_ID, rowTypeWithMetadata, false, false);

    // Create record with timestamp
    GenericRowData row = new GenericRowData(5);
    row.setField(0, StringData.fromString("id1"));
    row.setField(1, StringData.fromString("Alice"));
    row.setField(2, 25);
    row.setField(3, 1729512000.0);
    row.setField(4, StringData.fromString(PARTITION_PATH));

    // Write record
    handle.write("id1", PARTITION_PATH, row);

    // Get write status
    WriteStatus writeStatus = handle.close();
    assertNotNull(writeStatus);
    assertNotNull(writeStatus.getStat());

    // Verify event times are null when not configured
    assertNull(writeStatus.getStat().getMinEventTime(),
        "Min event time should be null when not configured");
    assertNull(writeStatus.getStat().getMaxEventTime(),
        "Max event time should be null when not configured");
  }
}
