/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.examples.k8s.quickstart.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Util class for testing data generation.
 */
public class DataGenerator {

  public static List<RowData> generateRandomRowData(DataType dataType) {

    // For Every Batch, it adds two new rows with RANDOM uuid and updates the row with uuid
    // "334e26e9-8355-45cc-97c6-c31daf0df330"  and  "7fd3fd07-cf04-4a1d-9511-142736932983"
    return Arrays.asList(
        DataGenerator.createRowData(dataType, TimestampData.fromEpochMillis(System.currentTimeMillis()),
            StringData.fromString(UUID.randomUUID().toString()), StringData.fromString("rider-A"),
            StringData.fromString("driver-K"), 1.0 + Math.random() * (90), StringData.fromString("san_francisco")),
        DataGenerator.createRowData(dataType,TimestampData.fromEpochMillis(System.currentTimeMillis()),
            StringData.fromString(UUID.randomUUID().toString()), StringData.fromString("rider-B"),
            StringData.fromString("driver-M"), 1.0 + Math.random() * (90), StringData.fromString("brazil")),
        DataGenerator.createRowData(dataType,TimestampData.fromEpochMillis(System.currentTimeMillis()),
            StringData.fromString("334e26e9-8355-45cc-97c6-c31daf0df330"), StringData.fromString("rider-C"),
            StringData.fromString("driver-L"), 15.4, StringData.fromString("chennai")),
        DataGenerator.createRowData(dataType,TimestampData.fromEpochMillis(System.currentTimeMillis()),
            StringData.fromString("7fd3fd07-cf04-4a1d-9511-142736932983"), StringData.fromString("rider-D"),
            StringData.fromString("driver-N"), 1.0 + Math.random() * (90), StringData.fromString("london"))
    );
  }

  public static BinaryRowData createRowData(DataType dataType, Object... fields) {
    RowType rowType = (RowType) dataType.getLogicalType();
    LogicalType[] types = rowType.getFields().stream().map(RowType.RowField::getType)
        .toArray(LogicalType[]::new);
    BinaryRowData row = new BinaryRowData(fields.length);
    BinaryRowWriter writer = new BinaryRowWriter(row);
    writer.reset();
    for (int i = 0; i < fields.length; i++) {
      Object field = fields[i];
      if (field == null) {
        writer.setNullAt(i);
      } else {
        BinaryWriter.write(writer, i, field, types[i], InternalSerializers.create(types[i]));
      }
    }
    writer.complete();
    return row;
  }
}
