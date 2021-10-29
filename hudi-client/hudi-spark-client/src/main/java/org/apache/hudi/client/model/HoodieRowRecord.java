/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.model;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.hudi.client.utils.ScalaConversions.toList;
import static org.apache.hudi.client.utils.ScalaConversions.toSeq;

public class HoodieRowRecord extends HoodieRecord<Row> implements Serializable {

  private String commitTime;
  private String commitSeqNumber;
  private String recordKey;
  private String partitionPath;
  private String fileName;
  private Row row;

  public HoodieRowRecord(HoodieKey key, Row data) {
    super(key, data);
  }

  public HoodieRowRecord(HoodieRecord < Row > record) {
    super(record);
  }

  public HoodieRowRecord(HoodieKey key, Row data, HoodieOperation operation) {
    super(key, data, operation);
  }

  public HoodieRowRecord() {
  }

  @Override
  public Row getData () {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  @Override
  public HoodieRecord<Row> newInstance() {
    return fromHoodieRow(row);
  }

  private static Row originalRow(Object[] values, StructType schema) {
    Object[] originalValues = Arrays.copyOfRange(values, 5, values.length);
    StructField[] fields = schema.fields();
    StructType originalSchema = new StructType();
    for (int i = 5; i < fields.length; i++) {
      originalSchema = originalSchema.add(fields[i]);
    }
    return new GenericRowWithSchema(originalValues, originalSchema);
  }

  public static HoodieRowRecord fromHoodieRow(Row r) {
    HoodieRowRecord rr = new HoodieRowRecord();
    rr.commitTime = r.getString(0);
    rr.commitSeqNumber = r.getString(1);
    rr.recordKey = r.getString(2);
    rr.partitionPath = r.getString(3);
    rr.fileName = r.getString(4);
    Object[] values = toList(r.toSeq()).toArray(new Object[r.length()]);
    rr.row = originalRow(values, r.schema());
    return rr;
  }

  public static HoodieRowRecord fromHoodieInternalRow(InternalRow ir, StructType schema) {
    HoodieRowRecord rr = new HoodieRowRecord();
    rr.commitTime = new String(ir.getUTF8String(0).getBytes());
    rr.commitSeqNumber = new String(ir.getUTF8String(1).getBytes());
    rr.recordKey = new String(ir.getUTF8String(2).getBytes());
    rr.partitionPath = new String(ir.getUTF8String(3).getBytes());
    rr.fileName = new String(ir.getUTF8String(4).getBytes());
    Object[] values = toList(ir.toSeq(schema)).toArray(new Object[schema.length()]);
    rr.row = originalRow(values, schema);
    return rr;
  }

  public InternalRow toInternalRow() {
    StructField[] fields = row.schema().fields();
    Object[] values = new Object[fields.length + 5];
    values[0] = UTF8String.fromString(commitTime);
    values[1] = UTF8String.fromString(commitSeqNumber);
    values[2] = UTF8String.fromString(recordKey);
    values[3] = UTF8String.fromString(partitionPath);
    values[4] = UTF8String.fromString(fileName);
    for (int i = 0; i < fields.length; i++) {
      StructField f = fields[i];
      if (f.dataType().equals(DataTypes.StringType)) {
        values[i + 5] = UTF8String.fromBytes(row.getAs(f.name()).toString().getBytes());
      } else {
        values[i + 5] = row.getAs(f.name());
      }
    }
    return GenericInternalRow.fromSeq(toSeq(values));
  }

  public static StructType getHoodieSchema(StructType original) {
    StructType schema = new StructType()
        .add(StructField.apply(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, true, Metadata.empty()))
        .add(StructField.apply(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, true, Metadata.empty()))
        .add(StructField.apply(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, true, Metadata.empty()))
        .add(StructField.apply(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, true, Metadata.empty()))
        .add(StructField.apply(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, true, Metadata.empty()));
    for (StructField f : original.fields()) {
      schema = schema.add(f);
    }
    return schema;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public void setCommitTime(String commitTime) {
    this.commitTime = commitTime;
  }

  public String getCommitSeqNumber() {
    return commitSeqNumber;
  }

  public void setCommitSeqNumber(String commitSeqNumber) {
    this.commitSeqNumber = commitSeqNumber;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public void setRecordKey(String recordKey) {
    this.recordKey = recordKey;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public Row getRow() {
    return row;
  }

  public void setRow(Row row) {
    this.row = row;
  }
}
