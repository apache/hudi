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

package org.apache.hudi.client.model;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HoodieFlinkRecord extends HoodieRecord<RowData> {
  private Comparable<?> orderingValue = 0;

  public HoodieFlinkRecord(RowData rowData) {
    super(null, rowData);
  }

  public HoodieFlinkRecord(HoodieKey key, HoodieOperation op, Comparable<?> orderingValue, RowData rowData) {
    super(key, rowData, op, Option.empty());
    this.orderingValue = orderingValue;
  }

  @Override
  public HoodieRecord<RowData> newInstance() {
    return new HoodieFlinkRecord(this.data);
  }

  @Override
  public HoodieRecord<RowData> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieFlinkRecord(key, op, orderingValue, this.data);
  }

  @Override
  public HoodieRecord<RowData> newInstance(HoodieKey key) {
    return null;
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    return this.orderingValue;
  }

  @Override
  public HoodieOperation getOperation() {
    return HoodieOperation.fromValue(getData().getRowKind().toByteValue());
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.FLINK;
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return "";
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return getRecordKey();
  }

  @Override
  protected void writeRecordPayload(RowData payload, Kryo kryo, Output output) {

  }

  @Override
  protected RowData readRecordPayload(Kryo kryo, Input input) {
    return null;
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    return new Object[0];
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    return null;
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    return null;
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    return null;
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) throws IOException {
    return false;
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    return false;
  }

  @Override
  public HoodieRecord<RowData> copy() {
    return null;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return null;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
                                                            Option<String> partitionNameOp, Boolean populateMetaFieldsOp, Option<Schema> schemaWithoutMetaFields) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    return null;
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) throws IOException {
    return null;
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    return null;
  }
}
