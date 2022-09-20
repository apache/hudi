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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HoodieEmptyRecord<T> extends HoodieRecord<T> {

  private final HoodieRecordType type;
  private final Comparable<?> orderingVal;

  public HoodieEmptyRecord(HoodieKey key, HoodieRecordType type) {
    super(key, null);
    this.type = type;
    this.orderingVal = null;
  }

  public HoodieEmptyRecord(HoodieKey key, HoodieOperation operation, Comparable<?> orderingVal, HoodieRecordType type) {
    super(key, null, operation);
    this.type = type;
    this.orderingVal = orderingVal;
  }

  public HoodieEmptyRecord(HoodieRecord<T> record, HoodieRecordType type) {
    super(record);
    this.type = type;
    this.orderingVal = record.getOrderingValue(new Properties());
  }

  public HoodieEmptyRecord(HoodieRecordType type) {
    this.type = type;
    this.orderingVal = null;
  }

  @Override
  public T getData() {
    return null;
  }

  @Override
  public Comparable<?> getOrderingValue(Properties props) {
    return orderingVal;
  }

  @Override
  public HoodieRecord<T> newInstance() {
    return this;
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieEmptyRecord<>(key, op, orderingVal, type);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key) {
    return new HoodieEmptyRecord<>(key, type);
  }

  @Override
  public HoodieRecordType getRecordType() {
    return type;
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    return key.getRecordKey();
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    return key.getRecordKey();
  }

  @Override
  public Object getRecordColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord updateMetadataValues(Schema recordSchema, Properties props, MetadataValues metadataValues) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDelete(Schema schema, Properties props) throws IOException {
    return true;
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties props) throws IOException {
    return false;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema schema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation, Option<String> partitionNameOp,
      Boolean populateMetaFieldsOp)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema schema, Properties props) throws IOException {
    return Option.empty();
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }
}
