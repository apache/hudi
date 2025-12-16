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

import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HoodieEmptyRecord<T> extends HoodieRecord<T> {

  private HoodieRecordType type;
  private Comparable<?> orderingVal;

  public HoodieEmptyRecord(HoodieKey key, HoodieRecordType type) {
    super(key, null);
    this.type = type;
    // IMPORTANT:
    // This should be kept in line with EmptyHoodieRecordPayload
    // default natural order
    this.orderingVal = 0;
  }

  public HoodieEmptyRecord(HoodieKey key, HoodieOperation operation, Comparable<?> orderingVal, HoodieRecordType type) {
    super(key, null, operation, Option.empty());
    this.type = type;
    this.orderingVal = orderingVal;
  }

  @Override
  public T getData() {
    return null;
  }

  @Override
  public Comparable<?> doGetOrderingValue(Schema recordSchema, Properties props, String[] orderingFields) {
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
  public String getRecordKey(Schema recordSchema,
      Option<BaseKeyGenerator> keyGeneratorOpt) {
    return key.getRecordKey();
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return key.getRecordKey();
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getColumnValueAsJava(Schema recordSchema, String column, Properties props) {
    return null;
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean checkIsDelete(DeleteContext deleteContext, Properties props) {
    return true;
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    return false;
  }

  @Override
  public HoodieRecord<T> copy() {
    return this;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation, Option<String> partitionNameOp, Boolean populateMetaFieldsOp, Option<Schema> schemaWithoutMetaFields) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema,
      Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    return Option.empty();
  }

  @Override
  public ByteArrayOutputStream getAvroBytes(Schema recordSchema, Properties props) {
    return new ByteArrayOutputStream(0);
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @Override
  protected final void writeRecordPayload(T payload, Kryo kryo, Output output) {
    kryo.writeObject(output, type);
    // NOTE: Since [[orderingVal]] is polymorphic we have to write out its class
    //       to be able to properly deserialize it
    kryo.writeClassAndObject(output, orderingVal);
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @Override
  protected final T readRecordPayload(Kryo kryo, Input input) {
    this.type = kryo.readObject(input, HoodieRecordType.class);
    this.orderingVal = (Comparable<?>) kryo.readClassAndObject(input);
    // NOTE: [[EmptyRecord]]'s payload is always null
    return null;
  }

  @Override
  public Object convertColumnValueForLogicalType(Schema fieldSchema,
                                                 Object fieldValue,
                                                 boolean keepConsistentLogicalTimestamp) {
    return null;
  }
}
