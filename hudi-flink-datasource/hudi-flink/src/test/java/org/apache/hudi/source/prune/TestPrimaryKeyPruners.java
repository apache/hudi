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

package org.apache.hudi.source.prune;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.sink.bulk.RowDataKeyGens;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.util.InstantiationUtil.deserializeObject;
import static org.apache.flink.util.InstantiationUtil.serializeObject;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class TestPrimaryKeyPruners {

  @Test
  void testBucketIdFollowsRecordKeyFieldOrderRegardlessOfFilterOrder() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id,tenant");
    ResolvedExpression tenantFilter = equality("tenant", "tenant-a", true);
    ResolvedExpression idFilter = equality("id", "id-1", false);

    Function<Integer, Integer> bucketId =
        PrimaryKeyPruners.getBucketIdFunc(Arrays.asList(tenantFilter, idFilter), conf);

    List<String> orderedValues = Arrays.asList("id-1", "tenant-a");
    assertEquals(BucketIdentifier.getBucketId(orderedValues, 8), bucketId.apply(8));
    assertEquals(BucketIdentifier.getBucketId(orderedValues, 16), bucketId.apply(16));
  }

  @ParameterizedTest
  @MethodSource("compoundKeyValues")
  void testCompoundKeyBucketIdMatchesWriter(String id, String tenant, String indexKeys) {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id,tenant");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
    conf.set(FlinkOptions.INDEX_KEY_FIELD, indexKeys);
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.STRING()),
        DataTypes.FIELD("tenant", DataTypes.STRING())).getLogicalType();
    String recordKey = RowDataKeyGens.instance(conf, rowType).getRecordKey(
        GenericRowData.of(StringData.fromString(id), StringData.fromString(tenant)));
    List<ResolvedExpression> filters = "id".equals(indexKeys)
        ? Collections.singletonList(equality("id", id, false))
        : Arrays.asList(equality("tenant", tenant, true), equality("id", id, false));
    Function<Integer, Integer> bucketId = PrimaryKeyPruners.getBucketIdFunc(filters, conf);

    for (int numBuckets : new int[] {8, 16, 31}) {
      assertEquals(BucketIdentifier.getBucketId(recordKey, indexKeys, numBuckets), bucketId.apply(numBuckets));
    }
  }

  private static Stream<Arguments> compoundKeyValues() {
    return Stream.of("id,tenant", "tenant,id", "id").flatMap(indexKeys -> Stream.of(
        Arguments.of("__empty__", "x", indexKeys),
        Arguments.of("__null__", "x", indexKeys),
        Arguments.of("", "x", indexKeys),
        Arguments.of("id-1", "__empty__", indexKeys),
        Arguments.of("id-1", "__null__", indexKeys),
        Arguments.of("id-1", "tenant-a", indexKeys)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"__empty__", "__null__", "id-1"})
  void testSingleKeyPlaceholdersRemainLiteral(String id) {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
    RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING())).getLogicalType();
    String recordKey = RowDataKeyGens.instance(conf, rowType).getRecordKey(GenericRowData.of(StringData.fromString(id)));
    Function<Integer, Integer> bucketId = PrimaryKeyPruners.getBucketIdFunc(
        Collections.singletonList(equality("id", id, false)), conf);

    for (int numBuckets : new int[] {8, 16, 31}) {
      assertEquals(BucketIdentifier.getBucketId(recordKey, "id", numBuckets), bucketId.apply(numBuckets));
    }
  }

  @Test
  void testPartitionBucketIdFunctionIsDisabledWithoutBucketFunction() {
    assertNull(PartitionBucketIdFunc.create(Option.empty(), mock(HoodieTableMetaClient.class), 8));
  }

  @Test
  void testBucketIdFunctionIsSerializable() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id,tenant");
    Function<Integer, Integer> bucketId = PrimaryKeyPruners.getBucketIdFunc(
        Arrays.asList(equality("tenant", "tenant-a", true), equality("id", "id-1", false)), conf);

    // the source V2 ships the function to the job manager within the job graph
    Function<Integer, Integer> deserialized = deserializeObject(serializeObject(bucketId), getClass().getClassLoader());

    List<String> orderedValues = Arrays.asList("id-1", "tenant-a");
    assertEquals(BucketIdentifier.getBucketId(orderedValues, 8), deserialized.apply(8));
    assertEquals(BucketIdentifier.getBucketId(orderedValues, 16), deserialized.apply(16));
  }

  private static ResolvedExpression equality(String field, String value, boolean literalFirst) {
    FieldReferenceExpression fieldReference =
        new FieldReferenceExpression(field, DataTypes.STRING(), 0, 0);
    ValueLiteralExpression literal =
        new ValueLiteralExpression(value, DataTypes.STRING().notNull());
    return CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        literalFirst ? Arrays.asList(literal, fieldReference) : Arrays.asList(fieldReference, literal),
        DataTypes.BOOLEAN());
  }
}
