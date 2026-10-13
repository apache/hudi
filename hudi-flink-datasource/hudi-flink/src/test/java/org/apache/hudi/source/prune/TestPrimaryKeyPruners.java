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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.bulk.RowDataKeyGen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

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

  @Test
  void testTimestampBucketIdMatchesWriter() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "ts");
    LocalDateTime timestamp = LocalDateTime.parse("1970-01-01T00:00:01.000001");
    ResolvedExpression filter = CallExpression.permanent(BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(new FieldReferenceExpression("ts", DataTypes.TIMESTAMP(6), 0, 0), new ValueLiteralExpression(timestamp)),
        DataTypes.BOOLEAN());
    for (boolean logicalTimestamp : new boolean[] {true, false}) {
      conf.setString(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), Boolean.toString(logicalTimestamp));
      String key = RowDataKeyGen.getRecordKey(TimestampData.fromLocalDateTime(timestamp), "ts", logicalTimestamp);
      Function<Integer, Integer> bucketId = PrimaryKeyPruners.getBucketIdFunc(Collections.singletonList(filter), conf);
      assertEquals(BucketIdentifier.getBucketId(Collections.singletonList(key), 128), bucketId.apply(128));
    }
  }

  @Test
  void testPartitionBucketIdFunctionIsDisabledWithoutBucketFunction() {
    assertNull(PartitionBucketIdFunc.create(Option.empty(), mock(HoodieTableMetaClient.class), 8));
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
