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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieNativeAvroHFileReader {

  private static final TypedProperties DEFAULT_PROPS = new TypedProperties();
  private static HoodieNativeAvroHFileReader reader;

  TestHoodieNativeAvroHFileReader() {
    HoodieStorage storage = mock(HoodieStorage.class);
    StoragePath path = new StoragePath("anyPath");
    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage).withProps(DEFAULT_PROPS)
        .withPath(path).build();
    reader = HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory).path(path).build();
  }

  @Test
  void testExtractKeysWithValidInPredicate() {
    // Mock expressions to return "key1", "key2"
    Expression expr1 = mock(Expression.class);
    Expression expr2 = mock(Expression.class);
    when(expr1.eval(null)).thenReturn("key1");
    when(expr2.eval(null)).thenReturn("key2");

    // Mock IN predicate
    Predicates.In inPredicate = mock(Predicates.In.class);
    when(inPredicate.getOperator()).thenReturn(Expression.Operator.IN);
    when(inPredicate.getRightChildren()).thenReturn(Arrays.asList(expr1, expr2));

    List<String> keys = reader.extractKeys(Option.of(inPredicate));
    assertEquals(Arrays.asList("key1", "key2"), keys);
  }

  @Test
  void testExtractKeysWithEmptyOption() {
    List<String> keys = reader.extractKeys(Option.empty());
    assertTrue(keys.isEmpty());
  }

  @Test
  void testExtractKeysWithNonInPredicate() {
    // Mock some other predicate
    Predicate otherPredicate = mock(Predicate.class);
    when(otherPredicate.getOperator()).thenReturn(Predicate.Operator.EQ);

    List<String> keys = reader.extractKeys(Option.of(otherPredicate));
    assertTrue(keys.isEmpty());
  }

  @Test
  void testExtractKeysWithInPredicateButNoChildren() {
    Predicates.In inPredicate = mock(Predicates.In.class);
    when(inPredicate.getOperator()).thenReturn(Predicate.Operator.IN);
    when(inPredicate.getRightChildren()).thenReturn(Collections.emptyList());

    List<String> keys = reader.extractKeys(Option.of(inPredicate));
    assertTrue(keys.isEmpty());
  }

  @Test
  void testExtractKeyPrefixesWithValidStartsWithPredicate() {
    // Mock expressions to return "prefix1", "prefix2"
    Expression expr1 = mock(Expression.class);
    Expression expr2 = mock(Expression.class);
    when(expr1.eval(null)).thenReturn("prefix1");
    when(expr2.eval(null)).thenReturn("prefix2");

    // Mock StringStartsWithAny predicate
    Predicates.StringStartsWithAny startsWithPredicate = mock(Predicates.StringStartsWithAny.class);
    when(startsWithPredicate.getOperator()).thenReturn(Expression.Operator.STARTS_WITH);
    when(startsWithPredicate.getRightChildren()).thenReturn(Arrays.asList(expr1, expr2));

    List<String> prefixes = reader.extractKeyPrefixes(Option.of(startsWithPredicate));
    assertEquals(Arrays.asList("prefix1", "prefix2"), prefixes);
  }

  @Test
  void testExtractKeyPrefixesWithEmptyOption() {
    List<String> prefixes = reader.extractKeyPrefixes(Option.empty());
    assertTrue(prefixes.isEmpty());
  }

  @Test
  void testExtractKeyPrefixesWithNonStartsWithPredicate() {
    Predicate otherPredicate = mock(Predicate.class);
    when(otherPredicate.getOperator()).thenReturn(Expression.Operator.EQ);  // Not STARTS_WITH

    List<String> prefixes = reader.extractKeyPrefixes(Option.of(otherPredicate));
    assertTrue(prefixes.isEmpty());
  }

  @Test
  void testExtractKeyPrefixesWithStartsWithPredicateButNoChildren() {
    Predicates.StringStartsWithAny startsWithPredicate = mock(Predicates.StringStartsWithAny.class);
    when(startsWithPredicate.getOperator()).thenReturn(Expression.Operator.STARTS_WITH);
    when(startsWithPredicate.getRightChildren()).thenReturn(Collections.emptyList());

    List<String> prefixes = reader.extractKeyPrefixes(Option.of(startsWithPredicate));
    assertTrue(prefixes.isEmpty());
  }

  @Test
  public void testReadHFile() {
    StoragePath path = new StoragePath("/Users/ethan/Work/tmp/20251014-mdt-compaction/secondary-file-group/.secondary-index-key-0000-0_20251013234907189.log.1_44-274-62496");
    HoodieStorage storage = HoodieTestUtils.getStorage(path);
    /**
     HoodieMergedLogRecordReader<T> logRecordReader = HoodieMergedLogRecordReader.<T>newBuilder()
     .withHoodieReaderContext(readerContext)
     .withStorage(storage)
     .withLogFiles(inputSplit.getLogFiles())
     .withReverseReader(false)
     .withBufferSize(getIntWithAltKeys(props, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE))
     .withInstantRange(readerContext.getInstantRange())
     .withPartition(inputSplit.getPartitionPath())
     .withRecordBuffer(recordBuffer)
     .withAllowInflightInstants(readerParameters.allowInflightInstants())
     .withMetaClient(hoodieTableMetaClient)
     .withOptimizedLogBlocksScan(readerParameters.enableOptimizedLogBlockScan())
     .build()
     **/
  }
}
