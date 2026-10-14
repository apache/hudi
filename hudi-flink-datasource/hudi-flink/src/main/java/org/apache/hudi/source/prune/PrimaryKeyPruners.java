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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.util.ExpressionUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.util.function.SerializableFunction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for primary key based file pruning.
 */
@Slf4j
public class PrimaryKeyPruners {

  /**
   * Returns the bucket id function for a table whose single-field record keys are stored as the bare value,
   * see {@link #getBucketIdFunc(List, Configuration, boolean)}.
   */
  @VisibleForTesting
  public static SerializableFunction<Integer, Integer> getBucketIdFunc(List<ResolvedExpression> hashKeyFilters, Configuration conf) {
    return getBucketIdFunc(hashKeyFilters, conf, false);
  }

  /**
   * Returns the function that computes the bucket id of the given hash key filters by the number of buckets.
   *
   * @param hashKeyFilters         The equivalence filters on all the bucket index key fields
   * @param conf                   The table configuration
   * @param prefixedSingleFieldKey Whether a single-field record key is stored as {@code <field>:<value>}
   */
  public static SerializableFunction<Integer, Integer> getBucketIdFunc(
      List<ResolvedExpression> hashKeyFilters, Configuration conf, boolean prefixedSingleFieldKey) {
    List<String> pkFields = Arrays.asList(OptionsResolver.getRecordKeys(conf));
    // step1: resolve the hash key values
    final boolean logicalTimestamp = OptionsResolver.isConsistentLogicalTimestampEnabled(conf);
    List<String> values = hashKeyFilters.stream()
        .map(filter -> {
          Pair<FieldReferenceExpression, ValueLiteralExpression> children = castChildAs(filter.getChildren());
          return Pair.of(pkFields.indexOf(children.getLeft().getName()), StringUtils.objToString(ExpressionUtils.getKeyFromLiteral(children.getRight(), logicalTimestamp)));
        })
        // IMPORTANT: follows KeyGenUtils#extractRecordKeysByFields,
        // the hash keys must be evaluated in the record key field sequence.
        .sorted(java.util.Map.Entry.comparingByKey())
        .map(Pair::getValue)
        .collect(Collectors.toList());
    // step2: generate bucket id
    if (prefixedSingleFieldKey && pkFields.size() == 1) {
      // writers hash the record key as stored, so rebuild the `<field>:<value>` key and hash it the same way
      String recordKey = pkFields.get(0) + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR + values.get(0);
      List<String> indexKeyFields = OptionsResolver.getIndexKeyFields(conf);
      return (numBuckets) -> BucketIdentifier.getBucketId(recordKey, indexKeyFields, numBuckets);
    }
    return (numBuckets) -> BucketIdentifier.getBucketId(values, numBuckets);
  }

  private static Pair<FieldReferenceExpression, ValueLiteralExpression> castChildAs(List<Expression> children) {
    Expression lExpr = children.get(0);
    Expression rExpr = children.get(1);
    if (lExpr instanceof FieldReferenceExpression) {
      return Pair.of((FieldReferenceExpression) lExpr, (ValueLiteralExpression) rExpr);
    } else {
      return Pair.of((FieldReferenceExpression) rExpr, (ValueLiteralExpression) lExpr);
    }
  }
}
