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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.util.ExpressionUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utilities for primary key based file pruning.
 */
public class PrimaryKeyPruners {
  private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyPruners.class);

  public static Function<Integer, Integer> getBucketIdFunc(List<ResolvedExpression> hashKeyFilters, Configuration conf) {
    List<String> pkFields = Arrays.asList(conf.get(FlinkOptions.RECORD_KEY_FIELD).split(","));
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
