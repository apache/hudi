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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;

public class HoodieTransformerSchemaException extends HoodieSchemaException {
  Option<Set<Attribute>> missingInputColumnsOpt = Option.empty();
  Option<Set<Attribute>> inputColumnsOpt = Option.empty();
  Option<Set<Attribute>> outputColumnsOpt = Option.empty();
  Option<LogicalPlan> failedLogicalPlanOpt = Option.empty();

  public HoodieTransformerSchemaException(String message, Throwable t) {
    super(message, t);
    if (t instanceof AnalysisException) {
      this.failedLogicalPlanOpt = Option.ofNullable(((AnalysisException) t).plan().getOrElse(null));
      this.failedLogicalPlanOpt.ifPresent(plan -> populateColumns(plan));
    }
  }

  private void populateColumns(LogicalPlan plan) {
    missingInputColumnsOpt = Option.of(JavaConversions.setAsJavaSet(plan.missingInput().toSet()));
    inputColumnsOpt = Option.of(JavaConversions.setAsJavaSet(plan.inputSet().toSet()));
    outputColumnsOpt = Option.of(JavaConversions.setAsJavaSet(plan.outputSet().toSet()));
  }

  public Option<Set<Attribute>> getNewColumns() {
    Option<Set<Attribute>> newColumns = Option.ofNullable(outputColumnsOpt.isPresent() ? new HashSet(outputColumnsOpt.get()) : null);
    newColumns.ifPresent(cols -> cols.removeAll(inputColumnsOpt.orElse(Collections.emptySet())));
    return newColumns;
  }

  public Option<List<Expression>> getUnresolvedExpressions() {
    return failedLogicalPlanOpt.map(plan -> (List<Expression>) JavaConversions.seqAsJavaList(plan.expressions()))
        .map(list -> list.stream().filter(e -> !e.resolved()).collect(Collectors.toList()));
  }

  public Option<Set<Attribute>> getMissingInputColumns() {
    return missingInputColumnsOpt;
  }

  public Option<Set<Attribute>> getInputColumns() {
    return inputColumnsOpt;
  }

  public Option<Set<Attribute>> getOutputColumns() {
    return outputColumnsOpt;
  }

  public Option<LogicalPlan> getFailedLogicalPlanOpt() {
    return failedLogicalPlanOpt;
  }
}
