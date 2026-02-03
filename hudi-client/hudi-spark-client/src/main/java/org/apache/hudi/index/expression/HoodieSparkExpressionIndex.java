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

package org.apache.hudi.index.expression;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Column;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class HoodieSparkExpressionIndex implements HoodieExpressionIndex<Column, Column>, Serializable {

  @Getter
  private String indexName;
  @Getter
  private String indexFunction;
  private String indexType;
  @Getter
  private List<String> orderedSourceFields;
  private Map<String, String> options;
  private ExpressionIndexSparkFunctions.SparkFunction sparkFunction;

  public HoodieSparkExpressionIndex(HoodieIndexDefinition indexDefinition) {
    this.indexName = indexDefinition.getIndexName();
    this.indexFunction = indexDefinition.getIndexFunction();
    this.indexType = indexDefinition.getIndexType();
    this.orderedSourceFields = indexDefinition.getSourceFields();
    this.options = indexDefinition.getIndexOptions();

    // Check if the function from the expression exists in our map
    this.sparkFunction = ExpressionIndexSparkFunctions.SparkFunction.getSparkFunction(indexFunction);
    if (this.sparkFunction == null) {
      throw new IllegalArgumentException("Unsupported Spark function: " + indexFunction);
    }
  }

  @Override
  public Column apply(List<Column> orderedSourceValues) {
    if (orderedSourceValues.size() != orderedSourceFields.size()) {
      throw new IllegalArgumentException("Mismatch in number of source values and fields in the expression");
    }
    sparkFunction.validateOptions(options, indexType);
    return sparkFunction.apply(orderedSourceValues, options);
  }

  @AllArgsConstructor
  @Getter
  public static class ExpressionIndexComputationMetadata {
    HoodieData<HoodieRecord> expressionIndexRecords;
    Option<HoodieData<HoodieRecord>> partitionStatRecordsOpt;

    public ExpressionIndexComputationMetadata(HoodieData<HoodieRecord> expressionIndexRecords) {
      this.expressionIndexRecords = expressionIndexRecords;
      this.partitionStatRecordsOpt = Option.empty();
    }
  }
}
