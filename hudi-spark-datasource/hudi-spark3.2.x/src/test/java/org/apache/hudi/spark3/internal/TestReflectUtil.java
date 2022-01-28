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

package org.apache.hudi.spark3.internal;

import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests {@link ReflectUtil}.
 */
public class TestReflectUtil extends HoodieClientTestBase {

  @Test
  public void testDataSourceWriterExtraCommitMetadata() throws Exception {
    SparkSession spark = sqlContext.sparkSession();

    String insertIntoSql = "insert into test_reflect_util values (1, 'z3', 1, '2021')";
    InsertIntoStatement statement = (InsertIntoStatement) spark.sessionState().sqlParser().parsePlan(insertIntoSql);

    InsertIntoStatement newStatment = ReflectUtil.createInsertInto(
        statement.table(),
        statement.partitionSpec(),
        scala.collection.immutable.List.empty(),
        statement.query(),
        statement.overwrite(),
        statement.ifPartitionNotExists());

    Assertions.assertTrue(
        ((UnresolvedRelation)newStatment.table()).multipartIdentifier().contains("test_reflect_util"));
  }
}
