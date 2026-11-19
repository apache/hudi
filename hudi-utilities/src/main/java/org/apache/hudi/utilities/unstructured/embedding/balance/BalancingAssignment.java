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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Output of a {@link RowBalancingStrategy}: {@code numPartitions} and a Dataset identifying
 * the source version (sha, modification time, size) and {@code _balance_partition} (INT in [0, numPartitions)).
 */
public class BalancingAssignment {

  public static final String SOURCE_PATH_SHA = BlobColumns.SOURCE_PATH_SHA;
  public static final String SOURCE_MODIFICATION_TIME = BlobColumns.SOURCE_MODIFICATION_TIME;
  public static final String SOURCE_FILE_SIZE = BlobColumns.SOURCE_FILE_SIZE;
  public static final List<String> KEY_COLUMNS = Collections.unmodifiableList(Arrays.asList(SOURCE_PATH_SHA,
      SOURCE_MODIFICATION_TIME, SOURCE_FILE_SIZE));
  public static final String PARTITION = "_balance_partition";
  public static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, true),
      DataTypes.createStructField(PARTITION, DataTypes.IntegerType, false)});

  private final int numPartitions;
  private final Dataset<Row> assignment;

  public BalancingAssignment(int numPartitions, Dataset<Row> assignment) {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("numPartitions must be positive, got " + numPartitions);
    }
    this.numPartitions = numPartitions;
    this.assignment = assignment;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public Dataset<Row> getAssignment() {
    return assignment;
  }
}
