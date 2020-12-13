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

package org.apache.hudi.integ.testsuite.reader;

import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;

/**
 * Implementations of {@link DeltaInputReader} will read the configured input type and provide an RDD of records to the
 * client.
 *
 * @param <O> Read result data type
 */
public interface DeltaInputReader<O> extends Serializable {

  /**
   * Attempts to reads an approximate number of records close to approxNumRecords.
   * This highly depends on the number of records already present in the input.
   */
  JavaRDD<O> read(long approxNumRecords) throws IOException;

  /**
   * @throws IOException Attempts to read approx number of records (exact if equal or more records available)
   * across requested number of
   * partitions.
   */
  JavaRDD<O> read(int numPartitions, long approxNumRecords) throws IOException;

  /**
   * @throws IOException Attempts to read approx number of records (exact if equal or more records available)
   * across requested number of
   * partitions and number of files.
   * 1. Find numFiles across numPartitions
   * 2. numRecordsToReadPerFile = approxNumRecords / numFiles
   */
  JavaRDD<O> read(int numPartitions, int numFiles, long approxNumRecords) throws IOException;

  /**
   * @throws IOException Attempts to a % of records per file across requested number of partitions and number of files.
   * 1. Find numFiles across numPartitions
   * 2. numRecordsToReadPerFile = approxNumRecordsPerFile * percentageRecordsPerFile
   */
  JavaRDD<O> read(int numPartitions, int numFiles, double percentageRecordsPerFile) throws IOException;

}
