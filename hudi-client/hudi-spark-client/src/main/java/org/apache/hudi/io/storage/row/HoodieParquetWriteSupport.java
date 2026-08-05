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

package org.apache.hudi.io.storage.row;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Common abstract base class for Hudi's Parquet write support implementations.
 * Extends Spark's ParquetWriteSupport and provides additional methods for
 * Hadoop configuration access and bloom filter key addition.
 */
public abstract class HoodieParquetWriteSupport extends ParquetWriteSupport {

  /**
   * Get the Hadoop configuration used by this write support.
   * @return Hadoop Configuration
   */
  public abstract Configuration getHadoopConf();

  /**
   * Add a record key to the bloom filter (if enabled).
   * @param recordKey the record key to add
   */
  public abstract void add(UTF8String recordKey);
}
