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

package org.apache.hudi.client.bootstrap;

import java.io.Serializable;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Creates RDD of Hoodie Records with complete record data, given a list of partitions to be bootstrapped.
 */
public abstract class FullRecordBootstrapDataProvider implements Serializable {

  protected static final Logger LOG = LogManager.getLogger(FullRecordBootstrapDataProvider.class);

  protected final TypedProperties props;
  protected final transient JavaSparkContext jsc;

  public FullRecordBootstrapDataProvider(TypedProperties props, JavaSparkContext jsc) {
    this.props = props;
    this.jsc = jsc;
  }

  /**
   * Generates a list of input partition and files and returns a RDD representing source.
   * @param tableName Hudi Table Name
   * @param sourceBasePath Source Base Path
   * @param partitionPaths Partition Paths
   * @return JavaRDD of input records
   */
  public abstract JavaRDD<HoodieRecord> generateInputRecordRDD(String tableName,
      String sourceBasePath, List<Pair<String, List<HoodieFileStatus>>> partitionPaths);
}
