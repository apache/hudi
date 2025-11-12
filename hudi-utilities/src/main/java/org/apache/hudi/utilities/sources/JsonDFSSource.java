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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * DFS Source that reads json data.
 */
public class JsonDFSSource extends JsonSource {

  private final DFSPathSelector pathSelector;

  public JsonDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = DFSPathSelector.createSourceSelector(props, sparkContext.hadoopConfiguration());
  }

  @Override
  protected InputBatch<JavaRDD<String>> readFromCheckpoint(Option<Checkpoint> lastCkptStr, long sourceLimit) {
    Pair<Option<String>, Checkpoint> selPathsWithMaxModificationTime =
        pathSelector.getNextFilePathsAndMaxModificationTime(sparkContext, lastCkptStr, sourceLimit);
    return selPathsWithMaxModificationTime.getLeft()
        .map(pathStr -> new InputBatch<>(Option.of(fromFiles(pathStr)), selPathsWithMaxModificationTime.getRight()))
        .orElseGet(() -> new InputBatch<>(Option.empty(), selPathsWithMaxModificationTime.getRight()));
  }

  private JavaRDD<String> fromFiles(String pathStr) {
    return sparkContext.textFile(pathStr);
  }
}
