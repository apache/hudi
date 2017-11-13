/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.io.Serializable;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Represents a source from which we can tail data. Assumes a constructor that takes properties.
 */
public abstract class Source implements Serializable {

  protected transient PropertiesConfiguration config;

  protected transient JavaSparkContext sparkContext;

  protected transient SourceDataFormat dataFormat;

  protected transient SchemaProvider schemaProvider;


  protected Source(PropertiesConfiguration config, JavaSparkContext sparkContext,
      SourceDataFormat dataFormat, SchemaProvider schemaProvider) {
    this.config = config;
    this.sparkContext = sparkContext;
    this.dataFormat = dataFormat;
    this.schemaProvider = schemaProvider;
  }

  /**
   * Fetches new data upto maxInputBytes, from the provided checkpoint and returns an RDD of the
   * data, as well as the checkpoint to be written as a result of that.
   */
  public abstract Pair<Optional<JavaRDD<GenericRecord>>, String> fetchNewData(
      Optional<String> lastCheckpointStr,
      long maxInputBytes);


  public PropertiesConfiguration getConfig() {
    return config;
  }
}
