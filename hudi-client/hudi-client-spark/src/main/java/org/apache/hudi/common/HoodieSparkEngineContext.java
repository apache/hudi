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

package org.apache.hudi.common;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.config.SerializableConfiguration;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * A Spark engine implementation of HoodieEngineContext.
 */
public class HoodieSparkEngineContext extends HoodieEngineContext {
  private JavaSparkContext javaSparkContext;

  private SQLContext sqlContext;

  public HoodieSparkEngineContext(JavaSparkContext jsc) {
    super(new SerializableConfiguration(jsc.hadoopConfiguration()), new SparkTaskContextSupplier());
    this.javaSparkContext = jsc;
    this.sqlContext = SQLContext.getOrCreate(jsc.sc());
  }

  public void setSqlContext(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  public JavaSparkContext getJavaSparkContext() {
    return javaSparkContext;
  }

  public SQLContext getSqlContext() {
    return sqlContext;
  }

  public static JavaSparkContext getSparkContext(HoodieEngineContext context) {
    return ((HoodieSparkEngineContext) context).getJavaSparkContext();
  }

}
